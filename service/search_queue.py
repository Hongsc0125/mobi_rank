"""
검색 요청 큐 시스템

PostgreSQL 기반 큐 시스템으로 /search 요청을 순차적으로 처리합니다.
"""

import logging
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any
from sqlalchemy import text, Column, String, DateTime, Text, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID, JSONB

from .db_session import SessionLocal, get_current_time, KST, engine

logger = logging.getLogger(__name__)

Base = declarative_base()

class SearchStatus(Enum):
    PENDING = "pending"      # 대기 중
    PROCESSING = "processing"  # 처리 중
    COMPLETED = "completed"    # 완료
    FAILED = "failed"         # 실패
    TIMEOUT = "timeout"       # 타임아웃

class SearchRequestQueue(Base):
    """검색 요청 큐 테이블"""
    __tablename__ = "search_request_queue"
    
    request_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    server = Column(String(50), nullable=False, index=True)
    character_name = Column(String(100), nullable=False, index=True)
    
    # 요청 상태 및 시간
    status = Column(String(20), nullable=False, default=SearchStatus.PENDING.value, index=True)
    priority = Column(Integer, nullable=False, default=10, index=True)  # 낮을수록 높은 우선순위
    
    # 시간 정보
    created_at = Column(DateTime(timezone=True), nullable=False, default=get_current_time)
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # 결과 데이터
    result = Column(JSONB, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # 클라이언트 정보
    client_ip = Column(String(45), nullable=True)
    user_agent = Column(Text, nullable=True)
    
    # 재시도 및 타임아웃
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    timeout_seconds = Column(Integer, nullable=False, default=120)

def create_search_queue_table():
    """검색 큐 테이블 생성"""
    try:
        # 테이블이 존재하지 않는 경우에만 생성
        Base.metadata.create_all(bind=engine, checkfirst=True)
        logger.info("검색 큐 테이블 생성 완료")
        
        # 인덱스 추가 (성능 최적화)
        with SessionLocal() as db:
            # 복합 인덱스 생성
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_search_queue_status_priority 
                ON search_request_queue(status, priority, created_at);
            """))
            
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_search_queue_server_char 
                ON search_request_queue(server, character_name);
            """))
            
            db.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_search_queue_cleanup 
                ON search_request_queue(status, completed_at);
            """))
            
            db.commit()
            logger.info("검색 큐 인덱스 생성 완료")
            
    except Exception as e:
        logger.error(f"검색 큐 테이블 생성 실패: {e}")
        raise

class SearchQueueManager:
    """검색 큐 관리 클래스"""
    
    def __init__(self):
        self.max_processing_time = 300  # 5분 최대 처리 시간
        
    def enqueue_search_request(self, server: str, character_name: str, 
                              client_ip: str = None, user_agent: str = None,
                              priority: int = 10) -> str:
        """검색 요청을 큐에 추가"""
        try:
            with SessionLocal() as db:
                # 동일한 요청이 이미 대기 중이거나 처리 중인지 확인
                existing = db.execute(text("""
                    SELECT request_id, status FROM search_request_queue
                    WHERE server = :server AND character_name = :character_name
                    AND status IN ('pending', 'processing')
                    AND created_at > :recent_threshold
                    ORDER BY created_at DESC
                    LIMIT 1
                """), {
                    'server': server,
                    'character_name': character_name,
                    'recent_threshold': get_current_time() - timedelta(minutes=5)
                }).fetchone()
                
                if existing:
                    logger.info(f"동일한 검색 요청이 이미 {existing.status} 상태로 존재: {existing.request_id}")
                    return str(existing.request_id)
                
                # 새로운 검색 요청 생성
                request = SearchRequestQueue(
                    server=server,
                    character_name=character_name,
                    client_ip=client_ip,
                    user_agent=user_agent,
                    priority=priority
                )
                
                db.add(request)
                db.commit()
                db.refresh(request)
                
                logger.info(f"검색 요청이 큐에 추가됨: {request.request_id} ({server}/{character_name})")
                return str(request.request_id)
                
        except Exception as e:
            logger.error(f"검색 요청 큐 추가 실패: {e}")
            raise
    
    def get_next_request(self) -> Optional[Dict[str, Any]]:
        """처리할 다음 요청 가져오기"""
        try:
            with SessionLocal() as db:
                # 타임아웃된 요청들을 먼저 정리
                self._cleanup_timeout_requests(db)
                
                # 우선순위가 높은 대기 중인 요청 가져오기 (FOR UPDATE 제거)
                request = db.execute(text("""
                    SELECT request_id, server, character_name, priority, created_at
                    FROM search_request_queue
                    WHERE status = :pending_status
                    ORDER BY priority ASC, created_at ASC
                    LIMIT 1
                """), {'pending_status': 'pending'}).fetchone()
                
                if not request:
                    return None
                
                # 상태를 처리 중으로 변경 (원자적 업데이트)
                update_result = db.execute(text("""
                    UPDATE search_request_queue
                    SET status = 'processing', started_at = :started_at
                    WHERE request_id = :request_id AND status = 'pending'
                """), {
                    'request_id': request.request_id,
                    'started_at': get_current_time()
                })
                
                # 업데이트가 실제로 발생했는지 확인
                if update_result.rowcount == 0:
                    # 다른 워커가 이미 이 요청을 가져갔을 가능성
                    logger.debug(f"요청 {request.request_id}이 이미 다른 워커에 의해 처리 중")
                    return None
                
                db.commit()
                
                logger.info(f"검색 요청 처리 시작: {request.request_id} ({request.server}/{request.character_name})")
                
                return {
                    'request_id': str(request.request_id),
                    'server': request.server,
                    'character_name': request.character_name,
                    'priority': request.priority,
                    'created_at': request.created_at
                }
                
        except Exception as e:
            logger.error(f"다음 검색 요청 가져오기 실패: {e}")
            return None
    
    def complete_request(self, request_id: str, result: Dict[str, Any]):
        """요청 처리 완료"""
        import json
        try:
            with SessionLocal() as db:
                # dict를 JSON 문자열로 변환
                result_json = json.dumps(result, ensure_ascii=False, default=str)
                
                db.execute(text("""
                    UPDATE search_request_queue
                    SET status = 'completed', 
                        completed_at = :completed_at,
                        result = :result
                    WHERE request_id = :request_id
                """), {
                    'request_id': request_id,
                    'completed_at': get_current_time(),
                    'result': result_json
                })
                
                db.commit()
                logger.info(f"검색 요청 처리 완료: {request_id}")
                
        except Exception as e:
            logger.error(f"검색 요청 완료 처리 실패: {e}")
            raise
    
    def fail_request(self, request_id: str, error_message: str, retry: bool = True):
        """요청 처리 실패"""
        try:
            with SessionLocal() as db:
                if retry:
                    # 재시도 가능한 경우
                    result = db.execute(text("""
                        UPDATE search_request_queue
                        SET status = CASE 
                            WHEN retry_count < max_retries THEN 'pending'
                            ELSE 'failed'
                        END,
                        retry_count = retry_count + 1,
                        error_message = :error_message,
                        started_at = NULL
                        WHERE request_id = :request_id
                        RETURNING status, retry_count
                    """), {
                        'request_id': request_id,
                        'error_message': error_message
                    }).fetchone()
                    
                    if result and result.status == 'pending':
                        logger.warning(f"검색 요청 재시도 예약: {request_id} (시도: {result.retry_count})")
                    else:
                        logger.error(f"검색 요청 최종 실패: {request_id}")
                else:
                    # 재시도 불가능한 경우
                    db.execute(text("""
                        UPDATE search_request_queue
                        SET status = 'failed',
                            error_message = :error_message,
                            completed_at = :completed_at
                        WHERE request_id = :request_id
                    """), {
                        'request_id': request_id,
                        'error_message': error_message,
                        'completed_at': get_current_time()
                    })
                    
                    logger.error(f"검색 요청 실패: {request_id} - {error_message}")
                
                db.commit()
                
        except Exception as e:
            logger.error(f"검색 요청 실패 처리 오류: {e}")
            raise
    
    def get_request_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """요청 상태 조회"""
        import json
        try:
            with SessionLocal() as db:
                request = db.execute(text("""
                    SELECT request_id, server, character_name, status, 
                           created_at, started_at, completed_at, 
                           result, error_message, retry_count
                    FROM search_request_queue
                    WHERE request_id = :request_id
                """), {'request_id': request_id}).fetchone()
                
                if not request:
                    return None
                
                # JSON 문자열을 Python dict로 변환
                result_data = None
                if request.result:
                    try:
                        if isinstance(request.result, str):
                            result_data = json.loads(request.result)
                        else:
                            # 이미 dict인 경우 (하위 호환성)
                            result_data = request.result
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"결과 JSON 파싱 실패 (request_id: {request_id}): {e}")
                        result_data = request.result  # 원본 데이터 그대로 반환
                
                return {
                    'request_id': str(request.request_id),
                    'server': request.server,
                    'character_name': request.character_name,
                    'status': request.status,
                    'created_at': request.created_at,
                    'started_at': request.started_at,
                    'completed_at': request.completed_at,
                    'result': result_data,
                    'error_message': request.error_message,
                    'retry_count': request.retry_count
                }
                
        except Exception as e:
            logger.error(f"검색 요청 상태 조회 실패: {e}")
            return None
    
    def _cleanup_timeout_requests(self, db):
        """타임아웃된 요청들 정리"""
        try:
            timeout_threshold = get_current_time() - timedelta(seconds=self.max_processing_time)
            
            result = db.execute(text("""
                UPDATE search_request_queue
                SET status = 'timeout',
                    error_message = 'Processing timeout',
                    completed_at = :completed_at
                WHERE status = 'processing'
                AND started_at < :timeout_threshold
                RETURNING request_id
            """), {
                'completed_at': get_current_time(),
                'timeout_threshold': timeout_threshold
            })
            
            timeout_requests = result.fetchall()
            if timeout_requests:
                logger.warning(f"타임아웃된 검색 요청 {len(timeout_requests)}개 정리 완료")
                
        except Exception as e:
            logger.error(f"타임아웃 요청 정리 실패: {e}")
    
    def cleanup_old_requests(self, days: int = 7):
        """오래된 요청들 정리"""
        try:
            with SessionLocal() as db:
                cleanup_threshold = get_current_time() - timedelta(days=days)
                
                result = db.execute(text("""
                    DELETE FROM search_request_queue
                    WHERE status IN ('completed', 'failed', 'timeout')
                    AND completed_at < :cleanup_threshold
                """), {'cleanup_threshold': cleanup_threshold})
                
                deleted_count = result.rowcount
                db.commit()
                
                if deleted_count > 0:
                    logger.info(f"오래된 검색 요청 {deleted_count}개 정리 완료")
                    
        except Exception as e:
            logger.error(f"오래된 요청 정리 실패: {e}")
    
    def get_queue_stats(self) -> Dict[str, int]:
        """큐 상태 통계"""
        try:
            with SessionLocal() as db:
                stats = db.execute(text("""
                    SELECT 
                        status,
                        COUNT(*) as count
                    FROM search_request_queue
                    WHERE created_at > :recent_threshold
                    GROUP BY status
                """), {
                    'recent_threshold': get_current_time() - timedelta(hours=24)
                }).fetchall()
                
                result = {status.value: 0 for status in SearchStatus}
                for stat in stats:
                    result[stat.status] = stat.count
                    
                return result
                
        except Exception as e:
            logger.error(f"큐 통계 조회 실패: {e}")
            return {}

# 전역 큐 매니저 인스턴스
search_queue_manager = SearchQueueManager()