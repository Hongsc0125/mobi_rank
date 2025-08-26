#!/usr/bin/env python3
"""
균형잡힌 서버별 독립 크롤링 시스템
- 각 서버별 독립 쓰레드
- DB 기반 상태 관리
- 깔끔한 상태창 표시
"""

import logging
import threading
import time
import signal
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

# 프로젝트 모듈
from service.db_session import SessionLocal, get_current_time, KST
# fetch_all_ranks 대신 개별 함수 사용
from service.db import insert_data

# 로깅 설정 - 깔끔한 출력만 (로그 파일 크기 제한)
from logging.handlers import RotatingFileHandler
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler('balanced_crawler.log', maxBytes=10*1024*1024, backupCount=2, encoding='utf-8')  # 10MB 제한
    ]
)
logger = logging.getLogger(__name__)

# Chrome 및 Selenium 로그 레벨 조정
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('service.full_data').setLevel(logging.WARNING)

# 전역 설정
SERVERS = [
    {"id": 1, "name": "데이안"},
    {"id": 2, "name": "아이라"}, 
    {"id": 3, "name": "던컨"},
    {"id": 4, "name": "알리사"},
    {"id": 5, "name": "메이븐"},
    {"id": 6, "name": "라사"},
    {"id": 7, "name": "칼릭스"}
]

# 종료 시그널
shutdown_event = threading.Event()

class CrawlerStatusDB:
    """DB 기반 크롤링 상태 관리"""
    
    def __init__(self):
        self.create_status_table()
    
    def create_status_table(self):
        """크롤링 상태 테이블 생성"""
        db = SessionLocal()
        try:
            db.execute(text("""
                CREATE TABLE IF NOT EXISTS crawler_status (
                    server_name VARCHAR(20) PRIMARY KEY,
                    worker_id VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'stopped',
                    current_character VARCHAR(100),
                    characters_processed INTEGER DEFAULT 0,
                    characters_remaining INTEGER DEFAULT 0,
                    last_update TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    errors_count INTEGER DEFAULT 0,
                    start_time TIMESTAMP WITH TIME ZONE,
                    session_processed INTEGER DEFAULT 0
                )
            """))
            
            # 서버별 초기 상태 설정
            for server in SERVERS:
                db.execute(text("""
                    INSERT INTO crawler_status (server_name)
                    VALUES (:server_name)
                    ON CONFLICT (server_name) DO NOTHING
                """), {"server_name": server["name"]})
            
            db.commit()
        except Exception as e:
            logger.error(f"상태 테이블 생성 실패: {e}")
            db.rollback()
        finally:
            db.close()
    
    def update_status(self, server_name, **kwargs):
        """서버 상태 업데이트"""
        db = SessionLocal()
        try:
            set_clauses = []
            params = {"server_name": server_name, "last_update": get_current_time()}
            
            for key, value in kwargs.items():
                if value is not None:
                    set_clauses.append(f"{key} = :{key}")
                    params[key] = value
            
            if set_clauses:
                query = f"""
                    UPDATE crawler_status 
                    SET {', '.join(set_clauses)}, last_update = :last_update
                    WHERE server_name = :server_name
                """
                db.execute(text(query), params)
                db.commit()
        except Exception as e:
            logger.error(f"상태 업데이트 실패 {server_name}: {e}")
            db.rollback()
        finally:
            db.close()
    
    def get_all_status(self):
        """모든 서버 상태 조회"""
        db = SessionLocal()
        try:
            result = db.execute(text("""
                SELECT server_name, worker_id, status, current_character,
                       characters_processed, characters_remaining, last_update,
                       errors_count, start_time, session_processed
                FROM crawler_status
                ORDER BY server_name
            """)).fetchall()
            
            return [{
                'server_name': row[0],
                'worker_id': row[1],
                'status': row[2],
                'current_character': row[3],
                'characters_processed': row[4],
                'characters_remaining': row[5],
                'last_update': row[6],
                'errors_count': row[7],
                'start_time': row[8],
                'session_processed': row[9]
            } for row in result]
        finally:
            db.close()

class ServerCrawler:
    """개별 서버 크롤링 워커"""
    
    def __init__(self, server_info, status_db):
        self.server_info = server_info
        self.server_name = server_info["name"]
        self.server_id = server_info["id"]
        self.status_db = status_db
        self.worker_id = f"worker_{self.server_name}_{int(time.time())}"
        self.dedicated_driver = None  # 전용 드라이버
        
    def initialize_dedicated_driver(self):
        """서버 전용 드라이버 초기화"""
        try:
            from service.driver_pool import get_driver_pool
            driver_pool = get_driver_pool()
            self.dedicated_driver = driver_pool.get_driver(timeout=60)
            logger.info(f"[{self.server_name}] 전용 드라이버 할당 완료")
        except Exception as e:
            logger.error(f"[{self.server_name}] 전용 드라이버 할당 실패: {e}")
            raise
    
    def cleanup_dedicated_driver(self):
        """전용 드라이버 정리"""
        if self.dedicated_driver:
            try:
                from service.driver_pool import get_driver_pool
                driver_pool = get_driver_pool()
                driver_pool.return_driver(self.dedicated_driver)
                logger.info(f"[{self.server_name}] 전용 드라이버 반환 완료")
            except Exception as e:
                logger.error(f"[{self.server_name}] 전용 드라이버 반환 실패: {e}")
            finally:
                self.dedicated_driver = None
    
    def _is_driver_valid(self):
        """드라이버 유효성 검사"""
        if not self.dedicated_driver:
            return False
        
        try:
            # 간단한 세션 ID 확인
            _ = self.dedicated_driver.session_id
            # 현재 URL 확인 (더 확실한 검증)
            _ = self.dedicated_driver.current_url
            return True
        except Exception:
            return False
        
    def is_character_updated_today(self, character_name):
        """캐릭터가 오늘 이미 업데이트되었는지 확인"""
        db = SessionLocal()
        try:
            today_start = get_current_time().replace(hour=0, minute=0, second=0, microsecond=0)
            
            result = db.execute(text("""
                SELECT COUNT(*) 
                FROM mabinogi_ranking 
                WHERE server_name = :server_name 
                AND character_name = :character_name
                AND retrieved_at >= :today_start
            """), {
                "server_name": self.server_name,
                "character_name": character_name,
                "today_start": today_start
            }).scalar()
            
            return result > 0
        finally:
            db.close()
    
    def get_characters_to_update(self):
        """오늘 아직 업데이트되지 않은 캐릭터 중 가장 오래된 캐릭터부터 순차적으로 조회"""
        db = SessionLocal()
        try:
            today_start = get_current_time().replace(hour=0, minute=0, second=0, microsecond=0)
            
            # 오늘 아직 업데이트되지 않은 캐릭터들을 가장 오래된 retrieved_at 순으로 조회
            result = db.execute(text("""
                SELECT DISTINCT character_name,
                       MAX(retrieved_at) as last_retrieved
                FROM mabinogi_ranking 
                WHERE server_name = :server_name
                AND character_name NOT IN (
                    SELECT DISTINCT character_name 
                    FROM mabinogi_ranking 
                    WHERE server_name = :server_name 
                    AND retrieved_at >= :today_start
                )
                GROUP BY character_name
                ORDER BY last_retrieved ASC
            """), {
                "server_name": self.server_name,
                "today_start": today_start
            }).fetchall()
            
            return [row[0] for row in result]
        finally:
            db.close()
    
    def run(self):
        """서버별 크롤링 실행"""
        logger.info(f"🚀 {self.server_name} 서버 크롤링 시작")
        
        # 전용 드라이버 초기화
        self.initialize_dedicated_driver()
        
        self.status_db.update_status(
            self.server_name,
            worker_id=self.worker_id,
            status='running',
            start_time=get_current_time(),
            session_processed=0
        )
        
        session_processed = 0
        
        try:
            while not shutdown_event.is_set():
                # 업데이트 필요한 캐릭터 목록 가져오기
                character_list = self.get_characters_to_update()
                
                if not character_list:
                    self.status_db.update_status(
                        self.server_name,
                        status='waiting',
                        current_character='모든 캐릭터 오늘 업데이트 완료 - 대기 중'
                    )
                    time.sleep(300)  # 5분 대기 후 재조회 (모든 캐릭터 완료 시)
                    continue
                
                self.status_db.update_status(
                    self.server_name,
                    status='processing',
                    characters_remaining=len(character_list)
                )
                
                # 전체 캐릭터 순차 처리 (배치 처리 없이 연속적으로)
                processed_in_cycle = 0
                
                for char_name in character_list:
                    if shutdown_event.is_set():
                        break
                    
                    try:
                        # 검색 전에 캐릭터가 이미 오늘 업데이트되었는지 재확인
                        if self.is_character_updated_today(char_name):
                            logger.debug(f"{self.server_name} 캐릭터 '{char_name}' 이미 오늘 업데이트됨 - 패스")
                            processed_in_cycle += 1
                            continue
                        
                        self.status_db.update_status(
                            self.server_name,
                            current_character=char_name,
                            characters_remaining=len(character_list) - processed_in_cycle
                        )
                        
                        # 캐릭터 데이터 수집 (기존 sequential_ranking_crawler 방식 사용)
                        from service.full_data import fetch_rank_via_dom, parse_rank_html
                        
                        try:
                            # 드라이버 상태 확인 및 재생성
                            if not self._is_driver_valid():
                                logger.warning(f"[{self.server_name}] 드라이버 세션 만료, 재생성 중...")
                                self.cleanup_dedicated_driver()
                                self.initialize_dedicated_driver()
                            
                            # 전투력 랭킹만 검색 (div=1) - 전용 드라이버 사용
                            html_data = fetch_rank_via_dom(self.server_name, char_name, rank_type=1, dedicated_driver=self.dedicated_driver)
                            
                            if html_data:
                                parsed_data = parse_rank_html(html_data)
                                
                                if parsed_data:
                                    insert_result = insert_data(
                                        parsed_data,
                                        server=self.server_name,
                                        div=1,
                                        force_update=True
                                    )
                                    
                                    if insert_result.get('success'):
                                        session_processed += 1
                                        processed_in_cycle += 1
                                        
                        except Exception as fetch_error:
                            error_msg = str(fetch_error)
                            logger.error(f"{self.server_name} 데이터 수집 오류 ({char_name}): {error_msg}")
                            
                            # invalid session id 오류 시 드라이버 재생성 시도
                            if "invalid session id" in error_msg.lower():
                                logger.warning(f"[{self.server_name}] 세션 오류로 인한 드라이버 재생성")
                                try:
                                    self.cleanup_dedicated_driver()
                                    self.initialize_dedicated_driver()
                                except Exception as reinit_error:
                                    logger.error(f"[{self.server_name}] 드라이버 재생성 실패: {reinit_error}")
                            
                            continue
                                        
                        self.status_db.update_status(
                            self.server_name,
                            characters_processed=session_processed,
                            session_processed=session_processed
                        )
                        
                        time.sleep(1.0)  # 서버 부하 방지 및 안정성 향상
                        
                    except Exception as e:
                        logger.error(f"{self.server_name} 처리 오류 ({char_name}): {str(e)[:100]}")
                        self.status_db.update_status(
                            self.server_name,
                            current_character=f'오류: {char_name}',
                            errors_count=1
                        )
                        time.sleep(5)  # 오류 발생 시 더 긴 대기
                
                # 한 사이클 완료 후 잠시 대기 (지속적인 루프 - 전체 캐릭터 순환)
                self.status_db.update_status(
                    self.server_name,
                    current_character=f'사이클 완료: {processed_in_cycle}개 처리 - 재시작'
                )
                time.sleep(10)  # 10초 대기 후 새로운 사이클 시작
                
        except Exception as e:
            logger.error(f"{self.server_name} 크롤링 실패: {e}")
            self.status_db.update_status(
                self.server_name,
                status='error',
                current_character=f'오류: {str(e)[:50]}'
            )
        finally:
            # 전용 드라이버 정리
            self.cleanup_dedicated_driver()
            
            self.status_db.update_status(
                self.server_name,
                status='stopped'
            )
            logger.info(f"⏹️ {self.server_name} 서버 크롤링 종료")

class StatusDisplay:
    """상태창 표시"""
    
    def __init__(self, status_db):
        self.status_db = status_db
        self.running = True
    
    def format_time_diff(self, timestamp):
        """시간 차이를 보기 좋게 포맷"""
        if not timestamp:
            return "없음"
        
        now = get_current_time()
        if timestamp.tzinfo is None:
            timestamp = KST.localize(timestamp)
        
        diff = now - timestamp
        total_seconds = int(diff.total_seconds())
        
        if total_seconds < 60:
            return f"{total_seconds}초 전"
        elif total_seconds < 3600:
            return f"{total_seconds // 60}분 전"
        else:
            return f"{total_seconds // 3600}시간 전"
    
    def display_status(self):
        """상태창 표시"""
        while self.running and not shutdown_event.is_set():
            try:
                status_list = self.status_db.get_all_status()
                
                # 화면 클리어 (ANSI escape code)
                print("\033[2J\033[H", end="")
                
                print("=" * 100)
                print("🔄 마비노기 모바일 랭킹 크롤러 상태")
                print(f"📅 {get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}")
                print("=" * 100)
                
                print(f"{'서버':^8} | {'상태':^10} | {'현재작업':^20} | {'처리완료':^8} | {'남은작업':^8} | {'오류':^6} | {'최종업데이트':^12}")
                print("-" * 100)
                
                total_processed = 0
                total_remaining = 0
                
                for status in status_list:
                    server = status['server_name']
                    state = status['status']
                    current = status['current_character'] or "대기중"
                    processed = status['session_processed'] or 0
                    remaining = status['characters_remaining'] or 0
                    errors = status['errors_count'] or 0
                    last_update = self.format_time_diff(status['last_update'])
                    
                    # 상태 이모지
                    state_emoji = {
                        'running': '🟢',
                        'processing': '🔄',
                        'waiting': '🟡', 
                        'stopped': '🔴',
                        'error': '❌'
                    }.get(state, '❓')
                    
                    # 현재 작업 문자열 길이 제한
                    if len(current) > 18:
                        current = current[:15] + "..."
                    
                    print(f"{server:^8} | {state_emoji} {state:^8} | {current:^20} | {processed:^8,} | {remaining:^8,} | {errors:^6} | {last_update:^12}")
                    
                    total_processed += processed
                    total_remaining += remaining
                
                print("-" * 100)
                print(f"📊 전체 통계: 처리완료 {total_processed:,}개 | 남은작업 {total_remaining:,}개")
                
                # 시간당 처리량 계산
                db = SessionLocal()
                try:
                    one_hour_ago = get_current_time() - timedelta(hours=1)
                    hourly_count = db.execute(text("""
                        SELECT COUNT(*) FROM mabinogi_ranking 
                        WHERE retrieved_at >= :time_threshold
                    """), {"time_threshold": one_hour_ago}).scalar()
                    
                    print(f"⚡ 최근 1시간 처리량: {hourly_count:,}개")
                finally:
                    db.close()
                
                print("=" * 100)
                print("💡 종료: Ctrl+C")
                
            except Exception as e:
                logger.error(f"상태 표시 오류: {e}")
            
            time.sleep(5)  # 5초마다 업데이트
    
    def stop(self):
        self.running = False

def signal_handler(sig, frame):
    """종료 시그널 핸들러"""
    logger.info("🛑 종료 신호 감지 - 모든 크롤러를 안전하게 종료합니다...")
    shutdown_event.set()

def main():
    """메인 실행 함수"""
    # 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("🚀 균형잡힌 크롤링 시스템 시작")
    
    # 상태 관리 시스템 초기화
    status_db = CrawlerStatusDB()
    
    # 상태 표시 시스템 시작
    status_display = StatusDisplay(status_db)
    status_thread = threading.Thread(target=status_display.display_status, daemon=True)
    status_thread.start()
    
    # 각 서버별 크롤러 실행
    crawlers = []
    threads = []
    
    for server in SERVERS:
        crawler = ServerCrawler(server, status_db)
        crawlers.append(crawler)
        
        thread = threading.Thread(target=crawler.run, daemon=True)
        threads.append(thread)
        thread.start()
    
    try:
        # 모든 쓰레드가 종료될 때까지 대기
        while not shutdown_event.is_set():
            time.sleep(1)
            
            # 모든 쓰레드가 종료되었는지 확인
            if not any(t.is_alive() for t in threads):
                break
    
    except KeyboardInterrupt:
        logger.info("🛑 키보드 인터럽트 감지")
    
    finally:
        shutdown_event.set()
        status_display.stop()
        
        # 모든 쓰레드 종료 대기
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5)
        
        logger.info("✅ 모든 크롤러가 안전하게 종료되었습니다")

if __name__ == "__main__":
    main()