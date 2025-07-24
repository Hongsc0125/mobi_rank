"""
검색 요청 워커 시스템

큐에서 검색 요청을 가져와서 순차적으로 처리하는 백그라운드 워커입니다.
"""

import logging
import threading
import time
import signal
from typing import Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from .search_queue import search_queue_manager, SearchStatus
from api.rankData import rank_data

logger = logging.getLogger(__name__)

class SearchWorker:
    """검색 요청 처리 워커"""
    
    def __init__(self, worker_id: str = "worker-1"):
        self.worker_id = worker_id
        self.running = False
        self.thread = None
        self.last_activity = None
        self.processed_count = 0
        self.failed_count = 0
        
    def start(self):
        """워커 시작"""
        if self.running:
            logger.warning(f"워커 {self.worker_id}가 이미 실행 중입니다")
            return
            
        self.running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        logger.info(f"검색 워커 {self.worker_id} 시작됨")
    
    def stop(self):
        """워커 중지"""
        if not self.running:
            return
            
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)
            
        logger.info(f"검색 워커 {self.worker_id} 중지됨 (처리: {self.processed_count}, 실패: {self.failed_count})")
    
    def _worker_loop(self):
        """워커 메인 루프"""
        logger.info(f"워커 {self.worker_id} 루프 시작")
        
        while self.running:
            try:
                # 다음 요청 가져오기
                request = search_queue_manager.get_next_request()
                
                if request is None:
                    # 요청이 없으면 잠시 대기
                    time.sleep(2)
                    continue
                
                self.last_activity = datetime.now()
                
                # 요청 처리
                self._process_request(request)
                
            except Exception as e:
                logger.error(f"워커 {self.worker_id} 루프 오류: {e}", exc_info=True)
                time.sleep(5)  # 오류 발생 시 잠시 대기
    
    def _process_request(self, request: dict):
        """개별 검색 요청 처리 (타임아웃 없이 완료까지 진행)"""
        request_id = request['request_id']
        server = request['server']
        character_name = request['character_name']
        
        logger.info(f"워커 {self.worker_id}: 검색 처리 시작 - {server}/{character_name} (ID: {request_id})")
        
        start_time = time.time()
        
        try:
            # 타임아웃 없이 검색 실행 (완료까지 대기)
            result = rank_data(server=server, name=character_name)
            
            if result and result.get('success'):
                # 성공적으로 처리됨
                search_queue_manager.complete_request(request_id, result)
                self.processed_count += 1
                
                processing_time = time.time() - start_time
                logger.info(f"워커 {self.worker_id}: 검색 완료 - {server}/{character_name} ({processing_time:.2f}초)")
                
            else:
                # 검색 결과가 없거나 실패
                error_msg = result.get('error', 'Unknown search error') if result else 'No result returned'
                search_queue_manager.fail_request(request_id, error_msg, retry=True)
                self.failed_count += 1
                
                logger.warning(f"워커 {self.worker_id}: 검색 실패 - {server}/{character_name}: {error_msg}")
                
        except ValueError as e:
            # ValueError 처리 (CHARACTER_NOT_FOUND 등)
            processing_time = time.time() - start_time
            error_message = str(e)
            
            if "CHARACTER_NOT_FOUND" in error_message:
                # 캐릭터 미발견 - 재시도 없이 완료 처리
                error_result = {
                    "success": False,
                    "message": f"캐릭터 '{character_name}'을(를) 서버 '{server}'에서 찾을 수 없습니다. 캐릭터명과 서버명을 다시 확인해주세요.",
                    "error_code": "CHARACTER_NOT_FOUND",
                    "from_cache": False
                }
                search_queue_manager.complete_request(request_id, error_result)
                self.processed_count += 1
                
                logger.info(f"워커 {self.worker_id}: 캐릭터 미발견 - {server}/{character_name} ({processing_time:.2f}초)")
            else:
                # 다른 ValueError
                error_msg = f"Search ValueError after {processing_time:.1f}s: {error_message}"
                search_queue_manager.fail_request(request_id, error_msg, retry=True)
                self.failed_count += 1
                
                logger.warning(f"워커 {self.worker_id}: 검색 중 ValueError - {server}/{character_name}: {e}")
                
        except Exception as e:
            # 일반 예외 발생
            processing_time = time.time() - start_time
            error_msg = f"Worker exception after {processing_time:.1f}s: {str(e)}"
            search_queue_manager.fail_request(request_id, error_msg, retry=True)
            self.failed_count += 1
            
            logger.error(f"워커 {self.worker_id}: 검색 중 예외 발생 - {server}/{character_name}: {e}", exc_info=True)
    
    def get_status(self) -> dict:
        """워커 상태 반환"""
        return {
            'worker_id': self.worker_id,
            'running': self.running,
            'processed_count': self.processed_count,
            'failed_count': self.failed_count,
            'last_activity': self.last_activity.isoformat() if self.last_activity else None,
            'thread_alive': self.thread.is_alive() if self.thread else False
        }

class SearchWorkerManager:
    """검색 워커 관리자"""
    
    def __init__(self):
        self.workers = []
        self.running = False
        self.cleanup_thread = None
        
    def start_workers(self, worker_count: int = 1):
        """워커들 시작"""
        if self.running:
            logger.warning("워커 매니저가 이미 실행 중입니다")
            return
            
        self.running = True
        
        # 워커 생성 및 시작
        for i in range(worker_count):
            worker = SearchWorker(f"worker-{i+1}")
            worker.start()
            self.workers.append(worker)
            
        # 정리 스레드 시작
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        logger.info(f"검색 워커 매니저 시작됨 ({worker_count}개 워커)")
    
    def stop_workers(self):
        """모든 워커 중지"""
        if not self.running:
            return
            
        self.running = False
        
        # 모든 워커 중지
        for worker in self.workers:
            worker.stop()
            
        self.workers.clear()
        
        # 정리 스레드 중지
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5)
            
        logger.info("검색 워커 매니저 중지됨")
    
    def _cleanup_loop(self):
        """정리 작업 루프"""
        while self.running:
            try:
                # 30분마다 오래된 요청 정리
                search_queue_manager.cleanup_old_requests(days=1)
                
                # 30분 대기
                for _ in range(1800):  # 30분 = 1800초
                    if not self.running:
                        break
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"정리 작업 중 오류: {e}")
                time.sleep(60)  # 오류 발생 시 1분 대기
    
    def get_status(self) -> dict:
        """워커 매니저 상태"""
        worker_statuses = [worker.get_status() for worker in self.workers]
        queue_stats = search_queue_manager.get_queue_stats()
        
        total_processed = sum(worker['processed_count'] for worker in worker_statuses)
        total_failed = sum(worker['failed_count'] for worker in worker_statuses)
        
        return {
            'running': self.running,
            'worker_count': len(self.workers),
            'workers': worker_statuses,
            'queue_stats': queue_stats,
            'total_processed': total_processed,
            'total_failed': total_failed
        }
    
    def restart_failed_workers(self):
        """실패한 워커들 재시작"""
        for i, worker in enumerate(self.workers):
            if worker.running and (not worker.thread or not worker.thread.is_alive()):
                logger.warning(f"워커 {worker.worker_id} 재시작 중...")
                worker.stop()
                
                new_worker = SearchWorker(f"worker-{i+1}")
                new_worker.start()
                self.workers[i] = new_worker

# 전역 워커 매니저 인스턴스
search_worker_manager = SearchWorkerManager()