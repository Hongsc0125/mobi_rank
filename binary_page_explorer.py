import heapq
import threading
import time
import json
import os
import logging
import signal
import sys
from datetime import datetime, timedelta
from queue import PriorityQueue
from concurrent.futures import ThreadPoolExecutor
import gc
import pytz

from every_rank_data_task import (
    get_driver, fetch_rank_page_by_pageno, parse_rank_html, parse_rank_range,
    insert_data, mark_range_crawled, is_range_recent, save_discovered_ranges,
    load_discovered_ranges, get_server_name, switch_server, get_optimal_settings,
    DriverPool, DataCollector, update_thread_status, log_all_thread_status,
    KST, shutdown_event, thread_status_lock, signal_handler, shutdown_all
)

# 설정 로깅
logger = logging.getLogger(__name__)

# 병렬 처리 풀 크기
DEFAULT_PARALLEL_EXPLORERS = 4

# 파일을 저장할 경로
BINARY_RANGES_FILE = os.path.join(os.path.dirname(__file__), "binary_ranges.json")

# 모든 활성 스레드 추적
active_threads = []

# 서버별 탐색기 인스턴스
explorers = {}

# DB에서 최하위 랭킹 캐릭터 및 미탐색 범위 탐색 기능 추가
def get_lowest_rank_char(server_num):
    """DB에서 특정 서버의 최하위 랭킹(가장 큰 수치) 캐릭터와 그 랭킹 가져오기"""
    try:
        from service.db import SessionLocal
        from sqlalchemy import text
        
        session = SessionLocal()
        server_name = get_server_name(server_num)
        
        try:
            # 해당 서버의 가장 낮은 랭킹(높은 숫자)을 가진 캐릭터 찾기
            query = text("""
                SELECT character_name, rank_position, retrieved_at
                FROM mabinogi_ranking
                WHERE server_name = :server
                ORDER BY rank_position DESC, retrieved_at DESC
                LIMIT 1
            """)
            
            result = session.execute(query, {'server': server_name})
            row = result.fetchone()
            
            if row:
                char_name, rank, timestamp = row
                logger.info(f"서버 {server_num}({server_name})의 최하위 랭킹 캐릭터: {char_name}, 랭킹: {rank}")
                return char_name, rank, timestamp
            
            return None, None, None
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"최하위 랭킹 조회 중 오류: {e}", exc_info=True)
        return None, None, None

def estimate_max_page(rank):
    """랭킹값으로 대략적인 마지막 페이지 추정 (페이지당 20명)"""
    if not rank:
        return 100  # 기본값
    
    # 랭킹을 페이지 번호로 변환 (올림)
    import math
    return math.ceil(rank / 20)

def find_unexplored_ranges(server_num, max_page=None):
    """현재 서버에서 아직 탐색하지 않은 범위 찾기"""
    try:
        from service.db import SessionLocal
        from sqlalchemy import text
        import re
        
        # 최대 페이지 설정 (지정되지 않은 경우)
        if not max_page:
            # 서버에서 확인된 최하위 랭킹으로 추정
            _, lowest_rank, _ = get_lowest_rank_char(server_num)
            max_page = estimate_max_page(lowest_rank) if lowest_rank else 300
            
            # 최소한 100페이지는 확인
            max_page = max(max_page + 50, 300)
            
        logger.info(f"서버 {server_num} 탐색 범위 추정: 1-{max_page} 페이지")
        
        # 이미 10분 내에 탐색된 페이지 조회
        session = SessionLocal()
        try:
            query = text("""
                SELECT range_text FROM discovered_ranges
                WHERE server_num = :server_num
                AND last_crawled > :threshold
            """)
            
            time_threshold = datetime.now(KST) - timedelta(minutes=10)
            result = session.execute(query, {
                'server_num': server_num,
                'threshold': time_threshold
            })
            
            recent_ranges = []
            for row in result:
                range_text = row[0]
                # "XXX위 ~ YYY위" 형식에서 페이지 범위 추출
                match = re.search(r'(\d+)위\s*~\s*(\d+)위', range_text)
                if match:
                    start_rank = int(match.group(1).replace(',', ''))
                    end_rank = int(match.group(2).replace(',', ''))
                    # 페이지 번호로 변환 (1페이지 = 1-20위, 2페이지 = 21-40위, ...)
                    start_page = (start_rank - 1) // 20 + 1
                    end_page = (end_rank - 1) // 20 + 1
                    
                    # 같은 페이지인 경우 (대부분의 경우)
                    if start_page == end_page:
                        recent_ranges.append((start_page, start_page))
                    # 여러 페이지에 걸친 경우 (거의 없음)
                    else:
                        recent_ranges.append((start_page, end_page))
                        
        finally:
            session.close()
            
        # 탐색된 페이지 번호 집합
        explored_pages = set()
        for start, end in recent_ranges:
            for page in range(start, end + 1):
                explored_pages.add(page)
        
        # 미탐색 범위 찾기
        unexplored_ranges = []
        start = None
        
        for page in range(1, max_page + 1):
            if page not in explored_pages:
                if start is None:
                    start = page
            elif start is not None:
                unexplored_ranges.append((start, page - 1))
                start = None
        
        # 마지막 범위 처리
        if start is not None:
            unexplored_ranges.append((start, max_page))
        
        # 미탐색 범위 로깅
        if unexplored_ranges:
            logger.info(f"서버 {server_num} 미탐색 범위: {unexplored_ranges}")
        else:
            logger.info(f"서버 {server_num} 모든 범위가 최근에 탐색됨")
        
        return unexplored_ranges, max_page
        
    except Exception as e:
        logger.error(f"미탐색 범위 조회 중 오류: {e}", exc_info=True)
        return [], 100  # 오류 시 기본값

class PageRangeExplorer:
    """
    서버당 미탐색 페이지 범위를 이진 분할 방식으로 관리.
    overlap: 분할 시 양쪽에 겹쳐 둘 페이지 수.
    skip_delta: 최근 탐색된 범위를 스킵할 시간 간격.
    """
    def __init__(self, server_num, min_page=1, max_page=50,
                 overlap=1, skip_delta=timedelta(minutes=10)):
        self.server = server_num
        self.overlap = overlap
        self.skip_delta = skip_delta

        self._lock = threading.Lock()
        self._counter = 0  # tie-breaker for heap
        self._pq = []      # (last_scanned_ts, counter, low, high)
        self._ranges = {}  # (low,high) -> last_scanned_ts

        # 초기(1,50) 범위 삽입
        self.add_range(min_page, max_page)

    def add_range(self, low, high):
        """새로운 범위를 큐에 추가 (중복 방지)"""
        key = (low, high)
        with self._lock:
            if key in self._ranges or low >= high:
                return
            ts = datetime.min.replace(tzinfo=KST)
            self._ranges[key] = ts
            heapq.heappush(self._pq, (ts, self._counter, low, high))
            self._counter += 1

    # DB 저장/로드 메서드로 변경
    def save_state(self):
        """현재 탐색 상태를 PostgreSQL DB에 저장"""
        try:
            # 세션 로컬 임포트
            from service.db import SessionLocal
            from sqlalchemy import text
            
            # 세션 생성
            session = SessionLocal()
            
            try:
                # _ranges 딕셔너리 복사
                with self._lock:
                    ranges_to_save = list(self._ranges.items())
                
                # 각 범위를 DB에 저장 (PostgreSQL UPSERT 구문)
                for key, ts in ranges_to_save:
                    low, high = key
                    ts_iso = ts.isoformat()
                    
                    query = text("""
                        INSERT INTO binary_page_ranges 
                            (server_num, low, high, last_crawled) 
                        VALUES 
                            (:server_num, :low, :high, :last_crawled)
                        ON CONFLICT (server_num, low, high) 
                        DO UPDATE SET 
                            last_crawled = :last_crawled,
                            timestamp = CURRENT_TIMESTAMP
                    """)
                    
                    session.execute(query, {
                        'server_num': self.server,
                        'low': low,
                        'high': high,
                        'last_crawled': ts_iso
                    })
                
                # 변경사항 커밋
                session.commit()
                logger.info(f"서버 {self.server} 탐색 상태 DB 저장: {len(ranges_to_save)}개 범위")
                
            except Exception as e:
                session.rollback()
                logger.error(f"서버 {self.server} 상태 DB 저장 중 오류: {e}")
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"DB 연결 오류: {e}")

    def load_state(self):
        """저장된 탐색 상태를 PostgreSQL DB에서 로드"""
        try:
            # 세션 로컬 임포트
            from service.db import SessionLocal
            from sqlalchemy import text
            
            # 세션 생성
            session = SessionLocal()
            
            try:
                # 현재 서버의 범위 정보 쿼리 (PostgreSQL 호환)
                query = text("""
                    SELECT low, high, last_crawled 
                    FROM binary_page_ranges 
                    WHERE server_num = :server_num
                """)
                
                result = session.execute(query, {'server_num': self.server})
                
                # 결과 처리를 위한 변수
                loaded_ranges = {}
                
                # 결과 행 처리
                for row in result:
                    low = row[0]
                    high = row[1]
                    last_crawled = row[2]
                    
                    # 타임존 정보 적용
                    if last_crawled.tzinfo is None:
                        last_crawled = KST.localize(last_crawled)
                    
                    # 범위 정보 저장
                    loaded_ranges[(low, high)] = last_crawled
                
                # 데이터가 있으면 상태 업데이트
                if loaded_ranges:
                    with self._lock:
                        self._pq = []
                        self._ranges = {}
                        self._counter = 0
                        
                        # 각 범위를 우선순위 큐에 추가
                        for key, ts in loaded_ranges.items():
                            self._ranges[key] = ts
                            heapq.heappush(self._pq, (ts, self._counter, key[0], key[1]))
                            self._counter += 1
                    
                    logger.info(f"서버 {self.server} 탐색 상태 DB 로드: {len(loaded_ranges)}개 범위")
                    return True
                else:
                    logger.info(f"서버 {self.server} DB에 저장된 탐색 상태 없음")
                    return False
                
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"서버 {self.server} 상태 DB 로드 중 오류: {e}")
            return False
    
    def reset(self, min_page=1, max_page=50):
        """탐색 상태를 초기화하고 전체 범위(1,50) 다시 추가"""
        try:
            # 세션 로컬 임포트
            from service.db import SessionLocal
            from sqlalchemy import text
            
            # 세션 생성
            session = SessionLocal()
            
            try:
                # 현재 서버의 모든 범위 정보 삭제
                query = text("""
                    DELETE FROM binary_page_ranges 
                    WHERE server_num = :server_num
                """)
                
                session.execute(query, {'server_num': self.server})
                session.commit()
                
                # 메모리 상태도 초기화
                with self._lock:
                    self._pq = []
                    self._ranges = {}
                    self._counter = 0
                    self.add_range(min_page, max_page)
                
                logger.info(f"서버 {self.server} 탐색 상태 초기화")
                return True
                
            except Exception as e:
                session.rollback()
                logger.error(f"서버 {self.server} 상태 초기화 중 오류: {e}")
                return False
            finally:
                session.close()
                
        except Exception as e:
            logger.error(f"DB 연결 오류: {e}")
            # 메모리 상태는 초기화
            with self._lock:
                self._pq = []
                self._ranges = {}
                self._counter = 0
                self.add_range(min_page, max_page)
            return True

    def get_stats(self):
        """현재 탐색 상태 통계 반환"""
        with self._lock:
            total_ranges = len(self._ranges)
            pending_ranges = len(self._pq)
            
            # 가장 오래된/최근 탐색 시간 찾기
            timestamps = [ts for ts in self._ranges.values() 
                         if ts != datetime.min.replace(tzinfo=KST)]
            
            oldest = min(timestamps) if timestamps else None
            newest = max(timestamps) if timestamps else None
            
            return {
                'total_ranges': total_ranges,
                'pending_ranges': pending_ranges,
                'oldest_scan': oldest,
                'newest_scan': newest
            }

    def get_next_range(self):
        """
        우선순위 큐에서 다음 탐색할 (low, high)를 꺼냄.
        10분 이내 최근 탐색된 범위는 스킵.
        """
        with self._lock:
            while self._pq:
                ts, _, low, high = heapq.heappop(self._pq)
                last_ts = self._ranges.get((low, high))
                if last_ts is None:
                    continue
                if datetime.now(KST) - last_ts < self.skip_delta:
                    continue
                return low, high
        return None

    def mark_scanned(self, low, high):
        """(low, high) 범위를 지금 시점으로 업데이트"""
        now = datetime.now(KST)
        key = (low, high)
        with self._lock:
            if key in self._ranges:
                self._ranges[key] = now

    def split_and_add(self, low, high, mid):
        """
        mid를 기준으로 양쪽 하위 범위로 분할(겹침 적용) 후 다시 큐에 삽입.
        """
        left_high  = min(high, mid - 1 + self.overlap)
        right_low  = max(low,   mid + 1 - self.overlap)
        # 좌측 구간
        if low < left_high:
            self.add_range(low, left_high)
        # 우측 구간
        if right_low < high:
            self.add_range(right_low, high)

    def update_exploration_ranges(self, unexplored_ranges):
        """미탐색 범위를 업데이트하여 큐 재초기화"""
        with self._lock:
            # 기존 큐 초기화
            self._pq = []
            self._ranges = {}
            self._counter = 0
            
            # 미탐색 범위 추가
            for low, high in unexplored_ranges:
                self.add_range(low, high)
            
            logger.info(f"서버 {self.server} 탐색 범위 업데이트: {len(unexplored_ranges)}개 범위 추가")
            return len(unexplored_ranges) > 0


# 서버별 이진 탐색 워커 함수
def binary_explore_worker(server_num, div=1):
    """서버별 이진 분할 탐색 워커 - 병렬 처리 지원"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "초기화 중", f"서버 {server_num} 이진 탐색")
    
    # 세션 로컬 임포트
    from service.db import SessionLocal
    
    # 서버별 탐색기 인스턴스 생성/얻기
    if server_num not in explorers:
        explorers[server_num] = PageRangeExplorer(server_num)
        
    explorer = explorers[server_num]
    
    # 저장된 상태 로드 (실패 시 기본 범위 사용)
    if not explorer.load_state():
        logger.info(f"서버 {server_num} 새 탐색 시작")
    
    # 실행 통계
    stats = {
        'pages_processed': 0,
        'ranges_found': 0,
        'empty_queries': 0,
        'total_items': 0
    }
    
    # 시스템 설정 가져오기
    settings = get_optimal_settings()
    pool_size = min(3, settings['drivers_per_server'])
    
    # 드라이버 풀과 데이터 수집기 초기화
    driver_pool = DriverPool(pool_size=pool_size)
    data_collector = DataCollector(batch_size=50, div=div)
    
    # 서버 이름 (로깅용)
    server_name = get_server_name(server_num)
    
    # 마지막 범위 초기화 시간
    last_reset = datetime.now(KST)
    # 범위 재탐색 간격 (1시간)
    reset_interval = timedelta(hours=1)
    # 범위 업데이트 간격 (15분)
    update_interval = timedelta(minutes=15)
    last_update = datetime.now(KST) - update_interval  # 시작 시 바로 업데이트하도록
    
    try:
        # 주 실행 루프
        while not shutdown_event.is_set():
            now = datetime.now(KST)
            
            # 주기적으로 미탐색 범위 업데이트 (15분마다)
            if now - last_update > update_interval:
                update_thread_status(thread_name, "범위 업데이트 중", f"서버 {server_num} 미탐색 범위 확인")
                
                # 최하위 랭킹 및 미탐색 범위 확인
                unexplored_ranges, max_page = find_unexplored_ranges(server_num)
                
                if unexplored_ranges:
                    # 기존 큐가 비어있거나 정기 업데이트 시간인 경우에만 전면 업데이트
                    remaining_ranges = len(explorer._pq)
                    if remaining_ranges == 0 or now - last_reset > reset_interval:
                        explorer.update_exploration_ranges(unexplored_ranges)
                        last_reset = now
                        logger.info(f"서버 {server_num} 범위 전체 업데이트 완료")
                    else:
                        logger.info(f"서버 {server_num} 아직 처리할 범위가 {remaining_ranges}개 남아있어 업데이트 보류")
                
                last_update = now
            
            # 주기적으로 전체 범위 초기화 정기 실행 (1시간마다)
            if now - last_reset > reset_interval:
                logger.info(f"서버 {server_num} 정기 초기화 수행 ({reset_interval})")
                unexplored_ranges, _ = find_unexplored_ranges(server_num)
                
                if unexplored_ranges:
                    explorer.update_exploration_ranges(unexplored_ranges)
                else:
                    # 미탐색 범위가 없으면 기본 1-300 범위로 재설정
                    explorer.reset(1, 300)
                
                explorer.save_state()
                last_reset = now
            
            # 다음 처리할 범위 가져오기
            range_tuple = explorer.get_next_range()
            if not range_tuple:
                # 모든 범위 처리 완료 - 미탐색 범위 재확인
                logger.info(f"서버 {server_num} 모든 범위 처리 완료, 미탐색 범위 재확인")
                unexplored_ranges, _ = find_unexplored_ranges(server_num)
                
                if unexplored_ranges:
                    explorer.update_exploration_ranges(unexplored_ranges)
                else:
                    # 남은 구간이 없으면 잠시 대기 후 재확인
                    update_thread_status(thread_name, "대기 중", f"서버 {server_num} 범위 모두 처리됨")
                    for _ in range(6):
                        if shutdown_event.is_set():
                            break
                        time.sleep(5)
                    continue
            
            # 범위 분해
            low, high = range_tuple
            mid = (low + high) // 2
            
            # 중간값 페이지 범위 생성
            page_range_text = f"{(mid-1)*20+1}위 ~ {mid*20}위"
            
            # 이미 최근 처리된 범위인지 확인
            if is_range_recent(server_num, page_range_text):
                update_thread_status(thread_name, "범위 스킵", 
                              f"서버 {server_num} 페이지 {mid} ({page_range_text}) 최근 처리됨")
                explorer.mark_scanned(low, high)
                continue
                
            # 탐색 시작 로깅
            update_thread_status(thread_name, "페이지 탐색", 
                          f"서버 {server_num} 범위 {low}~{high} 중간값 {mid}")
            
            # 드라이버 풀에서 드라이버 가져오기
            driver = driver_pool.get_driver()
            success = False
            
            try:
                # 페이지 크롤링 시도
                html, driver = fetch_rank_page_by_pageno(driver, server_num, mid, div)
                
                # 페이지 데이터 없으면 다음으로
                if not html:
                    logger.warning(f"서버 {server_num} 페이지 {mid} 데이터 없음")
                    stats['empty_queries'] += 1
                    explorer.mark_scanned(low, high)
                    continue
                
                # 데이터 파싱
                parsed_data = parse_rank_html(html)
                
                if not parsed_data:
                    logger.warning(f"서버 {server_num} 페이지 {mid} 파싱 결과 없음")
                    stats['empty_queries'] += 1
                    explorer.mark_scanned(low, high)
                    continue
                
                # 범위 마킹 및 데이터 저장
                now_kst = datetime.now(KST)
                data_collector.add_data(parsed_data)
                mark_range_crawled(server_num, page_range_text)
                
                # 통계 업데이트
                stats['pages_processed'] += 1
                stats['ranges_found'] += 1
                stats['total_items'] += len(parsed_data)
                
                # 성공 플래그
                success = True
                
                # 상태 업데이트
                items_count = len(parsed_data)
                update_thread_status(thread_name, "데이터 저장", 
                          f"서버 {server_num} 페이지 {mid}: {items_count}개 항목")
                
                # 3초마다 한 번씩 현재 통계 로깅
                if stats['pages_processed'] % 10 == 0:
                    logger.info(f"서버 {server_num} 진행: {stats['pages_processed']} 페이지, "
                               f"{stats['total_items']} 항목, {stats['ranges_found']} 범위")
                
            except Exception as e:
                logger.error(f"서버 {server_num} 페이지 {mid} 처리 오류: {e}")
            finally:
                # 드라이버 반환
                driver_pool.return_driver(driver)
                
                # 탐색 범위 처리 완료 표시
                explorer.mark_scanned(low, high)
                
                # 성공 여부와 관계없이 하위 범위 분할 추가
                explorer.split_and_add(low, high, mid)
                
                # 주기적으로 상태 저장
                if stats['pages_processed'] % 20 == 0:
                    explorer.save_state()
            
            # 과도한 부하 방지를 위한 짧은 대기
            time.sleep(0.5)
            
            # 메모리 관리
            if stats['pages_processed'] % 100 == 0:
                gc.collect()
    
    except Exception as e:
        update_thread_status(thread_name, "오류 발생", str(e))
        logger.error(f"이진 탐색 워커 오류 (서버 {server_num}): {e}", exc_info=True)
    
    finally:
        # 종료 전 상태 저장
        explorer.save_state()
        # 남은 데이터 저장
        data_collector.flush()
        # 드라이버 정리
        driver_pool.close_all()
        
        update_thread_status(thread_name, "종료됨", 
                    f"서버 {server_name} 워커 종료. 총 {stats['pages_processed']}개 페이지, "
                    f"{stats['total_items']}개 항목, {stats['ranges_found']}개 범위 처리됨")

# 다중 서버 탐색기 모니터링 함수
def binary_explorer_monitor():
    """이진 탐색 워커들의 상태를 모니터링하고 상태 요약을 로깅"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "모니터링 시작", "이진 탐색 모니터")
    
    # 마지막으로 재시작한 시간 저장 (너무 빈번한 재시작 방지)
    last_restart_time = {}
    
    try:
        while not shutdown_event.is_set():
            # 모든 활성 스레드 확인
            alive_threads = [t for t in active_threads if t.is_alive()]
            dead_threads = [t for t in active_threads if not t.is_alive()]
            
            # 종료된 스레드 감지 및 재시작
            if dead_threads:
                dead_thread_names = [t.name for t in dead_threads]
                update_thread_status(thread_name, "스레드 종료 감지", 
                            f"{len(dead_threads)}개 스레드 종료됨: {dead_thread_names}")
                
                # 죽은 스레드 재시작
                for dead_thread in dead_threads:
                    if dead_thread.name.startswith("binary-") and "explorer" in dead_thread.name:
                        try:
                            # 서버 번호 추출
                            server_num = int(dead_thread.name.split('-')[1])
                            
                            # 최소 1분 간격으로만 재시작 (너무 잦은 재시작 방지)
                            current_time = datetime.now(KST)
                            if dead_thread.name in last_restart_time:
                                if (current_time - last_restart_time[dead_thread.name]).total_seconds() < 60:
                                    logger.info(f"스레드 {dead_thread.name} 최근에 재시작되었습니다. 잠시 대기...")
                                    continue
                            
                            # 새 스레드 생성 및 시작
                            logger.info(f"스레드 {dead_thread.name} 재시작 중...")
                            new_thread = threading.Thread(
                                target=binary_explore_worker,
                                args=(server_num,),
                                name=f"binary-{server_num}-explorer",
                                daemon=True
                            )
                            
                            # 오래된 스레드 제거, 새 스레드 추가
                            with thread_status_lock:
                                active_threads.remove(dead_thread)
                                active_threads.append(new_thread)
                            
                            # 스레드 시작
                            new_thread.start()
                            last_restart_time[new_thread.name] = current_time
                            update_thread_status(thread_name, "스레드 재시작됨", 
                                        f"서버 {server_num} 이진 탐색 스레드가 재시작되었습니다.")
                        except Exception as e:
                            logger.error(f"스레드 재시작 중 오류: {e}", exc_info=True)
            
            # 1분마다 전체 상태 요약 출력
            now_kst_monitor = datetime.now(KST)
            if now_kst_monitor.minute % 5 == 0 and now_kst_monitor.second < 10:
                # 탐색기 통계 출력
                logger.info("----- 이진 탐색 진행 상태 -----")
                for server_num, explorer in explorers.items():
                    try:
                        stats = explorer.get_stats()
                        server_name = get_server_name(server_num)
                        
                        # 가장 오래된/최근 시간 문자열 변환
                        oldest = stats['oldest_scan'].strftime("%H:%M:%S") if stats['oldest_scan'] else "없음"
                        newest = stats['newest_scan'].strftime("%H:%M:%S") if stats['newest_scan'] else "없음"
                        
                        logger.info(f"서버 {server_num} ({server_name}): "
                                   f"전체 {stats['total_ranges']}개 범위, "
                                   f"남은 {stats['pending_ranges']}개 범위, "
                                   f"가장 오래된 스캔: {oldest}, 최근 스캔: {newest}")
                    except Exception as e:
                        logger.error(f"서버 {server_num} 통계 보고 오류: {e}")
                        
                logger.info("----------------------------")
                
                # 전체 스레드 상태 로깅
                log_all_thread_status()
                
                # 상태 파일 저장
                for explorer in explorers.values():
                    explorer.save_state()
                
            # 10초마다 모니터링
            for _ in range(10):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
    
    except Exception as e:
        update_thread_status(thread_name, "모니터링 오류", str(e))
        logger.error(f"이진 탐색 모니터링 오류: {e}", exc_info=True)
    
    finally:
        update_thread_status(thread_name, "모니터링 종료")

# 이진 탐색 시작 함수
def start_binary_exploration():
    """모든 서버에 대한 이진 탐색 시작"""
    global active_threads
    
    # 시작 전 종료 이벤트 초기화
    shutdown_event.clear()
    active_threads = []
    
    # 시스템 설정 로드
    settings = get_optimal_settings()
    
    # 메모리 최적화를 위한 설정
    gc.enable()
    gc.set_threshold(100, 5, 5)
    
    # 탐색 스레드 생성
    exploration_threads = []
    for server_num in range(1, 8):  # 7개 서버
        thread = threading.Thread(
            target=binary_explore_worker,
            args=(server_num,),
            name=f"binary-{server_num}-explorer",
            daemon=True
        )
        exploration_threads.append(thread)
        active_threads.append(thread)
    
    # 모니터링 스레드 생성
    monitor_thread = threading.Thread(
        target=binary_explorer_monitor,
        name="binary-monitor",
        daemon=True
    )
    active_threads.append(monitor_thread)
    
    # 모든 스레드 시작
    for thread in exploration_threads:
        thread.start()
        update_thread_status("main", f"이진 탐색 스레드 시작됨: {thread.name}")
        # 시작 간격을 두어 초기 부하 분산
        time.sleep(2)
    
    monitor_thread.start()
    update_thread_status("main", "이진 탐색 모니터링 스레드 시작됨")
    
    # 메인 스레드 유지
    try:
        while not shutdown_event.is_set():
            # 주기적으로 상태 저장
            for explorer in explorers.values():
                explorer.save_state()
            
            # 종료 이벤트 확인 (30초마다)
            shutdown_event.wait(30)
            
    except KeyboardInterrupt:
        update_thread_status("main", "사용자에 의한 이진 탐색 종료 요청")
        shutdown_all()

# 개별 서버 탐색 (디버깅/테스트용)
def run_binary_explore(server_num, div=1):
    """단일 서버에 대한 이진 탐색 실행 (테스트용)"""
    explorer = PageRangeExplorer(server_num)
    driver = get_driver(high_performance=False)

    try:
        # 기존 상태 로드 시도
        explorer.load_state()
        
        # 메인 루프
        while True:
            rng = explorer.get_next_range()
            if not rng:
                logger.info(f"서버 {server_num} 모든 범위 처리 완료. 재설정")
                explorer.reset()
                continue

            low, high = rng
            mid = (low + high) // 2
            
            page_range_text = f"{(mid-1)*20+1}위 ~ {mid*20}위"
            logger.info(f"서버 {server_num} 탐색: 범위 {low}~{high}, 중간 {mid} ({page_range_text})")

            # 이미 처리된 범위면 스킵
            if is_range_recent(server_num, page_range_text):
                logger.info(f"범위 {page_range_text} 이미 최근에 처리됨, 스킵")
                explorer.mark_scanned(low, high)
                explorer.split_and_add(low, high, mid)
                continue

            # 페이지 크롤링
            html, driver = fetch_rank_page_by_pageno(driver, server_num, mid, div)
            if not html:
                logger.warning(f"서버 {server_num}, 페이지 {mid} 데이터 없음")
                explorer.mark_scanned(low, high)
                explorer.split_and_add(low, high, mid)
                continue

            # 데이터 파싱
            data = parse_rank_html(html)
            if not data:
                logger.warning(f"서버 {server_num}, 페이지 {mid} 파싱 결과 없음")
                explorer.mark_scanned(low, high)
                explorer.split_and_add(low, high, mid)
                continue

            # DB 저장 및 범위 표시
            now_kst = datetime.now(KST)
            insert_data(data, server=None, character=None, div=div, retrieved_at_kst=now_kst)
            mark_range_crawled(server_num, page_range_text)
            
            # 범위 처리 완료 및 분할
            explorer.mark_scanned(low, high)
            explorer.split_and_add(low, high, mid)
            
            # 진행 상황 저장
            if explorer.get_stats()['pages_processed'] % 10 == 0:
                explorer.save_state()

            # 짧은 휴식
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"이진 탐색 오류: {e}", exc_info=True)
    finally:
        # 상태 저장
        explorer.save_state()
        # 드라이버 종료
        if driver:
            driver.quit()

if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 명령줄 인수 처리
    if len(sys.argv) > 1:
        if sys.argv[1] == "--stop":
            # 강제 종료 플래그 설정
            shutdown_event.set()
            logger.info("종료 명령 실행 중...")
            sys.exit(0)
        elif sys.argv[1].isdigit():
            # 단일 서버 실행 모드
            server_num = int(sys.argv[1])
            if 1 <= server_num <= 7:
                logger.info(f"서버 {server_num} 단일 이진 탐색 시작")
                run_binary_explore(server_num)
                sys.exit(0)
    
    # 전체 서버 이진 탐색 시작
    try:
        logger.info("모든 서버 이진 분할 방식 랭킹 탐색 시작")
        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        # 탐색 시작
        start_binary_exploration()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 탐색 중단됨")
        shutdown_all()
    except Exception as e:
        logger.error(f"이진 탐색 중 오류 발생: {str(e)}", exc_info=True)
        shutdown_all()
