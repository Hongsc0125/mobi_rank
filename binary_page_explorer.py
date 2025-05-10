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

    # 상태 저장 및 로드 기능 추가
    def save_state(self, filename=BINARY_RANGES_FILE):
        """현재 탐색 상태를 파일에 저장"""
        data = {
            'server': self.server,
            'ranges': {},
            'counter': self._counter
        }
        
        with self._lock:
            # 시간 정보를 ISO 포맷으로 직렬화
            for key, ts in self._ranges.items():
                data['ranges'][f"{key[0]},{key[1]}"] = ts.isoformat()
        
        # 서버별로 폴더에 저장
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        filepath = filename.replace('.json', f'_{self.server}.json')
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"서버 {self.server} 탐색 상태 저장: {len(self._ranges)}개 범위")

    def load_state(self, filename=BINARY_RANGES_FILE):
        """저장된 탐색 상태를 파일에서 로드"""
        filepath = filename.replace('.json', f'_{self.server}.json')
        if not os.path.exists(filepath):
            logger.info(f"서버 {self.server} 저장된 탐색 상태 없음")
            return False
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if data['server'] != self.server:
                logger.warning(f"서버 번호 불일치: {data['server']} != {self.server}")
                return False
                
            with self._lock:
                self._counter = data['counter']
                self._pq = []
                self._ranges = {}
                
                # 범위 및 시간 정보 복원
                for key_str, ts_str in data['ranges'].items():
                    low, high = map(int, key_str.split(','))
                    ts = datetime.fromisoformat(ts_str)
                    
                    # KST 타임존 정보 추가
                    if ts.tzinfo is None:
                        ts = KST.localize(ts)
                    
                    key = (low, high)
                    self._ranges[key] = ts
                    heapq.heappush(self._pq, (ts, self._counter, low, high))
                    self._counter += 1
                    
            logger.info(f"서버 {self.server} 탐색 상태 로드: {len(self._ranges)}개 범위")
            return True
            
        except Exception as e:
            logger.error(f"상태 로드 오류 (서버 {self.server}): {e}")
            return False
    
    def reset(self, min_page=1, max_page=50):
        """탐색 상태를 초기화하고 전체 범위(1,50) 다시 추가"""
        with self._lock:
            self._pq = []
            self._ranges = {}
            self._counter = 0
            self.add_range(min_page, max_page)
        
        logger.info(f"서버 {self.server} 탐색 상태 초기화")
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
    # 재설정 간격 (1시간)
    reset_interval = timedelta(hours=1)
    
    try:
        # 주 실행 루프
        while not shutdown_event.is_set():
            # 주기적으로 전체 범위 초기화 (재탐색 보장)
            now = datetime.now(KST)
            if now - last_reset > reset_interval:
                logger.info(f"서버 {server_num} 정기 초기화 수행 ({reset_interval})")
                explorer.reset()
                explorer.save_state()
                last_reset = now
            
            # 다음 처리할 범위 가져오기
            range_tuple = explorer.get_next_range()
            if not range_tuple:
                # 모든 범위 처리 완료 - 재초기화
                logger.info(f"서버 {server_num} 모든 범위 처리 완료, 초기화 후 재시작")
                explorer.reset()
                # 상태 저장
                explorer.save_state()
                # 잠시 대기 후 다음 반복
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
