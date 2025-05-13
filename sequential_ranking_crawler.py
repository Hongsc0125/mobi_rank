"""
순차적 랭킹 크롤러 - 각 서버별로 마지막 랭킹부터 1등까지 순차적으로 크롤링하는 독립 실행 모듈
다른 태스크와 독립적으로 동작하며, 최대한 많은 쓰레드로 빠르게 작업합니다.
"""

import time
import chromedriver_autoinstaller
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
import logging
import threading
from datetime import datetime, timedelta
import re
import signal
import sys
import psutil
import gc
from concurrent.futures import ThreadPoolExecutor
import random
from selenium.common.exceptions import WebDriverException

# KST 타임존 설정
import pytz
KST = pytz.timezone('Asia/Seoul')

# 로깅 설정
logger = logging.getLogger("순차랭킹크롤러")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sequential_crawler.log')
    ]
)

# 전역 변수들
shutdown_event = threading.Event()  # 종료 이벤트
active_threads = []  # 활성 쓰레드 관리
thread_status = {}   # 쓰레드 상태 관리
thread_status_lock = threading.Lock()  # 쓰레드 상태 락

# 마지막 크롤링 위치를 서버별로 추적
last_crawled_position = {}
last_crawled_position_lock = threading.Lock()

# 최대 가져오기 재시도 횟수
MAX_FETCH_RETRIES = 3

# 서버 이름 매핑
SERVER_NAMES = {
    1: "데이안",
    2: "아이라", 
    3: "던컨",
    4: "알리사",
    5: "메이븐",
    6: "라사",
    7: "칼릭스"
}

# SIGINT 및 SIGTERM 시그널 핸들러
def signal_handler(sig, frame):
    """Ctrl+C 또는 kill 명령어 감지 시 안전하게 종료"""
    logger.info("종료 신호를 감지했습니다. 모든 쓰레드를 안전하게 종료합니다...")
    shutdown_event.set()

# 시그널 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # kill 명령어

def update_thread_status(thread_name, status, details=None):
    """쓰레드 상태 업데이트 및 로깅"""
    with thread_status_lock:
        timestamp = datetime.now(KST).strftime("%H:%M:%S")
        thread_status[thread_name] = {
            'status': status,
            'details': details,
            'updated_at': timestamp
        }
        if details:
            logger.info(f"[{thread_name}] {status}: {details}")
        else:
            logger.info(f"[{thread_name}] {status}")

def log_all_thread_status():
    """모든 쓰레드의 현재 상태 로깅"""
    with thread_status_lock:
        if not thread_status:
            logger.info("활성 쓰레드 없음")
            return
            
        logger.info("===== 쓰레드 상태 요약 =====")
        for thread_name, status_info in thread_status.items():
            status = status_info['status']
            details = status_info.get('details', '')
            updated_at = status_info.get('updated_at', '')
            logger.info(f"{thread_name}: {status} {details} (갱신: {updated_at})")
        logger.info("===========================")

def shutdown_all():
    """모든 쓰레드 및 리소스 종료"""
    logger.info("모든 리소스를 종료합니다...")
    shutdown_event.set()
    
    # 모든 쓰레드가 종료될 때까지 최대 10초 대기
    for thread in active_threads:
        if thread.is_alive():
            thread.join(timeout=10)
    
    logger.info("종료 완료")

def get_driver(high_performance=True):
    """성능 최적화된 크롬 드라이버 인스턴스 생성"""
    chromedriver_autoinstaller.install()
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("window-size=1200,800")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    # 고성능 모드 설정
    if high_performance:
        # 메모리/성능 최적화 설정
        opts.add_argument("--js-flags=--expose-gc")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-software-rasterizer")
        opts.add_argument("--disable-features=site-per-process")
        opts.add_argument("--disable-features=NetworkService")
        opts.add_argument("--disable-features=IsolateOrigins")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--enable-javascript-harmony")
        opts.add_argument("--disable-infobars")
        opts.add_argument("--enable-precise-memory-info")
        opts.add_argument("--disable-default-apps")
        
        # 더 많은 메모리 사용 허용 (RAM 활용)
        opts.add_argument("--memory-model=low")
        opts.add_argument("--js-flags=--max-old-space-size=4096")
        
        # 하드웨어 가속 활성화 (vRAM 활용)
        opts.add_argument("--enable-gpu-rasterization")
        opts.add_argument("--enable-zero-copy")
    
    return webdriver.Chrome(service=Service(), options=opts)

def get_server_name(server_num):
    """서버 번호를 서버 이름으로 변환"""
    return SERVER_NAMES.get(server_num, "데이안")  # 기본값 "데이안"

def fetch_rank_page_by_pageno(driver, server_num, page_num=1, div=1, high_performance_driver=True):
    """페이지 번호로 기존 드라이버를 사용하여 랭킹 데이터 가져오기, 오류 시 재시도"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    attempts = 0
    last_exception = None

    while attempts < MAX_FETCH_RETRIES:
        try:
            if driver is None:
                 logger.info(f"드라이버 없음, 서버 {server_num}, 페이지 {page_num}에 대한 새 드라이버 생성")
                 driver = get_driver(high_performance=high_performance_driver)

            # 이미 리스트 페이지에 있지 않은 경우에만 페이지 이동
            if not driver.current_url.startswith(list_url):
                driver.get(list_url)
                time.sleep(2) # 페이지 로드 대기

            # 세션 가져오기
            sess = requests.Session()
            for ck in driver.get_cookies():
                sess.cookies.set(ck['name'], ck['value'])
                
            headers = {
                "User-Agent":          driver.execute_script("return navigator.userAgent;"),
                "Accept":              "*/*",
                "Referer":             list_url,
                "X-Requested-With":    "XMLHttpRequest",
                "Origin":              "https://mabinogimobile.nexon.com",
                "Content-Type":        "application/x-www-form-urlencoded; charset=UTF-8",
            }
            data = {
                "t":       str(div),
                "pageno":  str(page_num),
                "s":       server_num,
                "c":       "0",
                "search":  "",
            }

            resp = sess.post(api_url, headers=headers, data=data)
            resp.raise_for_status() # HTTP 오류 발생 시 예외 발생
            return resp.text, driver # 성공 시 HTML과 드라이버 반환

        except (requests.exceptions.RequestException, WebDriverException) as e:
            logger.warning(f"페이지 가져오기 실패: 서버 {server_num}, 페이지 {page_num}, 시도 {attempts + 1}/{MAX_FETCH_RETRIES}. 오류: {e}")
            last_exception = e
            attempts += 1
            if attempts < MAX_FETCH_RETRIES:
                if driver:
                    try:
                        driver.quit()
                    except Exception as dq_err:
                        logger.error(f"문제 있는 드라이버 종료 실패: {dq_err}")
                logger.info(f"서버 {server_num}, 페이지 {page_num}에 대한 드라이버 재생성 (시도 {attempts + 1})")
                driver = get_driver(high_performance=high_performance_driver)
                time.sleep(5) # 새 드라이버 안정화 및 IP 변경 등 외부 요인 대기
            else:
                logger.error(f"최종 실패: 서버 {server_num}, 페이지 {page_num}. 마지막 오류: {last_exception}")
                raise last_exception
    return None, driver # 이론적으로 도달하지 않지만, 만약을 위해 추가

def parse_rank_range(html: str):
    """HTML에서 랭킹 범위(예: "5,361위 ~ 5,380위") 파싱"""
    soup = BeautifulSoup(html, 'html.parser')
    range_span = soup.select_one("div.pager div.current_range span")
    if not range_span:
        return None
    
    range_text = range_span.text.strip()
    # 정규식으로 범위 추출
    match = re.search(r'([\d,]+)위\s*~\s*([\d,]+)위', range_text)
    if match:
        return range_text
    return None

def parse_rank_html(html: str):
    """HTML에서 랭킹 데이터 파싱"""
    soup = BeautifulSoup(html, 'html.parser')
    result = []

    # "데이터 없음" 메시지가 있는지 확인
    no_data = soup.select_one("div.no_data")
    if no_data:
        # 결과 없음 시 빈 리스트 반환
        return []

    for li in soup.select("ul.list li.item"):
        try:
            rank = li.select_one("div dl dt").text.strip()
            change_tag = li.select_one("div dl dd")
            change = change_tag.text.strip()
            change_type = "up" if "up" in change_tag.get("class", []) else "down"

            server = li.select("div dl")[1].select_one("dd").text.strip()
            character = li.select("div dl")[2].select_one("dd").get("data-charactername").strip()
            char_class = li.select("div dl")[3].select_one("dd").text.strip()
            power = li.select("div dl")[4].select_one("dd").text.strip()

            # "알수없음"을 캐릭터 이름으로 가진 항목 건너뛰기
            if character == "알수없음":
                continue

            result.append({
                "rank": rank,
                "change": change,
                "change_type": change_type,
                "server": server,
                "character": character,
                "class": char_class,
                "power": power
            })
        except (AttributeError, IndexError) as e:
            # 잘못된 형식의 항목 건너뛰기
            continue
    
    return result

def get_max_rank_position(server_num):
    """서버의 가장 낮은 랭킹(높은 숫자)를 DB에서 조회
    
    Args:
        server_num: 서버 번호
        
    Returns:
        max_rank: 가장 낮은 랭킹 위치 (높은 숫자)
        max_page: 최대 페이지 번호
    """
    try:
        from service.db import SessionLocal
        from sqlalchemy import text
        
        session = SessionLocal()
        server_name = get_server_name(server_num)
        
        try:
            # 해당 서버의 가장 낮은 랭킹(높은 숫자)을 가진 캐릭터 찾기
            query = text("""
                SELECT max(rank_position) as max_rank
                FROM mabinogi_ranking
                WHERE server_name = :server AND div = 1
            """)
            
            result = session.execute(query, {'server': server_name})
            row = result.fetchone()
            
            if row and row[0]:
                max_rank = row[0]
                # 페이지당 20개 항목, 올림으로 계산
                max_page = (max_rank + 19) // 20
                logger.info(f"서버 {server_num}({server_name})의 최대 랭킹: {max_rank}, 최대 페이지: {max_page}")
                return max_rank, max_page
            
            # 데이터가 없으면 기본값 반환
            logger.warning(f"서버 {server_num}({server_name})의 랭킹 데이터가 없음. 기본값 사용.")
            return 1000, 50  # 기본값: 1000위, 50페이지
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"최대 랭킹 조회 중 오류: {e}", exc_info=True)
        return 1000, 50  # 오류 시 기본값

def insert_ranking_data(data, div=1):
    """랭킹 데이터를 DB에 저장"""
    try:
        from service.db import insert_data
        now_kst = datetime.now(KST)
        insert_data(data, server=None, character=None, div=div, retrieved_at_kst=now_kst)
        logger.info(f"{len(data)}개 항목 저장 완료")
        return True
    except Exception as e:
        logger.error(f"데이터 저장 중 오류: {e}", exc_info=True)
        return False

# 드라이버 풀 관리자 클래스
class DriverPool:
    """WebDriver 인스턴스 풀을 관리하는 클래스"""
    def __init__(self, pool_size=5, high_performance=True):
        self.pool = []
        self.lock = threading.Lock()
        self.high_performance = high_performance
        
        # 풀 초기화
        for _ in range(pool_size):
            try:
                driver = get_driver(high_performance)
                self.pool.append(driver)
            except Exception as e:
                logger.error(f"드라이버 생성 중 오류: {e}")
    
    def get_driver(self):
        """사용 가능한 드라이버를 가져옴, 없으면 새로 생성"""
        with self.lock:
            if not self.pool:
                return get_driver(self.high_performance)
            return self.pool.pop()
    
    def return_driver(self, driver):
        """사용 완료된 드라이버를 풀에 반환"""
        with self.lock:
            self.pool.append(driver)
    
    def close_all(self):
        """모든 드라이버 종료"""
        with self.lock:
            for driver in self.pool:
                try:
                    driver.quit()
                except:
                    pass
            self.pool = []

# 배치 처리를 위한 데이터 수집기
class DataCollector:
    """데이터를 배치로 모아서 DB에 일괄 저장"""
    def __init__(self, batch_size=50, div=1):
        self.batch = []
        self.batch_size = batch_size
        self.div = div
        self.lock = threading.Lock()
    
    def add_data(self, data):
        """데이터 추가, 배치 크기에 도달하면 자동 저장"""
        flush_needed = False
        with self.lock:
            self.batch.extend(data)
            if len(self.batch) >= self.batch_size:
                flush_needed = True
        
        if flush_needed:
            self.flush()
    
    def flush(self):
        """현재 배치 데이터 저장"""
        with self.lock:
            if self.batch:
                try:
                    insert_ranking_data(self.batch, div=self.div)
                    # logger.info(f"{len(self.batch)}개 항목 일괄 저장 완료")
                except Exception as e:
                    logger.error(f"배치 데이터 저장 중 오류: {e}")
                self.batch = []
                # 명시적 가비지 컬렉션으로 메모리 최적화
                gc.collect()


def sequential_rank_crawl_worker(server_num, div=1):
    """마지막 랭킹부터 1등까지 순차적으로 크롤링하는 워커 함수
    완료 후 다시 마지막 랭킹부터 시작하는 순환 방식
    
    Args:
        server_num: 서버 번호
        div: 랭킹 분류 (기본값: 1 = 전체랭킹)
    """
    thread_name = f"순차크롤러_{server_num}"
    update_thread_status(thread_name, "시작됨")
    
    # 드라이버 준비
    driver = None
    try:
        driver = get_driver(high_performance=True)
        
        # 데이터 수집기 초기화
        collector = DataCollector(batch_size=50, div=div)
        
        while not shutdown_event.is_set():
            try:
                # 서버의 최대 랭킹과 페이지 가져오기
                max_rank, max_page = get_max_rank_position(server_num)
                
                # 시작 위치 가져오기 (처음이면 마지막 페이지부터, 아니면 저장된 위치부터)
                with last_crawled_position_lock:
                    if server_num not in last_crawled_position:
                        current_page = max_page
                    else:
                        current_page = last_crawled_position[server_num]
                        # 이전 위치가 최대 페이지보다 크면 최대 페이지로 조정
                        if current_page > max_page:
                            current_page = max_page
                        # 마지막 위치가 1이면 다시 마지막 페이지로 순환
                        elif current_page <= 1:
                            current_page = max_page
                        else:
                            # 이전 위치에서 1 감소
                            current_page -= 1
                
                update_thread_status(thread_name, f"크롤링 중", f"서버 {server_num}, 페이지 {current_page}/{max_page}")
                
                # 페이지 가져오기 및 처리
                html, driver = fetch_rank_page_by_pageno(driver, server_num, current_page, div, high_performance_driver=True)
                if html is None:
                    logger.error(f"서버 {server_num}, 페이지 {current_page} 가져오기 실패. 다음으로 넘어감.")
                    # 실패해도 위치는 업데이트
                    with last_crawled_position_lock:
                        last_crawled_position[server_num] = current_page
                    # 잠시 대기 후 다시 시도
                    time.sleep(5)
                    continue
                
                # 랭킹 범위 파싱 및 로깅
                range_text = parse_rank_range(html)
                if range_text:
                    logger.info(f"서버 {server_num}, 페이지 {current_page}, 범위: {range_text}")
                
                # 랭킹 데이터 파싱
                parsed_data = parse_rank_html(html)
                
                if not parsed_data:
                    logger.warning(f"서버 {server_num}, 페이지 {current_page}에서 데이터를 찾을 수 없음")
                else:
                    # 데이터 저장 (KST 시간 적용)
                    collector.add_data(parsed_data)
                    
                    logger.info(f"서버 {server_num}, 페이지 {current_page}에서 {len(parsed_data)}개 항목 처리됨")
                
                # 처리 완료된 페이지 위치 저장
                with last_crawled_position_lock:
                    last_crawled_position[server_num] = current_page
                
                # 가끔 수집기 비우기
                if current_page % 10 == 0:
                    collector.flush()
                
                # 과부하 방지 대기
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"서버 {server_num} 순차 크롤링 중 오류: {e}", exc_info=True)
                # 오류 발생 시 잠시 대기 후 계속
                time.sleep(10)
    
    except Exception as e:
        logger.error(f"서버 {server_num} 순차 크롤러 초기화 중 오류: {e}", exc_info=True)
    
    finally:
        # 종료 처리
        update_thread_status(thread_name, "종료됨")
        if driver:
            try:
                driver.quit()
            except:
                pass


def start_sequential_rank_crawling():
    """모든 서버에 대해 순차적 랭킹 크롤링 시작"""
    try:
        logger.info("순차적 랭킹 크롤링 시작")
        
        # 시스템 사양에 따른 쓰레드 수 결정
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)  # GB 단위로 변환
        
        # 서버별 쓰레드 수 (메모리와 CPU 고려)
        threads_per_server = max(1, min(3, int(memory_gb/4), cpu_count//4))
        logger.info(f"서버당 쓰레드 수: {threads_per_server} (CPU: {cpu_count}코어, 메모리: {memory_gb:.1f}GB)")
        
        # 서버 번호 리스트
        server_nums = list(range(1, 8))  # 1부터 7까지의 서버
        
        # 각 서버별로 순차 크롤링 워커 시작
        for server_num in server_nums:
            for i in range(threads_per_server):
                thread = threading.Thread(
                    target=sequential_rank_crawl_worker,
                    args=(server_num, 1),  # div=1 (전체랭킹)
                    name=f"순차크롤러_{server_num}_{i}"
                )
                thread.daemon = True  # 데몬 쓰레드로 설정
                thread.start()
                active_threads.append(thread)
                logger.info(f"서버 {server_num} 순차 크롤러 #{i} 시작됨")
        
        # 모니터링 쓰레드 시작
        monitor_thread = threading.Thread(target=thread_monitor, name="모니터링")
        monitor_thread.daemon = True
        monitor_thread.start()
        active_threads.append(monitor_thread)
        
        # 메인 쓰레드는 종료 이벤트 대기
        try:
            while not shutdown_event.is_set():
                # 1분마다 상태 로깅
                now = datetime.now(KST)
                if now.second < 10:
                    log_all_thread_status()
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("키보드 인터럽트 감지, 종료 중...")
            shutdown_event.set()
        
        # 모든 쓰레드 종료 대기
        for thread in active_threads:
            if thread.is_alive():
                thread.join(timeout=10)
        
        logger.info("순차적 랭킹 크롤링 종료")
        
    except Exception as e:
        logger.error(f"순차적 랭킹 크롤링 시작 중 오류: {e}", exc_info=True)
        shutdown_event.set()


def thread_monitor():
    """모든 쓰레드의 상태를 주기적으로 로깅하고 종료된 쓰레드를 재시작하는 모니터링 쓰레드"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "모니터링 시작")
    
    # 마지막으로 재시작한 시간 저장 (너무 빈번한 재시작 방지)
    last_restart_time = {}
    
    try:
        while not shutdown_event.is_set():
            # 모든 활성 쓰레드 확인
            alive_threads = [t for t in active_threads if t.is_alive()]
            dead_threads = [t for t in active_threads if not t.is_alive()]
            
            # 종료된 쓰레드 감지 및 재시작
            if dead_threads:
                dead_thread_names = [t.name for t in dead_threads]
                update_thread_status(thread_name, "쓰레드 종료 감지", 
                            f"{len(dead_threads)}개 쓰레드 종료됨: {dead_thread_names}")
                
                # 죽은 쓰레드 재시작
                for dead_thread in dead_threads:
                    # 순차 크롤러 쓰레드만 재시작
                    if dead_thread.name.startswith("순차크롤러_"):
                        try:
                            # 서버 번호와 쓰레드 인덱스 추출
                            parts = dead_thread.name.split('_')
                            server_num = int(parts[1])
                            thread_idx = int(parts[2]) if len(parts) > 2 else 0
                            
                            # 최소 1분 간격으로만 재시작 (너무 잦은 재시작 방지)
                            current_time = datetime.now(KST)
                            if dead_thread.name in last_restart_time:
                                if (current_time - last_restart_time[dead_thread.name]).total_seconds() < 60:
                                    logger.info(f"쓰레드 {dead_thread.name} 최근에 재시작되었습니다. 잠시 대기...")
                                    continue
                            
                            # 새 쓰레드 생성 및 시작
                            logger.info(f"쓰레드 {dead_thread.name} 재시작 중...")
                            new_thread = threading.Thread(
                                target=sequential_rank_crawl_worker,
                                args=(server_num, 1),  # div=1 (전체랭킹)
                                name=f"순차크롤러_{server_num}_{thread_idx}",
                                daemon=True
                            )
                            
                            # 오래된 쓰레드 제거, 새 쓰레드 추가
                            with thread_status_lock:
                                active_threads.remove(dead_thread)
                                active_threads.append(new_thread)
                            
                            # 쓰레드 시작
                            new_thread.start()
                            last_restart_time[new_thread.name] = current_time
                            update_thread_status(thread_name, "쓰레드 재시작됨", 
                                        f"서버 {server_num} 쓰레드 #{thread_idx}가 재시작되었습니다.")
                        except Exception as e:
                            logger.error(f"쓰레드 재시작 중 오류: {e}", exc_info=True)
            
            # 쓰레드 모니터링은 10초마다 수행
            for _ in range(10):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
                
    except Exception as e:
        update_thread_status(thread_name, "모니터링 오류", str(e))
    finally:
        update_thread_status(thread_name, "모니터링 종료")


if __name__ == "__main__":
    try:
        logger.info("===== 순차적 랭킹 크롤러 시작 =====")
        start_sequential_rank_crawling()
    except KeyboardInterrupt:
        logger.info("사용자에 의해 크롤링 중단됨")
        shutdown_all()
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {str(e)}", exc_info=True)
        shutdown_all()
