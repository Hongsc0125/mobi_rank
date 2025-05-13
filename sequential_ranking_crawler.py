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
import queue

# DB 연결 모듈 가져오기
from service.db import insert_data, get_980_data, delete_character_data
from service.db_session import SessionLocal, get_current_time, KST
from sqlalchemy import text

# KST 타임존 설정
import pytz
KST = pytz.timezone('Asia/Seoul')

# 로깅 설정
logger = logging.getLogger("순차크롤러")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',  # 간단한 형식으로 변경
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sequential_crawler.log')
    ]
)

# 기본 로그 레벨을 WARNING으로 설정 (중요한 로그만 표시)
logging.getLogger().setLevel(logging.WARNING)
# 순차크롤러 로그는 INFO 레벨 유지
logger.setLevel(logging.INFO)

# 시스템 리소스 제한 설정
MAX_CPU_PERCENT = 70.0  # CPU 사용량 최대 70%
MAX_MEMORY_PERCENT = 70.0  # 메모리 사용량 최대 70%
RESOURCE_CHECK_INTERVAL = 5  # 리소스 확인 주기(초)
THROTTLE_DELAY = 0.5  # 리소스 사용량이 높을 때 추가 디레이(초)

# 리소스 사용량 상태 저장
resource_usage_stats = {
    "cpu_percent": 0.0,
    "memory_percent": 0.0,
    "last_check_time": datetime.now(KST),
    "throttling": False
}

def check_system_resources():
    """
    현재 시스템의 CPU와 메모리 사용량을 확인하고 사용량이 높은지 확인합니다.
    
    Returns:
        bool: 리소스 사용량이 안전한지 여부 (True: 안전, False: 과부하)
    """
    global resource_usage_stats
    
    # 현재 CPU 사용량 (모든 코어의 평균)
    cpu_percent = psutil.cpu_percent(interval=0.1)
    
    # 현재 메모리 사용량
    memory_info = psutil.virtual_memory()
    memory_percent = memory_info.percent
    
    # 리소스 사용량 업데이트
    resource_usage_stats["cpu_percent"] = cpu_percent
    resource_usage_stats["memory_percent"] = memory_percent
    resource_usage_stats["last_check_time"] = datetime.now(KST)
    
    # CPU 또는 메모리 사용량이 임계치를 초과하는지 확인
    if cpu_percent > MAX_CPU_PERCENT or memory_percent > MAX_MEMORY_PERCENT:
        if not resource_usage_stats["throttling"]:
            resource_usage_stats["throttling"] = True
            logger.warning(f"시스템 리소스 과부하 감지: CPU {cpu_percent:.1f}%, 메모리 {memory_percent:.1f}%, 크롤링 속도 제한 시작")
        return False
    else:
        if resource_usage_stats["throttling"]:
            resource_usage_stats["throttling"] = False
            logger.info(f"시스템 리소스 상태 정상화: CPU {cpu_percent:.1f}%, 메모리 {memory_percent:.1f}%, 크롤링 속도 제한 해제")
        return True

# 전역 변수들
shutdown_event = threading.Event()  # 종료 이벤트
active_threads = []  # 활성 쓰레드 관리
thread_status = {}   # 쓰레드 상태 관리
thread_status_lock = threading.Lock()  # 쓰레드 상태 락

# 마지막으로 크롤링한 캐릭터 인덱스를 서버별로 저장
last_crawled_character_index = {}
last_crawled_character_lock = threading.Lock()

# 서버별 처리할 캐릭터 목록
server_characters = {}
server_characters_lock = threading.Lock()

# 데이터베이스 작업을 위한 큐 시스템
db_queue = queue.Queue(maxsize=100000)  # 최대 10만 항목으로 제한
db_worker_running = threading.Event()   # DB 작업자 쓰레드 상태
db_worker_running.set()                 # 초기에는 작동 상태로 설정

# 크롤링 작업 통계 및 상황판 관리
stats_lock = threading.Lock()          # 통계 데이터 락
server_stats = {}                      # 서버별 통계 정보
db_stats = {                           # DB 저장 통계 정보
    'total_processed': 0,              # 총 처리된 항목 수
    'current_queue_size': 0,           # 현재 큐 크기
    'last_batch_size': 0,              # 마지막 배치 크기
    'processing_rate': 0,              # 처리 속도 (초당)
    'start_time': datetime.now(KST),   # 시작 시간
    'last_update': datetime.now(KST),  # 마지막 업데이트 시간
}
stats_display_interval = 5  # 상황판 표시 간격(초)
last_stats_display = datetime.now(KST)  # 마지막 상황판 표시 시간

# 서버별 통계 갱신 함수
def update_server_stats(server_name, items_collected=0, items_flushed=0):
    """서버별 통계 정보 갱신"""
    global server_stats
    with stats_lock:
        if server_name not in server_stats:
            server_stats[server_name] = {
                'items_collected': 0,  # 수집된 항목 총계
                'items_flushed': 0,   # 크롤러가 비운 항목 총계
                'collector_size': 0,  # 수집기에 현재 들어있는 항목 수
                'last_update': datetime.now(KST),  # 마지막 갱신 시간
            }
            
        server_stats[server_name]['items_collected'] += items_collected
        server_stats[server_name]['items_flushed'] += items_flushed
        server_stats[server_name]['last_update'] = datetime.now(KST)

# DB 통계 갱신 함수
def update_db_stats(processed=0, queue_size=0, batch_size=0):
    """데이터베이스 작업 통계 갱신"""
    global db_stats
    with stats_lock:
        current_time = datetime.now(KST)
        
        # 총 처리된 항목 수 갱신
        db_stats['total_processed'] += processed
        
        # 현재 큐 크기 갱신
        db_stats['current_queue_size'] = queue_size
        
        # 마지막 배치 크기 갱신
        if batch_size > 0:
            db_stats['last_batch_size'] = batch_size
        
        # 처리 속도 계산 (시간당 항목 수)
        elapsed = (current_time - db_stats['start_time']).total_seconds()
        if elapsed > 0:
            db_stats['processing_rate'] = db_stats['total_processed'] / elapsed
            
        # 마지막 업데이트 시간 갱신
        db_stats['last_update'] = current_time

# 상황판 표시 함수
def display_stats_dashboard():
    """수집 및 DB 저장 현황을 통합하여 표시"""
    global last_stats_display
    current_time = datetime.now(KST)
    
    # 상황판 표시 간격 확인
    if (current_time - last_stats_display).total_seconds() < stats_display_interval:
        return
        
    with stats_lock:
        # 현재 시간 표시
        time_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
        elapsed = (current_time - db_stats['start_time']).total_seconds()
        elapsed_str = f"{int(elapsed//3600):02d}:{int((elapsed%3600)//60):02d}:{int(elapsed%60):02d}"
        
        # 서버별 통계 요약
        server_summary = ""
        total_collected = 0
        total_collector_size = 0
        
        for server, stats in sorted(server_stats.items()):
            total_collected += stats['items_collected']
            current_collector_size = stats['items_collected'] - stats['items_flushed']
            total_collector_size += current_collector_size
            server_summary += f"{server}: 수집={stats['items_collected']}, 배치사이즈={current_collector_size} | "
        
        if not server_summary:
            server_summary = "서버 데이터 없음"
        
        # DB 저장 현황
        db_summary = f"DB저장: 총 {db_stats['total_processed']}개 처리, 큐사이즈: {db_stats['current_queue_size']}, 속도: {db_stats['processing_rate']:.1f}개/초"
        
        # 종합 요약
        logger.info(f"===== 크롤링 상황판 ({time_str}, 총실행시간: {elapsed_str}) =====")
        logger.info(f"[1] 총계: 수집된 항목 {total_collected}개, 콜렉터 배치 총크기: {total_collector_size}개")
        logger.info(f"[2] {server_summary}")
        logger.info(f"[3] {db_summary}")
        logger.info("=================================================")
        
        # 상황판 표시 시간 갱신
        last_stats_display = current_time

# 서버별 콜렉터 사이즈 업데이트 함수
def update_collector_size(server_name, size):
    """현재 콜렉터에 저장된 항목 수 갱신"""
    global server_stats
    with stats_lock:
        if server_name in server_stats:
            server_stats[server_name]['collector_size'] = size

# 최대 가져오기 재시도 횟수
MAX_FETCH_RETRIES = 5  # 재시도 횟수 증가

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
    """쓰레드 상태 업데이트 및 로깅 (최소한의 로그만 출력)"""
    with thread_status_lock:
        timestamp = datetime.now(KST).strftime("%H:%M:%S")
        thread_status[thread_name] = {
            'status': status,
            'details': details,
            'updated_at': timestamp
        }
        # 상태 변경시에만 간결한 로그 출력 (최소로 유지)
        if status in ['시작됨', '종료됨', '오류 발생']:
            if details:
                logger.info(f"[{thread_name}] {status}: {details}")
            else:
                logger.info(f"[{thread_name}] {status}")

def log_all_thread_status():
    """모든 쓰레드의 현재 상태 로깅 (요약 형태로 줄이기)"""
    with thread_status_lock:
        if not thread_status:
            return
            
        # 활성 쓰레드수만 간단하게 출력
        active_count = sum(1 for info in thread_status.values() if info['status'] not in ['종료됨'])
        total_count = len(thread_status)
        logger.info(f"[STAT] {active_count}/{total_count} 쓰레드 운영중")

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

def fetch_rank_page(driver, server_num, search_name="", div=1, high_performance_driver=True):
    """캐릭터 이름으로 기존 드라이버를 사용하여 랭킹 데이터 가져오기, 오류 시 재시도"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"
    
    attempts = 0
    last_exception = None
    driver_recreated = False
    
    while attempts < MAX_FETCH_RETRIES:
        try:
            # 드라이버 상태 확인 (연결 확인)
            try:
                # 이미 리스트 페이지에 있지 않은 경우에만 페이지 이동
                if not driver.current_url.startswith(list_url):
                    driver.get(list_url)
                    time.sleep(2) # 페이지 로드 대기
            except Exception as e:
                # 드라이버 연결 문제 발생 시 드라이버 재생성
                if not driver_recreated:  # 한 번만 재생성 시도
                    logger.warning(f"드라이버 연결 오류 감지, 드라이버 재생성 시도: {str(e)[:100]}...")
                    try:
                        # 기존 드라이버 종료 시도 (이미 종료되었을 수 있음)
                        try:
                            driver.quit()
                        except:
                            pass
                            
                        # 새 드라이버 생성
                        driver = get_driver(high_performance=high_performance_driver)
                        driver.get(list_url)
                        time.sleep(3)  # 새 드라이버 로드에 더 많은 시간 부여
                        driver_recreated = True
                        continue  # 드라이버 재생성 후 다시 시도
                    except Exception as recreate_error:
                        logger.error(f"드라이버 재생성 실패: {recreate_error}")
                        raise  # 재생성도 실패하면 다음 예외 처리로 이동

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
                "s":       server_num,
                "c":       "0",
                "search":  search_name,
            }

            resp = sess.post(api_url, headers=headers, data=data)
            resp.raise_for_status()
            return resp.text, driver

        except (requests.exceptions.RequestException, WebDriverException) as e:
            logger.warning(f"캐릭터 검색 실패: 서버 {server_name}, 검색어 '{search_name}', 시도 {attempts + 1}/{MAX_FETCH_RETRIES}. 오류: {e}")
            last_exception = e
            attempts += 1
            if attempts < MAX_FETCH_RETRIES:
                if driver:
                    try:
                        driver.quit()
                    except Exception as dq_err:
                        logger.error(f"문제가 있는 드라이버 종료 오류: {dq_err}")
                logger.info(f"서버 {server_name}용 드라이버 재생성 (시도 {attempts + 1}).")
                driver = get_driver(high_performance=high_performance_driver)
                time.sleep(5) # 새 드라이버 안정화 및 IP 변경 등 외부 요인 대기
            else:
                logger.error(f"캐릭터 검색 실패: 서버 {server_name}, 검색어 '{search_name}'에 대한 모든 {MAX_FETCH_RETRIES}번의 재시도 실패. 마지막 오류: {last_exception}")
                return None, driver # 모든 시도 실패 시 None 반환

def parse_rank_range(html: str):
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
    soup = BeautifulSoup(html, 'html.parser')
    result = []

    no_data = soup.select_one("div.no_data")
    if no_data:
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


def insert_ranking_data(data, div=1):
    """랭킹 데이터를 큐에 넣어 데드락 방지"""
    try:
        # 현재 KST 시간 가져오기
        current_time_kst = get_current_time()
        
        # 데이터를 전처리하여 큐에 추가
        processed_data = []
        for item in data:
            try:
                # 콤마가 포함된 문자열을 숫자로 변환
                rank_position = int(item['rank'].replace(',', '').replace('위', ''))
                power_value = int(item['power'].replace(',', ''))
                
                # change 값이 '-'인 경우 0으로 처리
                change_value = item['change']
                if change_value == '-':
                    change_value = '0'
                
                # 데이터 딘셔너리로 준비
                processed_item = {
                    'rank': rank_position,
                    'change': int(change_value),
                    'change_type': item['change_type'],
                    'server': item['server'],
                    'character': item['character'],
                    'class': item['class'],
                    'power': power_value,
                    'div': div,
                    'retrieved_at_val': current_time_kst
                }
                processed_data.append(processed_item)
            except Exception as item_error:
                logger.warning(f"데이터 전처리 중 항목 오류, 무시함: {item_error}")
                continue
        
        # 처리된 데이터가 있으면 큐에 추가
        if processed_data:
            # 마무리된 데이터만 큐에 추가
            db_queue.put((processed_data, div))
            return True
        else:
            return False
            
    except Exception as e:
        logger.error(f"데이터 큐 처리 중 오류: {e}")
        return False


def db_worker():
    """데이터베이스 작업을 처리하는 단일 쓰레드
    데드락 방지를 위해 모든 DB 작업을 하나의 쓰레드에서 처리
    """
    thread_name = "DB작업자"
    update_thread_status(thread_name, "시작됨")
    logger.info("DB 작업자 쓰레드 시작")
    
    # 큐 처리 통계 변수
    total_processed = 0
    last_report_time = datetime.now(KST)
    batch_start_time = datetime.now(KST)
    
    # 데이터베이스 연결 객체 - 단일 연결 재사용
    db = None
    db_reconnect_time = datetime.now(KST)
    db_check_interval = timedelta(minutes=10)  # 10분마다 연결 재검사
    
    try:
        # 최초 한 번만 DB 연결 생성
        db = SessionLocal()
        
        while db_worker_running.is_set() and not shutdown_event.is_set():
            try:
                # DB 연결상태 10분마다 점검
                current_time = datetime.now(KST)
                if (current_time - db_reconnect_time) > db_check_interval:
                    # 기존 연결 정리 및 새 연결 생성
                    try:
                        if db:
                            db.close()
                    except Exception:
                        pass
                    
                    # 새 연결 생성
                    db = SessionLocal()
                    db_reconnect_time = current_time
                    logger.info("DB 연결 새로 생성 (주기적 갱신)")
                    
                # 큐에서 데이터 가져오기 (1초 타임아웃)
                try:
                    data_batch, div = db_queue.get(timeout=1)
                except queue.Empty:
                    # 큐가 비었을 때는 계속 체크
                    continue
                
                # 현재 큐 상태 로깅
                queue_size = db_queue.qsize()
                if queue_size > 1000:
                    logger.warning(f"DB 큐 사이즈 경고: {queue_size} 항목")
                
                # 데이터 처리 시도
                retry_count = 0
                max_retries = 3
                success = False
                batch_size = len(data_batch)
                
                while retry_count < max_retries and not success:
                    try:
                        # 트랜잭션 시작
                        if not db.in_transaction():
                            db.begin()
                        
                        # 각 데이터 항목에 대해 DB 작업 수행
                        for item in data_batch:
                            query = text("""
                                INSERT INTO mabinogi_ranking 
                                (rank_position, change_amount, change_type, server_name, character_name, class_name, power_value, div, retrieved_at)
                                VALUES (:rank, :change, :change_type, :server, :character, :class, :power, :div, :retrieved_at_val AT TIME ZONE 'Asia/Seoul')
                                ON CONFLICT (character_name, server_name, div) 
                                DO UPDATE SET 
                                    rank_position = :rank,
                                    change_amount = :change,
                                    change_type = :change_type,
                                    class_name = :class,
                                    power_value = :power,
                                    retrieved_at = :retrieved_at_val AT TIME ZONE 'Asia/Seoul'
                            """)
                            
                            db.execute(query, item)
                        
                        # 트랜잭션 커밋
                        db.commit()
                        success = True
                        
                        # 통계 갱신
                        total_processed += batch_size
                        update_db_stats(processed=batch_size, queue_size=queue_size, batch_size=batch_size)
                        
                        # 성공 로그 (상황판으로 대체하고 최소한의 로그만 출력)
                        logger.debug(f"{batch_size}개 항목 일괄 저장 완료")
                        
                    except Exception as e:
                        # 오류 발생
                        retry_count += 1
                        logger.error(f"DB 처리 중 오류 (시도 {retry_count}/{max_retries}): {e}")
                        
                        try:
                            # 트랜잭션 롤백
                            db.rollback()
                        except Exception:
                            pass
                        
                        # 연결 문제가 의심되면 연결 재생성
                        if "QueuePool limit" in str(e) or "connection" in str(e).lower():
                            try:
                                if db:
                                    db.close()
                            except Exception:
                                pass
                                
                            # 연결 재생성 전 잠시 대기
                            time.sleep(2)
                            db = SessionLocal()
                            db_reconnect_time = datetime.now(KST)
                            logger.info("DB 연결 재생성 완료 (오류 후 재연결)")
                        
                        if retry_count < max_retries:
                            # 재시도 전 대기 시간 (지수 백오프)
                            sleep_time = 2 ** retry_count
                            time.sleep(sleep_time)
                
                # 작업 완료 표시
                db_queue.task_done()
                
                # 상황판 표시 (각 배치 처리 후)  
                display_stats_dashboard()
                
                # 30초마다 로그에 간단한 통계 정보 출력 (상황판이 기본이며 이것은 사용하지 않음)
                now = datetime.now(KST)
                if (now - last_report_time).total_seconds() > 30:
                    # 추가 로그 출력 없음
                    last_report_time = now
                    
                # 작업 간 짧은 휴식 (시스템 부하 방지)
                time.sleep(0.01)
                
            except Exception as e:
                logger.error(f"DB 작업자 오류: {e}", exc_info=True)
                time.sleep(1)  # 오류 발생 시 잠시 대기
    
    except Exception as e:
        logger.error(f"DB 작업자 쓰레드 중단 오류: {e}", exc_info=True)
    finally:
        # 마무리 작업
        if db:
            try:
                db.close()
                logger.info("DB 연결 종료")
            except Exception:
                pass
                
        update_thread_status(thread_name, "종료됨")
        logger.info(f"DB 작업자 쓰레드 종료. 총 처리 항목: {total_processed}")


def save_to_db_directly(data, div=1):
    """로깅 등 특별한 경우 직접 DB에 저장
    주의: 데드락 발생 가능성이 있으니 데이터브레이커 모듈과 같이 필수적인 경우만 사용
    """
    db = SessionLocal()
    try:
        # 현재 KST 시간 가져오기
        current_time_kst = get_current_time()
        
        for item in data:
            # 콤마가 포함된 문자열을 숫자로 변환
            rank_position = int(item['rank'].replace(',', '').replace('위', ''))
            power_value = int(item['power'].replace(',', ''))
            
            # change 값 처리
            change_value = item['change']
            if change_value == '-':
                change_value = '0'
                
            # change_type이 'down'인 경우 음수로 변환 (랭킹이 내려간 경우)
            change_value_int = int(change_value.replace(',', ''))
            if item['change_type'] == 'down':
                change_value_int = -change_value_int  # 내려간 경우 음수로 표현
            
            # 레코드 추가 또는 업데이트 (div 매개변수 포함)
            query = text("""
                INSERT INTO mabinogi_ranking 
                (rank_position, change_amount, change_type, server_name, character_name, class_name, power_value, div, retrieved_at)
                VALUES (:rank, :change, :change_type, :server, :character, :class, :power, :div, :retrieved_at_val AT TIME ZONE 'Asia/Seoul')
                ON CONFLICT (character_name, server_name, div) 
                DO UPDATE SET 
                    rank_position = :rank,
                    change_amount = :change,
                    change_type = :change_type,
                    class_name = :class,
                    power_value = :power,
                    retrieved_at = :retrieved_at_val AT TIME ZONE 'Asia/Seoul'
            """)
            
            db.execute(query, {
                'rank': rank_position,
                'change': change_value_int,  # 음수/양수로 변환된 change 값 사용
                'change_type': item['change_type'],
                'server': item['server'],
                'character': item['character'],
                'class': item['class'],
                'power': power_value,
                'div': div,
                'retrieved_at_val': current_time_kst
            })
        
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"데이터 저장 중 오류: {e}")
        return False
    finally:
        db.close()

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

# DB 큐 및 안전한 크롤링을 위한 임계값 설정
MAX_QUEUE_SIZE = 10000  # DB 큐 최대 크기 (이 이상이면 크롤링 일시 중단)
MAX_COLLECTOR_SIZE = 5000  # 콜렉터 최대 크기 (이 이상이면 크롤링 일시 중단)
PAUSE_DURATION = 10  # 크롤링 일시 중단 시간(초)
SAFE_QUEUE_SIZE = 5000  # DB 큐 안전 크기 (이 이하면 크롤링 재개)
SAFE_COLLECTOR_SIZE = 2000  # 콜렉터 안전 크기 (이 이하면 크롤링 재개)
STATUS_CHECK_INTERVAL = 5  # 상태 확인 주기(초)

# 크롤링 일시 정지 상태 저장용 이벤트 객체
crawling_paused = {}

# 서버별 통계 관리를 위한 변수
server_stats = {}
# DB 통계
db_stats = {
    "processed": 0,
    "queue_size": 0,
    "batch_size": 0,
    "rate": 0.0,
    "last_calc_time": datetime.now(KST),
    "last_processed": 0,
    "paused": False  # 전체 크롤링 일시 정지 여부
}
last_dashboard_update = datetime.now(KST)
dashboard_update_interval = 5  # 초 단위

def update_server_stats(server_name, items_collected=0, items_flushed=0):
    """서버별 통계 정보 갱신"""
    global server_stats
    if server_name not in server_stats:
        server_stats[server_name] = {
            "total_collected": 0,
            "total_flushed": 0,
            "collector_size": 0,
            "last_update": datetime.now(KST)
        }
    
    server_stats[server_name]["total_collected"] += items_collected
    server_stats[server_name]["total_flushed"] += items_flushed
    server_stats[server_name]["last_update"] = datetime.now(KST)

def update_collector_size(server_name, size):
    """콜렉터 사이즈 갱신"""
    global server_stats
    if server_name in server_stats:
        server_stats[server_name]["collector_size"] = size

def update_db_stats(processed=0, queue_size=0, batch_size=0):
    """데이터베이스 통계 정보 갱신"""
    global db_stats
    db_stats["processed"] += processed
    db_stats["queue_size"] = queue_size
    db_stats["batch_size"] = batch_size
    now = datetime.now(KST)
    
    # 속도 계산 (1분 동안의 평균)
    if (now - db_stats.get("last_calc_time", now - timedelta(minutes=1))).total_seconds() > 60:
        # 1분마다 평균 속도 계산
        time_diff = (now - db_stats.get("last_calc_time", now - timedelta(minutes=1))).total_seconds()
        if time_diff > 0:
            processed_diff = db_stats["processed"] - db_stats.get("last_processed", 0)
            db_stats["rate"] = processed_diff / time_diff
        db_stats["last_calc_time"] = now
        db_stats["last_processed"] = db_stats["processed"]

def display_stats_dashboard():
    """통계 상황판 출력 (5초마다)"""
    global last_dashboard_update, dashboard_update_interval
    now = datetime.now(KST)
    
    # 지정된 간격마다만 상황판 업데이트
    if (now - last_dashboard_update).total_seconds() < dashboard_update_interval:
        return
    
    last_dashboard_update = now
    
    # 현재 시간 표시
    time_str = now.strftime("%Y-%m-%d %H:%M:%S KST")
    
    # 출력할 상황판 문자열 생성
    dashboard = f"\n{'-'*80}\n"
    dashboard += f"[{time_str}] 데이터 수집 상황\n"
    dashboard += f"{'-'*80}\n"
    
    # 서버별 통계
    dashboard += "\n[서버별 통계]\n"
    dashboard += "{:<10} {:<15} {:<15} {:<15}\n".format("서버", "수집된 항목", "저장된 항목", "현재 배치")
    dashboard += "-" * 60 + "\n"
    
    for server, stats in server_stats.items():
        dashboard += "{:<10} {:<15,d} {:<15,d} {:<15,d}\n".format(
            server,
            stats["total_collected"],
            stats["total_flushed"],
            stats["collector_size"]
        )
    
    # DB 통계
    dashboard += "\n[DB 통계]\n"
    dashboard += f"\ud504로세스된 항목: {db_stats['processed']:,d}\n"
    dashboard += f"큐 사이즈: {db_stats['queue_size']:,d}\n"
    dashboard += f"최근 배치 크기: {db_stats['batch_size']:,d}\n"
    dashboard += f"평균 처리 속도: {db_stats['rate']:.2f} 항목/초\n"
    
    dashboard += f"{'-'*80}\n"
    
    # 로그로 출력
    logger.info(dashboard)

# 배치 처리를 위한 데이터 수집기
class DataCollector:
    """데이터를 배치로 모아서 큐를 통해 DB에 저장 (데드락 방지)"""
    def __init__(self, batch_size=2000, div=1, server_name=None):
        self.batch = []
        self.batch_size = batch_size
        self.div = div
        self.server_name = server_name  # 서버 이름 추가
        self.lock = threading.Lock()
        self.last_flush_time = datetime.now(KST)
        # 마지막 저장 시간 (최대 5분마다 자동 저장)
        self.max_save_interval = timedelta(minutes=5)
        # 처리된 출력용 통계
        self.total_items_collected = 0
        self.total_items_flushed = 0
        
        # 초기화 시 서버 통계 초기화
        if self.server_name:
            update_server_stats(self.server_name)
    
    def add_data(self, data):
        """데이터 추가, 배치 크기에 도달하면 자동 저장"""
        flush_needed = False
        current_time = datetime.now(KST)
        time_based_flush = False
        items_count = len(data)
        
        with self.lock:
            self.batch.extend(data)
            self.total_items_collected += items_count
            
            # 통계 갱신
            if self.server_name:
                update_server_stats(self.server_name, items_collected=items_count)
                update_collector_size(self.server_name, len(self.batch))
            
            # 배치 크기 기반 저장 조건
            if len(self.batch) >= self.batch_size:
                flush_needed = True
            # 시간 기반 저장 조건
            elif (current_time - self.last_flush_time) > self.max_save_interval and self.batch:
                flush_needed = True
                time_based_flush = True
        
        if flush_needed:
            batch_size = len(self.batch)
            self.flush()
            if time_based_flush:
                logger.debug(f"시간 기반 자동 저장: {batch_size}개 항목 ({(current_time - self.last_flush_time).total_seconds():.0f}초 경과)")
                
        # 상황판 업데이트 (지정된 간격마다)
        display_stats_dashboard()
    
    def flush(self):
        """현재 배치 데이터를 큐로 전송"""
        with self.lock:
            if self.batch:
                try:
                    # 배치 데이터 복사 (안전한 처리를 위해)
                    batch_copy = self.batch.copy()
                    batch_size = len(batch_copy)
                    
                    # 통계 갱신
                    self.total_items_flushed += batch_size
                    if self.server_name:
                        update_server_stats(self.server_name, items_flushed=batch_size)
                    
                    # 데이터를 큐에 넣어 DB 작업자가 처리하도록 함
                    # 이렇게 하면 데드락 방지 가능
                    insert_ranking_data(batch_copy, div=self.div)
                    
                    # 성공 시 배치 초기화 및 마지막 저장 시간 갱신
                    self.batch = []
                    self.last_flush_time = datetime.now(KST)
                    
                    # 서버 콜렉터 사이즈 갱신
                    if self.server_name:
                        update_collector_size(self.server_name, 0)
                    
                    # 명시적 가비지 컬렉션으로 메모리 최적화
                    if batch_size > 500:
                        gc.collect()
                        
                except Exception as e:
                    logger.error(f"배치 데이터 큐 전송 중 오류: {e}")
                    # 오류 발생 시에도 시간 업데이트 (배치는 그대로 유지)
                    self.last_flush_time = datetime.now(KST)


def get_character_list_for_server(server_name, div=1):
    """서버에서 크롤링할 캐릭터 목록 가져오기 (마지막 랭킹부터 1등까지 정렬)"""
    try:
        db = SessionLocal()
        try:
            query = text("""
            WITH ranked_chars AS (
                SELECT character_name, rank_position, 
                       ROW_NUMBER() OVER (PARTITION BY character_name ORDER BY retrieved_at DESC) as rn
                FROM mabinogi_ranking 
                WHERE server_name = :server_name AND div = :div
            )
            SELECT character_name 
            FROM ranked_chars 
            WHERE rn = 1 AND character_name != '알수없음'
            ORDER BY rank_position DESC
            """)
            
            result = db.execute(query, {"server_name": server_name, "div": div})
            characters = [row[0] for row in result.fetchall()]
            
            if not characters:
                logger.warning(f"서버 {server_name}에서 크롤링할 캐릭터를 찾을 수 없음. 기본 캐릭터 사용.")
                return ["힝트"]
                
            logger.info(f"서버 {server_name}에서 {len(characters)}개의 캐릭터를 마지막 랭킹부터 크롤링할 예정")
            return characters
        finally:
            db.close()
                
    except Exception as e:
        logger.error(f"캐릭터 목록 가져오기 실패: {e}", exc_info=True)
        # 오류 발생 시 기본 목록 반환
        return ["힝트"]

def is_crawling_safe(server_name, collector_size):
    """데이터베이스 큐 및 콜렉터 크기를 기반으로 크롤링 안전성을 확인합니다.
    
    Args:
        server_name: 서버 이름
        collector_size: 현재 콜렉터 크기
        
    Returns:
        bool: 크롤링이 안전한지 여부
    """
    # 전체 크롤링 일시 중단 상태 확인
    global db_stats
    if db_stats.get("paused", False):
        return False
    
    # DB 큐 크기 확인
    queue_size = db_stats.get("queue_size", 0)
    if queue_size > MAX_QUEUE_SIZE:
        logger.warning(f"DB 큐 크기 임계값 초과: {queue_size} > {MAX_QUEUE_SIZE}, 전체 크롤링 일시 중단")
        db_stats["paused"] = True
        return False
    
    # 개별 서버 콜렉터 크기 확인
    if collector_size > MAX_COLLECTOR_SIZE:
        logger.warning(f"서버 {server_name} 콜렉터 크기 임계값 초과: {collector_size} > {MAX_COLLECTOR_SIZE}, 해당 서버 크롤링 일시 중단")
        return False
    
    return True

def is_safe_to_resume(server_name, collector_size):
    """크롤링을 재개해도 안전한지 확인합니다.
    
    Args:
        server_name: 서버 이름
        collector_size: 현재 콜렉터 크기
        
    Returns:
        bool: 크롤링 재개가 안전한지 여부
    """
    # 전체 크롤링 일시 중단 상태 확인
    global db_stats
    if db_stats.get("paused", False):
        # DB 큐가 안전 수준 이하로 내려갔는지 확인
        queue_size = db_stats.get("queue_size", 0)
        if queue_size <= SAFE_QUEUE_SIZE:
            logger.info(f"DB 큐 크기 안전 수준 도달: {queue_size} <= {SAFE_QUEUE_SIZE}, 크롤링 재개 가능")
            db_stats["paused"] = False
            return True
        return False
    
    # 콜렉터 크기가 안전 수준 이하로 내려갔는지 확인
    if collector_size <= SAFE_COLLECTOR_SIZE:
        return True
    
    return False

def sequential_rank_crawl_worker(server_num, div=1):
    """마지막 랭킹부터 1등까지 순차적으로 크롤링하는 워커 함수
    캠릭터 이름 기반 검색 방식
    
    Args:
        server_num: 서버 번호
        div: 랭킹 분류 (기본값: 1)
    """
    server_name = get_server_name(server_num)
    thread_name = f"순차크롤러_{server_name}"
    update_thread_status(thread_name, "시작됨")
    
    # 드라이버 준비
    driver = None
    driver_restart_count = 0  # 드라이버 재시작 횟수 추적
    MAX_DRIVER_RESTARTS = 5   # 최대 드라이버 재시작 횟수
    
    # 크롤링 일시 중단 상태 관리
    crawling_paused[server_name] = False
    last_status_check = datetime.now(KST)
    
    try:
        driver = get_driver(high_performance=True)
        
        try:
            # 데이터 수집기 초기화 (100만 건용 배치 크기 증가)
            # 서버 이름을 전달하여 통계 만들기 (오류 반환 시 로깅)
            logger.info(f"서버 {server_num} ({server_name}) 순차 콜렉터 초기화 시작")
            collector = DataCollector(batch_size=2000, div=div, server_name=server_name)
            logger.info(f"서버 {server_name} 콜렉터 초기화 완료")
        except Exception as e:
            logger.error(f"서버 {server_num} 순차 크롤러 초기화 중 오류: {e}")
            # 오류 발생 시 빈 콜렉터 생성
            collector = DataCollector(batch_size=2000, div=div)
            # 오류 구분을 위해 서버 이름 관련 정보 출력
            logger.error(f"서버 이름: {server_name}, div: {div}, 서버 번호: {server_num}")
            
        # 이미 서버 이름을 가져왔기 때문에 다시 가져올 필요 없음
        
        # 서버별 캐릭터 목록 초기화
        with server_characters_lock:
            if server_name not in server_characters:
                server_characters[server_name] = get_character_list_for_server(server_name, div)
        
        while not shutdown_event.is_set():
            try:
                # 현재 콜렉터 크기 확인
                collector_size = 0
                with collector.lock:
                    collector_size = len(collector.batch)
                
                # 상태 확인 주기에 따라 DB 큐 및 콜렉터 크기 모니터링
                current_time = datetime.now(KST)
                if (current_time - last_status_check).total_seconds() >= STATUS_CHECK_INTERVAL:
                    last_status_check = current_time
                    
                    # 크롤링 일시 중단 상태일 경우 재개 가능한지 확인
                    if crawling_paused[server_name]:
                        if is_safe_to_resume(server_name, collector_size):
                            logger.info(f"서버 {server_name} 크롤링 재개: 콜렉터 크기={collector_size}, DB 큐 크기={db_stats.get('queue_size', 0)}")
                            crawling_paused[server_name] = False
                            update_thread_status(thread_name, f"크롤링 재개됨", f"서버 {server_name}, 가능 상태: 안전")
                        else:
                            # 아직 안전하지 않은 경우 잠시 대기 후 다시 확인
                            logger.warning(f"서버 {server_name} 크롤링 계속 일시 중단: 콜렉터 크기={collector_size}, DB 큐 크기={db_stats.get('queue_size', 0)}")
                            update_thread_status(thread_name, f"일시 중단 중", f"서버 {server_name}, 안전한 상태 대기 중")
                            display_stats_dashboard()  # 현재 상태 표시
                            time.sleep(PAUSE_DURATION)
                            continue
                    # 크롤링 중이면 안전한지 확인
                    elif not is_crawling_safe(server_name, collector_size):
                        logger.warning(f"서버 {server_name} 크롤링 일시 중단 시작: 콜렉터 크기={collector_size}, DB 큐 크기={db_stats.get('queue_size', 0)}")
                        crawling_paused[server_name] = True
                        update_thread_status(thread_name, f"일시 중단 시작", f"서버 {server_name}, 가능 상태: 위험")
                        
                        # 크롤링 중단 시 강제 배치 저장 수행
                        collector.flush()
                        display_stats_dashboard()  # 현재 상태 표시
                        time.sleep(PAUSE_DURATION)
                        continue
                
                # 크롤링이 일시 중단된 경우 처리
                if crawling_paused[server_name]:
                    time.sleep(PAUSE_DURATION)  # 대기 후 다시 상태 확인
                    continue
                
                # 서버의 캐릭터 목록 확인
                with server_characters_lock:
                    chars = server_characters[server_name]
                
                # 현재 처리할 캐릭터 인덱스 가져오기
                current_char_idx = 0
                total_chars = len(chars)
                
                with last_crawled_character_lock:
                    if server_name not in last_crawled_character_index:
                        current_char_idx = 0  # 처음부터 시작 (이미 랭킹 내림차순으로 정렬되어 있음)
                    else:
                        current_char_idx = (last_crawled_character_index[server_name] + 1) % total_chars  # 다음 캐릭터로
                    
                    # 현재 캐릭터 인덱스 저장
                    last_crawled_character_index[server_name] = current_char_idx
                
                # 현재 처리할 캐릭터 이름 가져오기
                current_char = chars[current_char_idx]
                
                update_thread_status(thread_name, f"캐릭터 크롤링 중", f"서버 {server_name}, 캐릭터 {current_char} ({current_char_idx+1}/{total_chars})")
                
                # 캐릭터 이름으로 검색하여 데이터 가져오기
                # API 호출에는 server_num을 사용
                html, driver = fetch_rank_page(driver, server_num, current_char, div, high_performance_driver=True)
                if html is None:
                    logger.error(f"서버 {server_name}, 캐릭터 '{current_char}' 검색 실패. 다음으로 넘어감.")
                    # 잠시 대기 후 다시 시도
                    time.sleep(5)
                    continue
                
                # 랭킹 데이터 파싱
                parsed_data = parse_rank_html(html)
                
                if not parsed_data:
                    logger.warning(f"서버 {server_name}, 캐릭터 '{current_char}'에서 데이터를 찾을 수 없음")
                    
                    # 찾을 수 없는 캠릭터는 DB에서 삭제
                    try:
                        # 캠릭터가 존재하지 않으므로 해당 캐릭터의 모든 랭킹 데이터(div) 삭제
                        deleted_count = delete_character_data(server_name, current_char, div=None)
                        
                        if deleted_count > 0:
                            logger.info(f"서버 {server_name}, 캐릭터 '{current_char}' DB에서 삭제됨. 총 {deleted_count}개 레코드 삭제")
                            
                            # 삭제 후 서버 통계 업데이트 (매개변수 음수 값)
                            with collector.lock:
                                collector.total_items_collected -= 1  # 통계용 1 감소
                            
                    except Exception as e:
                        logger.error(f"서버 {server_name}, 캠릭터 '{current_char}' 삭제 중 오류 발생: {e}")
                else:
                    # 데이터 수집 (통계 갱신은 collector에서 처리, KST 시간 적용)
                    collector.add_data(parsed_data)
                
                # 일정 간격으로 수집기 비우기 (중복 제거 및 간격 조정)
                if current_char_idx % 10 == 0:
                    # 배치 저장
                    collector.flush()
                    # 상황판 갱신
                    display_stats_dashboard()
                
                # 시스템 리소스 사용량 확인 및 과부하 방지
                current_time = datetime.now(KST)
                if (current_time - resource_usage_stats["last_check_time"]).total_seconds() >= RESOURCE_CHECK_INTERVAL:
                    # 시스템 리소스 사용량 확인
                    resources_safe = check_system_resources()
                    
                    # 리소스 사용량이 높은 경우 상태 업데이트
                    if not resources_safe:
                        update_thread_status(thread_name, "리소스 제한 중", 
                                             f"CPU: {resource_usage_stats['cpu_percent']:.1f}%, 메모리: {resource_usage_stats['memory_percent']:.1f}%")
                        
                        # 리소스 사용량이 너무 높은 경우 추가 강제 배치 저장 수행
                        if resource_usage_stats['cpu_percent'] > MAX_CPU_PERCENT + 10 or \
                           resource_usage_stats['memory_percent'] > MAX_MEMORY_PERCENT + 10:
                            # 심각한 과부하인 경우 배치 저장
                            if len(collector.batch) > 0:
                                logger.warning(
                                    f"심각한 리소스 과부하: CPU {resource_usage_stats['cpu_percent']:.1f}%, "
                                    f"메모리 {resource_usage_stats['memory_percent']:.1f}%, 배치 강제 저장 후 디레이 적용")
                                collector.flush()  # 강제 배치 저장
                                time.sleep(THROTTLE_DELAY * 4)  # 더 긴 디레이
                                continue
                            time.sleep(THROTTLE_DELAY * 2)  # 더 긴 디레이
                        else:
                            # 준위한 경우 기본 스로틀링
                            time.sleep(THROTTLE_DELAY)
                
                # 기본 대기 시간 (과부하 방지)
                # 스로틀링 중이 아닌 경우에도 기본 대기 시간 적용
                if resource_usage_stats["throttling"]:
                    time.sleep(THROTTLE_DELAY)  # 스로틀링 중일 때 추가 디레이
                else:
                    time.sleep(0.5)  # 일반 대기 시간 약간 감소
                
            except Exception as e:
                error_str = str(e)
                logger.error(f"서버 {server_num} 순차 크롤링 중 오류: {error_str[:100]}...", exc_info=True)
                
                # 드라이버 연결 관련 오류인지 확인
                connection_errors = [
                    "HTTPConnectionPool", "Max retries exceeded",
                    "NewConnectionError", "Failed to establish a new connection",
                    "ConnectionRefusedError", "Connection refused",
                    "WebDriverException", "chrome not reachable",
                    "StaleElementReferenceException"
                ]
                
                is_connection_error = any(err in error_str for err in connection_errors)
                
                if is_connection_error and driver_restart_count < MAX_DRIVER_RESTARTS:
                    # 드라이버 재시작 로직
                    driver_restart_count += 1
                    logger.warning(f"드라이버 연결 오류 발생, 재시작 시도 {driver_restart_count}/{MAX_DRIVER_RESTARTS}")
                    
                    try:
                        # 기존 드라이버 정리
                        if driver:
                            try:
                                driver.quit()
                            except:
                                pass
                        
                        # 메모리 정리 및 가비지 컬렉션
                        gc.collect()
                        
                        # 새 드라이버 생성 전 잠시 대기 (시스템 리소스 정리 시간)
                        time.sleep(5)
                        
                        # 새 드라이버 생성
                        driver = get_driver(high_performance=True)
                        logger.info(f"서버 {server_name} 워커의 드라이버 재생성 성공")
                        
                        # 드라이버 재시작 성공 후 잠시 대기
                        time.sleep(2)
                    except Exception as restart_error:
                        logger.error(f"드라이버 재시작 실패: {restart_error}")
                else:
                    # 일반 오류 또는 최대 재시작 횟수 초과 시 대기 후 계속
                    wait_time = 10 + (driver_restart_count * 5)  # 재시작 횟수에 따라 대기 시간 증가
                    logger.warning(f"오류 후 {wait_time}초 대기 후 계속")
                    time.sleep(wait_time)
    
    except Exception as e:
        logger.error(f"서버 {server_num} 순차 크롤러 초기화 중 오류: {e}", exc_info=True)
    
    finally:
        # 종료 처리 - 남은 데이터 저장 및 리소스 정리
        try:
            # 남은 데이터가 있으면 마지막으로 저장 시도
            collector.flush()
            logger.info(f"서버 {server_name} 워커 종료 전 데이터 저장 완료")
        except Exception as e:
            logger.error(f"서버 {server_name} 워커 종료 전 데이터 저장 실패: {e}")
            
        update_thread_status(thread_name, "종료됨")
        
        # 드라이버 정리
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"드라이버 종료 중 오류: {e}")
                
        # 메모리 정리
        gc.collect()


def start_sequential_rank_crawling():
    """모든 서버에 대해 순차적 랭킹 크롤링 시작"""
    try:
        logger.info("순차적 랭킹 크롤링 시작")
        
        # 서버당 스레드 수를 1개로 고정
        threads_per_server = 1
        
        # 시스템 사양 정보 로깅
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)  # GB 단위로 변환
        logger.info(f"서버당 스레드 수: {threads_per_server} (단일 스레드 사용) (CPU: {cpu_count}코어, 메모리: {memory_gb:.1f}GB)")
        
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
                # 5분마다 상태 로깅 (비번도 줄임)
                now = datetime.now(KST)
                if now.minute % 5 == 0 and now.second < 10:
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
        
        # DB 작업자 쓰레드 시작 (데드락 방지용 단일 쓰레드)
        db_worker_thread = threading.Thread(target=db_worker, name="DB작업자")
        db_worker_thread.daemon = True
        db_worker_thread.start()
        active_threads.append(db_worker_thread)
        logger.info("DB 작업자 쓰레드 시작됨 (데드락 방지)")
        
        # 랭킹 크롤링 시작
        start_sequential_rank_crawling()
        
        # 메인 쓰레드는 다른 작업을 할 수 있도록 계속 실행
        while not shutdown_event.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Ctrl+C로 종료 시도")
    finally:
        # DB 작업자 쓰레드 종료 신호
        db_worker_running.clear()
        logger.info("DB 작업자 종료 신호 전송, 남은 작업 처리 대기 중...")
        
        # 그 외 모든 리소스 정리
        shutdown_all()
