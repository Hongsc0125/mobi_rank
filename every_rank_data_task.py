# full_data_selenium_requests.py

import time
import chromedriver_autoinstaller
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import os
import logging
from service.db import insert_data, get_980_data
from collections import deque
import re
import threading
import json
from datetime import datetime, timedelta
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import psutil
import gc
import signal
import sys
import pytz # Added import

logger = logging.getLogger(__name__)

# Define KST timezone
KST = pytz.timezone('Asia/Seoul') # Added KST timezone

MAX_FETCH_RETRIES = 3 # Maximum number of retries for fetching a page

# Track global discovered ranges to avoid duplicate work across runs
all_discovered_ranges = {}  # server_num -> set of ranges

# Global state tracking
discovered_ranges = {}  # server_num -> {range_text: timestamp}
discovered_ranges_lock = threading.Lock() # Added lock for discovered_ranges
last_base_refresh = {}  # server_num -> timestamp
range_queue = {}        # server_num -> deque of ranges to explore

# File to persist discovered ranges between runs
RANGES_FILE = os.path.join(os.path.dirname(__file__), "discovered_ranges.json")

# 전역 종료 이벤트 - 모든 스레드에게 종료 신호를 보내기 위한 플래그
shutdown_event = threading.Event()

# 모든 활성 스레드를 추적하기 위한 컨테이너
active_threads = []

# 전역 변수로 각 스레드의 현재 상태 추적
thread_status = {}
thread_status_lock = threading.Lock()

def update_thread_status(thread_name, status, details=None):
    """스레드 상태 업데이트 및 로깅"""
    with thread_status_lock:
        timestamp = datetime.now(KST).strftime("%H:%M:%S") # Use KST
        thread_status[thread_name] = {
            'status': status,
            'details': details,
            'updated_at': timestamp
        }
        # if details:
            # logger.info(f"[{thread_name}] {status}: {details}")
        # else:
            # logger.info(f"[{thread_name}] {status}")

def log_all_thread_status():
    """모든 스레드의 현재 상태 로깅"""
    with thread_status_lock:
        logger.info("----- 스레드 상태 요약 -----")
        for thread_name, info in thread_status.items():
            status = info['status']
            details = info.get('details', '')
            updated = info['updated_at']
            
            if details:
                logger.info(f"[{updated}] {thread_name}: {status} - {details}")
            else:
                logger.info(f"[{updated}] {thread_name}: {status}")
        logger.info("----------------------------")

# 모든 스레드 및 리소스 종료 함수
def shutdown_all():
    """모든 스레드와 리소스를 안전하게 종료"""
    logger.info("모든 스레드 종료 시작...")
    
    # 종료 이벤트 설정 - 모든 스레드에게 종료 신호 전달
    shutdown_event.set()
    
    # 모든 스레드가 종료될 때까지 최대 10초 대기
    for thread in active_threads:
        if thread.is_alive():
            logger.info(f"스레드 '{thread.name}' 종료 대기 중...")
            thread.join(timeout=10)
    
    # 데이터베이스 작업 마무리 및 저장
    save_discovered_ranges()
    
    logger.info("모든 스레드가 종료되었습니다.")
    return True

# SIGINT 및 SIGTERM 시그널 핸들러
def signal_handler(sig, frame):
    logger.info(f"시그널 {sig} 감지, 종료 프로세스 시작...")
    shutdown_all()
    sys.exit(0)

# 시그널 핸들러 등록
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # kill 명령어

def load_discovered_ranges():
    """이전에 발견된 범위를 파일에서 로드"""
    global discovered_ranges
    newly_loaded_ranges = {} 
    try:
        if os.path.exists(RANGES_FILE):
            # File I/O outside the lock
            with open(RANGES_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # Processing data outside the lock
            for server_str, ranges_dict_val in data.items():
                server_num = int(server_str)
                newly_loaded_ranges[server_num] = {}
                if isinstance(ranges_dict_val, dict): # Check if ranges_dict_val is a dict
                    for range_text, timestamp_str in ranges_dict_val.items():
                        dt_obj = datetime.fromisoformat(timestamp_str)
                        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
                            newly_loaded_ranges[server_num][range_text] = KST.localize(dt_obj)
                        else:
                            newly_loaded_ranges[server_num][range_text] = dt_obj.astimezone(KST)
            if newly_loaded_ranges: 
                logger.info(f"파일에서 {sum(len(r) for r in newly_loaded_ranges.values() if isinstance(r, dict))}개 범위 로드 완료")
        
        with discovered_ranges_lock: # Lock only for assignment
            if newly_loaded_ranges:
                discovered_ranges = newly_loaded_ranges
            else: 
                discovered_ranges = {i: {} for i in range(1, 8)}
            
    except Exception as e:
        logger.error(f"범위 로드 중 오류: {e}", exc_info=True)
        with discovered_ranges_lock: 
            discovered_ranges = {i: {} for i in range(1, 8)}

def save_discovered_ranges():
    """발견된 범위를 파일에 저장"""
    data_to_save = {}
    with discovered_ranges_lock: # Lock for reading
        for server, ranges_dict_val in discovered_ranges.items():
            if isinstance(ranges_dict_val, dict):
                 data_to_save[str(server)] = {k: v.isoformat() for k, v in ranges_dict_val.items()}
            else:
                 data_to_save[str(server)] = {}
    
    try:    
        with open(RANGES_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"범위 저장 중 오류: {e}", exc_info=True)

def is_range_recent(server_num, range_text):
    """범위가 최근(10분 이내)에 수집되었는지 확인"""
    last_crawled_local = None
    with discovered_ranges_lock: # Acquire lock for reading
        if server_num not in discovered_ranges:
            return False
        if range_text not in discovered_ranges[server_num]:
            return False
        last_crawled_local = discovered_ranges[server_num][range_text]
    
    # Ensure comparison is between aware datetime objects
    return (datetime.now(KST) - last_crawled_local) < timedelta(minutes=10)

def mark_range_crawled(server_num, range_text):
    """범위를 현재 타임스탬프와 함께 수집됨으로 표시"""
    save_needed = False
    with discovered_ranges_lock: # Acquire lock for writing
        if server_num not in discovered_ranges:
            discovered_ranges[server_num] = {}
        discovered_ranges[server_num][range_text] = datetime.now(KST)
        
        if len(discovered_ranges[server_num]) % 10 == 0:
            save_needed = True
    
    if save_needed:
        save_discovered_ranges() # Called outside lock to prevent deadlock

def should_refresh_base_pages(server_num):
    """기본 1-50 페이지를 갱신할 시간인지 확인 (~20분 후)"""
    if server_num not in last_base_refresh:
        return True
        
    return (datetime.now(KST) - last_base_refresh[server_num]) > timedelta(minutes=20)

def get_driver(high_performance=False):
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

def fetch_rank_page(driver_pool_ref, driver_instance, server_num, search_name="", div=1, retries=MAX_FETCH_RETRIES):
    """기존 드라이버를 사용하여 랭킹 데이터 가져오기, 오류 시 재시도 및 드라이버 교체"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    if not driver_instance.current_url.startswith(list_url):
        try:
            driver_instance.get(list_url)
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Driver.get failed for {list_url}: {e}. Attempting driver replacement.")
            if retries > 0:
                new_driver_instance = driver_pool_ref.replace_driver_in_pool(driver_instance)
                time.sleep(5)
                return fetch_rank_page(driver_pool_ref, new_driver_instance, server_num, search_name, div, retries - 1)
            else:
                try:
                    driver_instance.quit()
                except: pass
                raise e


    sess = requests.Session()
    try:
        for ck in driver_instance.get_cookies():
            sess.cookies.set(ck['name'], ck['value'])
    except Exception as e:
        logger.warning(f"Failed to get cookies: {e}. Attempting driver replacement.")
        if retries > 0:
            new_driver_instance = driver_pool_ref.replace_driver_in_pool(driver_instance)
            time.sleep(5)
            return fetch_rank_page(driver_pool_ref, new_driver_instance, server_num, search_name, div, retries - 1)
        else:
            try:
                driver_instance.quit()
            except: pass
            raise e
            
    headers = {
        "User-Agent":          driver_instance.execute_script("return navigator.userAgent;"),
        "Accept":              "*/*",
        "Referer":             list_url,
        "X-Requested-With":    "XMLHttpRequest",
        "Origin":              "https://mabinogimobile.nexon.com",
        "Content-Type":        "application/x-www-form-urlencoded; charset=UTF-8",
    }
    data = {
        "t":       str(div),
        "pageno":  "1",
        "s":       server_num,
        "c":       "0",
        "search":  search_name,
    }

    try:
        resp = sess.post(api_url, headers=headers, data=data, timeout=30)
        resp.raise_for_status()
        return driver_instance, resp.text
    except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
        if (isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 504 and retries > 0) or \
           (isinstance(e, requests.exceptions.RequestException) and not isinstance(e, requests.exceptions.HTTPError) and retries > 0): # Non-HTTPError RequestExceptions are also retried
            logger.warning(f"오류 발생 ({type(e).__name__}). 드라이버 교체 후 재시도... (남은 횟수: {retries-1}), URL: {api_url}, Data: {data}")
            new_driver_instance = driver_pool_ref.replace_driver_in_pool(driver_instance)
            time.sleep(5)
            return fetch_rank_page(driver_pool_ref, new_driver_instance, server_num, search_name, div, retries - 1)
        else:
            logger.error(f"Fetch_rank_page 최종 실패 또는 재시도 불가 오류: {e}", exc_info=True)
            try:
                driver_instance.quit() # Quit the problematic driver before raising
            except Exception as eq:
                logger.error(f"문제가 발생한 드라이버 종료 중 오류: {eq}", exc_info=True)
            raise e # Re-raise the original requests exception

def fetch_rank_page_by_pageno(driver_pool_ref, driver_instance, server_num, page_num=1, div=1, retries=MAX_FETCH_RETRIES):
    """페이지 번호로 기존 드라이버를 사용하여 랭킹 데이터 가져오기, 오류 시 재시도 및 드라이버 교체"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    if not driver_instance.current_url.startswith(list_url):
        try:
            driver_instance.get(list_url)
            time.sleep(2)
        except Exception as e:
            logger.warning(f"Driver.get failed for {list_url} (pageno): {e}. Attempting driver replacement.")
            if retries > 0:
                new_driver_instance = driver_pool_ref.replace_driver_in_pool(driver_instance)
                time.sleep(5)
                return fetch_rank_page_by_pageno(driver_pool_ref, new_driver_instance, server_num, page_num, div, retries - 1)
            else:
                try:
                    driver_instance.quit()
                except: pass
                raise e

    sess = requests.Session()
    try:
        for ck in driver_instance.get_cookies():
            sess.cookies.set(ck['name'], ck['value'])
    except Exception as e:
        logger.warning(f"Failed to get cookies (pageno): {e}. Attempting driver replacement.")
        if retries > 0:
            new_driver_instance = driver_pool_ref.replace_driver_in_pool(driver_instance)
            time.sleep(5)
            return fetch_rank_page_by_pageno(driver_pool_ref, new_driver_instance, server_num, page_num, div, retries - 1)
        else:
            try:
                driver_instance.quit()
            except: pass
            raise e
            
    headers = {
        "User-Agent":          driver_instance.execute_script("return navigator.userAgent;"),
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

    try:
        resp = sess.post(api_url, headers=headers, data=data, timeout=30)
        resp.raise_for_status()
        return driver_instance, resp.text
    except (requests.exceptions.HTTPError, requests.exceptions.RequestException) as e:
        if (isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 504 and retries > 0) or \
           (isinstance(e, requests.exceptions.RequestException) and not isinstance(e, requests.exceptions.HTTPError) and retries > 0): # Non-HTTPError RequestExceptions
            logger.warning(f"오류 발생 ({type(e).__name__}) (페이지 {page_num}). 드라이버 교체 후 재시도... (남은 횟수: {retries-1}), URL: {api_url}, Data: {data}")
            new_driver_instance = driver_pool_ref.replace_driver_in_pool(driver_instance)
            time.sleep(5)
            return fetch_rank_page_by_pageno(driver_pool_ref, new_driver_instance, server_num, page_num, div, retries - 1)
        else:
            logger.error(f"Fetch_rank_page_by_pageno 최종 실패 또는 재시도 불가 오류 (페이지 {page_num}): {e}", exc_info=True)
            try:
                driver_instance.quit() # Quit the problematic driver
            except Exception as eq:
                logger.error(f"문제가 발생한 드라이버 종료 중 오류 (페이지 {page_num}): {eq}", exc_info=True)
            raise e # Re-raise the original requests exception

def switch_server(server_name_or_num):
    """서버 이름 또는 번호를 서버 번호로 변환"""
    server_map = {
        "데이안": 1,
        "아이라": 2,
        "던컨": 3,
        "알리사": 4,
        "메이븐": 5,
        "라사": 6,
        "칼릭스": 7
    }

    # 이미 숫자인 경우 그대로 반환
    if isinstance(server_name_or_num, int) or (isinstance(server_name_or_num, str) and server_name_or_num.isdigit()):
        server_num = int(server_name_or_num)
        if 1 <= server_num <= 7:
            return server_num

    # 서버 이름인 경우 숫자로 변환
    return server_map.get(server_name_or_num, 1)  # 기본값 1 (데이안)

def get_server_name(server_num):
    """서버 번호를 서버 이름으로 변환"""
    server_names = {
        1: "데이안",
        2: "아이라",
        3: "던컨",
        4: "알리사",
        5: "메이븐",
        6: "라사",
        7: "칼릭스"
    }
    return server_names.get(server_num, "데이안")  # 기본값 "데이안"

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

def crawl_base_pages(driver_pool_ref, driver_instance, server_num, div=1):
    """기본 페이지(1-50)를 10분 최신성 확인과 함께 크롤링. 업데이트된 드라이버와 경계 캐릭터 반환"""
    logger.info(f"서버 {server_num} 기본 1-50 페이지 크롤링 시작")
    
    boundary_characters = []
    new_ranges_count = 0
    current_driver_instance = driver_instance 
    
    for page_num in range(1, 51):
        if shutdown_event.is_set(): break

        range_text = f"{(page_num-1)*20+1}위 ~ {page_num*20}위"
        
        if is_range_recent(server_num, range_text):
            # logger.info(f"서버 {server_num}, 페이지 {page_num} (범위: {range_text}) 최근 10분 내 수집됨, 스킵")
            continue
            
        try:
            current_driver_instance, html = fetch_rank_page_by_pageno(driver_pool_ref, current_driver_instance, server_num, page_num, div)
        except requests.exceptions.RequestException as e:
            logger.error(f"crawl_base_pages: fetch_rank_page_by_pageno 실패 (서버 {server_num}, 페이지 {page_num}): {e}", exc_info=True)
            # The driver was quit by fetch_rank_page_by_pageno. Get a new one.
            logger.warning(f"서버 {server_num}, 페이지 {page_num} 가져오기 실패. 새 드라이버로 교체합니다.")
            current_driver_instance = driver_pool_ref.replace_driver_in_pool(current_driver_instance) # Pass the (now quit) old driver instance
            time.sleep(5) 
            continue # Skip this page attempt

        parsed_data = parse_rank_html(html)
        
        if not parsed_data:
            # logger.warning(f"서버 {server_num}, 페이지 {page_num}에서 데이터를 찾을 수 없음")
            continue
        
        now_kst = datetime.now(KST)
        insert_data(parsed_data, server=None, character=None, div=div, retrieved_at_kst=now_kst)
        
        mark_range_crawled(server_num, range_text)
        new_ranges_count += 1
        
        if page_num == 50:
            boundary_characters = [item['character'] for item in parsed_data]
            # logger.info(f"경계 캐릭터들 (981-1000위): {boundary_characters}")
    
    last_base_refresh[server_num] = datetime.now(KST)
    
    # logger.info(f"서버 {server_num} 기본 페이지 크롤링 완료: {new_ranges_count}개 페이지 업데이트됨")
    return current_driver_instance, boundary_characters # 업데이트된 드라이버와 경계 캐릭터 반환

def initialize_range_queue(server_num, boundary_characters):
    """범위 탐색 큐 초기화 또는 갱신"""
    if server_num not in range_queue:
        range_queue[server_num] = deque()
    
    # 경계 캐릭터 추가
    for char in boundary_characters:
        range_queue[server_num].append(char)
    
    # 이전에 발견된 범위의 캐릭터 추가하여 지속적인 탐색
    current_discovered_ranges_snapshot = {}
    with discovered_ranges_lock:
        current_discovered_ranges_snapshot = dict(discovered_ranges.items())

    if server_num in current_discovered_ranges_snapshot:
        try:
            timestamp_for_srv = min([ts for ts in current_discovered_ranges_snapshot[server_num].values() if isinstance(ts, datetime)])
            oldest_server = None
            oldest_timestamp = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=KST)
            
            for srv, ranges_dict_val in current_discovered_ranges_snapshot.items():
                if ranges_dict_val:
                    valid_timestamps = [ts for ts in ranges_dict_val.values() if isinstance(ts, datetime)]
                    if not valid_timestamps:
                        continue
                    min_timestamp_for_srv = min(valid_timestamps)
                    if min_timestamp_for_srv < oldest_timestamp:
                        oldest_timestamp = min_timestamp_for_srv
                        oldest_server = srv
            
            effective_server_num = server_num
            if oldest_server and oldest_server != server_num:
                effective_server_num = oldest_server
            
            server_name = get_server_name(effective_server_num)
            logger.info(f"서버 {effective_server_num}, {server_name}에서 981등 이상 캐릭터 로드 중...")
            results = get_980_data(server_name)
            logger.info(f"서버 {effective_server_num}의 908등 이상 캐릭터 로드 완료 :: {len(results)}개 캐릭터 발견")
            
            chars_added = 0
            if effective_server_num not in range_queue:
                range_queue[effective_server_num] = deque()
                
            for row in results:
                char = row[0]
                if char not in range_queue[effective_server_num]:
                    range_queue[effective_server_num].append(char)
                    chars_added += 1
            logger.info(f"서버 {effective_server_num}: 908등 이상 {chars_added}개 캐릭터를 탐색 큐에 추가")
        except Exception as e:
            logger.error(f"이전 범위 캐릭터 로드 중 오류: {e}", exc_info=True)

def explore_ranges(driver_pool_ref, driver_instance, server_num, div=1):
    """시간 기반 결정으로 BFS를 사용하여 새 범위 탐색. 업데이트된 드라이버와 성공 여부 반환."""
    logger.info(f"서버 {server_num} 새 범위 탐색 시작")
    current_driver_instance = driver_instance
    # ... (try/except for initialize_range_queue) ...
    try:
        initialize_range_queue(server_num, boundary_characters=[])
    except Exception as e:
        logger.error(f"우선순위 조정 중 오류: {e}")

    if server_num not in range_queue or not range_queue[server_num]:
        logger.warning(f"서버 {server_num}에 탐색할 큐가 없음")
        return current_driver_instance, False # Return current (possibly unchanged) driver

    visited_ranges = set()
    visited_chars_this_run = set()
    new_ranges_found = 0
    
    while range_queue[server_num]:
        if shutdown_event.is_set(): break
        if should_refresh_base_pages(server_num):
            logger.info(f"서버 {server_num} 기본 페이지 갱신 시간 (20분 경과)")
            return current_driver_instance, True  # Return driver, signal base page refresh

        current_char = range_queue[server_num].popleft()
        if current_char in visited_chars_this_run:
            continue
        visited_chars_this_run.add(current_char)
        # logger.info(f"서버 {server_num}에서 '{current_char}' 검색 중... (큐 크기: {len(range_queue[server_num])})")
        
        try:
            current_driver_instance, html = fetch_rank_page(driver_pool_ref, current_driver_instance, server_num, current_char, div)
        except requests.exceptions.RequestException as e:
            logger.error(f"explore_ranges: fetch_rank_page 실패 (서버 {server_num}, 캐릭터 {current_char}): {e}", exc_info=True)
            # Driver was quit by fetch_rank_page. Get a new one.
            current_driver_instance = driver_pool_ref.replace_driver_in_pool(current_driver_instance)
            time.sleep(5)
            continue # Skip this character
        
        rank_range = parse_rank_range(html)
        if not rank_range:
            # logger.warning(f"'{current_char}' 검색 결과에서 범위를 찾을 수 없음")
            continue
            
        if is_range_recent(server_num, rank_range) or rank_range in visited_ranges:
            # logger.info(f"범위 {rank_range} 이미 처리됨/최근 업데이트됨, 스킵")
            continue
            
        visited_ranges.add(rank_range)
        # logger.info(f"서버 {server_num}에서 새 범위 처리: {rank_range}")
        
        parsed_data = parse_rank_html(html)
        if not parsed_data:
            # logger.warning(f"'{current_char}' 검색 결과에서 데이터를 찾을 수 없음")
            continue
        
        now_kst = datetime.now(KST)
        insert_data(parsed_data, server=None, character=None, div=div, retrieved_at_kst=now_kst)
        mark_range_crawled(server_num, rank_range)
        new_ranges_found += 1
        
        # logger.info(f"서버 {server_num}에서 {len(parsed_data)}개 항목 저장됨 (범위: {rank_range})")
        
        for item in parsed_data:
            char_name = item['character']
            if char_name not in visited_chars_this_run: # Add to queue only if not processed in this run
                range_queue[server_num].append(char_name)
    
    # logger.info(f"서버 {server_num} 범위 탐색 완료: {new_ranges_found}개 새 범위 발견")
    return current_driver_instance, new_ranges_found > 0 # Return updated driver and success status

def get_optimal_settings():
    """시스템 리소스를 확인하고 최적의 설정값을 결정"""
    cpu_count = multiprocessing.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)  # GB 단위로 변환

    # 기본값 설정
    settings = {
        'drivers_per_server': 1,  # 서버당 드라이버 수
        'max_concurrent_servers': cpu_count,  # 동시에 처리할 서버 수
        'batch_size': 50,  # 데이터베이스 일괄 처리 크기
        'refresh_threads': max(1, cpu_count // 4),  # 기본 페이지 갱신 스레드 수
        'exploration_threads_per_server': 1,  # 서버당 탐색 스레드 수
    }

    # 시스템 사양에 따라 설정 조정
    if memory_gb > 16:  # 메모리가 16GB 이상
        settings['drivers_per_server'] = 2
        settings['exploration_threads_per_server'] = 2

    if cpu_count > 8:  # 8코어 이상
        settings['refresh_threads'] = max(2, cpu_count // 4)

    logger.info(f"시스템 사양: CPU {cpu_count}코어, 메모리 {memory_gb:.1f}GB")
    logger.info(f"최적화 설정: {settings}")
    return settings

class DriverPool:
    """WebDriver 인스턴스 풀을 관리하는 클래스"""
    def __init__(self, pool_size=5, high_performance=True):
        self.pool = deque() # deque로 변경하여 popleft() 사용
        self.lock = threading.Lock()
        self.high_performance = high_performance
        self.pool_size = pool_size

        # 풀 초기화
        for _ in range(pool_size):
            try:
                driver = get_driver(self.high_performance)
                self.pool.append(driver)
            except Exception as e:
                logger.error(f"드라이버 풀 초기화 중 오류: {e}", exc_info=True)

    def get_driver_instance(self):
        """사용 가능한 드라이버를 가져옴, 없으면 새로 생성 (풀 크기 제한 고려)"""
        with self.lock:
            if self.pool:
                return self.pool.popleft()
            return get_driver(self.high_performance) # 임시로 새 드라이버 생성 또는 예외 발생
    
    def return_driver_instance(self, driver):
        """사용 완료된 드라이버를 풀에 반환"""
        if driver is None:
            return
        with self.lock:
            if len(self.pool) < self.pool_size:
                self.pool.append(driver)
            else:
                try:
                    driver.quit() # 풀이 가득 찼으면 초과분은 종료
                except Exception:
                    pass # 종료 중 오류는 무시

    def replace_driver_in_pool(self, old_driver_to_replace):
        """특정 드라이버를 풀에서 제거(종료)하고 새 드라이버로 교체 후 반환"""
        # old_driver_to_replace might have already been quit by fetch_rank_page
        # quit() is generally idempotent or handles errors gracefully.
        if old_driver_to_replace:
            try:
                old_driver_to_replace.quit()
            except Exception as e:
                logger.warning(f"드라이버 풀: 이전 드라이버 종료 중 오류 (replace_driver_in_pool): {e}", exc_info=True)
        
        # Create and return a new driver
        new_driver = get_driver(self.high_performance) 
        return new_driver

    def close_all(self):
        """모든 드라이버 종료"""
        with self.lock:
            while self.pool:
                driver = self.pool.popleft()
                try:
                    driver.quit()
                except:
                    pass

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
                    now_kst = datetime.now(KST)
                    insert_data(self.batch, server=None, character=None, div=self.div, retrieved_at_kst=now_kst)
                    # logger.info(f"{len(self.batch)}개 항목 일괄 저장 완료")
                except Exception as e:
                    logger.error(f"배치 데이터 저장 중 오류: {e}")
                self.batch = []
                gc.collect()

def optimized_base_pages_refresh_worker():
    """순차적으로 모든 서버의 1-50 페이지를 갱신하는 워커"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "초기화 중", "베이스 페이지 워커")
    logger.info("베이스 페이지 갱신 워커 시작")
    
    from service.db import SessionLocal
    settings = get_optimal_settings()
    driver_pool = DriverPool(pool_size=1, high_performance=True) # Pool of 1 for this sequential worker
    driver_instance = driver_pool.get_driver_instance()
    
    refresh_interval = timedelta(minutes=20)
    last_refresh = {i: datetime.now(KST) - refresh_interval for i in range(1, 8)}
    current_server = 1
    
    try:
        while not shutdown_event.is_set():
            if datetime.now(KST) - last_refresh[current_server] > refresh_interval:
                update_thread_status(thread_name, "페이지 갱신 중", f"서버 {current_server} 갱신 시작")
                try:
                    div = 1
                    # crawl_base_pages now returns the (potentially new) driver instance
                    driver_instance, boundary_chars = crawl_base_pages(driver_pool, driver_instance, current_server, div)
                    
                    if boundary_chars:
                        initialize_range_queue(current_server, boundary_chars)
                        # logger.info(f"서버 {current_server} 경계 캐릭터 {len(boundary_chars)}개 발견")
                    
                    last_refresh[current_server] = datetime.now(KST)
                except Exception as e:
                    logger.error(f"서버 {current_server} 기본 페이지 갱신 중 오류: {e}", exc_info=True)
                    # If crawl_base_pages fails catastrophically, driver_instance might be None or invalid.
                    # Try to get a new driver for the next cycle.
                    if driver_instance:
                         driver_instance = driver_pool.replace_driver_in_pool(driver_instance)
                    else: # if driver_instance became None somehow
                         driver_instance = driver_pool.get_driver_instance()

                update_thread_status(thread_name, "완료", f"서버 {current_server} 갱신 완료")
            else:
                update_thread_status(thread_name, "대기 중", f"서버 {current_server} 갱신 필요 없음")
            
            current_server = current_server % 7 + 1
            gc.collect()
            
            if current_server == 1:
                for _ in range(6): 
                    if shutdown_event.is_set(): break
                    time.sleep(5)
            else:
                if shutdown_event.is_set(): break
                time.sleep(1)
                
    except Exception as e:
        update_thread_status(thread_name, "오류 발생", str(e))
        logger.error(f"베이스 페이지 갱신 워커 오류: {e}", exc_info=True)
    finally:
        update_thread_status(thread_name, "종료됨")
        # DriverPool's close_all will handle quitting any drivers it still holds.
        # If driver_instance was replaced and the old one not passed to replace_driver_in_pool,
        # it might be orphaned if not quit by fetch_...
        # The current logic ensures fetch_... or replace_driver_in_pool quits old drivers.
        driver_pool.close_all()

def optimized_range_exploration_worker(server_num):
    """성능 최적화된 범위 탐색 워커"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "초기화 중", f"서버 {server_num} 최적화 탐색")
    logger.info(f"서버 {server_num} 최적화된 범위 탐색 워커 시작")
    
    from service.db import SessionLocal
    settings = get_optimal_settings()
    drivers_per_server = settings['drivers_per_server']
    driver_pool = DriverPool(pool_size=drivers_per_server, high_performance=True)
    
    # processed_chars는 이 워커 스레드 내에서 탐색 시도한 캐릭터들을 추적합니다.
    # 워커가 재시작되거나 새로 시작될 때마다 초기화됩니다.
    processed_chars = set() 
    data_collector = DataCollector(batch_size=100)
    
    if server_num not in range_queue:
        range_queue[server_num] = deque()
    
    stats = {
        'ranges_found': 0,
        'characters_processed': 0,
        'data_items_saved': 0, # Corrected key
        'new_chars_added_to_queue': 0
    }
    
    try:
        div = 1 

        while not shutdown_event.is_set():
            try:
                if not range_queue[server_num]:
                    update_thread_status(thread_name, "큐 로드 중", f"서버 {server_num} 큐 비어있음")
                    logger.info(f"서버 {server_num}의 범위 큐가 비어있음, 908등 이상 캐릭터 불러오기 시도")
                    try:
                        server_name = get_server_name(server_num)
                        logger.info(f"서버 {server_num}({server_name})의 캐릭터 로드 시작")
                        logger.info(f"서버 {server_num}, {server_name}에서 981등 이상 캐릭터 로드 중...")
                        results = get_980_data(server_name)
                        logger.info(f"서버 {server_num}의 908등 이상 캐릭터 로드 완료 :: {len(results)}개 캐릭터 발견")
                        
                        chars_added = 0
                        if results:
                            for row in results:
                                char = row[0]
                                if char not in processed_chars: # processed_chars_local_set 대신 processed_chars 사용
                                    range_queue[server_num].append(char)
                                    # 큐에 추가할 때 processed_chars에도 추가하여 중복 처리 방지
                                    processed_chars.add(char) 
                                    chars_added += 1
                            logger.info(f"서버 {server_num}: 이전에 발견된 {chars_added}개 캐릭터를 큐에 추가")
                        else:
                            time.sleep(30)
                            continue
                    except Exception as e:
                        logger.error(f"서버 {server_num} 캐릭터 로드 중 오류: {e}", exc_info=True)
                        time.sleep(30)
                        continue

                with ThreadPoolExecutor(max_workers=drivers_per_server) as executor:
                    char_batch = []
                    temp_batch_source = []
                    with discovered_ranges_lock:
                        while range_queue[server_num] and len(temp_batch_source) < (drivers_per_server * 5) :
                            char_candidate = range_queue[server_num].popleft()
                            if char_candidate not in processed_chars:
                                temp_batch_source.append(char_candidate)
                            else:
                                pass

                    for char_in_batch in temp_batch_source:
                        char_batch.append(char_in_batch)
                        processed_chars.add(char_in_batch)

                    if not char_batch:
                        update_thread_status(thread_name, "대기 중", f"서버 {server_num} 처리할 캐릭터 없음")
                        time.sleep(10)
                        continue
                    
                    update_thread_status(thread_name, "캐릭터 처리 중", 
                                f"서버 {server_num}, {len(char_batch)}개 캐릭터 처리 예정")
                    stats['characters_processed'] += len(char_batch)
                    
                    def process_character(char_to_process):
                        current_driver = driver_pool.get_driver_instance()
                        driver_ok = True
                        returned_new_chars = []

                        try:
                            current_driver, html = fetch_rank_page(driver_pool, current_driver, server_num, char_to_process, div)
                            
                            rank_range = parse_rank_range(html)
                            if not rank_range:
                                return returned_new_chars 
                            
                            if is_range_recent(server_num, rank_range):
                                pass

                            parsed_data = parse_rank_html(html)
                            if not parsed_data:
                                return returned_new_chars

                            should_save_this_data = False
                            with discovered_ranges_lock:
                                if not is_range_recent(server_num, rank_range):
                                    mark_range_crawled(server_num, rank_range)
                                    should_save_this_data = True
                            
                            if should_save_this_data:
                                data_collector.add_data(parsed_data)
                                stats['data_items_saved'] += len(parsed_data)
                            
                            stats['ranges_found'] += 1

                            for item in parsed_data:
                                new_char_candidate = item['character']
                                returned_new_chars.append(new_char_candidate)
                            
                            return returned_new_chars

                        except requests.exceptions.RequestException as e:
                            logger.error(f"process_character RequestsException (캐릭터: {char_to_process}): {e}", exc_info=False)
                            driver_ok = False 
                            return returned_new_chars 
                        except Exception as e:
                            logger.error(f"process_character 처리 중 예외 (캐릭터: {char_to_process}): {e}", exc_info=True)
                            driver_ok = False
                            return returned_new_chars
                        finally:
                            if current_driver:
                                if driver_ok:
                                    driver_pool.return_driver_instance(current_driver)
                                else:
                                    pass
                    
                    futures = [executor.submit(process_character, char) for char in char_batch]
                    for future in futures:
                        new_chars_from_task = future.result()
                        if new_chars_from_task:
                            for new_char_for_q in new_chars_from_task:
                                if new_char_for_q not in processed_chars:
                                    range_queue[server_num].append(new_char_for_q)
                                    processed_chars.add(new_char_for_q)
                                    stats['new_chars_added_to_queue'] +=1
                
                data_collector.flush()
                if len(processed_chars) > 10000:
                    processed_chars.clear() # processed_chars = set() 대신 .clear() 사용
                    gc.collect()
                    
                if shutdown_event.is_set(): break
                if not range_queue[server_num]:
                    update_thread_status(thread_name, "대기 중", f"서버 {server_num} 큐 비어 있음")
                    for _ in range(6):
                        if shutdown_event.is_set(): break
                        time.sleep(5)
                else:
                    time.sleep(1)
            except Exception as e:
                update_thread_status(thread_name, "오류 발생", str(e))
                logger.error(f"서버 {server_num} 범위 탐색 중 오류: {e}", exc_info=True)
                time.sleep(30)
    finally:
        update_thread_status(thread_name, "종료됨", 
                    f"서버 {server_num} 워커 종료. 총 {stats['ranges_found']}개 범위, {stats['characters_processed']}개 캐릭터 처리됨")
        driver_pool.close_all()
        data_collector.flush()
        logger.info(f"서버 {server_num} 범위 탐색 워커 종료됨")

def thread_monitor():
    """모든 스레드의 상태를 주기적으로 로깅하고 종료된 스레드를 재시작하는 모니터링 스레드"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "모니터링 시작")
    
    last_restart_time = {}
    
    try:
        while not shutdown_event.is_set():
            alive_threads = [t for t in active_threads if t.is_alive()]
            dead_threads = [t for t in active_threads if not t.is_alive()]
            
            if dead_threads:
                dead_thread_names = [t.name for t in dead_threads]
                update_thread_status(thread_name, "스레드 종료 감지", 
                            f"{len(dead_threads)}개 스레드 종료됨: {dead_thread_names}")
                
                for dead_thread in dead_threads:
                    if (dead_thread.name.startswith("server-") and "explorer" in dead_thread.name):
                        try:
                            server_num = int(dead_thread.name.split('-')[1])
                            current_time = datetime.now(KST)
                            if dead_thread.name in last_restart_time:
                                if (current_time - last_restart_time[dead_thread.name]).total_seconds() < 60:
                                    logger.info(f"스레드 {dead_thread.name} 최근에 재시작되었습니다. 잠시 대기...")
                                    continue
                            
                            logger.info(f"스레드 {dead_thread.name} 재시작 중...")
                            new_thread = threading.Thread(
                                target=optimized_range_exploration_worker,
                                args=(server_num,),
                                name=f"server-{server_num}-explorer",
                                daemon=True
                            )
                            
                            with thread_status_lock:
                                active_threads.remove(dead_thread)
                                active_threads.append(new_thread)
                            
                            new_thread.start()
                            last_restart_time[new_thread.name] = current_time
                            update_thread_status(thread_name, "스레드 재시작됨", 
                                        f"서버 {server_num} 탐색 스레드가 재시작되었습니다.")
                        except Exception as e:
                            logger.error(f"스레드 재시작 중 오류: {e}", exc_info=True)
            
            now_kst_monitor = datetime.now(KST)
            if now_kst_monitor.minute % 1 == 0 and now_kst_monitor.second < 10:
                log_all_thread_status()
            
            for _ in range(10):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
                
    except Exception as e:
        update_thread_status(thread_name, "모니터링 오류", str(e))
    finally:
        update_thread_status(thread_name, "모니터링 종료")

def start_optimized_continuous_crawling():
    """시스템 자원을 최대한 활용하는, 최적화된 연속 크롤링 시작"""
    global active_threads, thread_status
    
    thread_status = {}
    
    shutdown_event.clear()
    active_threads = []
    
    load_discovered_ranges()
    settings = get_optimal_settings()
    
    gc.enable()
    gc.set_threshold(100, 5, 5)
    
    base_thread = threading.Thread(
        target=optimized_base_pages_refresh_worker,
        name="base-refresh-worker",
        daemon=True
    )
    active_threads.append(base_thread)
    
    exploration_threads = []
    for server_num in range(1, 8):
        thread = threading.Thread(
            target=optimized_range_exploration_worker,
            args=(server_num,),
            name=f"server-{server_num}-explorer",
            daemon=True
        )
        exploration_threads.append(thread)
        active_threads.append(thread)
    
    monitor_thread = threading.Thread(
        target=thread_monitor,
        name="thread-monitor",
        daemon=True
    )
    active_threads.append(monitor_thread)
    
    base_thread.start()
    update_thread_status("main", "베이스 갱신 스레드 시작됨")
    
    for thread in exploration_threads:
        thread.start()
        update_thread_status("main", f"탐색 스레드 시작됨: {thread.name}")
    
    monitor_thread.start()
    update_thread_status("main", "모니터링 스레드 시작됨")
    
    try:
        while not shutdown_event.is_set():
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            update_thread_status("main", "시스템 모니터링", 
                      f"CPU {cpu_percent}%, 메모리 {memory_percent}%")
            
            save_discovered_ranges()
            
            shutdown_event.wait(10)
            
    except KeyboardInterrupt:
        update_thread_status("main", "사용자에 의한 종료 요청")
        shutdown_all()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--stop":
            shutdown_event.set()
            logger.info("종료 명령 실행 중...")
            sys.exit(0)
            
        logger.info("최적화된 연속 랭킹 크롤링 시작")
        start_optimized_continuous_crawling()
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 크롤링 중단됨")
        shutdown_all()
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {str(e)}", exc_info=True)
        shutdown_all()
