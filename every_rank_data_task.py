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

logger = logging.getLogger(__name__)

# Track global discovered ranges to avoid duplicate work across runs
all_discovered_ranges = {}  # server_num -> set of ranges

# Global state tracking
discovered_ranges = {}  # server_num -> {range_text: timestamp}
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
        timestamp = datetime.now().strftime("%H:%M:%S")
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
    try:
        if os.path.exists(RANGES_FILE):
            with open(RANGES_FILE, 'r', encoding='utf-8') as f:
                # 문자열 타임스탬프를 datetime 객체로 변환
                data = json.load(f)
                for server, ranges in data.items():
                    server_num = int(server)  # JSON 키는 문자열
                    discovered_ranges[server_num] = {}
                    for range_text, timestamp_str in ranges.items():
                        discovered_ranges[server_num][range_text] = datetime.fromisoformat(timestamp_str)
                logger.info(f"파일에서 {sum(len(ranges) for ranges in discovered_ranges.values())}개 범위 로드 완료")
    except Exception as e:
        logger.error(f"범위 로드 중 오류: {e}")
        discovered_ranges = {i: {} for i in range(1, 8)}

def save_discovered_ranges():
    """발견된 범위를 파일에 저장"""
    try:
        # datetime 객체를 ISO 포맷 문자열로 변환
        data_to_save = {}
        for server, ranges in discovered_ranges.items():
            data_to_save[str(server)] = {k: v.isoformat() for k, v in ranges.items()}
            
        with open(RANGES_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=2)
        # logger.info(f"{sum(len(ranges) for ranges in discovered_ranges.values())}개 범위 파일 저장 완료")
    except Exception as e:
        logger.error(f"범위 저장 중 오류: {e}")

def is_range_recent(server_num, range_text):
    """범위가 최근(10분 이내)에 수집되었는지 확인"""
    if server_num not in discovered_ranges:
        return False
        
    if range_text not in discovered_ranges[server_num]:
        return False
        
    last_crawled = discovered_ranges[server_num][range_text]
    return (datetime.now() - last_crawled) < timedelta(minutes=10)

def mark_range_crawled(server_num, range_text):
    """범위를 현재 타임스탬프와 함께 수집됨으로 표시"""
    if server_num not in discovered_ranges:
        discovered_ranges[server_num] = {}
        
    discovered_ranges[server_num][range_text] = datetime.now()
    
    # 가끔씩 파일에 저장 (I/O 부하 감소)
    if len(discovered_ranges[server_num]) % 10 == 0:
        save_discovered_ranges()

def should_refresh_base_pages(server_num):
    """기본 1-50 페이지를 갱신할 시간인지 확인 (~20분 후)"""
    if server_num not in last_base_refresh:
        return True
        
    return (datetime.now() - last_base_refresh[server_num]) > timedelta(minutes=20)

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

def fetch_rank_page(driver, server_num, search_name="", div=1):
    """기존 드라이버를 사용하여 랭킹 데이터 가져오기"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    # 이미 리스트 페이지에 있지 않은 경우에만 페이지 이동
    if not driver.current_url.startswith(list_url):
        driver.get(list_url)
        time.sleep(2)

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
        "t":       str(div),  # div 파라미터 추가
        "pageno":  "1",
        "s":       server_num,
        "c":       "0",
        "search":  search_name,
    }

    resp = sess.post(api_url, headers=headers, data=data)
    resp.raise_for_status()
    return resp.text

def fetch_rank_page_by_pageno(driver, server_num, page_num=1, div=1):
    """페이지 번호로 기존 드라이버를 사용하여 랭킹 데이터 가져오기"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    # 이미 리스트 페이지에 있지 않은 경우에만 페이지 이동
    if not driver.current_url.startswith(list_url):
        driver.get(list_url)
        time.sleep(2)

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
    resp.raise_for_status()
    return resp.text

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

def crawl_base_pages(driver, server_num, div=1):
    """기본 페이지(1-50)를 10분 최신성 확인과 함께 크롤링"""
    logger.info(f"서버 {server_num} 기본 1-50 페이지 크롤링 시작")
    
    boundary_characters = []
    new_ranges_count = 0
    
    for page_num in range(1, 51):
        # 이 페이지/범위가 최근(10분 이내)인지 확인
        range_text = f"{(page_num-1)*20+1}위 ~ {page_num*20}위"
        
        if is_range_recent(server_num, range_text):
            logger.info(f"서버 {server_num}, 페이지 {page_num} (범위: {range_text}) 최근 10분 내 수집됨, 스킵")
            continue
            
        # logger.info(f"서버 {server_num}, 페이지 {page_num} 크롤링 중 (범위: {range_text})")
        
        # 페이지 가져오기 및 처리
        html = fetch_rank_page_by_pageno(driver, server_num, page_num, div)
        parsed_data = parse_rank_html(html)
        
        if not parsed_data:
            logger.warning(f"서버 {server_num}, 페이지 {page_num}에서 데이터를 찾을 수 없음")
            continue
        
        # 데이터 저장
        insert_data(parsed_data, server=None, character=None, div=div)
        # logger.info(f"서버 {server_num}, 페이지 {page_num}에서 {len(parsed_data)}개 항목 저장됨")
        
        # 수집됨으로 표시
        mark_range_crawled(server_num, range_text)
        new_ranges_count += 1
        
        # 마지막 페이지 캐릭터들 경계 탐색용으로 저장
        if page_num == 50:
            boundary_characters = [item['character'] for item in parsed_data]
            logger.info(f"경계 캐릭터들 (981-1000위): {boundary_characters}")
    
    # 마지막 갱신 시간 업데이트
    last_base_refresh[server_num] = datetime.now()
    
    logger.info(f"서버 {server_num} 기본 페이지 크롤링 완료: {new_ranges_count}개 페이지 업데이트됨")
    return boundary_characters

# initialize_range_queue 함수 수정 - 908등 이상 캐릭터 사용
def initialize_range_queue(server_num, boundary_characters):
    """범위 탐색 큐 초기화 또는 갱신"""
    if server_num not in range_queue:
        range_queue[server_num] = deque()
    
    # 경계 캐릭터 추가
    for char in boundary_characters:
        range_queue[server_num].append(char)
        
    # 이전에 발견된 범위의 캐릭터 추가하여 지속적인 탐색
    if server_num in discovered_ranges:
        try:
            # 모든 서버의 데이터 비교를 위해 타임스탬프 확인
            oldest_server = None
            oldest_timestamp = datetime.max
            
            for srv, ranges in discovered_ranges.items():
                if ranges:
                    min_timestamp = min(ranges.values())
                    if min_timestamp < oldest_timestamp:
                        oldest_timestamp = min_timestamp
                        oldest_server = srv
            
            # 우선순위가 가장 오래된 서버로 변경
            if oldest_server and oldest_server != server_num:
                logger.info(f"서버 {server_num} 대신 서버 {oldest_server}가 우선순위로 처리됨 (가장 오래된 데이터)")
                server_num = oldest_server
            
            # 서버 이름으로 변환
            server_name = get_server_name(server_num)
            
            # 908등 이상의 캐릭터를 가져옴 (981등 ~ 1000등)
            logger.info(f"서버 {server_num}, {server_name}에서 981등 이상 캐릭터 로드 중...")
            results = get_980_data(server_name)

            # 탐색을 위해 이 캐릭터들을 큐에 추가
            chars_added = 0
            for row in results:
                char = row[0]
                if char not in range_queue[server_num]:
                    range_queue[server_num].append(char)
                    chars_added += 1
                    
            logger.info(f"서버 {server_num}: 908등 이상 {chars_added}개 캐릭터를 탐색 큐에 추가")
        except Exception as e:
            logger.error(f"이전 범위 캐릭터 로드 중 오류: {e}")

def explore_ranges(driver, server_num, div=1):
    """시간 기반 결정으로 BFS를 사용하여 새 범위 탐색"""
    logger.info(f"서버 {server_num} 새 범위 탐색 시작")
    
    # 지속적으로 우선순위 조정
    try:
        initialize_range_queue(server_num, boundary_characters=[])
    except Exception as e:
        logger.error(f"우선순위 조정 중 오류: {e}")
    
    if server_num not in range_queue or not range_queue[server_num]:
        logger.warning(f"서버 {server_num}에 탐색할 큐가 없음")
        return False
    
    visited_ranges = set()
    visited_chars_this_run = set()
    new_ranges_found = 0
    
    # 큐가 비거나 기본 페이지를 갱신해야 할 때까지 탐색
    while range_queue[server_num]:
        # 기본 페이지 갱신 시간인지 확인
        if should_refresh_base_pages(server_num):
            logger.info(f"서버 {server_num} 기본 페이지 갱신 시간 (20분 경과)")
            return True  # 기본 페이지 갱신 신호
            
        # 탐색할 다음 캐릭터 가져오기
        current_char = range_queue[server_num].popleft()
        
        # 이미 이번 실행에서 탐색한 경우 건너뛰기
        if current_char in visited_chars_this_run:
            continue
            
        visited_chars_this_run.add(current_char)
        logger.info(f"서버 {server_num}에서 '{current_char}' 검색 중... (큐 크기: {len(range_queue[server_num])})")
        
        # 재사용 드라이버로 검색
        html = fetch_rank_page(driver, server_num, current_char, div)
        
        # 범위 정보 가져오기
        rank_range = parse_rank_range(html)
        if not rank_range:
            logger.warning(f"'{current_char}' 검색 결과에서 범위를 찾을 수 없음")
            continue
            
        # 범위가 최근이거나 이미 이번 실행에서 처리되었는지 확인
        if is_range_recent(server_num, rank_range) or rank_range in visited_ranges:
            # logger.info(f"범위 {rank_range} 이미 처리됨/최근 업데이트됨, 스킵")
            continue
            
        # 처리할 범위 발견
        visited_ranges.add(rank_range)
        logger.info(f"서버 {server_num}에서 새 범위 처리: {rank_range}")
        
        # 데이터 파싱
        parsed_data = parse_rank_html(html)
        if not parsed_data:
            logger.warning(f"'{current_char}' 검색 결과에서 데이터를 찾을 수 없음")
            continue
        
        # 데이터 저장 및 범위 수집됨으로 표시
        insert_data(parsed_data, server=None, character=None, div=div)
        mark_range_crawled(server_num, rank_range)
        new_ranges_found += 1
        
        logger.info(f"서버 {server_num}에서 {len(parsed_data)}개 항목 저장됨 (범위: {rank_range})")
        
        # 탐색 큐에 캐릭터 추가
        for item in parsed_data:
            char_name = item['character']
            if char_name not in visited_chars_this_run:
                range_queue[server_num].append(char_name)
    
    logger.info(f"서버 {server_num} 범위 탐색 완료: {new_ranges_found}개 새 범위 발견")
    return new_ranges_found > 0

# 시스템 리소스 확인 및 최적 설정 결정
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
                    insert_data(self.batch, server=None, character=None, div=self.div)
                    # logger.info(f"{len(self.batch)}개 항목 일괄 저장 완료")
                except Exception as e:
                    logger.error(f"배치 데이터 저장 중 오류: {e}")
                self.batch = []
                # 명시적 가비지 컬렉션으로 메모리 최적화
                gc.collect()

# 최적화된 베이스 페이지 갱신 워커 수정 - 순차적으로 서버 처리
def optimized_base_pages_refresh_worker():
    """순차적으로 모든 서버의 1-50 페이지를 갱신하는 워커"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "초기화 중", "베이스 페이지 워커")
    
    logger.info("베이스 페이지 갱신 워커 시작")
    
    # 세션 로컬 임포트
    from service.db import SessionLocal
    
    # 시스템 설정 가져오기
    settings = get_optimal_settings()
    
    # 단일 드라이버 생성 (모든 서버 작업에 재사용)
    driver = get_driver(high_performance=True)
    
    # 모든 서버에 대한 마지막 갱신 시간 추적
    refresh_interval = timedelta(minutes=20)
    last_refresh = {i: datetime.now() - refresh_interval for i in range(1, 8)}
    
    # 현재 처리 중인 서버 (1-7 사이에서 순환)
    current_server = 1
    
    try:
        while not shutdown_event.is_set():
            # 현재 서버가 갱신 필요한지 확인
            if datetime.now() - last_refresh[current_server] > refresh_interval:
                update_thread_status(thread_name, "페이지 갱신 중", f"서버 {current_server} 갱신 시작")
                # logger.info(f"서버 {current_server} 기본 페이지 갱신 시작")
                
                try:
                    # 1-50 페이지 크롤링
                    div = 1  # 기본 구분 값 (t parameter)
                    boundary_chars = crawl_base_pages(driver, current_server, div)
                    
                    # 범위 큐 초기화 (경계 캐릭터 추가)
                    if boundary_chars:
                        initialize_range_queue(current_server, boundary_chars)
                        logger.info(f"서버 {current_server} 경계 캐릭터 {len(boundary_chars)}개 발견")
                    
                    # 마지막 갱신 시간 업데이트
                    last_refresh[current_server] = datetime.now()
                except Exception as e:
                    logger.error(f"서버 {current_server} 기본 페이지 갱신 중 오류: {e}")
                
                update_thread_status(thread_name, "완료", f"서버 {current_server} 갱신 완료")
            else:
                update_thread_status(thread_name, "대기 중", f"서버 {current_server} 갱신 필요 없음")
                # logger.info(f"서버 {current_server} 갱신 필요 없음, 최근 갱신: {last_refresh[current_server]}")
            
            # 다음 서버로 이동 (1-7 사이 순환)
            current_server = current_server % 7 + 1
            
            # 주기적인 메모리 관리
            gc.collect()
            
            # 모든 서버를 한 번씩 확인한 후에만 대기 시간 추가 (7번째 서버 확인 후)
            if current_server == 1:
                # 작업 사이 잠시 대기 (종료 이벤트 확인 가능하도록 짧게 나눔)
                for _ in range(6):  # 30초를 5초씩 나누어 종료 이벤트 확인
                    if shutdown_event.is_set():
                        break
                    time.sleep(5)
            else:
                # 서버 변경 간 짧은 대기
                time.sleep(1)
                
    except Exception as e:
        update_thread_status(thread_name, "오류 발생", str(e))
        logger.error(f"베이스 페이지 갱신 워커 오류: {e}", exc_info=True)
    finally:
        update_thread_status(thread_name, "종료됨")
        if driver:
            driver.quit()

# 최적화된 범위 탐색 워커 수정 - 908등 캐릭터 사용
def optimized_range_exploration_worker(server_num):
    """성능 최적화된 범위 탐색 워커"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "초기화 중", f"서버 {server_num} 최적화 탐색")
    
    # logger.info(f"서버 {server_num} 최적화된 범위 탐색 워커 시작")
    
    # 세션 로컬 임포트
    from service.db import SessionLocal
    
    # 시스템 설정 가져오기
    settings = get_optimal_settings()
    drivers_per_server = settings['drivers_per_server']
    
    # 드라이버 풀 생성
    driver_pool = DriverPool(pool_size=drivers_per_server, high_performance=True)
    
    # 처리 결과 캐시 및 데이터 수집기
    processed_chars = set()
    data_collector = DataCollector(batch_size=100)
    
    # 서버에 대한 큐 초기화
    if server_num not in range_queue:
        range_queue[server_num] = deque()
    
    # 주기적으로 통계 업데이트를 위한 카운터
    stats = {
        'ranges_found': 0,
        'characters_processed': 0,
        'data_items_saved': 0
    }
    
    try:
        div = 1  # 기본 구분 값 (t 파라미터)
        
        while not shutdown_event.is_set():
            try:
                # 큐가 비어있으면 재로드
                if not range_queue[server_num]:
                    update_thread_status(thread_name, "큐 로드 중", f"서버 {server_num} 큐 비어있음")
                    # logger.info(f"서버 {server_num}의 범위 큐가 비어있음, 908등 이상 캐릭터 불러오기 시도")
                    try:
                        # 서버 이름으로 변환
                        server_name = get_server_name(server_num)
                        logger.info(f"서버 {server_num}({server_name})의 캐릭터 로드 시작")

                        # 980+ 순위 캐릭터 로드
                        logger.info(f"서버 {server_num}, {server_name}에서 981등 이상 캐릭터 로드 중...")
                        results = get_980_data(server_name)
                        logger.info(f"서버 {server_num}의 908등 이상 캐릭터 로드 완료 :: {len(results)}개 캐릭터 발견")
                        
                        chars_added = 0
                        if results:
                            for row in results:
                                char = row[0]
                                if char not in processed_chars:
                                    range_queue[server_num].append(char)
                                    chars_added += 1
                            
                            logger.info(f"서버 {server_num}: 이전에 발견된 {chars_added}개 캐릭터를 큐에 추가")
                        else:
                            # 캐릭터가 없는 경우 잠시 대기 - 기본 갱신 스레드에서 채워줄 것임
                            # logger.info(f"서버 {server_num}에 대한 1001+ 캐릭터 없음, 대기")
                            time.sleep(30)
                            continue
                    except Exception as e:
                        logger.error(f"서버 {server_num} 캐릭터 로드 중 오류: {e}")
                        time.sleep(30)
                        continue
                
                # 병렬 처리로 캐릭터 탐색
                with ThreadPoolExecutor(max_workers=drivers_per_server) as executor:
                    # 처리할 캐릭터 배치 추출 (드라이버 수만큼)
                    char_batch = []
                    for _ in range(min(len(range_queue[server_num]), drivers_per_server * 5)):
                        if range_queue[server_num]:
                            char = range_queue[server_num].popleft()
                            if char not in processed_chars:
                                char_batch.append(char)
                                processed_chars.add(char)
                    
                    if not char_batch:
                        update_thread_status(thread_name, "대기 중", f"서버 {server_num} 처리할 캐릭터 없음")
                        # logger.info(f"서버 {server_num} 처리할 캐릭터 없음, 대기")
                        time.sleep(10)
                        continue
                    
                    update_thread_status(thread_name, "캐릭터 처리 중", 
                                f"서버 {server_num}, {len(char_batch)}개 캐릭터 처리 예정")
                    # logger.info(f"서버 {server_num}에서 {len(char_batch)}개 캐릭터 병렬 처리")
                    stats['characters_processed'] += len(char_batch)
                    
                    # 각 캐릭터에 대한 처리 함수
                    def process_character(char):
                        driver = driver_pool.get_driver()
                        try:
                            # 캐릭터로 검색
                            html = fetch_rank_page(driver, server_num, char, div)
                            
                            # 범위 확인
                            rank_range = parse_rank_range(html)
                            if not rank_range:
                                return None
                                
                            # 이미 처리한 범위면 스킵
                            if is_range_recent(server_num, rank_range):
                                return None
                                
                            # 데이터 파싱
                            parsed_data = parse_rank_html(html)
                            if not parsed_data:
                                return None
                                
                            # 결과 저장 및 범위 마킹
                            data_collector.add_data(parsed_data)
                            mark_range_crawled(server_num, rank_range)
                            stats['ranges_found'] += 1
                            
                            # 새로운 캐릭터 추출하여 반환 (큐에 추가하기 위해)
                            new_chars = []
                            for item in parsed_data:
                                new_char = item['character']
                                if new_char not in processed_chars:
                                    new_chars.append(new_char)
                                    
                            return new_chars
                        finally:
                            driver_pool.return_driver(driver)
                    
                    # 병렬 실행
                    futures = [executor.submit(process_character, char) for char in char_batch]
                    
                    # 결과 수집 및 새 캐릭터 큐에 추가
                    for future in futures:
                        new_chars = future.result()
                        if new_chars:
                            for new_char in new_chars:
                                if new_char not in processed_chars:
                                    range_queue[server_num].append(new_char)
                                    processed_chars.add(new_char)
                    
                    # 결과 업데이트
                    update_thread_status(thread_name, "처리 완료", 
                                f"서버 {server_num}, 총 {stats['characters_processed']}개 캐릭터 처리됨")
                
                # 남은 데이터 저장
                data_collector.flush()
                
                # 너무 많은 메모리를 사용하지 않도록 주기적으로 캐시 정리
                if len(processed_chars) > 10000:
                    processed_chars = set()
                    gc.collect()
                    
                # 종료 이벤트 확인
                if shutdown_event.is_set():
                    break
                    
                # 적절한 대기 시간
                if not range_queue[server_num]:
                    update_thread_status(thread_name, "대기 중", f"서버 {server_num} 큐 비어 있음")
                    # logger.info(f"서버 {server_num} 큐 비어 있음, 30초 대기")
                    
                    # 30초 대기하지만 종료 이벤트 확인을 위해 짧게 분할
                    for _ in range(6):
                        if shutdown_event.is_set():
                            break
                        time.sleep(5)
                else:
                    time.sleep(1)
            except Exception as e:
                update_thread_status(thread_name, "오류 발생", str(e))
                logger.error(f"서버 {server_num} 범위 탐색 중 오류: {e}", exc_info=True)
                # 오류 발생 시 그냥 종료하지 않고 재시도
                time.sleep(30)  # 더 긴 대기 시간으로 조정
    
    finally:
        update_thread_status(thread_name, "종료됨", 
                    f"서버 {server_num} 워커 종료. 총 {stats['ranges_found']}개 범위, {stats['characters_processed']}개 캐릭터 처리됨")
        # 스레드 종료 시 드라이버 정리
        driver_pool.close_all()
        data_collector.flush()
        logger.info(f"서버 {server_num} 범위 탐색 워커 종료됨")

# 스레드 모니터링 함수
def thread_monitor():
    """모든 스레드의 상태를 주기적으로 로깅하고 종료된 스레드를 재시작하는 모니터링 스레드"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "모니터링 시작")
    
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
                    # 서버 탐색 스레드만 재시작
                    if dead_thread.name.startswith("server-") and "explorer" in dead_thread.name:
                        try:
                            # 서버 번호 추출
                            server_num = int(dead_thread.name.split('-')[1])
                            
                            # 최소 1분 간격으로만 재시작 (너무 잦은 재시작 방지)
                            current_time = datetime.now()
                            if dead_thread.name in last_restart_time:
                                if (current_time - last_restart_time[dead_thread.name]).total_seconds() < 60:
                                    logger.info(f"스레드 {dead_thread.name} 최근에 재시작되었습니다. 잠시 대기...")
                                    continue
                            
                            # 새 스레드 생성 및 시작
                            logger.info(f"스레드 {dead_thread.name} 재시작 중...")
                            new_thread = threading.Thread(
                                target=optimized_range_exploration_worker,
                                args=(server_num,),
                                name=f"server-{server_num}-explorer",
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
                                        f"서버 {server_num} 탐색 스레드가 재시작되었습니다.")
                        except Exception as e:
                            logger.error(f"스레드 재시작 중 오류: {e}", exc_info=True)
            
            # 1분마다 전체 상태 요약 출력
            if datetime.now().minute % 1 == 0 and datetime.now().second < 10:
                log_all_thread_status()
            
            # 스레드 모니터링은 10초마다 수행
            for _ in range(10):
                if shutdown_event.is_set():
                    break
                time.sleep(1)
                
    except Exception as e:
        update_thread_status(thread_name, "모니터링 오류", str(e))
    finally:
        update_thread_status(thread_name, "모니터링 종료")

# 최적화된 연속 크롤링 시작 함수
def start_optimized_continuous_crawling():
    """시스템 자원을 최대한 활용하는, 최적화된 연속 크롤링 시작"""
    global active_threads, thread_status
    
    # 스레드 상태 초기화
    thread_status = {}
    
    # 시작 전 종료 이벤트 초기화
    shutdown_event.clear()
    active_threads = []
    
    # 설정 로드 및 초기화
    load_discovered_ranges()
    settings = get_optimal_settings()
    
    # 메모리 최적화를 위한 설정
    gc.enable()
    gc.set_threshold(100, 5, 5)  # 더 적극적인 가비지 컬렉션
    
    # 베이스 페이지 갱신 스레드 생성
    base_thread = threading.Thread(
        target=optimized_base_pages_refresh_worker,
        name="base-refresh-worker",
        daemon=True
    )
    active_threads.append(base_thread)
    
    # 범위 탐색 스레드 생성
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
    
    # 스레드 모니터링 스레드 생성
    monitor_thread = threading.Thread(
        target=thread_monitor,
        name="thread-monitor",
        daemon=True
    )
    active_threads.append(monitor_thread)
    
    # 모든 스레드 시작
    base_thread.start()
    update_thread_status("main", "베이스 갱신 스레드 시작됨")
    
    for thread in exploration_threads:
        thread.start()
        update_thread_status("main", f"탐색 스레드 시작됨: {thread.name}")
    
    monitor_thread.start()
    update_thread_status("main", "모니터링 스레드 시작됨")
    
    # 메인 스레드 유지
    try:
        while not shutdown_event.is_set():
            # 시스템 리소스 사용량 로깅
            cpu_percent = psutil.cpu_percent(interval=1)
            memory_percent = psutil.virtual_memory().percent
            update_thread_status("main", "시스템 모니터링", 
                      f"CPU {cpu_percent}%, 메모리 {memory_percent}%")
            
            # 주기적 저장
            save_discovered_ranges()
            
            # 종료 이벤트 확인 (10초마다)
            shutdown_event.wait(10)
            
    except KeyboardInterrupt:
        update_thread_status("main", "사용자에 의한 종료 요청")
        shutdown_all()

# 이 모듈을 직접 실행하면 전체 서버 크롤링
if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # 명령줄 인수 처리
        if len(sys.argv) > 1 and sys.argv[1] == "--stop":
            # 강제 종료 플래그 설정
            shutdown_event.set()
            logger.info("종료 명령 실행 중...")
            sys.exit(0)
            
        logger.info("최적화된 연속 랭킹 크롤링 시작")
        start_optimized_continuous_crawling()  # 최적화된 버전 사용
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 크롤링 중단됨")
        shutdown_all()
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {str(e)}", exc_info=True)
        shutdown_all()
