import chromedriver_autoinstaller
import os
import logging
import threading
import time
from datetime import datetime, timedelta
import re
import signal
import sys
import gc
import io
import queue
from queue import Empty
from threading import Lock
from typing import Dict, List, Optional, Tuple, Union

# 로깅 설정 - 대시보드만 표시하도록 INFO 레벨로 설정하고 다른 로그는 제외
# 기본 로깅 설정
for handler in logging.root.handlers:
    logging.root.removeHandler(handler)
    
# 콘솔 핸들러 설정
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

# 루트 로거 설정
logging.root.setLevel(logging.INFO)
logging.root.addHandler(ch)

# 다른 모듈의 로깅 레벨 설정 (불필요한 로그 제거)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('chardet').setLevel(logging.WARNING)

# PostgreSQL 직접 접근용 라이브러리 (COPY 명령어 사용 시 필요)
import psycopg2
from psycopg2.extensions import connection as pg_connection

# 서드파티 라이브러리
import psutil
import requests
from bs4 import BeautifulSoup
# from fake_useragent import UserAgent  # 사용하지 않으므로 주석 처리
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# DB 연결 모듈 가져오기
from service.db import delete_character_data
from service.db_session import (
    SessionLocal, 
    ScopedSession, 
    get_current_time, 
    KST, 
    SQLALCHEMY_DATABASE_URL
)
from sqlalchemy import text

# 로거 설정
logger = logging.getLogger("순차크롤러")
logger.setLevel(logging.INFO)
# 중복 로그 방지를 위한 propagate 설정 추가
logger.propagate = False

# 콘솔 핸들러 설정
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(console_formatter)

# 파일 핸들러 설정
file_handler = logging.FileHandler('sequential_crawler.log', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(file_formatter)

# 기존 핸들러 제거 (중복 방지)
if logger.handlers:
    logger.handlers.clear()

# 핸들러 추가
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# 로그 시작 메시지
logger.info("로거가 초기화되었습니다.")

# 다른 라이브러리(Selenium 등)의 로그 레벨 조정

# Selenium 로그 레벨은 WARNING으로 설정 (너무 많은 로그 방지)
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# 순차크롤러 로그는 INFO 레벨 유지
logger.setLevel(logging.INFO)

# 시스템 리소스 제한 설정
MAX_CPU_PERCENT = 80.0  # CPU 사용량 최대 80%
MAX_MEMORY_PERCENT = 80.0  # 메모리 사용량 최대 80%
RESOURCE_CHECK_INTERVAL = 5  # 리소스 확인 주기(초)
THROTTLE_DELAY = 1  # 리소스 사용량이 높을 때 추가 딜레이(초)

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
    
    # 호출 주기 판정 - 마지막 체크 이후 일정 시간이 지나야 다시 체크
    current_time = datetime.now(KST)  # KST 시간 사용 (중요)
    time_diff = (current_time - resource_usage_stats["last_check_time"]).total_seconds()
    
    # 일정 시간이 지나지 않았고 현재 스로틀링 상태가 아니라면 마지막 결과 그대로 반환
    if time_diff < RESOURCE_CHECK_INTERVAL and not resource_usage_stats["throttling"]:
        return True  # 안전한 상태로 간주
    
    # 현재 CPU 사용량 (모든 코어의 평균)
    cpu_percent = psutil.cpu_percent(interval=0.1)
    
    # 현재 메모리 사용량
    memory_info = psutil.virtual_memory()
    memory_percent = memory_info.percent
    
    # 리소스 사용량 업데이트
    resource_usage_stats["cpu_percent"] = cpu_percent
    resource_usage_stats["memory_percent"] = memory_percent
    resource_usage_stats["last_check_time"] = current_time  # 현재 시간(이미 가져온 KST)으로 업데이트
    
    # CPU 또는 메모리 사용량이 임계치를 초과하는지 확인
    if cpu_percent > MAX_CPU_PERCENT or memory_percent > MAX_MEMORY_PERCENT:
        # 스로틀링 상태로 전환 (이전에 아니었을 경우에만 로깅)
        if not resource_usage_stats["throttling"]:
            resource_usage_stats["throttling"] = True
            logger.warning(f"시스템 리소스 과부하 감지: CPU {cpu_percent:.1f}%, 메모리 {memory_percent:.1f}%, 크롤링 속도 제한 시작")
        return False
    else:
        # 스로틀링 해제 (이전에 스로틀링 중이었을 경우에만 로깅)
        if resource_usage_stats["throttling"]:
            resource_usage_stats["throttling"] = False
            logger.info(f"시스템 리소스 상태 정상화: CPU {cpu_percent:.1f}%, 메모리 {memory_percent:.1f}%, 크롤링 속도 제한 해제")
        return True

# 전역 변수들
shutdown_event = threading.Event()  # 종료 이벤트
active_threads = []  # 활성 쓰레드 관리
thread_status = {}   # 쓰레드 상태 관리
thread_status_lock = threading.Lock()  # 쓰레드 상태 락

# DB 워커 준비 완료 이벤트 (실행 순서 보장을 위한 시그널)
db_workers_ready = threading.Event()  # 초기에는 시그널이 없음(False)

# 카운트다운 래치 스타일의 워커 준비 카운터
worker_ready_count = 0
worker_ready_lock = threading.Lock()  # 카운터 락

# 마지막으로 크롤링한 캐릭터 인덱스를 서버별로 저장
last_crawled_character_index = {}
last_crawled_character_lock = threading.Lock()

# 서버별 처리할 캐릭터 목록
server_characters = {}
server_characters_lock = threading.Lock()

# 이미 처리된 캐릭터 추적을 위한 세트
processed_characters = set()
processed_characters_lock = threading.Lock()  # 캐릭터 세트 락

# DB 데이터 저장을 위한 큐 시스템 (성능 최적화)
db_queue = queue.Queue(maxsize=200000)  # 최대 20만 항목으로 제한 (용량 두 배 확장)
db_worker_running = threading.Event()   # DB 작업자 쓰레드 상태
db_worker_running.set()                 # 초기에는 작동 상태로 설정

# 전체 시스템 종료 시그널
shutdown_event = threading.Event()      # 종료 이벤트 (False로 초기화)

# DB 작업자 쓰레드 수 (CPU 코어 수 및 데드락 방지를 고려하여 최적화)
DB_WORKER_COUNT = max(2, min(6, psutil.cpu_count() // 2))  # CPU 코어 수의 절반으로 제한하고 최소 2개, 최대 6개로 설정
# 너무 많은 워커가 동시에 동작하면 데드락 발생 가능성 증가
db_worker_threads = []  # DB 작업자 쓰레드 목록

# 크롤링 작업 통계 및 상황판 관리
stats_lock = threading.Lock()          # 통계 데이터 락
server_stats = {}                      # 서버별 통계 정보

# DB 연결 및 세션 생성을 위한 유틸리티 함수
def create_db_connection(application_name="MobiRank_Crawler") -> pg_connection:
    """화상력 있는 PostgreSQL 연결을 생성하고, KST 타임존 설정을 적용합니다.
    
    Args:
        application_name: 연결의 애플리케이션 이름 (로그 추적용)
        
    Returns:
        psycopg2 연결 객체
    """
    try:
        # 새 연결 생성
        conn = psycopg2.connect(
            SQLALCHEMY_DATABASE_URL,
            application_name=application_name
        )
        
        # KST 타임존 설정 - 모든 데이터 저장/업데이트에 일관되게 적용
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'Asia/Seoul'")
        conn.commit()  # 타임존 설정 커밋
        cur.close()
        
        logger.debug(f"DB 연결 생성 성공 (application_name: {application_name})")
        return conn
    except Exception as e:
        logger.error(f"DB 연결 생성 실패: {str(e)[:200]}")
        raise

def create_sqlalchemy_session(recreate=False):
    """일관된 KST 타임존이 적용된 SQLAlchemy 세션을 생성합니다.
    
    Args:
        recreate: 기존 세션을 제거하고 새로 생성할지 여부
        
    Returns:
        SQLAlchemy 세션 객체
    """
    try:
        if recreate:
            try:
                ScopedSession.remove()  # 기존 세션 제거
            except Exception as remove_error:
                logger.warning(f"기존 세션 제거 중 오류 (무시하고 계속): {str(remove_error)[:100]}")
        
        # 새 세션 생성
        session = ScopedSession()
        
        # KST 타임존 설정 - 모든 데이터 저장 및 업데이트에 일관되게 적용
        session.execute(text('SET TIME ZONE \'Asia/Seoul\''))
        
        logger.debug(f"SQLAlchemy 세션 생성 성공 (recreate: {recreate})")
        return session
    except Exception as e:
        logger.error(f"SQLAlchemy 세션 생성 실패: {str(e)[:200]}")
        raise

# DB 저장 통계 정보
db_stats = {
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
                'total_collected': 0,  # 수집된 항목 총계
                'total_flushed': 0,   # 저장된 항목 총계
                'collector_size': 0,  # 수집기에 현재 들어있는 항목 수 (현재 배치)
                'rank_range': "미정보",  # 현재 크롤링 중인 랭킹 구간
                'current_character': "",  # 현재 처리 중인 캐릭터
                'last_update': datetime.now(KST),  # 마지막 갱신 시간
            }
            
        server_stats[server_name]['total_collected'] += items_collected
        server_stats[server_name]['total_flushed'] += items_flushed
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
        rank_info_summary = ""
        total_collected = 0
        total_collector_size = 0
        
        for server, stats in sorted(server_stats.items()):
            total_collected += stats['total_collected']
            current_collector_size = stats['collector_size']
            total_collector_size += current_collector_size
            server_summary += f"{server}: 수집={stats['total_collected']}, 배치={current_collector_size} | "
            
            # 랭킹 구간 정보 추가
            current_char = stats.get('current_character', '')
            rank_range = stats.get('rank_range', '미정보')
            if current_char:
                rank_info_summary += f"{server}: [랭킹{rank_range}] '{current_char}' 처리중 | "
            else:
                rank_info_summary += f"{server}: [랭킹{rank_range}] | "
        
        if not server_summary:
            server_summary = "서버 데이터 없음"
        
        if not rank_info_summary:
            rank_info_summary = "랭킹 정보 없음"
        
        # DB 저장 현황
        db_summary = f"DB저장: 총 {db_stats['total_processed']}개 처리, 큐사이즈: {db_stats['current_queue_size']}, 속도: {db_stats['processing_rate']:.1f}개/초"
        
        # 종합 요약 - 오직 이 로그만 INFO 레벨로 표시
        print("\n") # 좋은 가시성을 위한 개행 추가
        logger.info(f"===== 크롤링 상황판 ({time_str}, 총실행시간: {elapsed_str}) =====")
        logger.info(f"[1] 총계: 수집된 항목 {total_collected}개, 콜렉터 배치 총크기: {total_collector_size}개")
        logger.info(f"[2] {server_summary}")
        logger.info(f"[3] 서버별 랭킹 정보: {rank_info_summary}")
        logger.info(f"[4] {db_summary}")
        logger.info("=================================================")
        print("\n") # 좋은 가시성을 위한 개행 추가
        
        # 상황판 표시 시간 갱신
        last_stats_display = current_time

# 서버별 콜렉터 사이즈 업데이트 함수
def update_collector_size(server_name, size):
    """현재 콜렉터에 저장된 항목 수 갱신
    
    Args:
        server_name: 서버 이름
        size: 현재 콜렉터에 있는 항목 수(배치 크기)
    """
    global server_stats
    with stats_lock:
        if server_name in server_stats:
            server_stats[server_name]['collector_size'] = size
            
            # 현재 배치 크기는 수집된 항목 - 저장된 항목과 일치해야 함
            # 일관성 검사
            expected_size = server_stats[server_name]['total_collected'] - server_stats[server_name]['total_flushed']
            if size != expected_size and abs(size - expected_size) > 10:  # 약간의 오차 허용
                logger.debug(f"[통계 불일치] 서버 {server_name}: 설정된 콜렉터 크기={size}, 예상 크기={expected_size}")
                # 오차가 크면 통계 재조정 (일관성 유지)
                if expected_size >= 0:
                    server_stats[server_name]['collector_size'] = expected_size
            
# 서버별 현재 크롤링 중인 랭킹 구간 정보 갱신
def update_server_rank_info(server_name, rank_range, current_character=""):
    """현재 서버에서 처리 중인 랭킹 구간 정보 갱신"""
    global server_stats
    with stats_lock:
        if server_name in server_stats:
            server_stats[server_name]['rank_range'] = rank_range
            server_stats[server_name]['current_character'] = current_character
            server_stats[server_name]['last_update'] = datetime.now(KST)

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
    # 종료 이벤트 설정
    global shutdown_event
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
    chromedriver_autoinstaller.install()
    opts = Options()
    
    # 필수 옵션만 사용
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-logging")
    opts.add_argument("--disable-gpu-sandbox")
    opts.add_argument("--silent")
    opts.add_argument("--log-level=3")
    opts.add_argument("--window-size=1200,800")
    
    # 로그 완전 차단
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    if high_performance:
        # 성능 최적화 (필수만)
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-features=TranslateUI")
        opts.add_argument("--disable-ipc-flooding-protection")
    
    # 로그 완전 차단을 위한 Service 설정
    service = Service()
    service.log_path = "NUL" if os.name == "nt" else "/dev/null"
    
    return webdriver.Chrome(service=service, options=opts)

def get_server_name(server_num):
    """서버 번호를 서버 이름으로 변환"""
    return SERVER_NAMES.get(server_num, "데이안")  # 기본값 "데이안"

def fetch_rank_page_dom(driver, server_num, search_name="", div=1, high_performance_driver=True):
    """service/full_data.py의 검증된 DOM 로직을 드라이버 풀 없이 사용"""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from service.full_data import select_server_option, search_character
    
    list_url = f"https://mabinogimobile.nexon.com/Ranking/List?t={div}"
    server_name = get_server_name(server_num)
    
    try:
        driver.set_page_load_timeout(30)
        wait = WebDriverWait(driver, 20)
        
        # 페이지 이동
        logger.debug(f"랭킹 페이지로 이동: {list_url}")
        driver.get(list_url)
        
        # 페이지 로딩 대기
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)
        
        # 서버 선택 (service/full_data.py와 동일한 함수 사용)
        if server_name:
            select_server_option(driver, server_name)
            time.sleep(2)
        
        # 캐릭터 검색 (service/full_data.py와 동일한 함수 사용)
        if search_name:
            search_character(driver, search_name)
            time.sleep(3)
            
        # 랭킹 데이터 로딩 대기
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-mm-rankinglist], ul.list")))
        
        # 추가 대기 시간 (DOM 업데이트 완료 대기)
        time.sleep(2)
        
        # 페이지 소스 반환
        page_source = driver.page_source
        return page_source, driver
        
    except Exception as e:
        logger.error(f"DOM 크롤링 실패: 서버 {server_name}, 검색어 '{search_name}'. 오류: {e}")
        return None, driver

# 기존 DOM 함수들은 service/full_data.py의 검증된 함수로 대체됨

def navigate_to_page_dom(driver, page_number):
    """JavaScript 기반 페이지 이동 (고속 처리)"""
    try:
        # mmRanking.list JavaScript 함수 호출
        script = f"mmRanking.list({page_number}, null); return true;"
        result = driver.execute_script(script)
        if result:
            logger.debug(f"페이지 {page_number} 이동 완료")
            time.sleep(0.5)  # 최소 대기시간
            return True
    except Exception as e:
        logger.warning(f"페이지 이동 실패: {e}")
    return False

def get_pagination_info_dom(driver):
    """페이지네이션 정보 빠르게 추출"""
    try:
        script = """
        var pagination = document.querySelector('div[data-mm-paging]');
        var currentRange = document.querySelector('.current_range span');
        var currentPage = document.querySelector('.pagination li.on');
        
        return {
            totalCount: pagination ? parseInt(pagination.getAttribute('data-totalcount')) : 0,
            currentRange: currentRange ? currentRange.textContent.trim() : '',
            currentPage: currentPage ? parseInt(currentPage.textContent.trim()) : 1
        };
        """
        result = driver.execute_script(script)
        return result
    except Exception as e:
        logger.warning(f"페이지네이션 정보 추출 실패: {e}")
        return {"totalCount": 0, "currentRange": "", "currentPage": 1}

def crawl_by_character_search_dom(driver, server_num, character_list, div=1):
    """캐릭터 검색 기반 순차 크롤링 (중복 제거 방식)"""
    server_name = get_server_name(server_num)
    all_data = []
    processed_characters = set()  # 중복 제거용
    
    logger.info(f"서버 {server_name} 캐릭터 검색 기반 크롤링 시작: {len(character_list)}개 캐릭터")
    
    try:
        for idx, character_name in enumerate(character_list):
            try:
                # 이미 처리된 캐릭터는 스킵
                if character_name in processed_characters:
                    continue
                
                # 캐릭터 검색으로 20개 랭킹 데이터 가져오기
                page_source, _ = fetch_rank_page_dom(driver, server_num, character_name, div)
                if page_source:
                    page_data = parse_rank_html(page_source)
                    
                    # 중복 제거하면서 데이터 추가
                    new_characters = 0
                    for item in page_data:
                        char_name = item.get('character', '')
                        if char_name and char_name not in processed_characters:
                            all_data.append(item)
                            processed_characters.add(char_name)
                            new_characters += 1
                    
                    logger.debug(f"서버 {server_name} [{idx+1}/{len(character_list)}] '{character_name}' 검색: {len(page_data)}개 수집, {new_characters}개 신규")
                    
                    # 진행률 로깅 (100개마다)
                    if (idx + 1) % 100 == 0:
                        logger.info(f"서버 {server_name} 진행률: {idx+1}/{len(character_list)} ({((idx+1)/len(character_list)*100):.1f}%), 총 수집: {len(all_data)}개")
                else:
                    logger.warning(f"서버 {server_name} 캐릭터 '{character_name}' 검색 실패")
                
                # 빠른 처리를 위한 최소 대기
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"서버 {server_name} 캐릭터 '{character_name}' 처리 오류: {e}")
                continue
        
        logger.info(f"서버 {server_name} 캐릭터 검색 크롤링 완료: {len(all_data)}개 수집 (중복 제거됨)")
        return all_data
        
    except Exception as e:
        logger.error(f"서버 {server_name} 캐릭터 검색 크롤링 실패: {e}")
        return all_data

def crawl_by_character_search_dom_safe(driver, server_num, character_list, div=1):
    """안전한 캐릭터 검색 기반 크롤링 (Chrome 크래시 방지)"""
    server_name = get_server_name(server_num)
    all_data = []
    processed_characters = set()
    consecutive_failures = 0
    max_consecutive_failures = 5
    
    logger.info(f"서버 {server_name} 안전한 캐릭터 검색 크롤링 시작: {len(character_list)}개 캐릭터")
    
    try:
        for idx, character_name in enumerate(character_list):
            try:
                # 연속 실패 체크
                if consecutive_failures >= max_consecutive_failures:
                    logger.warning(f"서버 {server_name} 연속 {consecutive_failures}회 실패로 크롤링 중단")
                    break
                
                # 이미 처리된 캐릭터는 스킵
                if character_name in processed_characters:
                    continue
                
                # 안전한 DOM 조작 시도
                success = False
                for attempt in range(2):  # 최대 2회 시도
                    try:
                        page_source, _ = fetch_rank_page_dom(driver, server_num, character_name, div)
                        if page_source:
                            page_data = parse_rank_html(page_source)
                            
                            # 중복 제거하면서 데이터 추가
                            new_characters = 0
                            for item in page_data:
                                char_name = item.get('character', '')
                                if char_name and char_name not in processed_characters:
                                    all_data.append(item)
                                    processed_characters.add(char_name)
                                    new_characters += 1
                            
                            logger.debug(f"서버 {server_name} [{idx+1}/{len(character_list)}] '{character_name}' 성공: {len(page_data)}개 수집, {new_characters}개 신규")
                            consecutive_failures = 0
                            success = True
                            break
                        else:
                            logger.warning(f"서버 {server_name} 캐릭터 '{character_name}' 검색 결과 없음 (시도 {attempt+1})")
                            
                    except Exception as e:
                        logger.error(f"서버 {server_name} 캐릭터 '{character_name}' 검색 실패 (시도 {attempt+1}): {str(e)[:100]}")
                        time.sleep(2)  # 실패 시 대기
                
                if not success:
                    consecutive_failures += 1
                    logger.error(f"서버 {server_name}, 캐릭터 '{character_name}' 검색 실패. 다음으로 넘어감.")
                
                # Chrome 안정화를 위한 대기
                time.sleep(0.5)
                
                # 진행률 로깅 (50개마다)
                if (idx + 1) % 50 == 0:
                    logger.info(f"서버 {server_name} 진행률: {idx+1}/{len(character_list)} ({((idx+1)/len(character_list)*100):.1f}%), 총 수집: {len(all_data)}개")
                
            except Exception as e:
                consecutive_failures += 1
                logger.error(f"서버 {server_name} 캐릭터 '{character_name}' 처리 오류: {e}")
                time.sleep(1)
                continue
        
        logger.info(f"서버 {server_name} 안전 크롤링 완료: {len(all_data)}개 수집 (중복 제거됨)")
        return all_data
        
    except Exception as e:
        logger.error(f"서버 {server_name} 안전 크롤링 실패: {e}")
        return all_data

def get_character_list_from_db(server_name, exclude_recent_hours=1):
    """DB에서 해당 서버의 캐릭터 목록 가져오기 (최근 업데이트된 캐릭터 제외)"""
    from service.db_session import SessionLocal, get_current_time
    from sqlalchemy import text
    from datetime import timedelta
    
    try:
        db = SessionLocal()
        
        # 현재 시간에서 지정된 시간을 뺀 시점 (KST 기준)
        cutoff_time = get_current_time() - timedelta(hours=exclude_recent_hours)
        
        query = text("""
            SELECT DISTINCT character_name 
            FROM mabinogi_ranking 
            WHERE server_name = :server_name 
            AND (retrieved_at < :cutoff_time OR retrieved_at IS NULL)
            ORDER BY character_name
        """)
        result = db.execute(query, {
            'server_name': server_name,
            'cutoff_time': cutoff_time
        }).fetchall()
        character_list = [row[0] for row in result]
        db.close()
        
        logger.info(f"서버 {server_name}의 DB 캐릭터 목록: {len(character_list)}개 (최근 {exclude_recent_hours}시간 내 업데이트 제외)")
        return character_list
        
    except Exception as e:
        logger.error(f"서버 {server_name} 캐릭터 목록 조회 실패: {e}")
        return []

def generate_character_search_sequence(server_name, exclude_recent_hours=1):
    """캐릭터 검색 순서 생성 (DB + 추가 패턴, 최근 업데이트된 캐릭터 제외)"""
    # DB에서 기존 캐릭터 목록 (최근 업데이트된 캐릭터 제외)
    db_characters = get_character_list_from_db(server_name, exclude_recent_hours)
    
    # 한글 패턴 추가 (ㄱ-ㅎ, ㅏ-ㅣ 등)
    korean_patterns = []
    for i in range(ord('가'), ord('힣'), 100):  # 한글 유니코드 범위에서 100개씩 건너뛰며
        korean_patterns.append(chr(i))
    
    # 영문 패턴 추가
    english_patterns = [chr(i) for i in range(ord('a'), ord('z')+1)]
    english_patterns.extend([chr(i) for i in range(ord('A'), ord('Z')+1)])
    
    # 숫자 패턴 추가
    number_patterns = [str(i) for i in range(10)]
    
    # 전체 검색 패턴 조합
    all_patterns = db_characters + korean_patterns + english_patterns + number_patterns
    
    logger.info(f"서버 {server_name} 검색 패턴 생성: DB {len(db_characters)}개 (최근 {exclude_recent_hours}시간 내 업데이트 제외) + 패턴 {len(korean_patterns + english_patterns + number_patterns)}개")
    return all_patterns

def fast_sequential_crawl_worker(server_num, div=1):
    """고속 DOM 조작 기반 순차 크롤링 워커 (service/full_data.py 방식 사용)"""
    server_name = get_server_name(server_num)
    thread_name = f"고속크롤러_{server_name}"
    
    logger.info(f"서버 {server_name} 고속 DOM 크롤링 시작 (service/full_data.py 방식)")
    
    try:
        # service/full_data.py와 동일한 방식 사용 (server.py와 완전 동일)
        from service.full_data import fetch_rank_via_dom, parse_rank_html
        from service.db import insert_data
        
        # 캐릭터 검색 순서 생성 (DB + 추가 패턴, 최근 1시간 내 업데이트된 캐릭터 제외)
        character_list = generate_character_search_sequence(server_name, exclude_recent_hours=1)
        logger.info(f"서버 {server_name} 캐릭터 검색 대상: {len(character_list)}개 (최근 업데이트 제외)")
        
        # server.py와 동일한 방식으로 안전하게 크롤링
        all_data = []
        success_count = 0
        fail_count = 0
        
        for idx, character_name in enumerate(character_list):
            try:
                logger.debug(f"서버 {server_name} [{idx+1}/{len(character_list)}] '{character_name}' 검색 중...")
                
                # service/full_data.py의 검증된 함수 사용 (server.py와 완전 동일)
                html_data = fetch_rank_via_dom(server=server_name, name=character_name, rank_type=div)
                
                if html_data:
                    # HTML 파싱
                    parsed_data = parse_rank_html(html_data)
                    if parsed_data:
                        # DB 저장 (server.py와 동일)
                        result = insert_data(parsed_data, server=server_name, div=div)
                        if result.get('success', False):
                            success_count += 1
                            all_data.extend(parsed_data)
                            logger.debug(f"서버 {server_name} '{character_name}' 성공: {len(parsed_data)}개 저장")
                        else:
                            fail_count += 1
                    else:
                        fail_count += 1
                else:
                    fail_count += 1
                    
                # 진행률 로깅 (100개마다)
                if (idx + 1) % 100 == 0:
                    logger.info(f"서버 {server_name} 진행률: {idx+1}/{len(character_list)} ({((idx+1)/len(character_list)*100):.1f}%), 성공: {success_count}, 실패: {fail_count}")
                
                # Chrome 안정성을 위한 대기
                time.sleep(0.3)
                
            except Exception as e:
                fail_count += 1
                logger.error(f"서버 {server_name} '{character_name}' 처리 오류: {e}")
                time.sleep(1)
                continue
        
        logger.info(f"서버 {server_name} 크롤링 완료: 성공 {success_count}회, 실패 {fail_count}회, 총 데이터 {len(all_data)}개")
        logger.info(f"서버 {server_name} 고속 크롤링 완료")
        
    except Exception as e:
        logger.error(f"서버 {server_name} 고속 크롤링 실패: {e}")
        # service/full_data.py는 자체적으로 드라이버 관리하므로 별도 정리 불필요

def fetch_rank_page(driver, server_num, search_name="", div=1, high_performance_driver=True):
    """DOM 조작 기반으로 랭킹 데이터 가져오기 - 기존 함수명 호환성 유지"""
    return fetch_rank_page_dom(driver, server_num, search_name, div, high_performance_driver)

def fetch_rank_page_legacy(driver, server_num, search_name="", div=1, high_performance_driver=True):
    """기존 requests 기반 크롤링 함수 (백업용)"""
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"
    
    # 서버 번호를 서버 이름으로 변환 (오류 수정: 참조 전에 서버 이름 정의)
    server_name = get_server_name(server_num)
    
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
                
            # 런타임 IP 차단 방지를 위한 UserAgent 랜덤화 (fake_useragent 사용)
            try:
                random_ua = UserAgent().random
                logger.debug(f"랜덤 UserAgent 사용: {random_ua[:30]}...")
            except Exception as ua_error:
                # UserAgent 생성 실패 시 드라이버 UA 사용
                random_ua = driver.execute_script("return navigator.userAgent;")
                logger.warning(f"UserAgent 랜덤화 실패, 기본 UA 사용: {ua_error}")
                
            # [디버깅] API 호출 정보 출력
            # logger.info(f"[디버깅] API 호출 시작: 서버 {server_name}, 검색어 '{search_name}'")
                
            headers = {
                "User-Agent":          random_ua,  # 랜덤 UserAgent 사용
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

            # API 호출 시도
            logger.debug(f"API 호출 시도: 서버 {server_name}, 검색어 '{search_name}'")
            
            resp = sess.post(api_url, headers=headers, data=data)
            resp.raise_for_status()
            
            # API 응답 성공
            logger.debug(f"API 응답 성공: 서버 {server_name}, 검색어 '{search_name}'")
            
            return resp.text, driver

        except (requests.exceptions.RequestException, WebDriverException) as e:
            logger.debug(f"캐릭터 검색 실패: 서버 {server_name}, 검색어 '{search_name}', 시도 {attempts + 1}/{MAX_FETCH_RETRIES}. 오류: {e}")
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
                time.sleep(5) 
            else:
                logger.debug(f"캐릭터 검색 실패: 서버 {server_name}, 검색어 '{search_name}'에 대한 모든 {MAX_FETCH_RETRIES}번의 재시도 실패. 마지막 오류: {last_exception}")
                return None, driver

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
    
    # 랭킹 범위 추출 시도
    rank_range = parse_rank_range(html)
    
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
                
                # change_type이 'down'인 경우 음수로 변환 (랭킹이 내려간 경우)
                change_value_int = int(change_value.replace(',', ''))
                if item['change_type'] == 'down':
                    change_value_int = -change_value_int
                else:
                    change_value_int = abs(change_value_int)  # up인 경우 양수 보장
                
                # 데이터 딕셔너리로 준비
                processed_item = {
                    'rank': rank_position,
                    'change': change_value_int,
                    'change_type': item['change_type'],
                    'server': item['server'],
                    'character': item['character'],
                    'class': item['class'],
                    'power': power_value,
                    'div': div,
                    'retrieved_at_val': current_time_kst  # KST 시간 사용
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


def db_worker(worker_id):
    """고성능 DB 작업자 쓰레드 - 초당 수천건 처리 가능한 최적화 버전"""
    thread_name = f"DB작업자_{worker_id}"
    update_thread_status(thread_name, "시작됨")
    logger.info(f"DB 작업자 쓰레드 #{worker_id} 시작 (초고성능 모드)")
    
    # 최초 시작 후 준비 상태 보고 (최소한의 시간 후 포스팅)
    worker_init_delay = 1.0 + (worker_id * 0.2)  # 워커마다 사이즐 분배
    time.sleep(worker_init_delay)  # 초기화에 약간의 시간 허용
    
    # 워커 준비 완료 시그널 증가 (카운트다운 래치 방식)
    global worker_ready_count
    with worker_ready_lock:
        worker_ready_count += 1
        ready_ratio = worker_ready_count / DB_WORKER_COUNT
        logger.info(f"DB 작업자 #{worker_id} 준비 완료 - 큐 처리 준비됨 ({worker_ready_count}/{DB_WORKER_COUNT}, {ready_ratio:.0%})")
        
        # 모든 워커가 준비되면 시그널 설정
        if worker_ready_count >= DB_WORKER_COUNT:
            db_workers_ready.set()
            logger.info(f"모든 DB 워커({DB_WORKER_COUNT}개) 준비 완료 - 크롤링 시작 가능")
    
    # 배치 수집 설정 - 성능 최적화
    MIN_BATCH_SIZE = 1000   # 최소 배치 크기 (성능 안정화)
    MAX_BATCH_SIZE = 5000   # 최대 배치 크기 (메모리 고려)
    MAX_WAIT = 2.0          # 최대 대기 시간(초) - 타임아웃 연장
    
    # 재시도 정책
    MAX_RETRIES = 5
    RETRY_DELAY_MAX = 10
    
    # 통계 추적
    total_processed = 0
    batch_count = 0
    last_error_count = 0
    stats_update_interval = 100  # 100배치마다 통계 업데이트
    
    # 워커별 서버 분리 (데드락 방지)
    server_shard = worker_id % 7  # 7개 서버 분산 처리
    
    # 세션 재사용 - 한 번만 생성 (오버헤드 감소)
    session = create_sqlalchemy_session()
    
    try:
        
        # 고성능 배치 처리 루프
        while db_worker_running.is_set() and not shutdown_event.is_set():
            try:
                # 1. 배치 수집 최적화 - 최소 크기까지 블록 대기
                batch = []
                div = 1
                start_time = time.time()
                
                # 최소 MIN_BATCH_SIZE개가 모일 때까지 블록 대기
                # 또는 MAX_WAIT 시간 초과까지 대기
                while len(batch) < MAX_BATCH_SIZE and time.time() - start_time < MAX_WAIT:
                    try:
                        # 큐에서 데이터를 블로킹 모드로 가져옴 (CPU 부하 감소)
                        # 남은 최대 대기 시간 계산
                        remaining_wait = MAX_WAIT - (time.time() - start_time)
                        if remaining_wait <= 0 and len(batch) >= MIN_BATCH_SIZE:
                            # 최소 배치 크기 확보 & 시간 초과
                            break
                            
                        try:
                            # 블로킹 모드로 가져오되 timeout 설정
                            timeout = max(0.1, min(remaining_wait, 0.5))  # 최소 0.1초, 최대 0.5초
                            data, cur_div = db_queue.get(block=True, timeout=timeout)
                            
                            # 워커별 서버 분리 처리 (옵션) - 데드락 방지
                            # batch.extend([item for item in data if hash(item['server']) % 7 == server_shard])
                            batch.extend(data)  # 일단 모든 데이터 처리
                            
                            db_queue.task_done()
                            div = cur_div
                            
                            # 충분한 배치 크기 모였으면 중단
                            if len(batch) >= MIN_BATCH_SIZE and time.time() - start_time > 0.5:
                                # 최소한 0.5초는 기다려서 더 모을 기회 제공
                                break
                        except Empty:
                            # 최소 배치 크기 확보했으면 바로 처리
                            if len(batch) >= MIN_BATCH_SIZE:
                                break
                            # 시간 초과 임박하면 체크 더 자주
                            continue
                    except Exception as fetch_error:
                        if batch:  # 배치가 있으면 계속 진행
                            break
                        time.sleep(0.1)
                        continue
                
                # 수집된 배치가 없으면 다음 반복으로
                if not batch:
                    time.sleep(0.2)  # 데이터가 없으면 잠시 대기
                    continue
                
                # 2. 현재 KST 시간 한 번만 계산
                current_time_kst = get_current_time()
                
                # 3. 일괄 전처리
                for item in batch:
                    item['retrieved_at_val'] = current_time_kst
                
                # 4. 고속 배치 처리
                try:
                    # 4-1. Advisory lock 사용하여 데드락 방지
                    lock_stmt = text(f"SELECT pg_advisory_xact_lock({worker_id})")
                    session.execute(lock_stmt)
                    
                    # 4-2. COPY 명령을 사용한 고성능 데이터 삽입 구현
                    # 직접 연결을 통해 COPY 프로토콜 사용 (SQLAlchemy보다 훨씬 빠름)
                    try:
                        # SQLAlchemy 세션과 완전히 분리된 새 psycopg2 연결 생성
                        # 트랜잭션 경계 문제 해결을 위한 유틸리티 함수 사용
                        conn = create_db_connection(application_name=f"COPY_Worker_{worker_id}")
                        cur = conn.cursor()
                        # KST 타임존은 이미 create_db_connection 함수에서 설정됨
                        
                        # 임시 테이블 네이밍 개선: 일관된 이름 사용과 DROP-CREATE 방식
                        # 메타데이터 캐시 누적 방지를 위해 몇 개의 고정된 테이블만 사용
                        tmp_table_name = f"temp_ranking_import_{worker_id}"
                        
                        # 기존 테이블이 있으면 먼저 DROP
                        cur.execute(f"DROP TABLE IF EXISTS {tmp_table_name}")
                        
                        # 새 테이블 생성
                        cur.execute(f"""
                            CREATE TEMP TABLE {tmp_table_name} (
                                rank_position INTEGER,
                                change_amount INTEGER, 
                                change_type VARCHAR(10),
                                server_name VARCHAR(20),
                                character_name VARCHAR(50),
                                class_name VARCHAR(30),
                                power_value INTEGER,
                                div INTEGER,
                                retrieved_at TIMESTAMP WITH TIME ZONE
                            )
                        """)
                        
                        # StringIO 버퍼 생성 및 데이터 기록
                        buf = io.StringIO()
                        for item in batch:
                            # 데이터를 탭으로 구분된 형식으로 변환
                            buf.write(f"{item['rank']}\t{item['change']}\t{item['change_type']}\t"
                                     f"{item['server']}\t{item['character']}\t{item['class']}\t"
                                     f"{item['power']}\t{item['div']}\t{item['retrieved_at_val']}\n")
                        
                        # 버퍼를 시작 위치로 되돌림
                        buf.seek(0)
                        
                        # COPY 명령으로 임시 테이블에 데이터 삽입 (초고속)
                        cur.copy_from(
                            buf, 
                            tmp_table_name, 
                            sep='\t',
                            columns=('rank_position', 'change_amount', 'change_type', 'server_name',
                                     'character_name', 'class_name', 'power_value', 'div', 'retrieved_at')
                        )
                        
                        # 임시 테이블에서 UPSERT 수행 (기존 항목 업데이트, 새 항목 삽입)
                        # DISTINCT ON을 사용하여 중복 레코드 제거 (character_name, server_name, div가 동일한 경우)
                        cur.execute(f"""
                            INSERT INTO mabinogi_ranking 
                            (rank_position, change_amount, change_type, server_name, 
                             character_name, class_name, power_value, div, retrieved_at)
                            SELECT DISTINCT ON (character_name, server_name, div)
                                   rank_position, change_amount, change_type, server_name,
                                   character_name, class_name, power_value, div, retrieved_at
                            FROM {tmp_table_name}
                            ORDER BY character_name, server_name, div, retrieved_at DESC
                            ON CONFLICT (character_name, server_name, div) 
                            DO UPDATE SET 
                                rank_position = EXCLUDED.rank_position,
                                change_amount = EXCLUDED.change_amount,
                                change_type = EXCLUDED.change_type,
                                class_name = EXCLUDED.class_name,
                                power_value = EXCLUDED.power_value,
                                retrieved_at = EXCLUDED.retrieved_at
                        """)
                        
                        # 변경사항 커밋
                        conn.commit()
                        
                        # 정리
                        cur.close()
                        conn.close()
                        
                        logger.debug(f"COPY 프로토콜로 {len(batch)}개 항목 성공적으로 처리 (워커 {worker_id})")
                        
                    except Exception as copy_error:
                        # COPY 실패 시 기존 방식으로 폴백
                        logger.warning(f"COPY 실패, 기존 INSERT 방식으로 대체: {str(copy_error)[:200]}")
                        
                        # 기존 INSERT...ON CONFLICT 쿼리 정의 (폴백)
                        stmt = text("""
                            INSERT INTO mabinogi_ranking 
                            (rank_position, change_amount, change_type, server_name, 
                             character_name, class_name, power_value, div, retrieved_at)
                            VALUES 
                            (:rank, :change, :change_type, :server, 
                             :character, :class, :power, :div, :retrieved_at_val)
                            ON CONFLICT (character_name, server_name, div) 
                            DO UPDATE SET 
                                rank_position = EXCLUDED.rank_position,
                                change_amount = EXCLUDED.change_amount,
                                change_type = EXCLUDED.change_type,
                                class_name = EXCLUDED.class_name,
                                power_value = EXCLUDED.power_value,
                                retrieved_at = EXCLUDED.retrieved_at
                        """)
                        
                        # 하나의 트랜잭션으로 실행
                        session.execute(stmt, batch)
                        session.commit()
                    
                    # 4-4. 성공 처리
                    processed = len(batch)
                    total_processed += processed
                    batch_count += 1
                    last_error_count = 0
                    
                    # 4-5. 통계 업데이트 (자주 하지 않도록 제한)
                    if batch_count % stats_update_interval == 0:
                        queue_size = db_queue.qsize()
                        update_db_stats(
                            processed=processed,
                            queue_size=queue_size,
                            batch_size=processed
                        )
                        logger.info(f"작업자 {worker_id}: 총 {total_processed:,}개 항목 처리, 초당 평균: {processed/(time.time()-start_time):.1f}개, 큐: {queue_size:,}")
                        
                except Exception as db_error:
                    # 4-6. 오류 처리 - 트랜잭션 롤백 및 세션 재설정
                    last_error_count += 1
                    logger.error(f"DB 오류 (워커 {worker_id}): {str(db_error)[:200]}")
                    
                    # 모든 에러에서 세션 완전 재생성 일관성 개선
                    try:
                        # 롤백 먼저 시도
                        try:
                            session.rollback()
                        except Exception as rollback_error:
                            logger.error(f"롤백 실패: {str(rollback_error)[:100]}")
                        
                        # 어떤 경우든 세션 완전 재생성 수행
                        # "롤백 실패 시"에만 재생성하는 것이 아니라 모든 오류에서 재생성
                        session.close()
                        ScopedSession.remove()  # 세션 레지스트리에서 제거 (중요)
                        
                        # 세션 완전 재생성 - create_sqlalchemy_session 함수 사용
                        session = create_sqlalchemy_session(recreate=True)
                        logger.info(f"DB 오류 후 세션 재생성 완료 (워커 {worker_id})")
                    except Exception as session_error:
                        logger.error(f"세션 재생성 실패: {str(session_error)[:100]}")
                    
                    # 오류 발생 시 지수 백오프 대기
                    wait_time = min(0.5 * (2 ** last_error_count), RETRY_DELAY_MAX)
                    time.sleep(wait_time)
                
                # 5. 주기적 관리 작업 (자주 하지 않도록 제한)
                # 가비지 컬렉션 및 상황판 업데이트
                if batch_count % 100 == 0:
                    gc.collect()
                if batch_count % 50 == 0:
                    display_stats_dashboard()
                    
            except Exception as worker_error:
                logger.error(f"작업자 {worker_id} 일반 오류: {str(worker_error)[:200]}")
                time.sleep(0.5)
                last_error_count += 1
                
                if last_error_count > 10:
                    # 연속 오류 과다 - 재시작 처리
                    logger.warning(f"작업자 {worker_id} 재시작 필요 (연속 오류 {last_error_count}회)")
                    
                    # SQLAlchemy 세션 재생성 및 초기화 (유틸리티 함수 사용)
                    try:
                        # 기존 세션을 제거하고 새로 생성하는 통합 함수 사용
                        session = create_sqlalchemy_session(recreate=True)
                        logger.info(f"DB 워커 #{worker_id} - 세션 재생성 성공 (KST 타임존 적용됨)")
                    except Exception as session_error:
                        logger.error(f"DB 워커 #{worker_id} - 세션 재생성 실패! 예외 발생: {str(session_error)[:200]}")
                        
                        # 시스템 재시작 문제로 시스템 로그 기록 후 예외 다시 발생
                        time.sleep(10)  # 재연결 시도 전 대기
                        raise
                        
                    # 공간 확보를 위한 가비지 커렉션
                    gc.collect()
                    
                    # 잠시 쉬고 재시작
                    time.sleep(5)
                    last_error_count = 0
    
    except Exception as fatal_error:
        logger.error(f"DB 작업자 {worker_id} 치명적 오류: {str(fatal_error)}", exc_info=True)
    finally:
        # 종료 처리
        try:
            session.close()
            ScopedSession.remove()
        except Exception:
            pass
            
        update_thread_status(thread_name, "종료됨")
        logger.info(f"DB 작업자 #{worker_id} 종료. 총 처리 항목: {total_processed:,}")
        
        # 메모리 정리
        gc.collect()


def save_to_db_directly(data, div=1):
    """순위 데이터를 고성능으로 DB에 저장하는 함수 (최적화된 UPSERT 방식)"""
    if not data:
        logger.info("저장할 데이터가 없습니다.")
        return 0
        
    logger.info(f"{len(data)}개 데이터 고성능 DB 저장 시작...")
    start_time = time.time()
    
    # 고성능 처리를 위한 설정
    OPTIMAL_BATCH_SIZE = 1000  # 최적화된 배치 크기
    MAX_RETRIES = 3
    
    # 데이터가 리스트가 아니면 리스트로 변환
    if not isinstance(data, list):
        data = [data]
        
    # KST 시간 한 번만 계산 (중요: KST 유지)
    current_time = get_current_time()
    
    # 필요한 필드 추가 - 일괄 처리
    for item in data:
        item['div'] = div
        item['retrieved_at_val'] = current_time
    
    # 통계 추적
    total_processed = 0
    processing_times = []
    
    # 전체 데이터를 적절한 크기의 배치로 분할
    batches = [data[i:i + OPTIMAL_BATCH_SIZE] for i in range(0, len(data), OPTIMAL_BATCH_SIZE)]
    batch_count = len(batches)
    logger.debug(f"{batch_count}개 배치로 분할됨 (각 최대 {OPTIMAL_BATCH_SIZE}개)")
    
    # 하나의 세션을 재사용하여 전체 처리 (성능 최적화)
    session = ScopedSession()
    
    try:
        # 한 번만 KST 타임존 설정 (중요: 항상 KST 유지)
        session.execute(text('SET TIME ZONE \'Asia/Seoul\''))
        
        # UPSERT 쿼리 생성 - 데드락 방지 최적화
        upsert_stmt = text("""
            INSERT INTO mabinogi_ranking 
            (rank_position, change_amount, change_type, server_name, 
             character_name, class_name, power_value, div, retrieved_at)
            VALUES 
            (:rank, :change, :change_type, :server, 
             :character, :class, :power, :div, :retrieved_at_val)
            ON CONFLICT (character_name, server_name, div) 
            DO UPDATE SET 
                rank_position = EXCLUDED.rank_position,
                change_amount = EXCLUDED.change_amount,
                change_type = EXCLUDED.change_type,
                class_name = EXCLUDED.class_name,
                power_value = EXCLUDED.power_value,
                retrieved_at = EXCLUDED.retrieved_at
        """)
        
        # 배치별 처리
        for batch_idx, batch in enumerate(batches):
            batch_start = time.time()
            success = False
            retry_count = 0
            
            # 재시도 루프
            while not success and retry_count < MAX_RETRIES:
                try:
                    # Advisory lock 사용으로 데드락 방지 (옵션)
                    lock_id = hash(f"direct_save_{batch_idx}") % 2147483647  # 양수 32비트 정수 범위
                    session.execute(text(f"SELECT pg_advisory_xact_lock({lock_id})"))
                    
                    # 하나의 트랜잭션으로 처리
                    session.execute(upsert_stmt, batch)
                    session.commit()
                    
                    success = True
                    processed = len(batch)
                    total_processed += processed
                    
                    batch_time = time.time() - batch_start
                    processing_times.append(batch_time)
                    
                    # 성능 측정 및 현황 보고 (제한된 비율로)
                    if batch_idx % 10 == 0 or batch_idx == batch_count - 1:
                        progress = (batch_idx + 1) / batch_count * 100
                        speed = processed / batch_time if batch_time > 0 else 0
                        logger.info(f"DB 저장 진행: {progress:.1f}% ({batch_idx+1}/{batch_count}), "
                                  f"초당 {speed:.1f}개, 남은 시간: "
                                  f"{(batch_count-batch_idx-1)*(sum(processing_times)/(batch_idx+1)):.1f}초")
                        
                except Exception as e:
                    retry_count += 1
                    session.rollback()  # 중요: 오류 발생 시 롤백
                    
                    error_msg = str(e)[:200] if str(e) else "알 수 없는 오류"
                    logger.warning(f"배치 {batch_idx + 1}/{batch_count} 처리 실패 (재시도 {retry_count}/{MAX_RETRIES}): {error_msg}")
                    
                    # 재시도 전 지수 백오프 대기
                    wait_time = min(1 * (2 ** retry_count), 10)
                    time.sleep(wait_time)
                    
                    # 세션 상태 확인 및 필요시 재생성
                    if retry_count >= 2:
                        try:
                            # 세션 안전하게 정리
                            session.close()
                            ScopedSession.remove()  # 세션 레지스트리에서도 제거
                        except Exception as close_error:
                            logger.error(f"세션 정리 중 오류: {str(close_error)[:100]}, 새 세션 생성 계속")
                        finally:
                            # 세션 완전 재생성
                            session = ScopedSession()
                            
                            # KST 타임존 설정 (중요) - 항상 KST 유지
                            session.execute(text('SET TIME ZONE \'Asia/Seoul\''))
                            logger.info(f"배치 {batch_idx + 1} 처리를 위한 세션 재생성 완료")
                            
                            # 공간 확보를 위한 가비지 커렉션
                            gc.collect()
            
            # 최대 재시도 초과 시
            if not success:
                logger.error(f"배치 {batch_idx + 1}/{batch_count} 처리 포기 (최대 재시도 {MAX_RETRIES}회 초과)")
        
        # 전체 처리 통계
        total_time = time.time() - start_time
        avg_batch_time = sum(processing_times) / len(processing_times) if processing_times else 0
        records_per_second = total_processed / total_time if total_time > 0 else 0
        
        logger.info(f"DB 저장 완료: 총 {total_processed}/{len(data)}개 항목 처리")
        logger.info(f"✨ 총 소요시간: {total_time:.2f}초, 초당 {records_per_second:.1f}개 처리, "
                   f"배치당 평균 {avg_batch_time:.3f}초")
        
        return total_processed
        
    except Exception as e:
        # 전체 처리 중 예외 발생 시
        logger.error(f"DB 저장 중 예외 발생: {str(e)[:200]}", exc_info=True)
        return total_processed
    finally:
        # 세션 정리 확인
        try:
            if session:
                session.close()
                ScopedSession.remove()
        except Exception as cleanup_error:
            logger.error(f"종료 정리 중 오류: {str(cleanup_error)[:100]}")
        
        # 가비지 커렉션 추가
        gc.collect()

# DB 큐 및 안전한 크롤링을 위한 임계값 설정 (최적화된 값으로 조정)
MAX_QUEUE_SIZE = 100000  # DB 큐 최대 크기 (이 이상이면 크롤링 일시 중단, 5배 증가)
MAX_COLLECTOR_SIZE = 10000  # 콜렉터 최대 크기 (이 이상이면 크롤링 일시 중단, 2배 증가)
PAUSE_DURATION = 5  # 크롤링 일시 중단 시간(초), 더 현저히 재시도
SAFE_QUEUE_SIZE = 50000  # DB 큐 안전 크기 (이 이하면 크롤링 재개, 10배 증가)
SAFE_COLLECTOR_SIZE = 5000  # 콜렉터 안전 크기 (이 이하면 크롤링 재개, 2.5배 증가)
STATUS_CHECK_INTERVAL = 3  # 상태 확인 주기(초), 더 빠른 반응을 위해 줄임

# 크롤링 일시 정지 상태 저장용 이벤트 객체
crawling_paused = {}

# 이미 상단에 정의된 server_stats, db_stats 사용
last_dashboard_update = datetime.now(KST)
dashboard_update_interval = 5  # 초 단위

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
    
    def filter_duplicate_batch(self, items):
        """배치 데이터에서 중복 항목 제거"""
        unique_items = []
        for item in items:
            if item not in unique_items:
                unique_items.append(item)
        return unique_items
    
    def add_data(self, data):
        """
        데이터 추가, 배치 크기에 도달하면 자동 저장
        
        Args:
            data: 추가할 데이터 (단일 아이템 또는 리스트)
        """
        current_time = datetime.now(KST)
        flush_needed = False
        time_based_flush = False
        
        with self.lock:
            # 단일 아이템을 리스트로 변환
            items = data if isinstance(data, list) else [data]
            
            # 중복 필터링
            filtered_items = self.filter_duplicate_batch(items)
            
            # 필터링된 아이템 추가
            if filtered_items:
                self.batch.extend(filtered_items)
                self.total_items_collected += len(filtered_items)
                
                # 서버 통계 업데이트
                if self.server_name:
                    update_server_stats(self.server_name, items_collected=len(filtered_items))
            
            # 배치 크기 또는 최대 저장 간격 도달 시 플러시
            if len(self.batch) >= self.batch_size:
                flush_needed = True
            elif (current_time - self.last_flush_time) >= self.max_save_interval and self.batch:
                flush_needed = True
                time_based_flush = True
        
        # 플러시가 필요한 경우에만 실행
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
                    
                    # 로그에 저장 결과 출력
                    logger.debug(f"서버 {self.server_name}의 데이터 {batch_size}개 항목을 DB 큐에 전송 완료")
                        
                except Exception as e:
                    logger.error(f"배치 데이터 큐 전송 중 오류: {e}")
                    # 오류 발생 시에도 시간 업데이트 (배치는 그대로 유지)
                    self.last_flush_time = datetime.now(KST)
            else:
                # 배치가 비어있을 때도 마지막 저장 시간 갱신
                self.last_flush_time = datetime.now(KST)


def get_character_list_for_server(server_name, div=1):
    """서버에서 크롤링할 캐릭터 목록 가져오기 (마지막 랭킹부터 1등까지 정렬)"""
    try:
        # DB에서 캐릭터 목록을 새로 가져올 때 처리된 캐릭터 세트 초기화
        with processed_characters_lock:
            processed_characters.clear()
            logger.debug(f"서버 {server_name} 캐릭터 목록 새로 조회, 처리된 캐릭터 세트 초기화")
            
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
            ORDER BY rank_position ASC
            """)
            
            result = db.execute(query, {"server_name": server_name, "div": div})
            characters = [row[0] for row in result.fetchall()]
            
            # 데이터베이스에서 가져온 캐릭터 정보 출력 (SQL 쿼리 결과)
            logger.debug(f"서버 {server_name}, div {div} - DB에서 가져온 캐릭터 수: {len(characters)}")
            if len(characters) > 0:
                logger.debug(f"서버 {server_name} 처음 5개 캐릭터: {characters[:5]}")
            
            if not characters:
                logger.warning(f"서버 {server_name}에서 크롤링할 캐릭터를 찾을 수 없음. 기본 캐릭터 사용.")
                return ["힝트"]
              # 서버별 크롤링 상태 정보
            logger.info(f"로딩된 서버 이름들: {[server for server in crawling_paused.keys()]}")
            logger.info(f"서버 {server_name}에서 {len(characters)}개의 캐릭터를 1등 랭킹부터 크롤링할 예정")
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
        # 이미 일시 중단된 상태면 즉시 False 반환
        return False
    
    # DB 큐 크기 확인
    queue_size = db_queue.qsize()  # 실시간 큐 크기 확인
    db_stats["queue_size"] = queue_size  # 통계에 현재 큐 크기 업데이트
    
    if queue_size > MAX_QUEUE_SIZE:
        logger.warning(f"[큐 관리] DB 큐 크기 임계값 심각하게 초과: {queue_size} > {MAX_QUEUE_SIZE}, 모든 크롤링 작업 중단하고 DB 작업만 계속함")
        db_stats["paused"] = True
        # 모든 서버에 대한 크롤링을 중지
        for server in crawling_paused:
            crawling_paused[server] = True
        return False
    
    # 개별 서버 콜렉터 크기 확인
    if collector_size > MAX_COLLECTOR_SIZE:
        logger.warning(f"[큐 관리] 서버 {server_name} 콜렉터 크기 임계값 초과: {collector_size} > {MAX_COLLECTOR_SIZE}, 해당 서버 크롤링 일시 중단")
        return False
    
    return True

def check_and_resume_crawling():
    """DB 큐 크기를 확인하고 안전한 수준이면 크롤링을 재개합니다."""
    global db_stats
    
    # 이미 일시 중단 상태가 아니면 바로 반환
    if not db_stats.get("paused", False):
        return True
        
    # 실시간 큐 크기 확인
    queue_size = db_queue.qsize()
    db_stats["queue_size"] = queue_size
    
    # DB 큐가 안전 수준 이하로 내려갔는지 확인
    if queue_size <= SAFE_QUEUE_SIZE:
        logger.info(f"[큐 관리] DB 큐 크기가 안전 수준으로 감소함: {queue_size} <= {SAFE_QUEUE_SIZE}, 모든 서버의 크롤링 작업 재개")
        db_stats["paused"] = False
        # 모든 서버에 대해 크롤링 재개
        for server in crawling_paused:
            crawling_paused[server] = False
        return True
    else:
        logger.debug(f"[큐 관리] DB 큐 크기 여전히 높음: {queue_size} > {SAFE_QUEUE_SIZE}, DB 작업만 계속 진행")
        return False

def is_safe_to_resume(server_name, collector_size):
    """크롤링을 재개해도 안전한지 확인합니다.
    
    Args:
        server_name: 서버 이름
        collector_size: 현재 콜렉터 크기
        
    Returns:
        bool: 크롤링 재개가 안전한지 여부
    """
    # 전체 크롤링 일시 중단 상태 확인
    if db_stats.get("paused", False):
        # DB 큐 크기를 확인하고 안전하면 크롤링 재개
        return check_and_resume_crawling()
    
    # 개별 서버 콜렉터 크기 확인
    if collector_size <= SAFE_COLLECTOR_SIZE:
        return True
    else:
        logger.debug(f"[큐 관리] 서버 {server_name} 콜렉터 크기 여전히 높음: {collector_size} > {SAFE_COLLECTOR_SIZE}")
        return False

def sequential_rank_crawl_worker(server_num, div=1):
    """
    마지막 랭킹부터 1등까지 순차적으로 크롤링하는 워커 함수
    캠릭터 이름 기반 검색 방식
    
    Args:
        server_num: 서버 번호
        div: 랭킹 분류 (기본값: 1)
    """
    server_name = get_server_name(server_num)
    thread_name = f"순차크롤러_{server_name}"
    update_thread_status(thread_name, "시작됨")
    
    # 실행 시간 정보 추가
    worker_start_time = time.time()
    last_activity_time = time.time()
    # 일정 시간(2시간) 동안 활동이 없을 경우 재시작
    max_inactivity_time = 2 * 60 * 60
    # 최대 실행 시간 (12시간)
    max_runtime = 12 * 60 * 60
    
    # 드라이버 준비
    driver = get_driver(high_performance=True)
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
                
                # 실행 시간 검사 - 최대 실행 시간 초과 시 중단
                current_runtime = time.time() - worker_start_time
                if current_runtime > max_runtime:
                    logger.warning(f"[타임아웃] 서버 {server_name} 크롤링 시간 초과: {current_runtime/3600:.1f}시간 > {max_runtime/3600:.1f}시간")
                    break
                
                # 비활성 시간 검사 - 오랫동안 활동이 없으면 재시작
                inactive_time = time.time() - last_activity_time
                if inactive_time > max_inactivity_time:
                    logger.warning(f"[활동없음] 서버 {server_name} 크롤러 {inactive_time/60:.1f}분 동안 활동 없음")
                    # 드라이버 재시작
                    try:
                        if driver is not None:
                            driver.quit()
                    except Exception as e:
                        logger.error(f"[드라이버종료오류] 서버 {server_name}: {e}")
                        
                    # 드라이버 재생성
                    logger.info(f"[드라이버재시작] 서버 {server_name}")
                    driver = get_driver(high_performance=True)
                    time.sleep(10)  # 재시작 후 잠시 대기
                    
                    # 활동 시간 초기화
                    last_activity_time = time.time()
                
                # 상태 확인 주기에 따라 DB 큐 및 콜렉터 크기 모니터링
                current_time = datetime.now(KST)
                if (current_time - last_status_check).total_seconds() >= STATUS_CHECK_INTERVAL:
                    last_status_check = current_time
                    
                    # DB 큐 크기 확인 - 실시간 큐 크기 가져오기
                    queue_size = db_queue.qsize()
                    db_stats["queue_size"] = queue_size  # 통계 업데이트
                    
                    # 전체 시스템이 일시 중단 상태인지 확인 (DB 큐가 너무 큰 경우)
                    if db_stats.get("paused", False):
                        # 큐 크기를 확인하고 안전하면 크롤링 재개 시도
                        if check_and_resume_crawling():
                            logger.info(f"[큐 관리] 서버 {server_name} 크롤링 재개: 큐 크기={queue_size}")
                            update_thread_status(thread_name, f"크롤링 재개됨", f"서버 {server_name}, 큐: {queue_size}")
                        else:
                            # DB 큐가 여전히 크면 크롤링 중단하고 DB 작업만 계속 수행
                            logger.warning(f"[큐 관리] 현재 전체 크롤링 중단 상태: 서버 {server_name}, DB 큐 크기={queue_size} > {SAFE_QUEUE_SIZE}")
                            update_thread_status(thread_name, f"DB 작업만 진행 중", f"서버 {server_name}, 큐 크기={queue_size}")
                            
                            # 배치 저장 수행
                            if collector.batch:
                                logger.info(f"[큐 관리] 서버 {server_name}의 남은 데이터({len(collector.batch)}개) 저장")
                                collector.flush()
                            
                            # 일정 시간 대기 후 다시 상태 확인
                            display_stats_dashboard()  # 현재 상태 표시
                            time.sleep(PAUSE_DURATION * 2)  # 좀 더 오래 대기
                            continue
                    
                    # 서버별 크롤링 일시 중단 상태 처리
                    if crawling_paused[server_name]:
                        # 크롤링 재개 가능한지 확인
                        if is_safe_to_resume(server_name, collector_size):
                            logger.info(f"[큐 관리] 서버 {server_name} 크롤링 작업 재개: 콜렉터 크기={collector_size}, DB 큐 크기={queue_size}")
                            crawling_paused[server_name] = False
                            update_thread_status(thread_name, f"크롤링 재개됨", f"서버 {server_name}, 상태: 안전, 큐: {queue_size}")
                        else:
                            # 아직 안전하지 않은 경우 대기
                            logger.warning(f"[큐 관리] 서버 {server_name} 크롤링 계속 중단 상태: 콜렉터 크기={collector_size}, DB 큐 크기={queue_size}")
                            update_thread_status(thread_name, f"일시 중단 중", f"서버 {server_name}, 큐: {queue_size}")
                            
                            # 배치 데이터가 있으면 저장
                            if collector.batch:
                                logger.info(f"[큐 관리] 일시 중단 상태에서 서버 {server_name}의 남은 데이터({len(collector.batch)}개) 저장")
                                collector.flush()
                                
                            display_stats_dashboard()  # 현재 상태 표시
                            time.sleep(PAUSE_DURATION)
                            continue
                        
                    # 크롤링 중이면 안전한지 확인
                    elif not is_crawling_safe(server_name, collector_size):
                        logger.warning(f"[큐 관리] 서버 {server_name} 크롤링 일시 중단 시작: 콜렉터 크기={collector_size}, DB 큐 크기={queue_size}")
                        crawling_paused[server_name] = True
                        update_thread_status(thread_name, f"일시 중단 시작", f"서버 {server_name}, 큐: {queue_size}")
                        
                        # 크롤링 중단 시 강제 배치 저장 수행
                        if collector.batch:
                            logger.info(f"[큐 관리] 일시 중단 시작: 서버 {server_name}의 데이터({len(collector.batch)}개) 즉시 저장")
                            collector.flush()
                        
                        display_stats_dashboard()  # 현재 상태 표시
                        time.sleep(PAUSE_DURATION)
                        continue
                
                # 크롤링이 일시 중단된 경우 처리
                if crawling_paused[server_name]:
                    # DB 큐 및 콜렉터 크기 확인
                    queue_size = db_queue.qsize()
                    collector_size = len(collector.batch) if hasattr(collector, 'batch') else 0
                    
                    # 재개 가능한지 확인
                    if queue_size <= SAFE_QUEUE_SIZE and not db_stats.get("paused", False):
                        logger.info(f"[큐 관리] 서버 {server_name} 크롤링 재개: 큐 크기={queue_size} <= {SAFE_QUEUE_SIZE}")
                        crawling_paused[server_name] = False
                        continue  # 크롤링 재개
                    else:
                        # 아직 재개 조건이 안 되면 대기
                        logger.warning(f"[큐 관리] 서버 {server_name} 크롤링 계속 중단: 큐 크기={queue_size}, 안전 크기={SAFE_QUEUE_SIZE}")
                        time.sleep(PAUSE_DURATION)
                        continue
                
                # 서버의 캐릭터 목록 확인
                with server_characters_lock:
                    chars = server_characters[server_name]
                
                # 현재 처리할 캐릭터 인덱스 가져오기
                current_char_idx = 0
                total_chars = len(chars)
                
                
                with last_crawled_character_lock:
                    # 이전 인덱스 저장
                    prev_idx = last_crawled_character_index.get(server_name, -1)
                    current_char_idx = (prev_idx + 1) % total_chars  # 다음 캐릭터로
                    
                    
                    # 현재 캐릭터 인덱스 저장
                    last_crawled_character_index[server_name] = current_char_idx
                    
                    # "진짜 한 바퀴 다 돌았을 때"만 실행 - 이전 인덱스가 마지막이었고 현재 인덱스가 0인 경우
                    is_full_cycle = prev_idx == total_chars - 1 and current_char_idx == 0
                    # 순환 완료 여부 확인
                    
                    if is_full_cycle:
                        # 새로운 순환 시작 - 처리된 캐릭터 세트 초기화
                        with processed_characters_lock:
                            # 전체 처리된 캐릭터 세트 초기화
                            processed_characters.clear()
                            logger.debug(f"서버 {server_name} 전체 순환 완료, 처리된 캐릭터 세트 초기화")
                        
                        # 순환 완료 시 남은 데이터 저장
                        if collector.batch:
                            logger.debug(f"서버 {server_name} 순환 완료, 남은 데이터 {len(collector.batch)}개 저장")
                            collector.flush()
                            
                        # 다음 순환을 위해 캐릭터 목록 새로 가져오기
                        with server_characters_lock:
                            logger.debug(f"서버 {server_name} 다음 순환을 위한 캐릭터 목록 새로 가져오기")
                            server_characters[server_name] = get_character_list_for_server(server_name, div)
                            # 현재 처리할 캐릭터 인덱스 초기화 (0부터 다시 시작)
                            last_crawled_character_index[server_name] = 0
                            
                        # 잠시 대기 후 다음 순환 시작 (KST 타임존 유지)
                        logger.debug(f"서버 {server_name} 다음 순환 시작까지 5초 대기")
                        time.sleep(5)
                
                # 현재 처리할 캐릭터 이름 가져오기
                current_char = chars[current_char_idx]
                # 현재 처리할 캐릭터 정보는 상황판에 표시됨
                
                # 이미 처리된 캐릭터인지 확인
                character_key = f"{server_name}_{current_char}_{div}"
                with processed_characters_lock:
                    already_processed = character_key in processed_characters
                    
                    # processed_characters 세트 크기 확인
                    
                if already_processed:
                    # 이미 처리된 캐릭터는 스킵하고 통계에 반영
                    # 이미 처리된 캐릭터는 크롤링 스킵
                    
                    # 통계에 이미 처리된 캐릭터를 반영
                    with stats_lock:
                        if server_name in server_stats:
                            # 이미 처리된 캐릭터는 수집도 했고 저장도 완료된 상태
                            server_stats[server_name]['total_collected'] += 1
                            server_stats[server_name]['total_flushed'] += 1
                            
                            # 콜렉터 사이즈는 유지 - 이미 처리된 것은 현재 배치에 포함되지 않음
                            update_collector_size(server_name, server_stats[server_name]['collector_size'])
                    
                    # 주기적으로 데이터 저장 - 마지막 저장 후 일정 시간 경과 시
                    current_time = datetime.now(KST)
                    if (current_time - collector.last_flush_time) > collector.max_save_interval and collector.batch:
                        logger.debug(f"시간 기반 자동 저장 수행: 마지막 저장 후 {(current_time - collector.last_flush_time).total_seconds():.0f}초 경과")
                        collector.flush()
                    
                    # 활동 시간 갱신
                    last_activity_time = time.time()
                    
                    # 다음 캐릭터로 인덱스 증가 처리 - 중요: 이미 처리된 경우에도 다음 캐릭터로 넘어가야 함
                    with last_crawled_character_lock:
                        current_char_idx = (current_char_idx + 1) % total_chars
                        last_crawled_character_index[server_name] = current_char_idx
                        # 다음 캐릭터 인덱스로 이동
                    
                    # 다음 반복으로 즐시 이동
                    continue
                
                # 변수 미리 초기화
                rank_range = None
                
                # 랜킹 정보를 포함하여 스레드 상태 업데이트
                rank_info_display = f"{current_char_idx+1}/{total_chars}"
                update_thread_status(thread_name, f"캐릭터 크롤링 중", f"서버 {server_name}, 캐릭터 {current_char} - [{rank_info_display}]")
                
                # 현재 처리 중인 캐릭터와 서버의 랭킹 구간 정보 갱신
                # 랭킹 구간은 현재 랭킹이 없을 경우 단순 캐릭터 인덱스로 표시
                if not rank_range:  # rank_range가 없는 경우에만 업데이트
                    rank_info = f"{current_char_idx+1}/{total_chars}"
                    update_server_rank_info(server_name, rank_info, current_char)
                
                # 캐릭터 이름으로 검색하여 데이터 가져오기
                # API 호출에는 server_num을 사용
                html, driver = fetch_rank_page(driver, server_num, current_char, div, high_performance_driver=True)
                if html is None:
                    logger.error(f"서버 {server_name}, 캐릭터 '{current_char}' 검색 실패. 다음으로 넘어감.")
                    # 잠시 대기 후 다시 시도
                    time.sleep(5)
                    # 실패하더라도 활동은 했으므로 활동 시간 갱신
                    last_activity_time = time.time()
                    continue
                
                # 랜킹 범위 가져오기
                rank_range = parse_rank_range(html)
                # 활동 시간 갱신
                last_activity_time = time.time()
                if rank_range:
                    # 랭킹 범위 정보 갱신
                    update_server_rank_info(server_name, rank_range, current_char)
                    # 스레드 상태 업데이트 (랭킹 정보 포함)
                    update_thread_status(thread_name, f"캐릭터 크롤링 중", f"서버 {server_name}, 캐릭터 {current_char} - [{rank_range}]")
                    # 랜킹 정보 로그 추가 - 로그에 현재 크롤링 중인 캐릭터의 랭킹 정보 추가
                    logger.debug(f"[랜킹정보] 서버 {server_name}, 캐릭터 '{current_char}' - {rank_range}")
                
                try:
                    # 랜킹 데이터 파싱
                    parsed_data = parse_rank_html(html)
                    # 파싱 성공 시 활동 시간 갱신
                    last_activity_time = time.time()
                    
                    # 파싱된 데이터의 첫 번째 캐릭터 정보 로그
                    if parsed_data and len(parsed_data) > 0:
                        first_char = parsed_data[0]['character']
                        first_rank = parsed_data[0]['rank']
                        # logger.info(f"[데이터확인] 서버 {server_name}, 캐릭터 '{current_char}' 검색 결과 - 첫 번째 캐릭터: '{first_char}' ({first_rank})")
                except Exception as parse_error:
                    logger.error(f"[심각] 서버 {server_name}, 캐릭터 '{current_char}' 데이터 파싱 중 오류 발생: {parse_error}")
                    # 오류 발생 시 로그를 남기고 계속 진행
                    parsed_data = []
                    time.sleep(3)  # 오류 발생 시 잠시 대기
                
                if not parsed_data:
                    logger.warning(f"서버 {server_name}, 캐릭터 '{current_char}'에서 데이터를 찾을 수 없음")
                    
                    # 찾을 수 없는 캐릭터는 DB에서 삭제
                    try:
                        # 캐릭터가 존재하지 않으므로 해당 캐릭터의 모든 랜킹 데이터(div) 삭제
                        deleted_count = delete_character_data(server_name, current_char, div=None)
                        
                        if deleted_count > 0:
                            logger.info(f"서버 {server_name}, 캐릭터 '{current_char}' DB에서 삭제됨. 총 {deleted_count}개 레코드 삭제")
        
                            # 삭제 히스토리 기록
                            try:
                                save_db_history(
                                    db=None,  # 새 세션 사용
                                    operation_type="DELETE",
                                    object_type="mabinogi_ranking",
                                    object_id=server_name,
                                    details=f"캐릭터 삭제 - {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}",
                                    count=deleted_count,
                                    character_list=[current_char]
                                )
                            except Exception as hist_error:
                                logger.error(f"삭제 히스토리 저장 중 오류: {hist_error}")

                            # 삭제 후 서버 통계 업데이트 (매개변수 음수 값)
                            with collector.lock:
                                collector.total_items_collected -= 1  # 통계용 1 감소
                            
                    except Exception as e:
                        logger.error(f"서버 {server_name}, 캐릭터 '{current_char}' 삭제 중 오류 발생: {e}")
                else:
                    # 데이터 수집 (통계 갱신은 collector에서 처리, KST 시간 적용)
                    collector.add_data(parsed_data)
                    # 데이터 추가 성공 시 활동 시간 갱신
                    last_activity_time = time.time()
                    
                    # 처리된 캐릭터 등록 (parsed_data에 있는 모든 캐릭터)
                    with processed_characters_lock:
                        # 오직 현재 처리중인 캐릭터만 추가
                        processed_characters.add(character_key)
                        
                        # 현재 처리중인 캐릭터만 추가
                        # 로그 기록 성공 시 활동 시간 갱신
                        last_activity_time = time.time()
                
                # 일정 간격으로 수집기 비우기 (중복 제거 및 간격 조정)
                if current_char_idx % 5 == 0:  # 5회마다 플러시 (더 자주 저장)
                    # 배치 저장
                    if collector.batch:
                        logger.info(f"주기적 저장 실행: 캐릭터 인덱스 {current_char_idx}, 배치 크기 {len(collector.batch)}개")
                        collector.flush()
                        # 저장 성공 시 활동 시간 갱신
                        last_activity_time = time.time()
                        
                        # 저장 후 상황판 표시 업데이트
                        display_stats_dashboard()
                
                # 처리된 캐릭터 세트 크기 로깅
                with processed_characters_lock:
                    # 현재 서버에 해당하는 처리된 캐릭터 개수 확인
                    server_processed = sum(1 for key in processed_characters if key.startswith(f"{server_name}_"))
                    logger.debug(f"서버 {server_name} - processed_characters 세트 크기: {len(processed_characters)}, 서버 관련 캐릭터: {server_processed}개")
                # 상황판 갱신
                display_stats_dashboard()
                
                # 시간 기반 주기적 체크 - 데이터가 없어도 주기적으로 시간 체크
                current_time = datetime.now(KST)
                # 시간 체크도 활동으로 간주
                last_activity_time = time.time()
                if (current_time - collector.last_flush_time) > collector.max_save_interval and collector.batch:
                    logger.debug(f"시간 기반 자동 저장 실행: 마지막 저장 후 {(current_time - collector.last_flush_time).total_seconds():.0f}초 경과")
                    collector.flush()
                
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
                                    f"메모리 {resource_usage_stats['memory_percent']:.1f}%, 배치 강제 저장 후 딜레이 적용")
                                collector.flush()  # 강제 배치 저장
                                time.sleep(THROTTLE_DELAY * 4)  # 더 긴 딜레이
                                continue
                            time.sleep(THROTTLE_DELAY * 2)  # 더 긴 딜레이
                        else:
                            # 준위한 경우 기본 스로틀링
                            time.sleep(THROTTLE_DELAY)
                
                # 기본 대기 시간 (과부하 방지)
                # 스로틀링 중이 아닌 경우에도 기본 대기 시간 적용
                if resource_usage_stats["throttling"]:
                    time.sleep(THROTTLE_DELAY)  # 스로틀링 중일 때 추가 딜레이
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


def sequential_server_crawl_worker():
    """서버별 순차 처리 워커 (DOM 안정성을 위해 하나씩 처리)"""
    thread_name = threading.current_thread().name
    update_thread_status(thread_name, "순차 서버 크롤링 시작")
    
    server_nums = list(range(1, 8))  # 1부터 7까지의 서버
    
    while not shutdown_event.is_set():
        try:
            # 모든 서버를 순차적으로 처리
            for server_num in server_nums:
                if shutdown_event.is_set():
                    break
                    
                try:
                    logger.info(f"서버 {get_server_name(server_num)} 크롤링 시작")
                    fast_sequential_crawl_worker(server_num, div=1)
                    logger.info(f"서버 {get_server_name(server_num)} 크롤링 완료")
                    
                    # 서버 간 간격 (Chrome 안정화)
                    time.sleep(5)
                    
                except Exception as e:
                    logger.error(f"서버 {get_server_name(server_num)} 크롤링 오류: {e}")
                    time.sleep(10)  # 오류 시 더 긴 대기
                    
            # 한 사이클 완료 후 대기
            logger.info("모든 서버 크롤링 완료, 다음 사이클까지 대기")
            time.sleep(60)  # 1분 대기 후 다음 사이클
            
        except Exception as e:
            logger.error(f"순차 서버 크롤러 오류: {e}")
            time.sleep(30)
    
    update_thread_status(thread_name, "종료됨")

def start_sequential_rank_crawling():
    """모든 서버에 대해 순차적 랭킹 크롤링 시작"""
    try:
        if not db_workers_ready.is_set():
            logger.warning("DB 워커가 아직 준비되지 않았습니다. 대기 중...")
            ready = db_workers_ready.wait(timeout=30)
            if not ready:
                logger.warning("DB 워커 준비 완료 시그널을 받지 못했지만, 강제로 시작합니다.")
            else:
                logger.info("DB 워커 준비 완료, 크롤링을 시작합니다.")
        
        logger.info("순차적 랭킹 크롤링 시작")
        
        # 처리된 캐릭터 세트 초기화
        with processed_characters_lock:
            processed_characters.clear()
            logger.info("처리된 캐릭터 추적 세트 초기화")
        
        # DOM 안정성을 위해 서버별 순차 처리 (동시 DOM 조작 방지)
        logger.info("Chrome 안정성을 위해 서버별 순차 처리로 변경")
        
        # 시스템 사양 정보 로깅
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        logger.info(f"시스템 사양: CPU {cpu_count}코어, 메모리 {memory_gb:.1f}GB")
        
        # 단일 쓰레드로 서버별 순차 처리 시작
        thread = threading.Thread(
            target=sequential_server_crawl_worker,
            name="순차_서버_크롤러"
        )
        thread.daemon = True
        thread.start()
        active_threads.append(thread)
        logger.info("서버별 순차 처리 크롤러 시작됨")
        
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



def save_db_history(db, operation_type, object_type, object_id=None, details=None, count=None, character_list=None):
    """DB 작업 내역을 히스토리 테이블에 저장
    Args:
        db: 데이터베이스 세션
        operation_type: 작업 유형 (INSERT, UPDATE, DELETE)
        object_type: 대상 객체 유형 (테이블명 등)
        object_id: 대상 객체 ID (있는 경우)
        details: 작업 상세 내용
        count: 영향받은 레코드 수
        character_list: 처리된 캐릭터 이름 리스트
    """
    current_time = datetime.now(KST)
    
    # 새 세션을 사용하여 트랜잭션 충돌 방지
    own_db = False
    if db is None:
        # KST 타임존이 적용된 세션 생성 (유틸리티 함수 사용)
        db = create_sqlalchemy_session(recreate=False)
        own_db = True
    
    try:
        query = text("""
            INSERT INTO operation_history 
            (operation_type, object_type, object_id, details, count, character_list, created_at)
            VALUES (:operation_type, :object_type, :object_id, :details, :count, :character_list, :created_at)
            RETURNING id
        """)
        
        result = db.execute(query, {
            'operation_type': operation_type,
            'object_type': object_type,
            'object_id': object_id,
            'details': details,
            'count': count,
            'character_list': character_list,
            'created_at': current_time
        })
        
        if own_db:
            db.commit()
        
        # 히스토리 ID 반환
        for row in result:
            return row[0]
        return None
    except Exception as e:
        if own_db:
            db.rollback()
        logger.error(f"히스토리 저장 실패: {e}")
        return None
    finally:
        if own_db:
            db.close()



if __name__ == "__main__":
    try:
        logger.info("===== 순차적 랭킹 크롤러 시작 =====")
        
        # DB 작업자 쓰레드 시작 (3개의 쓰레드로 성능 향상)
        for i in range(DB_WORKER_COUNT):
            db_worker_thread = threading.Thread(target=db_worker, args=(i,), name=f"DB작업자_{i}")
            db_worker_thread.daemon = True
            db_worker_thread.start()
            db_worker_threads.append(db_worker_thread)
            active_threads.append(db_worker_thread)
            logger.info(f"DB 작업자 쓰레드 #{i} 시작됨")
        logger.info(f"총 {DB_WORKER_COUNT}개의 DB 쓰레드가 병렬로 동작 중")
        
        logger.info("워커 쓰레드들이 준비되기를 기다리는 중...")
        
        ready_success = db_workers_ready.wait(timeout=30)
        
        if ready_success:
            logger.info(f"모든 DB 워커 쓰레드({DB_WORKER_COUNT}개) 준비 완료되었습니다. 크롤링을 시작합니다.")
        else:
            # 타임아웃이 발생해도 계속 진행 (고장상황 대비)
            logger.warning(f"DB 워커 준비 시그널 타임아웃 (30초). 현재 {worker_ready_count}/{DB_WORKER_COUNT} 워커만 준비됨. 그래도 계속 진행합니다.")
            # 시그널을 강제로 설정하여 크롤링 진행
            db_workers_ready.set()
        
        # 랭킹 크롤링 시작
        start_sequential_rank_crawling()
        
        # 상황판 업데이트를 위한 시간 추적
        last_dashboard_check = datetime.now(KST)
        dashboard_check_interval = 5  # 5초마다 상황판 표시 확인
        
        # 메인 쓰레드는 다른 작업을 할 수 있도록 계속 실행
        last_activity_time = time.time()
        worker_start_time = time.time()
        max_runtime = 3600 * 12  # 12시간
        
        while not shutdown_event.is_set():
            # 현재 활동 시간 갱신
            last_activity_time = time.time()
            
            # 실행 시간 검사
            current_runtime = time.time() - worker_start_time
            if current_runtime > max_runtime:
                logger.warning(f"[타임아웃] 크롤링 시간 초과: {current_runtime/3600:.1f}시간 > {max_runtime/3600:.1f}시간")
                # 최대 실행 시간 초과 시 재시작
                break
            
            # 상황판 업데이트 확인
            current_time = datetime.now(KST)
            if (current_time - last_dashboard_check).total_seconds() >= dashboard_check_interval:
                display_stats_dashboard()
                last_dashboard_check = current_time
                
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Ctrl+C로 종료 시도")
    finally:
        # DB 작업자 쓰레드 종료 신호
        db_worker_running.clear()
        logger.info(f"모든 DB 작업자 쓰레드({DB_WORKER_COUNT}개)에 종료 신호 전송, 남은 작업 처리 대기 중...")
        
        # DB 작업자 쓰레드가 모두 종료될 때까지 대기
        for idx, thread in enumerate(db_worker_threads):
            if thread.is_alive():
                logger.info(f"DB 작업자 쓰레드 #{idx} 종료 대기 중...")
                thread.join(timeout=5)
        logger.info("모든 DB 작업자 쓰레드 종료됨")
        
        # 그 외 모든 리소스 정리
        shutdown_all()
