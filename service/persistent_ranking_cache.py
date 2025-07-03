"""
지속적인 랭킹 페이지 캐시 시스템

3개의 랭킹 페이지를 항상 켜두고, 검색 요청이 들어올 때만 
JavaScript로 서버 선택과 캐릭터 검색을 수행하여 속도를 대폭 개선합니다.
"""

import time
import logging
import threading
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime
import pytz
from service.db_session import KST
from service.full_data import parse_rank_html
import gc
# sequential_ranking_crawler의 함수들을 직접 import하지 않고 
# 필요한 함수들을 이 파일에서 구현하거나 다른 방식으로 처리

logger = logging.getLogger(__name__)

def get_driver_for_cache(high_performance=True):
    """캐시용 드라이버 생성 (안정성 옵션 포함)"""
    try:
        chromedriver_autoinstaller.install()
        logger.info("ChromeDriver 설치 확인 완료")
    except Exception as e:
        logger.error(f"ChromeDriver 설치 실패: {e}")
        raise
    
    opts = Options()
    
    # 필수 옵션
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-logging")
    opts.add_argument("--silent")
    opts.add_argument("--log-level=3")
    opts.add_argument("--window-size=1200,800")
    
    # 동적 포트 할당
    opts.add_argument("--remote-debugging-port=0")
    
    # 안정성 향상 옵션
    opts.add_argument("--disable-crash-reporter")
    opts.add_argument("--disable-in-process-stack-traces")
    opts.add_argument("--no-first-run")
    opts.add_argument("--disable-background-networking")
    
    # 로그 차단
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    if high_performance:
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-background-timer-throttling")
    
    service = Service()
    import os
    service.log_path = "NUL" if os.name == "nt" else "/dev/null"
    
    # 재시도 로직
    for attempt in range(3):
        try:
            driver = webdriver.Chrome(service=service, options=opts)
            driver.set_page_load_timeout(45)
            driver.implicitly_wait(10)
            return driver
        except Exception as e:
            if attempt == 2:
                raise e
            time.sleep(2 ** attempt)

def safe_quit_driver_cache(driver):
    """캐시용 안전한 드라이버 종료"""
    if not driver:
        return True
    
    try:
        driver.quit()
        time.sleep(1)
        return True
    except Exception as e:
        logger.warning(f"드라이버 종료 중 오류: {e}")
        return False

def validate_driver_cache(driver):
    """캐시용 드라이버 유효성 검증"""
    if not driver:
        return False
    
    try:
        driver.current_url
        driver.title
        return True
    except Exception as e:
        logger.warning(f"드라이버 검증 실패: {e}")
        return False

class PersistentRankingCache:
    """지속적인 랭킹 페이지 캐시 관리 클래스"""
    
    def __init__(self):
        self.drivers = {}  # {rank_type: driver} 매핑
        self.last_server = {}  # {rank_type: server} 마지막 선택된 서버
        self.lock = threading.RLock()
        self.initialized = False
        self.running = False
        
    def initialize(self):
        """3개 랭킹 페이지를 초기화하고 켜둡니다."""
        with self.lock:
            if self.initialized:
                return True
                
            logger.info("지속적인 랭킹 페이지 캐시 초기화 시작...")
            
            # Chrome 사용 가능성 미리 체크
            try:
                test_driver = get_driver_for_cache(high_performance=True)
                if not validate_driver_cache(test_driver):
                    logger.error("Chrome WebDriver 테스트 실패")
                    safe_quit_driver_cache(test_driver)
                    return False
                safe_quit_driver_cache(test_driver)
                logger.info("Chrome WebDriver 테스트 성공")
            except Exception as e:
                logger.error(f"Chrome WebDriver 사용 불가: {e}")
                return False
            
            rank_types = {
                1: "전투력",
                2: "매력", 
                3: "생활력"
            }
            
            success_count = 0
            
            for rank_type, rank_name in rank_types.items():
                try:
                    logger.info(f"{rank_name} 랭킹 페이지 초기화 중...")
                    
                    # 드라이버 생성
                    driver = get_driver_for_cache(high_performance=True)
                    
                    if not validate_driver_cache(driver):
                        logger.error(f"{rank_name} 랭킹 드라이버 검증 실패")
                        safe_quit_driver_cache(driver)
                        continue
                    
                    # 랭킹 페이지로 이동
                    url = f"https://mabinogimobile.nexon.com/Ranking/List?t={rank_type}"
                    driver.get(url)
                    
                    # 페이지 로딩 대기
                    wait = WebDriverWait(driver, 20)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    time.sleep(2)
                    
                    # 초기 상태 검증
                    if self._verify_page_loaded(driver):
                        self.drivers[rank_type] = driver
                        self.last_server[rank_type] = None
                        success_count += 1
                        logger.info(f"{rank_name} 랭킹 페이지 초기화 성공")
                    else:
                        logger.error(f"{rank_name} 랭킹 페이지 로딩 검증 실패")
                        safe_quit_driver_cache(driver)
                        
                except Exception as e:
                    logger.error(f"{rank_name} 랭킹 페이지 초기화 실패: {e}", exc_info=True)
                    if 'driver' in locals():
                        safe_quit_driver_cache(driver)
            
            self.initialized = (success_count > 0)
            self.running = self.initialized
            
            if self.initialized:
                logger.info(f"랭킹 페이지 캐시 초기화 완료 ({success_count}/3 성공)")
                # 백그라운드 헬스체크 시작
                self._start_health_monitor()
            else:
                logger.error("랭킹 페이지 캐시 초기화 실패")
                
            return self.initialized
    
    def _verify_page_loaded(self, driver):
        """페이지가 정상적으로 로드되었는지 확인"""
        try:
            # 랭킹 리스트 요소 확인
            element = driver.find_element(By.CSS_SELECTOR, "div[data-mm-rankinglist], ul.list")
            logger.info(f"페이지 로드 검증 성공: {element.tag_name} 요소 발견")
            return True
        except Exception as e:
            logger.error(f"페이지 로드 검증 실패: {e}")
            logger.error(f"현재 URL: {driver.current_url}")
            logger.error(f"페이지 타이틀: {driver.title}")
            return False
    
    def _start_health_monitor(self):
        """백그라운드 헬스 모니터링 시작"""
        def health_check():
            while self.running:
                try:
                    time.sleep(300)  # 5분마다 체크
                    self._health_check()
                except Exception as e:
                    logger.error(f"헬스체크 중 오류: {e}")
        
        health_thread = threading.Thread(target=health_check, daemon=True)
        health_thread.start()
        logger.info("랭킹 페이지 헬스 모니터링 시작")
    
    def _health_check(self):
        """드라이버들의 상태를 점검하고 필요시 재생성"""
        with self.lock:
            if not self.running:
                return
                
            for rank_type, driver in list(self.drivers.items()):
                try:
                    if not validate_driver_cache(driver):
                        logger.warning(f"랭킹 타입 {rank_type} 드라이버 상태 불량, 재생성 시도...")
                        self._recreate_driver(rank_type)
                except Exception as e:
                    logger.error(f"랭킹 타입 {rank_type} 헬스체크 실패: {e}")
                    self._recreate_driver(rank_type)
    
    def _recreate_driver(self, rank_type):
        """특정 랭킹 타입의 드라이버를 재생성"""
        try:
            # 기존 드라이버 정리
            if rank_type in self.drivers:
                safe_quit_driver_cache(self.drivers[rank_type])
                del self.drivers[rank_type]
                
            # 새 드라이버 생성
            driver = get_driver_for_cache(high_performance=True)
            url = f"https://mabinogimobile.nexon.com/Ranking/List?t={rank_type}"
            driver.get(url)
            
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)
            
            if self._verify_page_loaded(driver):
                self.drivers[rank_type] = driver
                self.last_server[rank_type] = None
                logger.info(f"랭킹 타입 {rank_type} 드라이버 재생성 성공")
            else:
                safe_quit_driver_cache(driver)
                logger.error(f"랭킹 타입 {rank_type} 드라이버 재생성 후 검증 실패")
                
        except Exception as e:
            logger.error(f"랭킹 타입 {rank_type} 드라이버 재생성 실패: {e}")
    
    def fast_search(self, server, character_name):
        """
        지속적인 페이지를 활용한 고속 검색
        
        Args:
            server: 서버명
            character_name: 캐릭터명
            
        Returns:
            dict: 모든 랭킹 타입의 검색 결과
        """
        if not self.initialized or not self.running:
            logger.error("랭킹 페이지 캐시가 초기화되지 않음")
            return None
            
        results = {}
        retrieved_at = datetime.now(KST).isoformat()
        
        rank_names = {1: "전투력", 2: "매력", 3: "생활력"}
        
        with self.lock:
            for rank_type in [1, 2, 3]:
                if rank_type not in self.drivers:
                    logger.warning(f"랭킹 타입 {rank_type} 드라이버 없음, 건너뜀")
                    continue
                    
                try:
                    driver = self.drivers[rank_type]
                    
                    # 서버 변경 (필요한 경우만)
                    if self.last_server.get(rank_type) != server:
                        self._select_server_fast(driver, server)
                        self.last_server[rank_type] = server
                        time.sleep(1)
                    
                    # 캐릭터 검색
                    self._search_character_fast(driver, character_name)
                    time.sleep(2)
                    
                    # 결과 파싱
                    page_source = driver.page_source
                    parsed_data = parse_rank_html(page_source, server)
                    
                    results[rank_names[rank_type]] = {
                        "data": parsed_data.get("data", []),
                        "retrieved_at": retrieved_at
                    }
                    
                    logger.info(f"{rank_names[rank_type]} 검색 완료: {len(parsed_data.get('data', []))}개 결과")
                    
                except Exception as e:
                    logger.error(f"랭킹 타입 {rank_type} 검색 중 오류: {e}")
                    results[rank_names[rank_type]] = {
                        "data": [],
                        "retrieved_at": retrieved_at
                    }
                    
                    # 오류 발생시 드라이버 재생성 시도
                    try:
                        self._recreate_driver(rank_type)
                    except:
                        pass
        
        return {
            "ranks": results,
            "retrieved_at": retrieved_at
        }
    
    def _select_server_fast(self, driver, server):
        """JavaScript를 사용한 고속 서버 선택 (full_data.py와 동일한 방식)"""
        try:
            # 서버명을 서버 ID로 매핑
            server_mapping = {
                "데이안": "1", "이데나": "2", "리브네": "3", "나샤": "4", 
                "티엔": "5", "레웨카": "6", "만돈": "7"
            }
            
            server_id = server_mapping.get(server, "1")
            
            # full_data.py와 동일한 방식의 서버 선택
            script = f"""
            var serverBox = document.querySelector('.select_server .select_box');
            if (serverBox) {{
                var option = document.querySelector('li[data-serverid="{server_id}"]');
                if (option) {{
                    option.click();
                }}
            }}
            """
            driver.execute_script(script)
            
        except Exception as e:
            logger.error(f"고속 서버 선택 실패: {e}")
            # 폴백: 기존 방식 사용
            self._select_server_fallback(driver, server)
    
    def _select_server_fallback(self, driver, server):
        """기존 방식의 서버 선택 (폴백)"""
        try:
            # 서버명을 서버 ID로 매핑
            server_mapping = {
                "데이안": "1", "이데나": "2", "리브네": "3", "나샤": "4", 
                "티엔": "5", "레웨카": "6", "만돈": "7"
            }
            
            server_id = server_mapping.get(server, "1")
            
            # 실제 웹사이트 구조에 맞는 셀렉터 사용
            option = driver.find_element(By.CSS_SELECTOR, f'li[data-serverid="{server_id}"]')
            option.click()
                    
        except Exception as e:
            logger.error(f"폴백 서버 선택 실패: {e}")
    
    def _search_character_fast(self, driver, character_name):
        """JavaScript를 사용한 고속 캐릭터 검색 (full_data.py와 동일한 방식)"""
        try:
            # full_data.py와 동일한 셀렉터 사용
            script = f"""
            var searchInput = document.querySelector('input[name="search"]');
            if (searchInput) {{
                searchInput.value = '{character_name}';
                searchInput.dispatchEvent(new Event('input'));
                
                var searchBtn = document.querySelector('button[data-searchtype="search"]');
                if (searchBtn) {{
                    searchBtn.click();
                }}
            }}
            """
            driver.execute_script(script)
            
        except Exception as e:
            logger.error(f"고속 캐릭터 검색 실패: {e}")
            # 폴백: 기존 방식 사용
            self._search_character_fallback(driver, character_name)
    
    def _search_character_fallback(self, driver, character_name):
        """기존 방식의 캐릭터 검색 (폴백)"""
        try:
            # full_data.py와 동일한 셀렉터 사용
            search_input = driver.find_element(By.CSS_SELECTOR, 'input[name="search"]')
            search_input.clear()
            search_input.send_keys(character_name)
            
            search_btn = driver.find_element(By.CSS_SELECTOR, 'button[data-searchtype="search"]')
            search_btn.click()
            
        except Exception as e:
            logger.error(f"폴백 캐릭터 검색 실패: {e}")
    
    def shutdown(self):
        """캐시 시스템 종료"""
        with self.lock:
            self.running = False
            logger.info("랭킹 페이지 캐시 종료 중...")
            
            for rank_type, driver in self.drivers.items():
                try:
                    safe_quit_driver_cache(driver)
                    logger.info(f"랭킹 타입 {rank_type} 드라이버 종료 완료")
                except Exception as e:
                    logger.error(f"랭킹 타입 {rank_type} 드라이버 종료 실패: {e}")
            
            self.drivers.clear()
            self.last_server.clear()
            self.initialized = False
            
            # 메모리 정리
            gc.collect()
            logger.info("랭킹 페이지 캐시 종료 완료")

# 전역 인스턴스
_ranking_cache = None

def get_ranking_cache():
    """전역 랭킹 캐시 인스턴스 반환"""
    global _ranking_cache
    if _ranking_cache is None:
        _ranking_cache = PersistentRankingCache()
    return _ranking_cache

def initialize_ranking_cache():
    """랭킹 캐시 초기화"""
    cache = get_ranking_cache()
    return cache.initialize()

def shutdown_ranking_cache():
    """랭킹 캐시 종료"""
    global _ranking_cache
    if _ranking_cache:
        _ranking_cache.shutdown()
        _ranking_cache = None