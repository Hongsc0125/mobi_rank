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
    
    # Bot 감지 회피 옵션
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36")
    
    # 로그 차단
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    
    if high_performance:
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
                    
                    # 페이지 로딩 대기 및 Cloudflare 체크
                    wait = WebDriverWait(driver, 30)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    
                    # Cloudflare 보호 페이지 대기 (최대 15초)
                    for retry in range(15):
                        if "잠시만 기다리십시오" in driver.title or "Just a moment" in driver.title:
                            logger.info(f"{rank_name} - Cloudflare 보호 페이지 감지, 대기 중... ({retry + 1}/15)")
                            time.sleep(1)
                        else:
                            break
                    
                    # 추가 대기 시간
                    time.sleep(5)
                    
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
                
                # 다음 드라이버 생성 전 잠시 대기 (동시 요청 방지)
                if rank_type < 3:  # 마지막이 아니면 대기
                    time.sleep(10)
            
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
            # 먼저 타이틀로 보호 페이지 확인
            if "잠시만 기다리십시오" in driver.title or "Just a moment" in driver.title:
                logger.warning(f"보호 페이지 감지: {driver.title}")
                return False
            
            # 랭킹 리스트 요소 확인 (더 많은 셀렉터 시도)
            selectors = [
                "div[data-mm-rankinglist]",
                "ul.list", 
                ".ranking_list",
                ".list_area",
                "[data-mm-rankinglist]"
            ]
            
            for selector in selectors:
                try:
                    element = driver.find_element(By.CSS_SELECTOR, selector)
                    logger.info(f"페이지 로드 검증 성공: {selector} 요소 발견")
                    return True
                except:
                    continue
                    
            # 모든 셀렉터 실패 시 페이지 소스 체크
            page_source = driver.page_source
            if "ranking" in page_source.lower() and "list" in page_source.lower():
                logger.info("페이지 소스에서 랭킹 관련 내용 발견")
                return True
                
            raise Exception("모든 검증 방법 실패")
            
        except Exception as e:
            logger.error(f"페이지 로드 검증 실패: {e}")
            logger.error(f"현재 URL: {driver.current_url}")
            logger.error(f"페이지 타이틀: {driver.title}")
            # 페이지 소스 일부도 로깅 (처음 200자)
            try:
                source_preview = driver.page_source[:200].replace('\n', ' ')
                logger.error(f"페이지 소스 미리보기: {source_preview}")
            except:
                pass
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
        지속적인 페이지를 활용한 고속 검색 - 캐시된 드라이버에서 검색만 수행
        
        Args:
            server: 서버명
            character_name: 캐릭터명
            
        Returns:
            dict: 기존 fetch_all_ranks와 동일한 구조의 결과
        """
        if not self.initialized or not self.running:
            logger.error("랭킹 페이지 캐시가 초기화되지 않음")
            return None
            
        results = {}
        retrieved_at = datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')
        
        rank_names = {1: "전투력", 2: "매력", 3: "생활력"}
        
        with self.lock:
            for rank_type in [1, 2, 3]:
                if rank_type not in self.drivers:
                    logger.warning(f"랭킹 타입 {rank_type} 드라이버 없음, 새로 생성 시도...")
                    try:
                        # 드라이버가 없으면 새로 생성
                        self._recreate_driver(rank_type)
                        if rank_type not in self.drivers:
                            logger.error(f"랭킹 타입 {rank_type} 드라이버 생성 실패, 건너뜀")
                            results[rank_names[rank_type]] = {
                                "type": rank_names[rank_type],
                                "data": [],
                                "retrieved_at": retrieved_at
                            }
                            continue
                        logger.info(f"랭킹 타입 {rank_type} 드라이버 생성 성공")
                    except Exception as e:
                        logger.error(f"랭킹 타입 {rank_type} 드라이버 생성 중 오류: {e}")
                        results[rank_names[rank_type]] = {
                            "type": rank_names[rank_type],
                            "data": [],
                            "retrieved_at": retrieved_at
                        }
                        continue
                    
                try:
                    driver = self.drivers[rank_type]
                    logger.info(f"{rank_names[rank_type]} 캐시된 페이지에서 검색 시작")
                    
                    # 서버 변경 (필요한 경우만)
                    if self.last_server.get(rank_type) != server:
                        logger.info(f"{rank_names[rank_type]} 서버 변경: {self.last_server.get(rank_type)} -> {server}")
                        self._select_server_fast(driver, server)
                        self.last_server[rank_type] = server
                        # 서버 변경 대기 시간은 _select_server_fast 내부에서 처리됨
                    
                    # 캐릭터 검색
                    logger.info(f"{rank_names[rank_type]} 캐릭터 '{character_name}' 검색 시작")
                    self._search_character_fast(driver, character_name)
                    
                    # 랭킹 데이터 로딩 대기 (full_data.py와 동일)
                    from selenium.webdriver.support.ui import WebDriverWait
                    from selenium.webdriver.support import expected_conditions as EC
                    from selenium.webdriver.common.by import By
                    
                    wait = WebDriverWait(driver, 20)
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-mm-rankinglist], ul.list")))
                    
                    # 추가 대기 시간 (DOM 업데이트 완료 대기)
                    time.sleep(2)
                    
                    # 결과 파싱
                    page_source = driver.page_source
                    parsed_data = parse_rank_html(page_source)
                    
                    results[rank_names[rank_type]] = {
                        "type": rank_names[rank_type],
                        "data": parsed_data,
                        "retrieved_at": retrieved_at
                    }
                    
                    logger.info(f"{rank_names[rank_type]} 검색 완료: {len(parsed_data)}개 결과")
                    
                except Exception as e:
                    logger.error(f"랭킹 타입 {rank_type} 검색 중 오류: {e}")
                    results[rank_names[rank_type]] = {
                        "type": rank_names[rank_type],
                        "data": [],
                        "error": str(e),
                        "retrieved_at": retrieved_at
                    }
                    
                    # 오류 발생시 드라이버 재생성 시도
                    try:
                        self._recreate_driver(rank_type)
                    except:
                        pass
        
        # 기존 fetch_all_ranks와 동일한 구조로 반환
        return {
            "character": character_name,
            "server": server,
            "ranks": results,
            "retrieved_at": retrieved_at
        }
    
    def _select_server_fast(self, driver, server):
        """balanced_ranking_crawler와 동일한 방식의 서버 선택 (full_data.py 사용)"""
        try:
            from service.full_data import select_server_option
            
            logger.info(f"서버 선택 시작: {server}")
            select_server_option(driver, server)
            time.sleep(2)  # full_data.py와 동일한 대기 시간
            logger.info(f"서버 선택 완료: {server}")
            
        except Exception as e:
            logger.error(f"서버 선택 실패: {e}")
            # 폴백: 기존 방식 사용
            self._select_server_fallback(driver, server)
    
    def _select_server_fallback(self, driver, server):
        """기존 방식의 서버 선택 (폴백)"""
        try:
            # 서버명을 서버 ID로 매핑 (full_data.py와 동일)
            server_mapping = {
                "데이안": "1", "아이라": "2", "던컨": "3", "알리사": "4",
                "메이븐": "5", "라사": "6", "칼릭스": "7"
            }
            
            server_id = server_mapping.get(server, "1")
            
            # 실제 웹사이트 구조에 맞는 셀렉터 사용
            option = driver.find_element(By.CSS_SELECTOR, f'li[data-serverid="{server_id}"]')
            option.click()
                    
        except Exception as e:
            logger.error(f"폴백 서버 선택 실패: {e}")
    
    def _search_character_fast(self, driver, character_name):
        """balanced_ranking_crawler와 동일한 방식의 캐릭터 검색 (full_data.py 사용)"""
        try:
            from service.full_data import search_character
            
            logger.info(f"캐릭터 검색 시작: {character_name}")
            search_character(driver, character_name)
            time.sleep(3)  # full_data.py와 동일한 대기 시간
            logger.info(f"캐릭터 검색 완료: {character_name}")
            
        except Exception as e:
            logger.error(f"캐릭터 검색 실패: {e}")
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
    
    def _get_current_selected_server(self, driver):
        """현재 선택된 서버명을 가져옵니다"""
        try:
            # 서버 선택 박스에서 현재 선택된 서버 텍스트 확인 (더 정확한 셀렉터 사용)
            script = """
            var serverBox = document.querySelector('.select_server .select_box .selected');
            if (serverBox) {
                var selectedText = serverBox.textContent || serverBox.innerText;
                return selectedText.trim();
            }
            
            // 대안: data-serverid로 현재 선택된 서버 찾기
            var selectedOption = document.querySelector('.select_server li.selected');
            if (selectedOption) {
                var serverId = selectedOption.getAttribute('data-serverid');
                var serverMap = {'1': '데이안', '2': '아이라', '3': '던컨', '4': '알리사', '5': '메이븐', '6': '라사', '7': '칼릭스'};
                return serverMap[serverId] || null;
            }
            
            return null;
            """
            
            selected_server = driver.execute_script(script)
            logger.debug(f"현재 선택된 서버: {selected_server}")
            return selected_server
            
        except Exception as e:
            logger.error(f"현재 서버 확인 실패: {e}")
            return None
    
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