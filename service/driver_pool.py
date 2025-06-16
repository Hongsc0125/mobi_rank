import threading
import time
import logging
import queue
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller
from service.db_session import get_current_time
from datetime import timedelta
import os

# 싱글톤 락으로 chromedriver 설치를 한 번만 보장
_chromedriver_installed = False
_chromedriver_install_lock = threading.Lock()
_chromedriver_path = None

def install_chromedriver_once():
    global _chromedriver_installed, _chromedriver_path
    with _chromedriver_install_lock:
        if not _chromedriver_installed:
            _chromedriver_path = chromedriver_autoinstaller.install()
            _chromedriver_installed = True
    return _chromedriver_path

# 서버 시작 시 한 번만 설치
install_chromedriver_once()

# 로그 설정
logger = logging.getLogger(__name__)

class ChromeDriverPool:
    """
    크롬 드라이버 풀 관리 클래스
    - 3개의 드라이버를 상시 유지
    - 요청시 사용 가능한 드라이버 반환
    - 사용 후 풀에 다시 반환
    - 주기적으로 드라이버 상태 체크 및 재생성
    """
    def __init__(self, pool_size=3):
        self.pool_size = pool_size
        self.drivers = []  # 사용 가능한 드라이버 리스트
        self.in_use = {}   # 사용 중인 드라이버 {드라이버: 마지막 사용 시간}
        self.lock = threading.RLock()  # 쓰레드 안전을 위한 락
        self.driver_queue = queue.Queue(maxsize=pool_size)  # 드라이버 큐
        self.initialize_drivers()
        
        # 상태 체크 스레드 시작
        self.check_thread = threading.Thread(target=self._health_check, daemon=True)
        self.check_thread.name = "chrome-driver-health-check"
        self.check_thread.start()
        
    def initialize_drivers(self):
        """초기 드라이버 풀 생성"""
        logger.info(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 크롬 드라이버 풀({self.pool_size}개) 초기화 시작")
        with self.lock:
            # 새 드라이버 생성
            for _ in range(self.pool_size):
                try:
                    driver = self._create_new_driver()
                    self.driver_queue.put(driver)
                    logger.info(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 생성 완료")
                except Exception as e:
                    logger.error(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 생성 실패: {e}")
        
        logger.info(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 크롬 드라이버 풀 초기화 완료")
    
    def _create_new_driver(self):
        """새 크롬 드라이버 인스턴스 생성"""
        # install_chromedriver_once()은 이미 서버 시작 시 한 번만 호출됨
        global _chromedriver_path
        # 드라이버 설치 경로가 없으면 재시도(백오프)
        retry = 0
        while _chromedriver_path is None or not os.path.exists(_chromedriver_path):
            logger.warning("chromedriver 경로가 유효하지 않아 재설치 시도")
            install_chromedriver_once()
            retry += 1
            time.sleep(min(2 ** retry, 10))  # 점진적 대기 (최대 10초)
            if retry > 5:
                raise RuntimeError("chromedriver 설치 실패: 경로 없음")

        opts = Options()
        
        # Chrome 안정성 개선 옵션
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
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-images")  # 속도 향상
        opts.add_argument("--disable-web-security")
        opts.add_argument("--disable-features=TranslateUI")
        opts.add_argument("--disable-ipc-flooding-protection")
        
        # 메모리 누수 방지 옵션 (기능은 유지)
        opts.add_argument("--max_old_space_size=4096")  # 기존 4GB 유지
        opts.add_argument("--memory-pressure-off")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-backgrounding-occluded-windows")
        
        # 로그 완전 차단
        opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)
        opts.add_experimental_option("detach", False)
        
        # User-Agent 설정
        opts.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # 로그 완전 차단을 위한 Service 설정
        service = Service(executable_path=_chromedriver_path)
        service.log_path = "NUL" if os.name == "nt" else "/dev/null"
        
        # 재시도 로직 추가
        max_retries = 3
        for attempt in range(max_retries):
            try:
                driver = webdriver.Chrome(service=service, options=opts)
                
                # DOM 조작을 위한 타임아웃 설정
                driver.set_page_load_timeout(30)
                driver.implicitly_wait(10)
                
                # webdriver 속성 숨기기 (anti-detection)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                
                return driver
            except Exception as e:
                logger.warning(f"드라이버 생성 시도 {attempt + 1}/{max_retries} 실패: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))  # 점진적 대기
                else:
                    raise
    
    def get_driver(self, timeout=30):
        """사용 가능한 드라이버 가져오기"""
        try:
            # 대기열에서 드라이버 가져오기 (타임아웃 설정)
            driver = self.driver_queue.get(block=True, timeout=timeout)
            
            # 드라이버가 정상 작동하는지 확인
            if not self._is_driver_alive(driver):
                logger.warning(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 비정상 드라이버 감지, 새 드라이버 생성")
                try:
                    self._quit_driver(driver)
                except:
                    pass
                driver = self._create_new_driver()
            
            # 사용 중 목록에 추가
            with self.lock:
                self.in_use[driver] = get_current_time()
                
            return driver
        except queue.Empty:
            logger.error(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 풀 타임아웃: 사용 가능한 드라이버 없음")
            # 긴급 새 드라이버 생성
            return self._create_new_driver()
    
    def release_driver(self, driver):
        """드라이버 풀에 반환"""
        with self.lock:
            # 사용 중 목록에서 제거
            if driver in self.in_use:
                del self.in_use[driver]
                
            # 드라이버가 정상인지 확인
            if self._is_driver_alive(driver):
                # 큐에 다시 넣기
                try:
                    # 이미 꽉 차 있으면 큐에 넣지 않고 종료
                    if self.driver_queue.qsize() < self.pool_size:
                        self.driver_queue.put_nowait(driver)
                    else:
                        self._quit_driver(driver)
                except queue.Full:
                    self._quit_driver(driver)
            else:
                # 비정상 드라이버는 종료하고 새로 생성해서 추가
                self._quit_driver(driver)
                try:
                    new_driver = self._create_new_driver()
                    self.driver_queue.put_nowait(new_driver)
                except:
                    logger.error(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 대체 드라이버 생성 실패")
    
    def _is_driver_alive(self, driver):
        """드라이버가 정상 작동하는지 체크"""
        try:
            # 간단한 명령 실행으로 상태 확인
            driver.current_url
            return True
        except:
            return False
    
    def _quit_driver(self, driver):
        """드라이버 안전하게 종료"""
        try:
            driver.quit()
        except Exception as e:
            logger.warning(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 종료 오류: {e}")
        finally:
            # 좀비 프로세스 방지를 위한 추가 정리
            try:
                import psutil
                import os
                for proc in psutil.process_iter(['pid', 'name']):
                    if 'chrome' in proc.info['name'].lower() and proc.info['pid'] != os.getpid():
                        try:
                            proc.terminate()
                        except:
                            pass
            except:
                pass
    
    def _health_check(self):
        """정기적으로 드라이버 상태 체크 및 필요시 재생성"""
        refresh_cycle = 0
        while True:
            try:
                time.sleep(60)  # 1분마다 체크
                refresh_cycle += 1
                
                with self.lock:
                    current_time = get_current_time()
                    
                    # 15분마다 주기적으로 모든 드라이버 새로고침 (메모리 누수 방지)
                    if refresh_cycle >= 15:  # 15분마다 새로고침
                        logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 주기적 새로고침 시작")
                        refresh_cycle = 0
                        
                        # 모든 드라이버 새로고침
                        old_drivers = []
                        while not self.driver_queue.empty():
                            try:
                                old_driver = self.driver_queue.get_nowait()
                                old_drivers.append(old_driver)
                            except:
                                break
                        
                        # 기존 드라이버 정리
                        for driver in old_drivers:
                            self._quit_driver(driver)
                        
                        # 새 드라이버 생성
                        for _ in range(self.pool_size):
                            try:
                                new_driver = self._create_new_driver()
                                self.driver_queue.put_nowait(new_driver)
                            except Exception as e:
                                logger.error(f"주기적 새로고침 중 드라이버 생성 실패: {e}")
                        
                        logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 주기적 새로고침 완료")
                        continue
                    
                    # 사용 중인 드라이버 체크
                    # 20분 이상 사용 중인 드라이버는 종료 예약 (30분에서 20분으로 단축)
                    stale_drivers = []
                    for driver, last_used in self.in_use.items():
                        if current_time - last_used > timedelta(minutes=20):
                            logger.warning(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 오래된 드라이버 감지 (20분 이상 사용)")
                            stale_drivers.append(driver)
                    
                    # 오래된 드라이버 정리
                    for driver in stale_drivers:
                        if driver in self.in_use:
                            del self.in_use[driver]
                        self._quit_driver(driver)
                    
                    # 사용 가능한 드라이버 수 확인
                    available_count = self.driver_queue.qsize()
                    
                    # 부족한 드라이버 생성
                    if available_count < self.pool_size:
                        # logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 사용 가능한 드라이버: {available_count}/{self.pool_size}, 추가 생성 시작")
                        for _ in range(self.pool_size - available_count):
                            try:
                                driver = self._create_new_driver()
                                self.driver_queue.put_nowait(driver)
                                # logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 추가 드라이버 생성 완료")
                            except Exception as e:
                                logger.error(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 추가 드라이버 생성 실패: {e}")
                                break
                            
                # logger.info(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 크롬 드라이버 상태 체크 완료, 사용 가능: {self.driver_queue.qsize()}")
                
            except Exception as e:
                logger.error(f"[{get_current_time().strftime('%Y-%m-%d %H:%M:%S KST')}] 드라이버 상태 체크 오류: {e}")

# 싱글톤 인스턴스
_driver_pool = None

def get_driver_pool():
    """드라이버 풀 싱글톤 인스턴스 반환"""
    global _driver_pool
    if _driver_pool is None:
        _driver_pool = ChromeDriverPool(pool_size=2)  # 기존 2개 유지
    return _driver_pool
