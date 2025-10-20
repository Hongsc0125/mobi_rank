import threading
import time
import logging
import queue
import undetected_chromedriver as uc
from service.db_session import get_current_time
from datetime import timedelta
import os

# 로그 설정
logger = logging.getLogger(__name__)

class ChromeDriverPool:
    """
    크롬 드라이버 풀 관리 클래스
    - 7개의 드라이버를 상시 유지
    - 요청시 사용 가능한 드라이버 반환
    - 사용 후 풀에 다시 반환
    - 주기적으로 드라이버 상태 체크 및 재생성
    """
    def __init__(self, pool_size=7):
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
        """새 크롬 드라이버 인스턴스 생성 (undetected-chromedriver 사용)"""
        # 재시도 로직 추가
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 각 시도마다 새로운 ChromeOptions 객체 생성 (재사용 방지)
                opts = uc.ChromeOptions()

                # Chrome 안정성 개선 옵션
                opts.add_argument("--headless=new")
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--disable-gpu")
                opts.add_argument("--window-size=1920,1080")
                opts.add_argument("--disable-gpu-sandbox")
                opts.add_argument("--silent")
                opts.add_argument("--log-level=3")
                opts.add_argument("--disable-images")  # 속도 향상
                opts.add_argument("--disable-features=TranslateUI")
                opts.add_argument("--disable-ipc-flooding-protection")

                # 포트 충돌 방지 옵션 - 완전 비활성화
                opts.add_argument("--disable-dev-tools")
                opts.add_argument("--disable-background-networking")
                opts.add_argument("--disable-default-apps")
                opts.add_argument("--disable-sync")
                opts.add_argument("--disable-translate")
                opts.add_argument("--disable-plugins")
                opts.add_argument("--disable-plugins-discovery")

                # 메모리 누수 방지 옵션 (기능은 유지)
                opts.add_argument("--max_old_space_size=2048")  # 4GB->2GB로 축소
                opts.add_argument("--memory-pressure-off")
                opts.add_argument("--disable-background-timer-throttling")
                opts.add_argument("--disable-renderer-backgrounding")
                opts.add_argument("--disable-backgrounding-occluded-windows")
                # opts.add_argument("--single-process")  # 안정성 문제로 제거

                # 프로필 디렉토리 권한 문제 해결 - 임시 디렉토리 사용
                import tempfile
                temp_dir = tempfile.mkdtemp(prefix="chrome_profile_")
                opts.add_argument(f"--user-data-dir={temp_dir}")
                opts.add_argument("--no-first-run")
                opts.add_argument("--incognito")  # 시크릿 모드로 프로필 저장 방지
                opts.add_argument("--disk-cache-size=0")  # 디스크 캐시 비활성화
                opts.add_argument("--media-cache-size=0")  # 미디어 캐시 비활성화

                # 로그 완전 차단 (experimental options 제거 - Chrome 136+에서 지원 안함)
                # opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
                # opts.add_experimental_option('useAutomationExtension', False)

                # User-Agent 설정
                opts.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36')

                # undetected-chromedriver 생성 (자동으로 Bot 감지 회피)
                driver = uc.Chrome(
                    options=opts,
                    use_subprocess=False
                    # version_main 생략 - 자동 감지
                )

                # DOM 조작을 위한 타임아웃 설정
                driver.set_page_load_timeout(30)
                driver.implicitly_wait(10)

                # webdriver 속성 숨기기 (anti-detection)
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                # 연결 테스트 (간단한 속성 확인만)
                _ = driver.session_id

                return driver
            except Exception as e:
                logger.warning(f"드라이버 생성 시도 {attempt + 1}/{max_retries} 실패: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))  # 점진적 대기 (5초씩 증가)로 여유시간 확보
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
            # 종료 후 추가 대기 시간으로 완전한 정리 보장
            time.sleep(3)  # 5초에서 3초로 단축
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
        while True:
            try:
                time.sleep(180)  # 3분마다 체크
                
                with self.lock:
                    current_time = get_current_time()
                    
                    # 죽은 드라이버만 안전하게 정리
                    stale_drivers = []
                    for driver, last_used in self.in_use.items():
                        # 드라이버가 실제로 살아있는지 먼저 확인
                        if not self._is_driver_alive(driver):
                            logger.warning(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 죽은 드라이버 감지 및 정리")
                            stale_drivers.append(driver)
                        # 2시간 이상 사용 중인 드라이버는 건강 상태만 체크
                        elif current_time - last_used > timedelta(hours=2):
                            logger.info(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S KST')}] 장기 사용 드라이버 건강 상태 체크")
                    
                    # 죽은 드라이버만 정리
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
        _driver_pool = ChromeDriverPool(pool_size=7)  # 드라이버 풀 크기 확장
    return _driver_pool
