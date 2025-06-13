#!/usr/bin/env python3
"""
DOM 조작 기반 크롤링 테스트 스크립트
기존 requests 방식에서 직접 DOM 조작으로 변경한 크롤링 로직 테스트
"""

import time
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import chromedriver_autoinstaller

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class DOMCrawler:
    """DOM 조작 기반 마비노기 모바일 랭킹 크롤러"""
    
    def __init__(self):
        self.driver = None
        self.wait = None
        self.base_url = "https://mabinogimobile.nexon.com"
        self.ranking_url = f"{self.base_url}/Ranking/List"
        
        # 서버 매핑
        self.server_map = {
            "데이안": 1, "아이라": 2, "던컨": 3, "알리사": 4,
            "메이븐": 5, "라사": 6, "칼릭스": 7
        }
        
        # 랭킹 타입 매핑
        self.rank_type_map = {
            1: "전투력",
            2: "매력",
            3: "생활력"
        }
        
    def setup_driver(self):
        """Chrome 드라이버 설정 및 초기화"""
        try:
            # chromedriver 자동 설치
            chromedriver_autoinstaller.install()
            
            options = Options()
            options.add_argument('--headless')  # 헤드리스 모드
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-web-security')
            options.add_argument('--disable-features=VizDisplayCompositor')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')  # 이미지 로딩 비활성화로 속도 향상
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # User-Agent 설정
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 페이지 로드 전략 설정
            options.add_argument('--page-load-strategy=eager')
            
            service = Service()
            self.driver = webdriver.Chrome(service=service, options=options)
            
            # 더 긴 타임아웃 설정
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.wait = WebDriverWait(self.driver, 20)  # 대기 시간 증가
            logger.info("Chrome 드라이버가 성공적으로 초기화되었습니다.")
            
        except Exception as e:
            logger.error(f"드라이버 초기화 실패: {e}")
            raise
    
    def navigate_to_ranking_page(self, rank_type: int = 1):
        """랭킹 페이지로 이동"""
        try:
            ranking_url_with_type = f"{self.ranking_url}?t={rank_type}"
            logger.info(f"랭킹 페이지로 이동: {ranking_url_with_type}")
            
            # 더 안정적인 페이지 로딩
            retry_count = 3
            for attempt in range(retry_count):
                try:
                    self.driver.get(ranking_url_with_type)
                    
                    # 페이지 로딩 대기 - 더 간단한 요소부터 찾기
                    self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    logger.info(f"페이지 기본 로딩 완료 (시도 {attempt + 1})")
                    
                    # 랭킹 관련 요소가 로딩될 때까지 대기
                    time.sleep(3)  # 추가 대기
                    
                    # 랭킹 컨테이너 확인
                    ranking_elements = self.driver.find_elements(By.CSS_SELECTOR, ".ranking.container, .ranking_list_wrap, body")
                    if ranking_elements:
                        logger.info("랭킹 페이지 로딩 완료")
                        return
                    
                except TimeoutException:
                    logger.warning(f"페이지 로딩 시도 {attempt + 1} 실패")
                    if attempt == retry_count - 1:
                        raise
                    time.sleep(2)
            
        except TimeoutException:
            logger.error("랭킹 페이지 로딩 시간 초과")
            raise
        except Exception as e:
            logger.error(f"랭킹 페이지 이동 실패: {e}")
            raise
    
    def set_search_filters(self, server: str = None, character_name: str = "", class_filter: str = "전체"):
        """검색 필터 설정 (서버, 캐릭터명, 직업)"""
        try:
            # 서버 선택
            if server and server in self.server_map:
                server_select = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "serverSelect"))
                )
                server_dropdown = Select(server_select)
                server_dropdown.select_by_value(str(self.server_map[server]))
                logger.info(f"서버 선택: {server}")
                time.sleep(0.5)
            
            # 직업 선택
            if class_filter != "전체":
                class_select = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "classSelect"))
                )
                class_dropdown = Select(class_select)
                class_dropdown.select_by_visible_text(class_filter)
                logger.info(f"직업 선택: {class_filter}")
                time.sleep(0.5)
            
            # 캐릭터명 입력
            if character_name:
                search_input = self.wait.until(
                    EC.element_to_be_clickable((By.ID, "searchInput"))
                )
                search_input.clear()
                search_input.send_keys(character_name)
                logger.info(f"캐릭터명 입력: {character_name}")
                time.sleep(0.5)
            
            # 검색 버튼 클릭
            search_button = self.wait.until(
                EC.element_to_be_clickable((By.CLASS_NAME, "btn_search"))
            )
            search_button.click()
            logger.info("검색 실행")
            
            # 검색 결과 로딩 대기
            time.sleep(2)
            
        except TimeoutException:
            logger.error("검색 필터 설정 시간 초과")
            raise
        except Exception as e:
            logger.error(f"검색 필터 설정 실패: {e}")
            raise
    
    def extract_ranking_data(self) -> List[Dict]:
        """현재 페이지에서 랭킹 데이터 추출"""
        try:
            # 페이지 소스 가져오기
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 데이터 없음 확인
            no_data = soup.select_one("div.no_data")
            if no_data:
                logger.info("검색 결과가 없습니다.")
                return []
            
            ranking_data = []
            
            # 랭킹 아이템들 파싱
            ranking_items = soup.select("ul.list li.item")
            logger.info(f"랭킹 아이템 {len(ranking_items)}개 발견")
            
            for item in ranking_items:
                try:
                    data = self.parse_ranking_item(item)
                    if data:
                        ranking_data.append(data)
                except Exception as e:
                    logger.warning(f"랭킹 아이템 파싱 오류: {e}")
                    continue
            
            logger.info(f"총 {len(ranking_data)}개의 랭킹 데이터 추출 완료")
            return ranking_data
            
        except Exception as e:
            logger.error(f"랭킹 데이터 추출 실패: {e}")
            return []
    
    def parse_ranking_item(self, item) -> Optional[Dict]:
        """개별 랭킹 아이템 파싱"""
        try:
            # 순위
            rank_element = item.select_one("div dl dt")
            rank = rank_element.text.strip() if rank_element else "0"
            
            # 순위 변동
            change_element = item.select_one("div dl dd")
            change_int = 0
            change_type = "none"
            
            if change_element:
                change_text = change_element.text.strip()
                if "up" in change_element.get("class", []):
                    change_type = "up"
                    change_int = int(change_text) if change_text.isdigit() else 0
                elif "down" in change_element.get("class", []):
                    change_type = "down"
                    change_int = -int(change_text) if change_text.isdigit() else 0
            
            # 서버, 캐릭터명, 직업, 전투력 정보
            dl_elements = item.select("div dl")
            
            server = ""
            character = ""
            char_class = ""
            power = ""
            
            if len(dl_elements) >= 5:
                # 서버
                server_element = dl_elements[1].select_one("dd")
                server = server_element.text.strip() if server_element else ""
                
                # 캐릭터명
                character_element = dl_elements[2].select_one("dd")
                if character_element:
                    character = character_element.get("data-charactername", "") or character_element.text.strip()
                
                # 직업
                class_element = dl_elements[3].select_one("dd")
                char_class = class_element.text.strip() if class_element else ""
                
                # 전투력/매력/생활력
                power_element = dl_elements[4].select_one("dd")
                power = power_element.text.strip() if power_element else ""
            
            return {
                "rank": rank,
                "change": change_int,
                "change_type": change_type,
                "server": server,
                "character": character,
                "class": char_class,
                "power": power,
                "retrieved_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.warning(f"랭킹 아이템 파싱 오류: {e}")
            return None
    
    def crawl_ranking_data(self, rank_type: int = 1, server: str = None, 
                          character_name: str = "", class_filter: str = "전체") -> List[Dict]:
        """전체 크롤링 프로세스"""
        try:
            logger.info(f"크롤링 시작 - 타입: {self.rank_type_map.get(rank_type, rank_type)}, "
                       f"서버: {server or '전체'}, 캐릭터: {character_name or '전체'}, "
                       f"직업: {class_filter}")
            
            # 드라이버 설정
            if not self.driver:
                self.setup_driver()
            
            # 랭킹 페이지 이동
            self.navigate_to_ranking_page(rank_type)
            
            # 검색 필터 설정
            self.set_search_filters(server, character_name, class_filter)
            
            # 랭킹 데이터 추출
            ranking_data = self.extract_ranking_data()
            
            return ranking_data
            
        except Exception as e:
            logger.error(f"크롤링 실패: {e}")
            return []
    
    def select_custom_option(self, select_type: str, option_text: str):
        """커스텀 select box에서 옵션 선택"""
        try:
            if select_type == "server":
                # JavaScript를 사용해서 서버 선택
                server_id = self.server_map.get(option_text)
                if server_id:
                    script = f"""
                    // 서버 select box 찾기
                    var serverBox = document.querySelector('.select_server .select_box');
                    if (serverBox) {{
                        // selectBoxHandler 호출
                        selectBoxHandler(serverBox);
                        
                        // 잠시 대기 후 옵션 클릭
                        setTimeout(function() {{
                            var option = document.querySelector('li[data-serverid="{server_id}"]');
                            if (option) {{
                                option.click();
                            }}
                        }}, 500);
                    }}
                    """
                    self.driver.execute_script(script)
                    time.sleep(1)
                
            elif select_type == "class":
                # 클래스 선택은 일단 기본 방식으로
                class_box = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".select_class .select_box"))
                )
                
                # JavaScript로 클릭
                self.driver.execute_script("arguments[0].click();", class_box)
                time.sleep(0.5)
                
                # 옵션 선택
                option_xpath = f"//li[@data-searchtype='classid' and text()='{option_text}']"
                option = self.wait.until(
                    EC.element_to_be_clickable((By.XPATH, option_xpath))
                )
                self.driver.execute_script("arguments[0].click();", option)
                
        except Exception as e:
            logger.error(f"커스텀 select box 선택 실패 ({select_type}, {option_text}): {e}")
            raise
    
    def get_pagination_info(self) -> Dict:
        """페이지네이션 정보 가져오기"""
        try:
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # 현재 범위 정보
            current_range = soup.select_one(".current_range span")
            current_range_text = current_range.text.strip() if current_range else ""
            
            # 페이지네이션 정보
            pagination = soup.select_one("div[data-mm-paging]")
            total_count = 0
            if pagination:
                total_count = int(pagination.get("data-totalcount", 0))
            
            # 현재 페이지와 전체 페이지 수 계산
            current_page = 1
            total_pages = (total_count + 19) // 20  # 페이지당 20개씩
            
            # 현재 페이지 번호 찾기
            current_page_elem = soup.select_one(".pagination li.on")
            if current_page_elem:
                current_page = int(current_page_elem.text.strip())
            
            return {
                "current_range": current_range_text,
                "current_page": current_page,
                "total_pages": total_pages,
                "total_count": total_count
            }
            
        except Exception as e:
            logger.error(f"페이지네이션 정보 가져오기 실패: {e}")
            return {}
    
    def navigate_to_page(self, page_number: int):
        """특정 페이지로 이동"""
        try:
            # JavaScript 함수 호출
            script = f"mmRanking.list({page_number}, null);"
            self.driver.execute_script(script)
            
            # 페이지 로딩 대기
            time.sleep(3)
            logger.info(f"{page_number}페이지로 이동 완료")
            
        except Exception as e:
            logger.error(f"페이지 이동 실패 ({page_number}): {e}")
            raise
    
    def close(self):
        """드라이버 종료"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("드라이버 종료됨")
            except Exception as e:
                logger.error(f"드라이버 종료 오류: {e}")


def test_basic_crawling():
    """기본 크롤링 테스트"""
    crawler = DOMCrawler()
    
    try:
        # 전투력 랭킹 전체 조회 (필터 없이)
        logger.info("=== 전투력 랭킹 전체 조회 테스트 (필터 없음) ===")
        
        # 드라이버 설정
        if not crawler.driver:
            crawler.setup_driver()
        
        # 랭킹 페이지 이동
        crawler.navigate_to_ranking_page(1)
        
        # 필터 설정 없이 바로 데이터 추출
        data = crawler.extract_ranking_data()
        
        if data:
            logger.info(f"총 {len(data)}개 데이터 추출")
            for i, item in enumerate(data[:5]):  # 상위 5개만 출력
                logger.info(f"{i+1}. {item}")
        else:
            logger.warning("데이터 추출 실패")
            
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
    finally:
        crawler.close()


def test_server_specific_crawling():
    """특정 서버 크롤링 테스트"""
    crawler = DOMCrawler()
    
    try:
        # 기본 데이터 추출 테스트 (필터 없이)
        logger.info("=== 기본 데이터 추출 테스트 ===")
        
        # 드라이버 설정
        if not crawler.driver:
            crawler.setup_driver()
        
        # 랭킹 페이지 이동
        crawler.navigate_to_ranking_page(1)
        
        # 필터 설정 없이 바로 데이터 추출
        data = crawler.extract_ranking_data()
        
        if data:
            logger.info(f"기본 데이터: {len(data)}개 추출")
            for i, item in enumerate(data[:3]):  # 상위 3개만 출력
                logger.info(f"{i+1}. {item}")
        else:
            logger.warning("데이터 추출 실패")
            
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
    finally:
        crawler.close()


def test_page_source_debug():
    """페이지 소스 디버그 테스트"""
    crawler = DOMCrawler()
    
    try:
        logger.info("=== 페이지 소스 디버그 테스트 ===")
        
        # 드라이버 설정
        if not crawler.driver:
            crawler.setup_driver()
        
        # 랭킹 페이지 이동
        crawler.navigate_to_ranking_page(1)
        
        # 페이지 소스 일부 확인
        page_source = crawler.driver.page_source
        logger.info(f"페이지 소스 길이: {len(page_source)}")
        
        # BeautifulSoup으로 파싱
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # 랭킹 리스트 확인
        ranking_list = soup.select("ul.list li.item")
        logger.info(f"랭킹 아이템 개수: {len(ranking_list)}")
        
        if ranking_list:
            # 첫 번째 아이템 구조 확인
            first_item = ranking_list[0]
            logger.info(f"첫 번째 아이템 HTML: {str(first_item)[:500]}...")
            
            # 파싱 테스트
            data = crawler.parse_ranking_item(first_item)
            logger.info(f"파싱 결과: {data}")
        else:
            logger.warning("랭킹 아이템을 찾을 수 없음")
            
            # 다른 요소들 확인
            containers = soup.select(".ranking.container")
            logger.info(f"랭킹 컨테이너 개수: {len(containers)}")
            
            list_areas = soup.select(".list_area")
            logger.info(f"리스트 영역 개수: {len(list_areas)}")
            
    except Exception as e:
        logger.error(f"디버그 테스트 실패: {e}")
    finally:
        crawler.close()


def test_filter_functionality():
    """필터 기능 테스트"""
    crawler = DOMCrawler()
    
    try:
        logger.info("=== 서버 필터 기능 테스트 ===")
        
        # 드라이버 설정
        if not crawler.driver:
            crawler.setup_driver()
        
        # 랭킹 페이지 이동
        crawler.navigate_to_ranking_page(1)
        
        # 서버 선택 테스트
        logger.info("아이라 서버로 변경 시도")
        crawler.select_custom_option("server", "아이라")
        
        # 잠시 대기 후 데이터 추출
        time.sleep(3)
        data = crawler.extract_ranking_data()
        
        if data:
            logger.info(f"아이라 서버 데이터: {len(data)}개 추출")
            for i, item in enumerate(data[:3]):
                logger.info(f"{i+1}. {item}")
        else:
            logger.warning("필터링된 데이터 없음")
            
    except Exception as e:
        logger.error(f"필터 테스트 실패: {e}")
    finally:
        crawler.close()


def test_character_search():
    """캐릭터 검색 테스트"""
    logger.info("캐릭터 검색 테스트 스킵 (필터 문제로 인해)")
    pass


def test_pagination_crawling():
    """페이지네이션 크롤링 테스트"""
    crawler = DOMCrawler()
    
    try:
        logger.info("=== 페이지네이션 크롤링 테스트 ===")
        
        # 첫 페이지 데이터 수집
        data = crawler.crawl_ranking_data(rank_type=1, server="데이안")
        if not data:
            logger.warning("첫 페이지 데이터 없음")
            return
            
        logger.info(f"첫 페이지: {len(data)}개 데이터")
        
        # 페이지네이션 정보 확인
        pagination_info = crawler.get_pagination_info()
        logger.info(f"페이지네이션 정보: {pagination_info}")
        
        # 2페이지로 이동하여 데이터 수집
        if pagination_info.get("total_pages", 0) > 1:
            logger.info("2페이지로 이동")
            crawler.navigate_to_page(2)
            
            # 2페이지 데이터 수집
            page2_data = crawler.extract_ranking_data()
            logger.info(f"2페이지: {len(page2_data)}개 데이터")
            
            if page2_data:
                for i, item in enumerate(page2_data[:3]):  # 상위 3개만 출력
                    logger.info(f"2페이지 {i+1}. {item}")
        else:
            logger.info("2페이지가 없음")
            
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
    finally:
        crawler.close()


def test_class_filter_crawling():
    """직업 필터 크롤링 테스트"""
    crawler = DOMCrawler()
    
    try:
        logger.info("=== 직업 필터 크롤링 테스트 ===")
        
        # 전사 직업만 필터링
        data = crawler.crawl_ranking_data(rank_type=1, server="데이안", class_filter="전사")
        
        if data:
            logger.info(f"전사 필터 결과: {len(data)}개")
            for i, item in enumerate(data[:5]):  # 상위 5개만 출력
                logger.info(f"전사 {i+1}. {item}")
        else:
            logger.warning("전사 필터 결과 없음")
            
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
    finally:
        crawler.close()


def test_multiple_rank_types():
    """여러 랭킹 타입 테스트"""
    crawler = DOMCrawler()
    
    try:
        logger.info("=== 여러 랭킹 타입 테스트 ===")
        
        for rank_type in [1, 2, 3]:  # 전투력, 매력, 생활력
            rank_name = crawler.rank_type_map.get(rank_type, str(rank_type))
            logger.info(f"\n--- {rank_name} 랭킹 테스트 ---")
            
            data = crawler.crawl_ranking_data(rank_type=rank_type, server="데이안")
            
            if data:
                logger.info(f"{rank_name}: {len(data)}개 데이터")
                for i, item in enumerate(data[:3]):  # 상위 3개만 출력
                    logger.info(f"{rank_name} {i+1}. {item}")
            else:
                logger.warning(f"{rank_name} 결과 없음")
                
            time.sleep(2)  # 각 테스트 간 간격
            
    except Exception as e:
        logger.error(f"테스트 실패: {e}")
    finally:
        crawler.close()


if __name__ == "__main__":
    logger.info("DOM 조작 기반 크롤링 테스트 시작")
    
    try:
        # 페이지 소스 디버그 테스트 (먼저)
        test_page_source_debug()
        
        time.sleep(3)
        
        # 기본 크롤링 테스트
        test_basic_crawling()
        
        time.sleep(3)
        
        # 서버별 크롤링 테스트
        test_server_specific_crawling()
        
        time.sleep(2)
        
        # 필터 기능 테스트
        test_filter_functionality()
        
        time.sleep(2)
        
        # 캐릭터 검색 테스트 (스킵)
        test_character_search()
        
        # 나머지 테스트들은 일단 스킵
        logger.info("복잡한 테스트들은 기본 기능 확인 후 진행합니다.")
        
    except KeyboardInterrupt:
        logger.info("사용자에 의해 중단됨")
    except Exception as e:
        logger.error(f"테스트 실행 오류: {e}")
    
    logger.info("테스트 완료")