# full_data_selenium_requests.py

import time
import chromedriver_autoinstaller
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging
from service.driver_pool import get_driver_pool

class SuppressChromedriverMessage(logging.Filter):
    def filter(self, record):
        return "Chromedriver is already installed." not in record.getMessage()

logging.getLogger().addFilter(SuppressChromedriverMessage())

def get_driver():
    """크롬 웹드라이버를 생성하고 반환합니다. 필요한 설정을 적용합니다."""
    chromedriver_autoinstaller.install()

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("window-size=1200,800")
    # 브라우저 프로세스 고아(orphaned) 방지
    opts.add_argument("--disable-features=site-per-process")
    # ShutdownOS="True" 추가하여 프로세스 종료 보장
    opts.add_experimental_option("detach", False)
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    
    # 서비스 타임아웃 증가
    service = Service()
    service.service_args = ['--verbose', '--log-path=chromedriver.log']
    
    return webdriver.Chrome(service=service, options=opts)

def fetch_rank_via_dom(server=None, name="", rank_type=1):
    """DOM 조작을 통해 특정 서버와 캐릭터 이름으로 랭킹 데이터를 가져옵니다."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    
    list_url = f"https://mabinogimobile.nexon.com/Ranking/List?t={rank_type}"
    driver = None
    
    # 드라이버 풀에서 드라이버 가져오기
    pool = get_driver_pool()
    
    try:
        # 드라이버 풀에서 드라이버 얻기
        driver = pool.get_driver()
        driver.set_page_load_timeout(30)
        wait = WebDriverWait(driver, 20)
        
        # 페이지 이동
        logging.info(f"랭킹 페이지로 이동: {list_url}")
        driver.get(list_url)
        
        # 페이지 로딩 대기
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(3)
        
        # 서버 선택
        if server:
            select_server_option(driver, server)
            time.sleep(2)
        
        # 캐릭터 검색
        if name:
            search_character(driver, name)
            time.sleep(3)
            
        # 랭킹 데이터 로딩 대기
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-mm-rankinglist], ul.list")))
        
        # 추가 대기 시간 (DOM 업데이트 완료 대기)
        time.sleep(2)
        
        # 디버깅: 검색 결과 확인
        page_source = driver.page_source
        if name:
            # 검색 결과가 있는지 확인
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')
            ranking_items = soup.select("ul.list li.item")
            logging.info(f"캐릭터 '{name}' 검색 결과: {len(ranking_items)}개 아이템 발견")
            
            # 검색 결과가 없으면 no_data 확인
            no_data = soup.select_one("div.no_data")
            if no_data:
                logging.info(f"캐릭터 '{name}' 검색 결과 없음 (no_data 메시지 발견)")
            elif ranking_items:
                # 첫 번째 아이템의 캐릭터명 확인
                first_item = ranking_items[0]
                character_element = first_item.select("div dl")[2].select_one("dd")
                if character_element:
                    found_character = character_element.get("data-charactername", "").strip()
                    logging.info(f"첫 번째 검색 결과 캐릭터: '{found_character}'")
        
        # 페이지 소스 반환
        return page_source
    
    except Exception as e:
        logging.error(f"DOM 랭킹 데이터 요청 중 오류 (타입 {rank_type}): {str(e)}")
        raise
    
    finally:
        # 드라이버를 풀에 반환
        if driver:
            pool.release_driver(driver)

def select_server_option(driver, server_name):
    """커스텀 select box에서 서버 선택"""
    server_map = {
        "데이안": 1, "아이라": 2, "던컨": 3, "알리사": 4,
        "메이븐": 5, "라사": 6, "칼릭스": 7
    }
    
    server_id = server_map.get(server_name)
    if not server_id:
        return
        
    try:
        script = f"""
        // 서버 select box 찾기
        var serverBox = document.querySelector('.select_server .select_box');
        if (serverBox) {{
            // selectBoxHandler 호출
            if (typeof selectBoxHandler === 'function') {{
                selectBoxHandler(serverBox);
                
                // 잠시 대기 후 옵션 클릭
                setTimeout(function() {{
                    var option = document.querySelector('li[data-serverid="{server_id}"]');
                    if (option) {{
                        option.click();
                    }}
                }}, 500);
            }}
        }}
        """
        driver.execute_script(script)
        logging.info(f"서버 선택: {server_name}")
    except Exception as e:
        logging.warning(f"서버 선택 실패: {e}")

def search_character(driver, character_name):
    """캐릭터 이름으로 검색"""
    from selenium.webdriver.common.by import By
    
    try:
        # 검색 입력창에 캐릭터명 입력
        search_input = driver.find_element(By.CSS_SELECTOR, "input[name='search']")
        search_input.clear()
        search_input.send_keys(character_name)
        
        # 검색 버튼 클릭
        search_button = driver.find_element(By.CSS_SELECTOR, "button[data-searchtype='search']")
        driver.execute_script("arguments[0].click();", search_button)
        
        logging.info(f"캐릭터 검색: {character_name}")
    except Exception as e:
        logging.warning(f"캐릭터 검색 실패: {e}")

def fetch_rank_via_requests(server=None, name="", rank_type=1):
    """기존 함수명 호환성을 위한 wrapper 함수"""
    return fetch_rank_via_dom(server, name, rank_type)

def switch_server(server_name):
    server_map = {
        "데이안": 1,
        "아이라": 2,
        "던컨": 3,
        "알리사": 4,
        "메이븐": 5,
        "라사": 6,
        "칼릭스": 7
    }
    return server_map.get(server_name, None)

def fetch_all_ranks(server=None, name=""):
    """
    캐릭터의 전투력(t=1), 매력(t=2), 생활력(t=3) 랭킹을 동시에 조회합니다.
    3개의 쓰레드를 사용하여 병렬로 처리합니다.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor
    import pytz
    from datetime import datetime
    import logging
    
    # 로거 가져오기
    logger = logging.getLogger(__name__)
    
    # logger.info(f"fetch_all_ranks 시작: 서버={server}, 캐릭터={name}")
    
    # 한국 시간대 설정
    kst = pytz.timezone('Asia/Seoul')
    current_time = datetime.now(kst)
    # logger.info(f"현재 KST 시간: {current_time}")
    
    def fetch_rank_thread(rank_type):
        try:
            # logger.info(f"랭킹 조회 시작 (rank_type={rank_type})")
            html = fetch_rank_via_requests(server, name, rank_type)
            result = parse_rank_html(html)
            # logger.info(f"랭킹 조회 결과 (rank_type={rank_type}) - 데이터 개수: {len(result)}, 데이터 일부: {result[:3] if result else '[]'}")
            
            # 랭킹 타입 이름 정의
            rank_type_names = {1: "전투력", 2: "매력", 3: "생활력"}
            rank_type_name = rank_type_names.get(rank_type, f"타입{rank_type}")
            
            response = {
                "type": rank_type_name,
                "data": result,
                "retrieved_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            # logger.info(f"{rank_type_name} 랭킹 조회 완료: 데이터 크기={len(result)}")
            return response
        except Exception as e:
            logger.error(f"Error fetching rank type {rank_type}: {e}")
            # 메모리 정리
            import gc
            gc.collect()
            return {
                "type": f"타입{rank_type}",
                "data": [],
                "error": str(e),
                "retrieved_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
            }
    
    results = {}
    
    # ThreadPoolExecutor로 3개의 랭킹 데이터 병렬 조회 (원래대로 3개 실행)
    with ThreadPoolExecutor(max_workers=3) as executor:
        logger.info("3개의 랭킹 병렬 조회 시작")
        # 모든 랭킹을 동시 실행
        futures = [executor.submit(fetch_rank_thread, t) for t in [1, 2, 3]]
        
        # 모든 랭킹 결과 처리
        for future in futures:
            try:
                result = future.result(timeout=60)  # 60초 타임아웃 설정
                results[result["type"]] = result
                logger.info(f"{result['type']} 랭킹 처리완료: 데이터 크기={len(result.get('data', []))}")
            except Exception as e:
                logger.error(f"랭킹 데이터 처리 실패: {str(e)}")
            
        # 추가 메모리 정리
        import gc
        gc.collect()
    
    response = {
        "character": name,
        "server": server,
        "ranks": results,
        "retrieved_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    logger.info(f"fetch_all_ranks 완료: 랭킹 수={len(results)}, 출력 예시={str(response)[:200]}...")
    return response

def parse_rank_html(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    result = []
    import logging
    logger = logging.getLogger(__name__)

    # Check if the "no data" message is present
    no_data = soup.select_one("div.no_data")
    if no_data:
        # Return empty list for no results
        return []
    
    # Check for character not found modal
    modal_message = soup.select_one("div.modal_inner div.message")
    if modal_message:
        message_text = modal_message.get_text().strip()
        # 다양한 캐릭터 없음 메시지 패턴 확인
        not_found_patterns = [
            "랭킹 정보를 찾을 수 없습니다",
            "입력한 캐릭터명의",
            "캐릭터명의 랭킹 정보를 찾을 수 없습니다"
        ]
        
        if any(pattern in message_text for pattern in not_found_patterns):
            logger.warning(f"캐릭터 랭킹 정보를 찾을 수 없음 - 모달 감지: {message_text}")
            raise ValueError("CHARACTER_NOT_FOUND")

    for li in soup.select("ul.list li.item"):
        try:
            rank = li.select_one("div dl dt").text.strip()
            change_tag = li.select_one("div dl dd")
            change_text = change_tag.text.strip()
            
            # class에 'up' 또는 'down' 포함 여부로 변화 방향 결정
            classes = change_tag.get("class", [])
            
            if "up" in classes:
                change_type = "up"
            elif "down" in classes:
                change_type = "down"
            else:
                # 클래스가 없는 경우는 변화 없음 (new 또는 - 표시)
                change_type = "none"
            
            # change 값 처리 개선 ('-' 처리 포함)
            if change_text == "-" or change_text == "new":
                change = "0"  # 변화 없음을 0으로 표시
            else:
                # 숫자만 추출
                import re
                num_match = re.search(r'\d+', change_text)
                if num_match:
                    change = num_match.group(0)
                else:
                    change = "0"  # 숫자가 없으면 0으로 설정
            
            # 변환된 숫자를 정수로 저장 (DB 저장 형식과 일치)
            try:
                change_int = int(change)
            except ValueError:
                change_int = 0
                
            server = li.select("div dl")[1].select_one("dd").text.strip()
            character = li.select("div dl")[2].select_one("dd").get("data-charactername").strip()
            char_class = li.select("div dl")[3].select_one("dd").text.strip()
            power = li.select("div dl")[4].select_one("dd").text.strip()

            # Skip items with "알수없음" as character name
            if character == "알수없음":
                continue

            result.append({
                "rank": rank,
                "change": change_int,  # 정수로 저장
                "change_type": change_type,
                "server": server,
                "character": character,
                "class": char_class,
                "power": power
            })
        except (AttributeError, IndexError) as e:
            # Skip malformed items
            continue
    
    return result
