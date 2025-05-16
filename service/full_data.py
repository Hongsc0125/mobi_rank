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

def fetch_rank_via_requests(server=None, name="", rank_type=1):
    """특정 서버와 캐릭터 이름으로 랭킹 데이터를 가져옵니다."""
    list_url = f"https://mabinogimobile.nexon.com/Ranking/List?t={rank_type}"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    s = switch_server(server_name=server)
    driver = None
    
    # 드라이버 풀에서 드라이버 가져오기
    pool = get_driver_pool()
    
    try:
        # 드라이버 풀에서 드라이버 얻기
        driver = pool.get_driver()
        driver.set_page_load_timeout(30)  # 페이지 로드 타임아웃 설정
        driver.get(list_url)
        time.sleep(2)

        # 쿠키 수집 및 세션 설정
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
            "t":       str(rank_type),
            "pageno":  "1",
            "s":       s,
            "c":       "0",
            "search":  name,
        }

        # API 요청 수행
        resp = sess.post(api_url, headers=headers, data=data, timeout=30)
        resp.raise_for_status()
        return resp.text
    
    except Exception as e:
        logging.error(f"랭킹 데이터 요청 중 오류 (타입 {rank_type}): {str(e)}")
        raise
    
    finally:
        # 드라이버를 풀에 반환
        if driver:
            pool.release_driver(driver)

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
