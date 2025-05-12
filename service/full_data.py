# full_data_selenium_requests.py

import time
import chromedriver_autoinstaller
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging

class SuppressChromedriverMessage(logging.Filter):
    def filter(self, record):
        return "Chromedriver is already installed." not in record.getMessage()

logging.getLogger().addFilter(SuppressChromedriverMessage())

def get_driver():

    chromedriver_autoinstaller.install()

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("window-size=1200,800")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Chrome(service=Service(), options=opts)

def fetch_rank_via_requests(server=None, name="", rank_type=1):
    list_url = f"https://mabinogimobile.nexon.com/Ranking/List?t={rank_type}"
    api_url  = "https://mabinogimobile.nexon.com/Ranking/List/rankdata"

    s = switch_server(server_name=server)

    driver = get_driver()
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
        "t":       str(rank_type),
        "pageno":  "1",
        "s":       s,
        "c":       "0",
        "search":  name,
    }

    resp = sess.post(api_url, headers=headers, data=data)
    driver.quit()
    resp.raise_for_status()
    return resp.text

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
    
    logger.info(f"fetch_all_ranks 시작: 서버={server}, 캐릭터={name}")
    
    # 한국 시간대 설정
    kst = pytz.timezone('Asia/Seoul')
    current_time = datetime.now(kst)
    logger.info(f"현재 KST 시간: {current_time}")
    
    def fetch_rank_thread(rank_type):
        try:
            logger.info(f"랭킹 조회 시작 (rank_type={rank_type})")
            html = fetch_rank_via_requests(server, name, rank_type)
            result = parse_rank_html(html)
            logger.info(f"랭킹 조회 결과 (rank_type={rank_type}) - 데이터 개수: {len(result)}, 데이터 일부: {result[:3] if result else '[]'}")
            
            # 랭킹 타입 이름 정의
            rank_type_names = {1: "전투력", 2: "매력", 3: "생활력"}
            rank_type_name = rank_type_names.get(rank_type, f"타입{rank_type}")
            
            response = {
                "type": rank_type_name,
                "data": result,
                "retrieved_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            logger.info(f"{rank_type_name} 랭킹 조회 완료: 데이터 크기={len(result)}")
            return response
        except Exception as e:
            print(f"Error fetching rank type {rank_type}: {e}")
            return {
                "type": f"타입{rank_type}",
                "data": [],
                "error": str(e),
                "retrieved_at": current_time.strftime("%Y-%m-%d %H:%M:%S")
            }
    
    results = {}
    
    # ThreadPoolExecutor로 3개의 랭킹 데이터 병렬 조회
    with ThreadPoolExecutor(max_workers=3) as executor:
        logger.info("3개의 랭킹 병렬 조회 시작")
        futures = [executor.submit(fetch_rank_thread, t) for t in [1, 2, 3]]
        
        for future in futures:
            result = future.result()
            results[result["type"]] = result
            logger.info(f"{result['type']} 랭킹 처리완료: 데이터 크기={len(result.get('data', []))}")
    
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

    # Check if the "no data" message is present
    no_data = soup.select_one("div.no_data")
    if no_data:
        # Return empty list for no results
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

            # Skip items with "알수없음" as character name
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
            # Skip malformed items
            continue
    
    return result
