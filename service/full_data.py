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
    # suppress DevTools listening log
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    return webdriver.Chrome(service=Service(), options=opts)

def fetch_rank_via_requests(server=None, name=""):
    list_url = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
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
        "t":       "1",
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
