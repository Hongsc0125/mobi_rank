# crawler.py

import time
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def get_driver():

    # chromedriver_path = chromedriver_autoinstaller.install()

    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("window-size=1200,800")
    opts.binary_location = "/usr/bin/google-chrome" 

    return webdriver.Chrome(
        service=Service("/usr/bin/chromedriver"),
        options=opts
    )

def search_character(server_name: str, character_name: str, wait_sec: float = 10):
    URL = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
    driver = get_driver()
    wait = WebDriverWait(driver, wait_sec)

    driver.get(URL)
    # 1) 서버 선택
    box = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select_server .select_box")))
    driver.execute_script("arguments[0].click();", box)
    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, ".select_option li").is_displayed())
    for opt in driver.find_elements(By.CSS_SELECTOR, ".select_option li"):
        if opt.text.strip() == server_name:
            driver.execute_script("arguments[0].click();", opt)
            break
    else:
        driver.quit()
        raise ValueError(f"서버 '{server_name}' 없음")
    time.sleep(1)

    # 2) 캐릭터 검색
    inp = wait.until(EC.element_to_be_clickable((By.NAME, "search")))
    inp.clear()
    inp.send_keys(character_name, Keys.ENTER)
    dd = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, f'dd[data-charactername="{character_name}"]')
    ))

    # 결과 li 요소
    li = dd.find_element(By.XPATH, "./ancestor::li")
    rank   = li.find_element(By.CSS_SELECTOR, "dt").text.strip()
    server = li.find_element(By.XPATH, ".//dt[text()='서버명']/following-sibling::dd").text.strip()
    cls    = li.find_element(By.XPATH, ".//div[4]//dd").text.strip()
    power  = li.find_element(By.CSS_SELECTOR, "div:nth-of-type(5) dd").text.strip()

    driver.quit()
    return {"server": server, "rank": rank, "class": cls, "power": power}
