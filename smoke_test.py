# dump_ranking_page.py
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# 1) ChromeDriver 경로
chromedriver_path = chromedriver_autoinstaller.install()

# 2) 옵션 세팅 (headless + UA)
opts = Options()
opts.add_argument("--headless")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-gpu")
opts.add_argument("--window-size=1200,800")
opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
opts.binary_location = "/usr/bin/google-chrome"

# 3) 드라이버 실행
driver = webdriver.Chrome(service=Service(chromedriver_path), options=opts)
driver.get("https://mabinogimobile.nexon.com/Ranking/List?t=1")

# 4) 덤프 저장
html = driver.page_source
with open("selenium_page.html", "w", encoding="utf-8") as f:
    f.write(html)
print("✅ selenium_page.html에 저장 완료")

driver.quit()
