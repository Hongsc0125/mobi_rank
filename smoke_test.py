# smoke_test.py
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# 1) 옵션 설정
opts = Options()
opts.add_argument("--headless")                # 헤드리스 모드
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-gpu")
opts.add_argument("--window-size=1200,800")
opts.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) "
                 "AppleWebKit/537.36 (KHTML, like Gecko) "
                 "Chrome/120.0.0.0 Safari/537.36")  # 브라우저처럼 보이게

# 2) ChromeDriver 경로 (chromedriver_autoinstaller 로 설치된 경로 또는 /usr/bin/chromedriver)
service = Service("/usr/local/lib/python3.12/site-packages/chromedriver_autoinstaller/136/chromedriver")
#    └── 환경에 따라 이 경로를 `chromedriver_autoinstaller.install()` 반환값으로 바꿔주세요.

# 3) 브라우저 열기 & 페이지 접근
driver = webdriver.Chrome(service=service, options=opts)
driver.get("https://httpbin.org/user-agent")

# 4) 결과 출력
print(driver.page_source)   # JSON 형태로 user-agent 정보가 나옵니다.
driver.quit()
