# smoke_playwright.py
from playwright.sync_api import sync_playwright

URL = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
    page = browser.new_page(
      user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                 "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    page.goto(URL, timeout=30000)
    content = page.content()
    if "data-charactername" in content:
        print("✅ Playwright 우회 성공")
    else:
        print("⛔️ Playwright로도 실패…", content[:200])
    browser.close()
