# smoke_stealth.py
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

def main():
    with sync_playwright() as pw:
        # headless 모드
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # Stealth plugin 적용
        stealth_sync(context)

        page = context.new_page()
        page.set_default_navigation_timeout(30000)

        print("▶️ Stealth 모드로 페이지 열기…")
        response = page.goto("https://mabinogimobile.nexon.com/Ranking/List?t=1")
        print("HTTP 상태 코드:", response.status)
        html = page.content()
        print("페이지 길이:", len(html))
        print("✅ <title> 태그:", "<title>" in html)

        browser.close()

if __name__ == "__main__":
    main()
