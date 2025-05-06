# smoke_playwright.py
from playwright.sync_api import sync_playwright

def main():
    with sync_playwright() as pw:
        # headless 모드로 Chromium 실행
        browser = pw.chromium.launch()
        page = browser.new_page(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # 타임아웃 늘려서 접속 시도
        page.set_default_navigation_timeout(30000)
        print("▶️ 페이지 열기 시도…")
        response = page.goto("https://mabinogimobile.nexon.com/Ranking/List?t=1")
        print("HTTP 상태 코드:", response.status)
        content = page.content()
        print("페이지 컨텐츠 길이:", len(content))
        # <title> 태그가 있는지 검사
        if "<title>" in content:
            print("✅ <title> 태그 확인됨")
        else:
            print("❌ <title> 태그가 없습니다")
        browser.close()

if __name__ == "__main__":
    main()
