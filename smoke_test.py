# pip install cloudscraper
import cloudscraper

URL = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
scraper = cloudscraper.create_scraper(
    browser={
      'custom': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
)
html = scraper.get(URL).text
if "data-charactername" in html:
    print("우회 성공, 페이지 정상 수집됨")
else:
    print("여전히 에러 페이지", html[:200])
