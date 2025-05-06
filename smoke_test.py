# smoke_http.py
import requests

URL = "https://mabinogimobile.nexon.com/Ranking/List?t=1"
UA  = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
resp = requests.get(URL, headers={"User-Agent": UA})
print("status:", resp.status_code)
print(resp.text[:200])
