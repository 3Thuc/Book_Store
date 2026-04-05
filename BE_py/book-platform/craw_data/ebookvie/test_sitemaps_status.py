import requests

BASE_URL = "https://ebookvie.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EbookvieCrawler/1.0; +https://example.com)"
}

sitemaps = [f"{BASE_URL}/product-sitemap{i}.xml" for i in range(1, 9)]

for url in sitemaps:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        print(url, "->", resp.status_code)
    except Exception as e:
        print(url, "-> Lỗi:", e)
