import requests

SITEMAP_INDEX_URL = "https://ebookvie.com/sitemap_index.xml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EbookvieCrawler/1.0; +https://example.com)"
}


def main():
    resp = requests.get(SITEMAP_INDEX_URL, headers=HEADERS, timeout=20)
    print("Status code:", resp.status_code)
    print("Độ dài nội dung:", len(resp.text))
    print("5 dòng đầu:")
    print("\n".join(resp.text.splitlines()[:5]))


if __name__ == "__main__":
    main()
