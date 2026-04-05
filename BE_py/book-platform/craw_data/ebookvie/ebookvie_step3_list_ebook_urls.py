import requests
import xml.etree.ElementTree as ET

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EbookvieCrawler/1.0; +https://example.com)"
}

# Dựa trên kết quả step2 bạn đã in:
# 3. https://ebookvie.com/product-sitemap1.xml
# ...
# 10. https://ebookvie.com/product-sitemap8.xml
PRODUCT_SITEMAPS = [
    f"https://ebookvie.com/product-sitemap{i}.xml"
    for i in range(1, 9)  # 1..8
]


def get_ebook_urls_from_sitemap(sitemap_url, max_urls=20):
    print("Đọc sitemap:", sitemap_url)
    resp = requests.get(sitemap_url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    ebook_urls = []
    for loc in root.findall(".//sm:loc", ns):
        url = loc.text.strip()
        if "/ebook/" in url:
            ebook_urls.append(url)
            if len(ebook_urls) >= max_urls:
                break
    return ebook_urls


if __name__ == "__main__":
    print("Có", len(PRODUCT_SITEMAPS), "product sitemaps.")
    all_urls = []

    for sm_url in PRODUCT_SITEMAPS:
        urls = get_ebook_urls_from_sitemap(sm_url, max_urls=1000)
        print("  ->", len(urls), "URL ebook trong sitemap này")
        all_urls.extend(urls)

    print("\nTổng URL ebook (tối đa 1000 mỗi sitemap để test):", len(all_urls))
    for i, u in enumerate(all_urls, start=1):
        print(f"{i}. {u}")
