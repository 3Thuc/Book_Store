import requests
import xml.etree.ElementTree as ET

SITEMAP_INDEX_URL = "https://ebookvie.com/sitemap_index.xml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EbookvieCrawler/1.0; +https://example.com)"
}


def get_sitemap_urls():
    resp = requests.get(SITEMAP_INDEX_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns)]
    return urls


if __name__ == "__main__":
    sitemap_urls = get_sitemap_urls()
    print("Tìm thấy", len(sitemap_urls), "sitemap con")
    for i, url in enumerate(sitemap_urls[:10], start=1):
        print(f"{i}. {url}")
