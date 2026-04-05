import requests
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from time import sleep
from bs4 import BeautifulSoup
import re
import pandas as pd

BASE_URL = "https://ebookvie.com"

# Dùng Session để tái sử dụng kết nối TCP, nhanh & ổn định hơn
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; EbookvieCrawler/1.0; +https://example.com)"
})

# product-sitemap1..8.xml (trong đó 3-8 hiện đang 500, nhưng ta để vẫn không sao, code sẽ skip)
PRODUCT_SITEMAPS = [
    f"{BASE_URL}/product-sitemap{i}.xml"
    for i in range(1, 9)
]


def extract_text(el):
    if not el:
        return ""
    return " ".join(el.get_text(separator=" ", strip=True).split())


def fetch(url, timeout=30, max_retries=3, sleep_between=2):
    """
    Hàm request chung, có retry nhẹ.
    Không cố 'lách', chỉ là nếu mạng/srv chập chờn thì thử lại vài lần.
    """
    for attempt in range(1, max_retries + 1):
        try:
            resp = SESSION.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            print(f"  !! Lỗi request ({attempt}/{max_retries}) với {url}: {e}")
            if attempt == max_retries:
                raise
            sleep(sleep_between)


def get_ebook_urls_from_sitemap(sitemap_url):
    """Lấy TẤT CẢ URL /ebook/ trong 1 sitemap sản phẩm."""
    print("Đọc sitemap:", sitemap_url)
    resp = fetch(sitemap_url, timeout=60, max_retries=2, sleep_between=3)

    root = ET.fromstring(resp.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    ebook_urls = []
    for loc in root.findall(".//sm:loc", ns):
        url = loc.text.strip()
        if "/ebook/" in url:
            ebook_urls.append(url)

    print("  ->", len(ebook_urls), "URL ebook")
    return ebook_urls


def get_all_ebook_urls(max_books=None):
    """
    Gom URL từ toàn bộ product-sitemap*, cắt max_books nếu đủ.
    Dùng set để tránh trùng & O(1) khi check.
    """
    all_urls = set()

    for sm_url in PRODUCT_SITEMAPS:
        try:
            ebook_urls = get_ebook_urls_from_sitemap(sm_url)
        except Exception as e:
            print("  !! Bỏ qua sitemap do lỗi:", e)
            continue

        for u in ebook_urls:
            if u not in all_urls:
                all_urls.add(u)
                # Chỉ cắt nếu max_books != None
                if max_books is not None and len(all_urls) >= max_books:
                    print("Đã đạt max_books, dừng gom URL.")
                    return list(all_urls)

    return list(all_urls)


def get_book_detail(url):
    """Cào chi tiết 1 trang ebook: title, author, categories, description, image."""
    resp = fetch(url, timeout=60, max_retries=2, sleep_between=3)
    soup = BeautifulSoup(resp.text, "lxml")

    # 1. Title
    title_el = soup.find("h1")
    title = extract_text(title_el)

    # 2. Author
    author_name = ""
    author_label = soup.find(string=re.compile(r"Tác giả", re.I))
    if author_label:
        parent = author_label.parent
        a = parent.find("a")
        if not a:
            a = parent.find_next("a")
        author_name = extract_text(a) if a else ""

    # 3. Categories - DÙNG THEO HTML BẠN GỬI
    categories = []
    cat_links = soup.select("div.right a[rel='tag']")
    for a in cat_links:
        txt = extract_text(a)
        if txt and txt not in categories:
            categories.append(txt)
    categories_str = ", ".join(categories)

    # 4. Description
    desc_div = soup.find("div", id="tab-description")
    if not desc_div:
        desc_div = soup.find("div", class_=re.compile("description", re.I))
    description = extract_text(desc_div)

    # 5. Image
    img = soup.select_one("div.woocommerce-product-gallery img")
    if not img:
        img = soup.find("img")
    image_url = ""
    if img and img.has_attr("src"):
        image_url = urljoin(BASE_URL, img["src"])

    return {
        "title": title,
        "author_name": author_name,
        "categories": categories_str,
        "description": description,
        "image_url": image_url,
        "source_url": url,
    }


def crawl_ebookvie_catalog(max_books=400, sleep_seconds=1.0, save_tmp_every=50):
    """
    Cào catalog ebookvie:
      - max_books: số sách tối đa muốn lấy (thực tế hiện tại ~400 vì sitemap3-8 lỗi).
      - sleep_seconds: nghỉ giữa mỗi sách.
      - save_tmp_every: mỗi N sách lưu tạm 1 file backup.
    """
    urls = get_all_ebook_urls(max_books=max_books)
    print("Tổng URL ebook sẽ cào:", len(urls))

    rows = []
    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{len(urls)}] {url}")
        try:
            data = get_book_detail(url)
            rows.append(data)
        except Exception as e:
            print("  !! Lỗi khi cào:", e)

        # Lưu tạm định kỳ để tránh mất dữ liệu nếu bị gián đoạn
        if save_tmp_every and i % save_tmp_every == 0:
            df_tmp = pd.DataFrame(rows, columns=[
                "title", "author_name", "categories",
                "description", "image_url", "source_url"
            ])
            df_tmp.to_excel("ebookvie_books_tmp.xlsx", index=False)
            print(
                f"  -> Đã lưu tạm ebookvie_books_tmp.xlsx với {len(rows)} dòng")

        sleep(sleep_seconds)  # cho lịch sự, tránh spam server

    return rows


if __name__ == "__main__":
    # Gợi ý: test nhỏ trước, rồi mới tăng max_books
    # 1) Test nhanh 50 sách:
    # rows = crawl_ebookvie_catalog(max_books=50, sleep_seconds=1.0)

    # 2) Khi thấy ổn, cào full 400 sách:
    rows = crawl_ebookvie_catalog(max_books=None, sleep_seconds=1.0)

    df = pd.DataFrame(rows, columns=[
        "title", "author_name", "categories",
        "description", "image_url", "source_url"
    ])

    df.to_excel("ebookvie_books.xlsx", index=False)
    print("Đã lưu ebookvie_books.xlsx với", len(df), "dòng")
