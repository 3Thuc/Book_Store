"""
fahasa_scraper_tieusuhoi.py – Cào dữ liệu sách NHÓM TIỂU SỬ - HỒI KÝ từ fahasa.com
======================================================================================
Cách chạy:
  python fahasa_scraper_tieusuhoi.py --csv --no-images
  python fahasa_scraper_tieusuhoi.py --csv
  python fahasa_scraper_tieusuhoi.py --pages 5 --csv --delay 2.0
"""

import argparse
import csv
import io
import re
import time
import random
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import mysql.connector
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

# ─── CẤU HÌNH ────────────────────────────────────────────────────────────────
BASE_URL          = "https://www.fahasa.com"
COVERS_DIR        = Path(__file__).resolve().parent / "covers" / "books"
CSV_OUT_DIR       = Path(__file__).resolve().parent / "scraped_data"
CSV_IMAGES_DIR    = Path(__file__).resolve().parent / "scraped_images"
DEFAULT_DELAY     = 1.5
DEFAULT_MAX_PAGES = 999
MAX_RETRIES       = 3

CSV_COLUMNS = [
    "category", "title", "author", "publisher", "price",
    "isbn", "publication_year", "language", "format",
    "description", "image_url", "local_image", "book_url",
]

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "1900561275Nghia",
    "database": "bookstore",
    "charset":  "utf8mb4",
}

# ─── CHỈ CÀO NHÓM TIỂU SỬ - HỒI KÝ ─────────────────────────────────────────
CATEGORIES = [
    ("Tiểu Sử - Hồi Ký",      "sach-trong-nuoc/tieu-su-hoi-ky.html"),
    ("Câu Chuyện Cuộc Đời",   "sach-trong-nuoc/tieu-su-hoi-ky/cau-chuyen-cuoc-doi.html"),
    ("Lịch Sử",               "sach-trong-nuoc/tieu-su-hoi-ky/lich-su.html"),
    ("Nghệ Thuật - Giải Trí", "sach-trong-nuoc/tieu-su-hoi-ky/nghe-thuat-giai-tri.html"),
    ("Kinh Tế",               "sach-trong-nuoc/tieu-su-hoi-ky/chinh-tri.html"),   # slug thực tế trên fahasa
    ("Chính Trị",             "sach-trong-nuoc/tieu-su-hoi-ky/chinh-tri.html"),
    ("Thể Thao",              "sach-trong-nuoc/tieu-su-hoi-ky/the-thao.html"),
]
# ─────────────────────────────────────────────────────────────────────────────


# ═══════════════════════════════════════════════════════════════════════════════
#  DATABASE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def db_connect(cfg): return mysql.connector.connect(**cfg)

def upsert_author(cur, name):
    name = name.strip()[:256]
    cur.execute("INSERT INTO authors (author_name, bio, status) VALUES (%s,'','active') "
                "ON DUPLICATE KEY UPDATE status='active', updated_at=NOW()", (name,))
    cur.execute("SELECT author_id FROM authors WHERE author_name=%s", (name,))
    return cur.fetchone()[0]

def upsert_publisher(cur, name):
    name = name.strip()[:100]
    cur.execute("INSERT INTO publishers (publisher_name, status) VALUES (%s,'active') "
                "ON DUPLICATE KEY UPDATE status='active', updated_at=NOW()", (name,))
    cur.execute("SELECT publisher_id FROM publishers WHERE publisher_name=%s", (name,))
    return cur.fetchone()[0]

def upsert_category(cur, name):
    name = name.strip()[:100]
    cur.execute("INSERT INTO categories (category_name, status) VALUES (%s,'active') "
                "ON DUPLICATE KEY UPDATE status='active', updated_at=NOW()", (name,))
    cur.execute("SELECT category_id FROM categories WHERE category_name=%s", (name,))
    return cur.fetchone()[0]

def upsert_book(cur, data):
    existing_id = None
    if data.get("isbn"):
        cur.execute("SELECT book_id FROM books WHERE isbn=%s", (data["isbn"],))
        row = cur.fetchone()
        if row: existing_id = row[0]
    if not existing_id:
        cur.execute("SELECT book_id FROM books WHERE title=%s", (data["title"][:255],))
        row = cur.fetchone()
        if row: existing_id = row[0]
    if existing_id:
        cur.execute("""UPDATE books SET author_id=%s, publisher_id=%s, price=%s, stock_quantity=%s,
               description=%s, publication_year=%s, isbn=%s, language=%s, format=%s,
               status='active', updated_at=NOW() WHERE book_id=%s""",
            (data["author_id"], data["publisher_id"], data["price"], data["stock_quantity"],
             data["description"], data["publication_year"], data["isbn"] or None,
             data["language"], data["format"], existing_id))
        return existing_id
    cur.execute("""INSERT INTO books (title, author_id, publisher_id, price, stock_quantity,
            description, publication_year, isbn, avg_rating, rating_count, language, format, status)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0.00,0,%s,%s,'active')""",
        (data["title"][:255], data["author_id"], data["publisher_id"], data["price"],
         data["stock_quantity"], data["description"], data["publication_year"],
         data["isbn"] or None, data["language"][:50], data["format"]))
    return cur.lastrowid

def upsert_book_image(cur, book_id, image_url, is_main):
    if is_main: cur.execute("DELETE FROM book_images WHERE book_id=%s AND is_main=1", (book_id,))
    cur.execute("INSERT IGNORE INTO book_images (book_id, image_url, is_main) VALUES (%s,%s,%s)",
                (book_id, image_url[:255], is_main))

def link_category(cur, book_id, category_id):
    cur.execute("INSERT IGNORE INTO book_categories (book_id, category_id) VALUES (%s,%s)", (book_id, category_id))


# ═══════════════════════════════════════════════════════════════════════════════
#  BROWSER HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _is_cf_challenge(driver):
    return any(kw in driver.title.lower() for kw in ("just a moment","chờ một chút","403","access denied","attention required","checking your browser"))

def wait_for_cf(driver, max_wait=30):
    print(f"  ⏳ Cloudflare, đợi {max_wait}s...", end="", flush=True)
    for _ in range(max_wait // 2):
        time.sleep(2); print(".", end="", flush=True)
        if not _is_cf_challenge(driver): print(f" ✅"); return True
    print(" ❌"); return False

def init_driver(headless=False):
    print("  🌐 Khởi động Chrome...")
    options = uc.ChromeOptions()
    for arg in ["--lang=vi-VN","--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage","--window-size=1366,768"]:
        options.add_argument(arg)
    if headless: options.add_argument("--headless=new")
    driver = uc.Chrome(options=options, use_subprocess=True)
    driver.set_page_load_timeout(30)
    driver.get(BASE_URL); time.sleep(4)
    if _is_cf_challenge(driver): wait_for_cf(driver, 40)
    print(f"  ✅ Homepage OK")
    driver.get(f"{BASE_URL}/sach-trong-nuoc.html"); time.sleep(3)
    if _is_cf_challenge(driver): wait_for_cf(driver, 30)
    return driver

def get_category_links(driver, url, delay):
    time.sleep(delay + random.uniform(0.3, 0.8))
    driver.get(url)
    try: WebDriverWait(driver, 15).until(EC.url_contains(url.split("fahasa.com/")[-1].split("?")[0][:35]))
    except: pass
    print(f"  🔍 {driver.current_url[:80]}")
    if _is_cf_challenge(driver): wait_for_cf(driver, 25)
    JS = """
        var items=document.querySelectorAll('a.product-image'),out=[];
        for(var i=0;i<items.length;i++){
            var h=items[i].href||'',base=h.split('?')[0];
            if(h.indexOf('fhs_campaign=CATEGORY')!==-1){out.push(base);}
            else if(base.indexOf('.html')!==-1){
                var slash=base.replace('https://www.fahasa.com/','').indexOf('/');
                if(slash===-1&&base.indexOf('/sach-trong-nuoc')===-1&&base.indexOf('/sach-nuoc-ngoai')===-1
                   &&base.indexOf('/promo')===-1&&base.indexOf('/deli')===-1) out.push(base);
            }
        }
        return [items.length,out];
    """
    deadline = time.time() + 30; p = 0
    while time.time() < deadline:
        p += 1
        try:
            res = driver.execute_script(JS)
            if res and len(res)==2:
                total,hrefs=res[0],res[1]
                print(f"  🔍 Poll {p}: {total} items, {len(hrefs)} links")
                if hrefs: return [h for h in hrefs if h.startswith("https://www.fahasa.com/")]
        except Exception as e: print(f"  ⚠ Poll {p}: {e}")
        time.sleep(1.5)
    print("  ⚠ Timeout"); return []

def driver_get_soup(driver, url, delay):
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(delay + random.uniform(0.3, 0.8))
            driver.get(url)
            try: WebDriverWait(driver, 15).until(EC.url_contains(url.split("fahasa.com/")[-1].split("?")[0][:30]))
            except: pass
            time.sleep(2.0)
            if _is_cf_challenge(driver):
                if not wait_for_cf(driver, 25):
                    if attempt < MAX_RETRIES-1: continue
                    return None
            try: WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.CSS_SELECTOR,
                "h1.product-name-no-ellipsis, h1.product-name, .product-info-main, .product-essential")))
            except: pass
            time.sleep(1.0)
            return BeautifulSoup(driver.execute_script("return document.documentElement.outerHTML;"), "lxml")
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ("invalid session id","disconnected","closed")): return "CRASHED"
            print(f"  ⚠ retry {attempt+1}: {e}"); time.sleep(5)
    return None

def get_driver_session(driver):
    s = requests.Session()
    s.headers["User-Agent"] = driver.execute_script("return navigator.userAgent")
    for c in driver.get_cookies(): s.cookies.set(c["name"], c["value"])
    return s

def download_image(session, url, save_path):
    try:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        resp = session.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=20)
        if resp.status_code != 200: return False
        if HAS_PIL: PILImage.open(io.BytesIO(resp.content)).convert("RGB").save(save_path,"JPEG",quality=92)
        else: save_path.write_bytes(resp.content)
        return True
    except: return False

JS_EXTRACT_META = """
(function(){var info={};
[
  ['.product-view-sa-supplier','Tên Nhà Cung Cấp'],
  ['.product-view-sa-author','Tác giả'],
  ['.product-view-sa-publisher','Nhà xuất bản'],
  ['.product-view-sa-sub-publisher','Hình thức bìa']
].forEach(function(p){
  var el=document.querySelector(p[0]+' a')||document.querySelector(p[0]+' span:last-child');
  if(el){var v=el.textContent.trim();if(v)info[p[1]]=v;}
});
document.querySelectorAll('#product_view_info tr,.product_view_tab_content_additional tr').forEach(function(r){
  var th=r.querySelector('th'),td=r.querySelector('td');
  if(th&&td){var k=th.textContent.trim(),v=td.textContent.trim();if(k&&v&&!info[k])info[k]=v;}
});
document.querySelectorAll('.product-view-sa-one-row').forEach(function(r){
  var l=r.querySelector('.product-view-sa-one-label'),v=r.querySelector('.product-view-sa-one-content');
  if(l&&v){var k=l.textContent.trim(),vv=v.textContent.trim();if(k&&vv&&!info[k])info[k]=vv;}
});
document.querySelectorAll('table.product-attibute tr,.product-info-detail tr,.product-spec tr').forEach(function(r){
  var c=r.querySelectorAll('td,th');
  if(c.length>=2){var k=c[0].textContent.trim(),v=c[1].textContent.trim();if(k&&v&&!info[k])info[k]=v;}
});
return info;})();
"""

def extract_meta_js(driver):
    try:
        r = driver.execute_script(JS_EXTRACT_META)
        return r if isinstance(r, dict) else {}
    except: return {}

def extract_price(text):
    nums = re.sub(r"[^\d]","", (text or "").split("đ")[0].split("₫")[0])
    return float(nums) if nums else 0.0

def extract_year(text):
    m = re.search(r"\b(19|20)\d{2}\b", text or "")
    return int(m.group()) if m else None

def parse_detail(soup, js_meta=None, js_desc=""):
    try:
        h1 = (soup.select_one("h1.product-name-no-ellipsis span") or soup.select_one("h1.product-name-no-ellipsis")
              or soup.select_one("h1.product-name span") or soup.select_one("h1.product-name") or soup.select_one("h1"))
        title = h1.get_text(strip=True) if h1 else ""
        if not title: return None
        p_tag = (soup.select_one(".price-box .special-price .price") or soup.select_one(".price-box .regular-price .price")
                 or soup.select_one(".product-info-price .price") or soup.select_one("[class*='price-final_price'] .price"))
        price = extract_price(p_tag.get_text()) if p_tag else 0.0
        info = {}
        if js_meta:
            for k,v in js_meta.items(): info[k.lower()] = v
        for _css,_key in [(".product-view-sa-supplier","tên nhà cung cấp"),(".product-view-sa-author","tác giả"),
                           (".product-view-sa-publisher","nhà xuất bản"),(".product-view-sa-sub-publisher","hình thức bìa")]:
            _el = soup.select_one(f"{_css} a") or soup.select_one(f"{_css} span:last-child")
            if _el and _el.get_text(strip=True) and _key not in info: info[_key] = _el.get_text(strip=True)
        for row in soup.select("#product_view_info tr,.product_view_tab_content_additional tr"):
            th,td = row.select_one("th"),row.select_one("td")
            if th and td:
                k = th.get_text(strip=True).lower()
                if k not in info: info[k] = td.get_text(strip=True)
        for row in soup.select(".product-view-sa-one-row"):
            le,ve = row.select_one(".product-view-sa-one-label"),row.select_one(".product-view-sa-one-content")
            if le and ve:
                k = le.get_text(strip=True).lower()
                if k not in info: info[k] = ve.get_text(strip=True)
        for row in soup.select("table.product-attibute tr,.product-spec tr"):
            cols = row.find_all(["td","th"])
            if len(cols)>=2:
                k = cols[0].get_text(strip=True).lower()
                if k not in info: info[k] = cols[1].get_text(strip=True)
        author    = (info.get("tác giả") or info.get("author") or "Không rõ").strip()
        publisher = (info.get("nhà xuất bản") or info.get("publisher") or info.get("tên nhà cung cấp") or "Không rõ").strip()
        isbn_raw  = (info.get("isbn") or info.get("isbn-13") or info.get("mã isbn") or info.get("mã hàng") or "").strip()
        isbn      = re.sub(r"[^\dX]","",isbn_raw)[:13] or None
        pub_year  = extract_year(info.get("năm xuất bản") or info.get("năm phát hành") or "")
        fmt_raw   = (info.get("hình thức") or info.get("hình thức bìa") or "").lower()
        fmt       = "hardcover" if "cứng" in fmt_raw else "ebook" if "ebook" in fmt_raw else "paperback"
        language  = (info.get("ngôn ngữ") or "Tiếng Việt")[:50]
        description = js_desc[:5000] if js_desc else ""
        if not description:
            desc_tag = (soup.select_one("#desc_content") or soup.select_one("#product_tabs_description_contents .std")
                        or soup.select_one(".product-view-sa-view-detail") or soup.select_one(".product-description-content")
                        or soup.select_one(".short-description"))
            description = desc_tag.get_text(" ", strip=True)[:5000] if desc_tag else ""
        images = []
        for a_tag in soup.select("a.include-in-gallery,a[id^='lightgallery-item']"):
            href = a_tag.get("href","")
            if href and href.startswith("http") and "placeholder" not in href:
                src = href.split("?")[0]
                if src not in images: images.append(src)
            else:
                img = a_tag.find("img")
                if img:
                    src = (img.get("data-zoom-image") or img.get("data-src") or img.get("src") or "").split("?")[0]
                    if src and "placeholder" not in src and src not in images and src.startswith("http"): images.append(src)
            if len(images)>=3: break
        if not images:
            for img in soup.select(".product-image-gallery img,.fotorama__img,#image-main,.product-image img"):
                src = (img.get("data-zoom-image") or img.get("data-src") or img.get("src") or "").split("?")[0]
                if src and "placeholder" not in src and src not in images and src.startswith("http"): images.append(src)
                if len(images)>=3: break
        return {"title":title[:255],"author_name":author,"publisher_name":publisher,"price":round(price,2),
                "stock_quantity":10,"description":description,"publication_year":pub_year,"isbn":isbn,
                "language":language,"format":fmt,"images":images}
    except Exception as e:
        print(f"  ⚠ Parse lỗi: {e}"); return None


# ═══════════════════════════════════════════════════════════════════════════════
#  SCRAPE CATEGORY
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_category(driver, img_session, conn, cat_name, cat_slug,
                    max_pages, delay, download_imgs, csv_writer=None, csv_images_dir=None):
    _CATEGORY_MAP = {
        "tiểu sử - hồi ký":     "Hồi ký - Tự truyện",
        "câu chuyện cuộc đời":   "Hồi ký - Tự truyện",
        "lịch sử":               "Lịch sử - Địa lý",
        "nghệ thuật - giải trí": "Hồi ký - Tự truyện",
        "kinh tế":               "Kinh tế - Quản lý",
        "chính trị":             "Chính trị - Pháp luật",
        "thể thao":              "Hồi ký - Tự truyện",
    }
    mapped = _CATEGORY_MAP.get(cat_name.strip().lower(), cat_name.strip())
    cur = None; category_id = None
    if conn:
        cur = conn.cursor()
        category_id = upsert_category(cur, mapped)
        conn.commit()
    count = 0

    for page in range(1, max_pages+1):
        url = f"{BASE_URL}/{cat_slug}?p={page}"
        print(f"\n  📄 [{cat_name}] Trang {page}")
        book_urls = get_category_links(driver, url, delay)
        if not book_urls: print("  ⚠ Hết sách."); break
        print(f"  📖 {len(book_urls)} sách")
        for idx, link in enumerate(book_urls, 1):
            print(f"    [{idx}/{len(book_urls)}] {link.split('/')[-1][:40]}", end=" ... ")
            detail = driver_get_soup(driver, link, delay)
            if detail == "CRASHED":
                print("🔄 Restart...")
                try: driver.quit()
                except: pass
                import sys
                driver = init_driver(headless=("--headless" in getattr(sys,"argv",[])))
                img_session = get_driver_session(driver)
                detail = driver_get_soup(driver, link, delay)
            if detail is None or detail == "CRASHED": print("❌"); continue

            _t0 = time.time()
            while time.time()-_t0 < 12:
                _ok = driver.execute_script(
                    "var sa=document.querySelector('.product-view-sa-author a,.product-view-sa-author span:last-child');"
                    "if(sa&&sa.textContent.trim().length>0)return 'static';"
                    "var ths=document.querySelectorAll('#product_view_info th');"
                    "for(var i=0;i<ths.length;i++){if(ths[i].textContent.trim().length>0)return 'dynamic';}"
                    "return false;")
                if _ok: break
                time.sleep(0.8)
            try: detail = BeautifulSoup(driver.execute_script("return document.documentElement.outerHTML;"),"lxml")
            except: pass

            js_meta = extract_meta_js(driver)
            if js_meta: print(f"[meta:{len(js_meta)}k]", end=" ")
            js_desc = ""
            try:
                js_desc = driver.execute_script(
                    "var el=document.querySelector('#desc_content,#product_tabs_description_contents .std,"
                    "#product-description,.product-view-sa-view-detail,.product-description-content,.product-collateral .std');"
                    "return el?el.innerText.trim().substring(0,5000):'';") or ""
            except: pass

            data = parse_detail(detail, js_meta, js_desc=js_desc)
            if not data: print("⚠ skip"); continue

            if csv_writer is not None:
                img_url = data["images"][0] if data["images"] else ""
                local_img = ""
                if csv_images_dir and img_url:
                    try:
                        img_path = csv_images_dir / f"{link.split('/')[-1].replace('.html','')[:80]}.jpg"
                        img_path.parent.mkdir(parents=True, exist_ok=True)
                        for c in driver.get_cookies(): img_session.cookies.set(c["name"],c["value"])
                        if download_image(img_session, img_url, img_path):
                            local_img = str(img_path.relative_to(Path(__file__).resolve().parent))
                            print(f"🖼", end=" ")
                    except Exception as e: print(f"(ảnh:{e})", end=" ")
                csv_writer.writerow({
                    "category":cat_name, "title":data["title"], "author":data["author_name"],
                    "publisher":data["publisher_name"], "price":data["price"],
                    "isbn":data["isbn"] or "", "publication_year":data["publication_year"] or "",
                    "language":data["language"], "format":data["format"],
                    "description":data["description"].replace("\n"," "),
                    "image_url":img_url, "local_image":local_img, "book_url":link,
                })
                count += 1; print(f"✅ #{count}"); continue

            try:
                data["author_id"]    = upsert_author(cur, data.pop("author_name"))
                data["publisher_id"] = upsert_publisher(cur, data.pop("publisher_name"))
                images = data.pop("images")
                book_id = upsert_book(cur, data)
                link_category(cur, book_id, category_id)
                for i, img_url in enumerate(images):
                    is_main = 1 if i==0 else 0
                    filename = f"{book_id}.jpg" if is_main else f"{book_id}_{i}.jpg"
                    upsert_book_image(cur, book_id, f"covers/books/{book_id}/{filename}", is_main)
                    if download_imgs:
                        for c in driver.get_cookies(): img_session.cookies.set(c["name"],c["value"])
                        if download_image(img_session, img_url, COVERS_DIR/str(book_id)/filename) and is_main:
                            print(f"🖼", end=" ")
                conn.commit(); count += 1; print(f"✅ #{book_id}")
            except Exception as e: conn.rollback(); print(f"❌ {e}")

    if cur: cur.close()
    return count, driver


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Cào nhóm Tiểu Sử - Hồi Ký từ Fahasa")
    parser.add_argument("--pages",    type=int,   default=DEFAULT_MAX_PAGES)
    parser.add_argument("--delay",    type=float, default=DEFAULT_DELAY)
    parser.add_argument("--no-images", action="store_true")
    parser.add_argument("--headless",  action="store_true")
    parser.add_argument("--db-host",  default=DB_CONFIG["host"])
    parser.add_argument("--db-port",  type=int, default=DB_CONFIG["port"])
    parser.add_argument("--db-user",  default=DB_CONFIG["user"])
    parser.add_argument("--db-pass",  default=DB_CONFIG["password"])
    parser.add_argument("--db-name",  default=DB_CONFIG["database"])
    parser.add_argument("--csv",       action="store_true")
    parser.add_argument("--csv-file",  default="")
    args = parser.parse_args()

    if args.csv:
        import datetime
        CSV_OUT_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = Path(args.csv_file) if args.csv_file else CSV_OUT_DIR / f"books_tieusuhoi_{ts}.csv"
        img_dir = None if args.no_images else CSV_IMAGES_DIR
        if img_dir: img_dir.mkdir(parents=True, exist_ok=True)
        print(f"📋 CSV → {csv_path}")
        driver = init_driver(headless=args.headless)
        img_session = get_driver_session(driver)
        total = 0
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            print(f"\n📚 Cào {len(CATEGORIES)} danh mục Tiểu Sử - Hồi Ký")
            try:
                for cat_name, cat_slug in CATEGORIES:
                    print(f"\n{'='*60}\n📂 {cat_name}")
                    n, driver = scrape_category(driver, img_session, None, cat_name, cat_slug,
                                                args.pages, args.delay, False,
                                                csv_writer=writer, csv_images_dir=img_dir)
                    total += n; print(f"  ✅ +{n} (tổng: {total})")
            finally:
                try: driver.quit()
                except: pass
        print(f"\n🎉 Xong! {total} sách → {csv_path}")
        return

    cfg = {**DB_CONFIG, "host":args.db_host, "port":args.db_port,
           "user":args.db_user, "password":args.db_pass, "database":args.db_name}
    conn = db_connect(cfg)
    COVERS_DIR.mkdir(parents=True, exist_ok=True)
    driver = init_driver(headless=args.headless)
    img_session = get_driver_session(driver)
    total = 0
    try:
        for cat_name, cat_slug in CATEGORIES:
            print(f"\n{'='*60}\n📂 {cat_name}")
            n, driver = scrape_category(driver, img_session, conn, cat_name, cat_slug,
                                        args.pages, args.delay, not args.no_images)
            total += n; print(f"  ✅ +{n} (tổng: {total})")
    finally:
        try: driver.quit()
        except: pass
        conn.close()
    print(f"\n🎉 Xong! {total} sách Tiểu Sử - Hồi Ký vào MySQL")


if __name__ == "__main__":
    main()
