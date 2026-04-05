"""
import_all_csv.py – Import tất cả file CSV từ scraped_data/ vào MySQL bookstore
================================================================================
- Đọc tất cả *.csv trong scraped_data/ (hoặc chỉ 1 file qua --file)
- INSERT/UPDATE các bảng: authors, publishers, categories, books, book_categories, book_images
- Nếu trùng ISBN hoặc title → UPDATE books
- Nếu trùng author_name → UPDATE authors
- Nếu trùng publisher_name → UPDATE publishers
- Ảnh: copy từ scraped_images/<slug>.jpg → covers/books/<book_id>/<book_id>.jpg
        và update image_url trong book_images theo chuẩn: covers/books/<book_id>/<book_id>.jpg

Cách dùng:
  python import_all_csv.py                        # import tất cả CSV
  python import_all_csv.py --no-images            # bỏ qua xử lý ảnh
  python import_all_csv.py --dry-run              # preview, không ghi DB
  python import_all_csv.py --file scraped_data/books_vanhoc_20260325_161811.csv
"""

import argparse
import csv
import re
import shutil
import sys
from pathlib import Path

import mysql.connector

# ─── CẤU HÌNH ────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent
SCRAPED_DATA  = BASE_DIR / "scraped_data"
SCRAPED_IMGS  = BASE_DIR / "scraped_images"
COVERS_DIR    = BASE_DIR / "covers" / "books"

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "1900561275Nghia",
    "database": "bookstore",
    "charset":  "utf8mb4",
}

# ─── MAP Fahasa category → DB category ────────────────────────────────────────
CATEGORY_MAPPING: dict[str, str] = {
    # Văn học
    "văn học":                           "Văn học - Tiểu thuyết",
    "tiểu thuyết":                       "Văn học - Tiểu thuyết",
    "tác phẩm kinh điển":                "Văn học - Tiểu thuyết",
    "thể loại khác (vh)":                "Văn học - Tiểu thuyết",
    "combo văn học":                     "Văn học - Tiểu thuyết",
    "tác giả - tác phẩm":               "Văn học - Tiểu thuyết",
    "light novel":                       "Văn học - Tiểu thuyết",
    "truyện ngắn - tản văn":             "Văn học - Thơ - Tản văn",
    "thơ ca - tục ngữ - ca dao":         "Văn học - Thơ - Tản văn",
    "phóng sự - ký sự - phê bình vh":   "Văn học - Thơ - Tản văn",
    "hài hước - truyện cười":            "Văn học - Thơ - Tản văn",
    "sách ảnh":                          "Văn học - Thơ - Tản văn",
    "ngôn tình":                         "Ngôn tình - Lãng mạn",
    "tuổi teen":                         "Ngôn tình - Lãng mạn",
    "truyện trinh thám - kiếm hiệp":     "Trinh thám - Pháp y",
    "huyền bí - giả tưởng - kinh dị":   "Kinh dị - Thriller",
    "truyện tranh":                      "Truyện tranh",
    "du ký":                             "Hồi ký - Tự truyện",
    "sách tô màu người lớn":             "Kỹ năng sống - Phát triển bản thân",
    "12 cung hoàng đạo":                "Tôn giáo - Tâm linh",
    # Kinh tế
    "kinh tế":                           "Kinh tế - Quản lý",
    "quản trị - lãnh đạo":               "Kinh tế - Quản lý",
    "marketing - bán hàng":              "Kinh tế - Quản lý",
    "tài chính - đầu tư":                "Kinh tế - Quản lý",
    "khởi nghiệp - kinh doanh":          "Kinh tế - Quản lý",
    "kinh tế học":                       "Kinh tế - Quản lý",
    "bất động sản":                      "Kinh tế - Quản lý",
    "chứng khoán":                       "Kinh tế - Quản lý",
    "nhân sự":                           "Kinh tế - Quản lý",
    "sách kinh tế combo":                "Kinh tế - Quản lý",
    # Tâm lý
    "tâm lý - kỹ năng sống":             "Kỹ năng sống - Phát triển bản thân",
    "kỹ năng sống":                      "Kỹ năng sống - Phát triển bản thân",
    "tâm lý":                            "Kỹ năng sống - Phát triển bản thân",
    "sách cho tuổi mới lớn":             "Kỹ năng sống - Phát triển bản thân",
    "chicken soup - hạt giống tâm hồn":  "Kỹ năng sống - Phát triển bản thân",
    "rèn luyện nhân cách":               "Kỹ năng sống - Phát triển bản thân",
    # Nuôi dạy con
    "nuôi dạy con":                      "Nuôi dạy con",
    "cẩm nang làm cha mẹ":               "Nuôi dạy con",
    "phát triển kỹ năng - trí tuệ cho trẻ": "Nuôi dạy con",
    "phương pháp giáo dục trẻ các nước": "Nuôi dạy con",
    "dinh dưỡng - sức khỏe cho trẻ":    "Nuôi dạy con",
    "giáo dục trẻ tuổi teen":            "Nuôi dạy con",
    "dành cho mẹ bầu":                   "Nuôi dạy con",
    # Thiếu nhi
    "thiếu nhi":                         "Sách thiếu nhi",
    "truyện thiếu nhi":                  "Sách thiếu nhi",
    "kiến thức - kỹ năng sống cho trẻ":  "Sách thiếu nhi",
    "kiến thức bách khoa":               "Sách thiếu nhi",
    "tô màu, luyện chữ":                 "Sách thiếu nhi",
    "từ điển thiếu nhi":                 "Sách thiếu nhi",
    "flashcard - thẻ học thông minh":    "Sách thiếu nhi",
    "sách nói":                          "Sách thiếu nhi",
    "tạp chí thiếu nhi":                 "Sách thiếu nhi",
    # Tiểu sử hồi ký
    "tiểu sử - hồi ký":                  "Hồi ký - Tự truyện",
    "câu chuyện cuộc đời":               "Hồi ký - Tự truyện",
    "lịch sử":                           "Lịch sử - Địa lý",
    "nghệ thuật - giải trí":             "Hồi ký - Tự truyện",
    "chính trị":                         "Chính trị - Pháp luật",
    "thể thao":                          "Hồi ký - Tự truyện",
    # Giáo khoa
    "giáo khoa - tham khảo":             "Giáo khoa - Tham khảo",
    "sách tham khảo":                    "Giáo khoa - Tham khảo",
    "sách giáo khoa":                    "Giáo khoa - Tham khảo",
    "mẫu giáo":                          "Giáo khoa - Tham khảo",
    "sách giáo viên":                    "Giáo khoa - Tham khảo",
    "đại học":                           "Giáo khoa - Tham khảo",
    # Ngoại ngữ
    "sách học ngoại ngữ":                "Sách học ngoại ngữ",
    "tiếng anh":                         "Tiếng Anh",
    "tiếng hoa":                         "Tiếng Hoa - Tiếng Trung",
    "tiếng nhật":                        "Tiếng Nhật",
    "tiếng hàn":                         "Tiếng Hàn",
    "ngoại ngữ khác":                    "Sách học ngoại ngữ",
    "tiếng việt cho người nước ngoài":   "Sách học ngoại ngữ",
    "tiếng đức":                         "Sách học ngoại ngữ",
    "tiếng pháp":                        "Sách học ngoại ngữ",
}


def map_category(raw: str) -> str:
    return CATEGORY_MAPPING.get(raw.strip().lower(), raw.strip())


# ─── DB HELPERS ──────────────────────────────────────────────────────────────

def upsert_author(cur, name: str) -> int:
    """INSERT hoặc UPDATE author, trả về author_id."""
    name = (name or "Không rõ").strip()[:100]
    cur.execute(
        """INSERT INTO authors (author_name, status)
           VALUES (%s, 'active')
           ON DUPLICATE KEY UPDATE
             status='active', updated_at=NOW()""",
        (name,)
    )
    cur.execute("SELECT author_id FROM authors WHERE author_name=%s", (name,))
    return cur.fetchone()[0]


def upsert_publisher(cur, name: str) -> int:
    """INSERT hoặc UPDATE publisher, trả về publisher_id."""
    name = (name or "Không rõ").strip()[:100]
    cur.execute(
        """INSERT INTO publishers (publisher_name, status)
           VALUES (%s, 'active')
           ON DUPLICATE KEY UPDATE
             status='active', updated_at=NOW()""",
        (name,)
    )
    cur.execute("SELECT publisher_id FROM publishers WHERE publisher_name=%s", (name,))
    return cur.fetchone()[0]


def upsert_category(cur, name: str, cache: dict) -> int:
    """INSERT hoặc lấy category_id (có cache để tránh query lặp)."""
    name = (name or "Chưa phân loại").strip()[:100]
    if name in cache:
        return cache[name]
    cur.execute(
        """INSERT INTO categories (category_name, status)
           VALUES (%s, 'active')
           ON DUPLICATE KEY UPDATE
             status='active', updated_at=NOW()""",
        (name,)
    )
    cur.execute("SELECT category_id FROM categories WHERE category_name=%s", (name,))
    cat_id = cur.fetchone()[0]
    cache[name] = cat_id
    return cat_id


def upsert_book(cur, data: dict) -> int:
    """
    Tìm sách theo ISBN (ưu tiên) hoặc title.
    Nếu tìm thấy → UPDATE, không → INSERT.
    Trả về book_id.
    """
    existing_id = None

    if data.get("isbn"):
        cur.execute("SELECT book_id FROM books WHERE isbn=%s", (data["isbn"],))
        row = cur.fetchone()
        if row:
            existing_id = row[0]

    if not existing_id:
        cur.execute("SELECT book_id FROM books WHERE title=%s", (data["title"][:255],))
        row = cur.fetchone()
        if row:
            existing_id = row[0]

    if existing_id:
        cur.execute("""
            UPDATE books SET
              author_id=%s, publisher_id=%s, price=%s, stock_quantity=%s,
              description=%s, publication_year=%s, isbn=%s,
              language=%s, format=%s, status='active', updated_at=NOW()
            WHERE book_id=%s""",
            (data["author_id"], data["publisher_id"], data["price"],
             data["stock_quantity"], data["description"], data["publication_year"],
             data["isbn"] or None, data["language"], data["format"], existing_id))
        return existing_id

    cur.execute("""
        INSERT INTO books
          (title, author_id, publisher_id, price, stock_quantity,
           description, publication_year, isbn, avg_rating, rating_count,
           language, format, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0.00,0,%s,%s,'active')""",
        (data["title"][:255], data["author_id"], data["publisher_id"],
         data["price"], data["stock_quantity"], data["description"],
         data["publication_year"], data["isbn"] or None,
         data["language"][:50], data["format"]))
    return cur.lastrowid


def upsert_book_image(cur, book_id: int, image_path: str, is_main: int):
    """Xóa ảnh main cũ (nếu is_main=1) rồi INSERT IGNORE ảnh mới."""
    if is_main:
        cur.execute("DELETE FROM book_images WHERE book_id=%s AND is_main=1", (book_id,))
    cur.execute(
        "INSERT IGNORE INTO book_images (book_id, image_url, is_main) VALUES (%s,%s,%s)",
        (book_id, image_path[:255], is_main)
    )


def link_category(cur, book_id: int, category_id: int):
    cur.execute(
        "INSERT IGNORE INTO book_categories (book_id, category_id) VALUES (%s,%s)",
        (book_id, category_id)
    )


# ─── XỬ LÝ ẢNH ───────────────────────────────────────────────────────────────

def handle_image(book_id: int, local_image_col: str, image_url_col: str) -> str:
    """
    Tìm file ảnh trong scraped_images/ (theo cột local_image).
    Copy sang covers/books/<book_id>/<book_id>.jpg.
    Trả về đường dẫn chuẩn để lưu vào book_images.image_url.
    Nếu không có file local → trả về image_url gốc (CDN).
    """
    standard_path = f"covers/books/{book_id}/{book_id}.jpg"
    dest = COVERS_DIR / str(book_id) / f"{book_id}.jpg"

    # Tìm file local
    local_img = (local_image_col or "").strip()
    src = None

    if local_img:
        # local_image có thể là: "scraped_images\slug.jpg" hoặc "scraped_images/slug.jpg"
        local_path = BASE_DIR / local_img.replace("\\", "/")
        if local_path.exists():
            src = local_path
        else:
            # Thử tìm theo tên file trong scraped_images/
            fname = Path(local_img.replace("\\", "/")).name
            candidate = SCRAPED_IMGS / fname
            if candidate.exists():
                src = candidate

    if src:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        return standard_path

    # Không có local → trả về CDN URL (vẫn lưu vào book_images)
    return (image_url_col or "").strip()[:255] or standard_path


# ─── IMPORT MỘT FILE CSV ─────────────────────────────────────────────────────

def import_csv(csv_path: Path, conn, args, cat_cache: dict) -> tuple[int, int, int]:
    """Import một file CSV. Trả về (ok, skip, err)."""
    cur = conn.cursor() if conn else None
    ok = skip = err = 0

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"\n{'='*60}")
    print(f"📂 File: {csv_path.name}  ({len(rows)} dòng)")

    for i, row in enumerate(rows, 1):
        title       = (row.get("title") or "").strip()
        author      = (row.get("author") or "Không rõ").strip()
        pub         = (row.get("publisher") or "Không rõ").strip()
        cat_raw     = (row.get("category") or "Chưa phân loại").strip()
        price_s     = (row.get("price") or "0").strip()
        isbn_s      = re.sub(r"[^\dX]", "", (row.get("isbn") or ""))[:13] or None
        year_s      = (row.get("publication_year") or "").strip()
        lang        = (row.get("language") or "Tiếng Việt").strip()[:50]
        fmt_raw     = (row.get("format") or "").lower()
        desc        = (row.get("description") or "").strip()
        image_url   = (row.get("image_url") or "").strip()
        local_image = (row.get("local_image") or "").strip()

        if not title:
            skip += 1
            continue

        try:
            price = float(price_s) if price_s else 0.0
        except ValueError:
            price = 0.0

        try:
            pub_year = int(year_s) if year_s else None
        except ValueError:
            pub_year = None

        fmt = ("hardcover" if "cứng" in fmt_raw or "hardcover" in fmt_raw
               else "ebook" if "ebook" in fmt_raw
               else "paperback")

        cat_name = map_category(cat_raw)

        print(f"  [{i}/{len(rows)}] {title[:45]}", end=" ... ")

        if args.dry_run or cur is None:
            print(f"🔍 {author[:20]} | {price}")
            ok += 1
            continue

        try:
            author_id    = upsert_author(cur, author)
            publisher_id = upsert_publisher(cur, pub)
            category_id  = upsert_category(cur, cat_name, cat_cache)

            data = {
                "title":            title[:255],
                "author_id":        author_id,
                "publisher_id":     publisher_id,
                "price":            round(price, 2),
                "stock_quantity":   10,
                "description":      desc[:5000],
                "publication_year": pub_year,
                "isbn":             isbn_s,
                "language":         lang,
                "format":           fmt,
            }

            book_id = upsert_book(cur, data)
            link_category(cur, book_id, category_id)

            if not args.no_images and (image_url or local_image):
                img_path = handle_image(book_id, local_image, image_url)
                upsert_book_image(cur, book_id, img_path, 1)
                print(f"🖼 ", end="")

            conn.commit()
            ok += 1
            print(f"✅ #{book_id}")

        except Exception as e:
            conn.rollback()
            print(f"❌ {e}")
            err += 1

    if cur:
        cur.close()
    return ok, skip, err


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import tất cả CSV scraped → MySQL bookstore")
    parser.add_argument("--file",       help="Chỉ import 1 file CSV cụ thể", default="")
    parser.add_argument("--dry-run",    action="store_true", help="Preview, không ghi DB")
    parser.add_argument("--no-images",  action="store_true", help="Bỏ qua xử lý ảnh")
    parser.add_argument("--db-host",    default=DB_CONFIG["host"])
    parser.add_argument("--db-port",    type=int, default=DB_CONFIG["port"])
    parser.add_argument("--db-user",    default=DB_CONFIG["user"])
    parser.add_argument("--db-pass",    default=DB_CONFIG["password"])
    parser.add_argument("--db-name",    default=DB_CONFIG["database"])
    args = parser.parse_args()

    # Danh sách file CSV cần xử lý
    if args.file:
        csv_files = [Path(args.file)]
        if not csv_files[0].exists():
            print(f"❌ Không tìm thấy file: {csv_files[0]}")
            sys.exit(1)
    else:
        csv_files = sorted(SCRAPED_DATA.glob("*.csv"))
        if not csv_files:
            print(f"❌ Không có file CSV nào trong {SCRAPED_DATA}")
            sys.exit(1)

    print(f"📋 Sẽ import {len(csv_files)} file CSV")
    for f in csv_files:
        print(f"   • {f.name}")

    if args.dry_run:
        print("\n🔍 DRY-RUN mode – không ghi dữ liệu vào DB")
        conn = None
    else:
        cfg = {**DB_CONFIG, "host": args.db_host, "port": args.db_port,
               "user": args.db_user, "password": args.db_pass, "database": args.db_name}
        print(f"\n🔗 Kết nối MySQL {cfg['host']}:{cfg['port']}/{cfg['database']}...")
        conn = mysql.connector.connect(**cfg)
        print("✅ Kết nối thành công")
        COVERS_DIR.mkdir(parents=True, exist_ok=True)

    total_ok = total_skip = total_err = 0
    cat_cache: dict[str, int] = {}

    for csv_path in csv_files:
        ok, skip, err = import_csv(csv_path, conn, args, cat_cache)
        total_ok   += ok
        total_skip += skip
        total_err  += err
        print(f"  → +{ok} sách | bỏ qua {skip} | lỗi {err}")

    if conn and not args.dry_run:
        conn.close()

    print(f"\n{'='*60}")
    print(f"🎉 HOÀN TẤT IMPORT!")
    print(f"   ✅ Thành công : {total_ok} sách")
    print(f"   ⚠  Bỏ qua    : {total_skip} dòng (không có tiêu đề)")
    print(f"   ❌ Lỗi       : {total_err} dòng")
    if not args.no_images:
        print(f"   🖼 Ảnh đã copy vào: covers/books/<book_id>/<book_id>.jpg")


if __name__ == "__main__":
    main()
