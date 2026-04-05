"""
deduplicate_mysql.py – Rà soát và xóa dữ liệu TRÙNG trong MySQL bookstore
==========================================================================
Rà soát các bảng:
  1. authors      – trùng author_name
  2. publishers   – trùng publisher_name
  3. categories   – trùng category_name
  4. books        – trùng title HOẶC isbn
  5. book_images  – trùng (book_id, image_url) hoặc nhiều ảnh main cùng book

Chiến lược: Giữ lại bản ghi có ID nhỏ nhất (được tạo sớm nhất),
            xóa các bản ghi có ID lớn hơn (duplicates).
            Trước khi xóa sẽ re-link FK sang bản ghi còn lại.

Cách dùng:
  python deduplicate_mysql.py              # Xem báo cáo + xóa thật
  python deduplicate_mysql.py --dry-run    # Chỉ xem báo cáo, KHÔNG xóa
  python deduplicate_mysql.py --report     # Chỉ in báo cáo chi tiết
"""

import argparse
import sys
from pathlib import Path

import mysql.connector

# ─── CẤU HÌNH ────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "1900561275Nghia",
    "database": "bookstore",
    "charset":  "utf8mb4",
}


def connect(cfg): return mysql.connector.connect(**cfg)


# ═══════════════════════════════════════════════════════════════════════════════
#  1. DEDUP AUTHORS
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_authors(cur, dry_run=False):
    print("\n" + "="*55)
    print("👤 RÀ SOÁT BẢNG: authors (trùng author_name)")
    print("="*55)

    # Tìm tên trùng
    cur.execute("""
        SELECT author_name, COUNT(*) AS cnt, MIN(author_id) AS keep_id
        FROM authors
        GROUP BY author_name
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    groups = cur.fetchall()

    if not groups:
        print("  ✅ Không có tác giả trùng.")
        return 0

    total_deleted = 0
    for (name, cnt, keep_id) in groups:
        # Lấy danh sách ID sẽ xóa
        cur.execute("SELECT author_id FROM authors WHERE author_name=%s AND author_id!=%s",
                    (name, keep_id))
        dup_ids = [r[0] for r in cur.fetchall()]
        print(f"  ⚠ '{name}' – {cnt} bản, giữ #{keep_id}, xóa {dup_ids}")

        if not dry_run:
            for dup_id in dup_ids:
                # Re-link books sang keep_id
                cur.execute("UPDATE books SET author_id=%s WHERE author_id=%s",
                            (keep_id, dup_id))
                # Xóa author trùng
                cur.execute("DELETE FROM authors WHERE author_id=%s", (dup_id,))
        total_deleted += len(dup_ids)

    print(f"  → Tổng xóa: {total_deleted} author trùng")
    return total_deleted


# ═══════════════════════════════════════════════════════════════════════════════
#  2. DEDUP PUBLISHERS
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_publishers(cur, dry_run=False):
    print("\n" + "="*55)
    print("🏢 RÀ SOÁT BẢNG: publishers (trùng publisher_name)")
    print("="*55)

    cur.execute("""
        SELECT publisher_name, COUNT(*) AS cnt, MIN(publisher_id) AS keep_id
        FROM publishers
        GROUP BY publisher_name
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    groups = cur.fetchall()

    if not groups:
        print("  ✅ Không có nhà xuất bản trùng.")
        return 0

    total_deleted = 0
    for (name, cnt, keep_id) in groups:
        cur.execute("SELECT publisher_id FROM publishers WHERE publisher_name=%s AND publisher_id!=%s",
                    (name, keep_id))
        dup_ids = [r[0] for r in cur.fetchall()]
        print(f"  ⚠ '{name}' – {cnt} bản, giữ #{keep_id}, xóa {dup_ids}")

        if not dry_run:
            for dup_id in dup_ids:
                cur.execute("UPDATE books SET publisher_id=%s WHERE publisher_id=%s",
                            (keep_id, dup_id))
                cur.execute("DELETE FROM publishers WHERE publisher_id=%s", (dup_id,))
        total_deleted += len(dup_ids)

    print(f"  → Tổng xóa: {total_deleted} publisher trùng")
    return total_deleted


# ═══════════════════════════════════════════════════════════════════════════════
#  3. DEDUP CATEGORIES
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_categories(cur, dry_run=False):
    print("\n" + "="*55)
    print("📂 RÀ SOÁT BẢNG: categories (trùng category_name)")
    print("="*55)

    cur.execute("""
        SELECT category_name, COUNT(*) AS cnt, MIN(category_id) AS keep_id
        FROM categories
        GROUP BY category_name
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    groups = cur.fetchall()

    if not groups:
        print("  ✅ Không có danh mục trùng.")
        return 0

    total_deleted = 0
    for (name, cnt, keep_id) in groups:
        cur.execute("SELECT category_id FROM categories WHERE category_name=%s AND category_id!=%s",
                    (name, keep_id))
        dup_ids = [r[0] for r in cur.fetchall()]
        print(f"  ⚠ '{name}' – {cnt} bản, giữ #{keep_id}, xóa {dup_ids}")

        if not dry_run:
            for dup_id in dup_ids:
                # Re-link book_categories
                cur.execute("UPDATE book_categories SET category_id=%s "
                            "WHERE category_id=%s", (keep_id, dup_id))
                # Bỏ link trùng sau khi update (vd book đã có keep_id rồi)
                cur.execute("""
                    DELETE bc1 FROM book_categories bc1
                    INNER JOIN book_categories bc2
                      ON bc1.book_id=bc2.book_id AND bc1.category_id=bc2.category_id
                    WHERE bc1.id > bc2.id
                """ if False else "SELECT 1")  # placeholder – xử lý dưới
                cur.execute("DELETE FROM categories WHERE category_id=%s", (dup_id,))
        total_deleted += len(dup_ids)

    print(f"  → Tổng xóa: {total_deleted} category trùng")
    return total_deleted


# ═══════════════════════════════════════════════════════════════════════════════
#  4. DEDUP BOOKS (theo title, rồi theo isbn)
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_books(cur, dry_run=False):
    print("\n" + "="*55)
    print("📚 RÀ SOÁT BẢNG: books (trùng title hoặc isbn)")
    print("="*55)

    total_deleted = 0

    # --- 4a. Trùng ISBN (bỏ qua NULL và rỗng) ---
    print("\n  [a] Theo ISBN:")
    cur.execute("""
        SELECT isbn, COUNT(*) AS cnt, MIN(book_id) AS keep_id
        FROM books
        WHERE isbn IS NOT NULL AND isbn != ''
        GROUP BY isbn
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)
    isbn_groups = cur.fetchall()

    if not isbn_groups:
        print("     ✅ Không có sách trùng ISBN.")
    else:
        for (isbn, cnt, keep_id) in isbn_groups:
            cur.execute("SELECT book_id, title FROM books WHERE isbn=%s AND book_id!=%s",
                        (isbn, keep_id))
            dups = cur.fetchall()
            print(f"     ⚠ ISBN={isbn} – {cnt} bản, giữ #{keep_id}")
            for (dup_id, dup_title) in dups:
                print(f"        xóa #{dup_id}: {dup_title[:50]}")
                if not dry_run:
                    _relink_and_delete_book(cur, dup_id, keep_id)
            total_deleted += len(dups)

    # --- 4b. Trùng Title ---
    print("\n  [b] Theo Title:")
    cur.execute("""
        SELECT title, COUNT(*) AS cnt, MIN(book_id) AS keep_id
        FROM books
        GROUP BY title
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 200
    """)
    title_groups = cur.fetchall()

    if not title_groups:
        print("     ✅ Không có sách trùng Title.")
    else:
        for (title, cnt, keep_id) in title_groups:
            cur.execute("SELECT book_id FROM books WHERE title=%s AND book_id!=%s",
                        (title, keep_id))
            dup_ids = [r[0] for r in cur.fetchall()]
            print(f"     ⚠ '{title[:45]}' – {cnt} bản, giữ #{keep_id}, xóa {dup_ids}")
            if not dry_run:
                for dup_id in dup_ids:
                    _relink_and_delete_book(cur, dup_id, keep_id)
            total_deleted += len(dup_ids)

    print(f"\n  → Tổng xóa: {total_deleted} sách trùng")
    return total_deleted


def _relink_and_delete_book(cur, dup_id, keep_id):
    """Chuyển tất cả FK reference từ dup_id sang keep_id rồi xóa dup_id."""
    # book_categories
    cur.execute("DELETE FROM book_categories WHERE book_id=%s", (dup_id,))
    # book_images – chuyển ảnh chưa có ở keep_id
    cur.execute("""
        UPDATE book_images SET book_id=%s
        WHERE book_id=%s
          AND image_url NOT IN (SELECT image_url FROM (
              SELECT image_url FROM book_images WHERE book_id=%s
          ) AS t)
    """, (keep_id, dup_id, keep_id))
    # Xóa ảnh không chuyển được (đã trùng url)
    cur.execute("DELETE FROM book_images WHERE book_id=%s", (dup_id,))
    # reviews, order_items, cart nếu có
    for tbl, col in [
        ("reviews",      "book_id"),
        ("order_items",  "book_id"),
        ("cart_items",   "book_id"),
        ("wishlist",     "book_id"),
    ]:
        try:
            cur.execute(f"UPDATE {tbl} SET {col}=%s WHERE {col}=%s", (keep_id, dup_id))
        except Exception:
            pass  # bảng không tồn tại thì bỏ qua
    cur.execute("DELETE FROM books WHERE book_id=%s", (dup_id,))


# ═══════════════════════════════════════════════════════════════════════════════
#  5. DEDUP BOOK_IMAGES
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_book_images(cur, dry_run=False):
    print("\n" + "="*55)
    print("🖼  RÀ SOÁT BẢNG: book_images")
    print("="*55)

    total_deleted = 0

    # --- 5a. Trùng (book_id, image_url) ---
    print("\n  [a] Trùng (book_id, image_url):")
    cur.execute("""
        SELECT book_id, image_url, COUNT(*) AS cnt, MIN(image_id) AS keep_id
        FROM book_images
        GROUP BY book_id, image_url
        HAVING cnt > 1
    """)
    groups = cur.fetchall()

    if not groups:
        print("     ✅ Không có ảnh trùng URL.")
    else:
        for (book_id, url, cnt, keep_id) in groups:
            cur.execute("SELECT image_id FROM book_images "
                        "WHERE book_id=%s AND image_url=%s AND image_id!=%s",
                        (book_id, url, keep_id))
            dup_ids = [r[0] for r in cur.fetchall()]
            print(f"     ⚠ book#{book_id} url='{url[-40:]}' – {cnt} bản, xóa {dup_ids}")
            if not dry_run:
                for dup_id in dup_ids:
                    cur.execute("DELETE FROM book_images WHERE image_id=%s", (dup_id,))
            total_deleted += len(dup_ids)

    # --- 5b. Nhiều ảnh main (is_main=1) cho cùng book ---
    print("\n  [b] Nhiều ảnh main (is_main=1) cho cùng book:")
    cur.execute("""
        SELECT book_id, COUNT(*) AS cnt, MIN(image_id) AS keep_id
        FROM book_images
        WHERE is_main = 1
        GROUP BY book_id
        HAVING cnt > 1
    """)
    main_groups = cur.fetchall()

    if not main_groups:
        print("     ✅ Không có book nào bị nhiều ảnh main.")
    else:
        for (book_id, cnt, keep_id) in main_groups:
            cur.execute("SELECT image_id FROM book_images "
                        "WHERE book_id=%s AND is_main=1 AND image_id!=%s",
                        (book_id, keep_id))
            dup_ids = [r[0] for r in cur.fetchall()]
            print(f"     ⚠ book#{book_id} – {cnt} ảnh main, giữ #{keep_id}, xóa {dup_ids}")
            if not dry_run:
                for dup_id in dup_ids:
                    cur.execute("DELETE FROM book_images WHERE image_id=%s", (dup_id,))
            total_deleted += len(dup_ids)

    print(f"\n  → Tổng xóa: {total_deleted} ảnh trùng")
    return total_deleted


# ═══════════════════════════════════════════════════════════════════════════════
#  6. XÓA BOOK_CATEGORIES TRÙNG
# ═══════════════════════════════════════════════════════════════════════════════

def dedup_book_categories(cur, dry_run=False):
    print("\n" + "="*55)
    print("🔗 RÀ SOÁT BẢNG: book_categories (trùng book_id+category_id)")
    print("="*55)

    # Kiểm tra bảng có cột id không
    cur.execute("SHOW COLUMNS FROM book_categories")
    cols = [r[0] for r in cur.fetchall()]

    if "id" not in cols:
        # Xóa trực tiếp nếu không có PK id
        cur.execute("""
            SELECT book_id, category_id, COUNT(*) AS cnt
            FROM book_categories
            GROUP BY book_id, category_id
            HAVING cnt > 1
        """)
        groups = cur.fetchall()
        if not groups:
            print("  ✅ Không có liên kết trùng.")
            return 0
        total = 0
        for (bid, cid, cnt) in groups:
            print(f"  ⚠ book#{bid} – category#{cid}: {cnt} lần")
            if not dry_run:
                # Xóa hết rồi insert lại 1 lần
                cur.execute("DELETE FROM book_categories WHERE book_id=%s AND category_id=%s",
                            (bid, cid))
                cur.execute("INSERT INTO book_categories (book_id, category_id) VALUES (%s,%s)",
                            (bid, cid))
            total += cnt - 1
        print(f"  → Tổng dư thừa: {total}")
        return total

    # Nếu có cột id
    cur.execute("""
        SELECT book_id, category_id, COUNT(*) AS cnt, MIN(id) AS keep_id
        FROM book_categories
        GROUP BY book_id, category_id
        HAVING cnt > 1
    """)
    groups = cur.fetchall()
    if not groups:
        print("  ✅ Không có liên kết trùng.")
        return 0
    total = 0
    for (bid, cid, cnt, keep_id) in groups:
        if not dry_run:
            cur.execute("DELETE FROM book_categories WHERE book_id=%s AND category_id=%s AND id!=%s",
                        (bid, cid, keep_id))
        total += cnt - 1
        print(f"  ⚠ book#{bid} – category#{cid}: xóa {cnt-1} dòng thừa")
    print(f"  → Tổng xóa: {total}")
    return total


# ═══════════════════════════════════════════════════════════════════════════════
#  BÁO CÁO THỐNG KÊ
# ═══════════════════════════════════════════════════════════════════════════════

def print_stats(cur):
    print("\n" + "="*55)
    print("📊 THỐNG KÊ DATABASE HIỆN TẠI")
    print("="*55)
    tables = [
        ("books",           "book_id",      "Sách"),
        ("authors",         "author_id",    "Tác giả"),
        ("publishers",      "publisher_id", "Nhà xuất bản"),
        ("categories",      "category_id",  "Danh mục"),
        ("book_images",     "image_id",     "Ảnh sách"),
        ("book_categories", None,           "Liên kết sách-danh mục"),
    ]
    for (tbl, pk, label) in tables:
        try:
            if pk:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            else:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            cnt = cur.fetchone()[0]
            print(f"  {label:36s}: {cnt:>6,} dòng")
        except Exception as e:
            print(f"  {label:36s}: (lỗi: {e})")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Rà soát và xóa dữ liệu trùng trong MySQL bookstore")
    parser.add_argument("--dry-run",  action="store_true", help="Chỉ báo cáo, KHÔNG xóa dữ liệu")
    parser.add_argument("--report",   action="store_true", help="Chỉ in thống kê tổng quan")
    parser.add_argument("--db-host",  default=DB_CONFIG["host"])
    parser.add_argument("--db-port",  type=int, default=DB_CONFIG["port"])
    parser.add_argument("--db-user",  default=DB_CONFIG["user"])
    parser.add_argument("--db-pass",  default=DB_CONFIG["password"])
    parser.add_argument("--db-name",  default=DB_CONFIG["database"])
    args = parser.parse_args()

    cfg = {**DB_CONFIG,
           "host": args.db_host, "port": args.db_port,
           "user": args.db_user, "password": args.db_pass, "database": args.db_name}

    print(f"🔗 Kết nối MySQL {cfg['host']}:{cfg['port']}/{cfg['database']}...")
    conn = connect(cfg)
    cur  = conn.cursor()
    print("✅ Kết nối thành công")

    # In thống kê trước
    print_stats(cur)

    if args.report:
        cur.close(); conn.close(); return

    mode = "DRY-RUN (không xóa)" if args.dry_run else "THỰC (sẽ xóa dữ liệu trùng)"
    print(f"\n🚀 Chế độ: {mode}")

    total = 0
    total += dedup_authors(cur, args.dry_run)
    if not args.dry_run: conn.commit()

    total += dedup_publishers(cur, args.dry_run)
    if not args.dry_run: conn.commit()

    total += dedup_categories(cur, args.dry_run)
    if not args.dry_run: conn.commit()

    total += dedup_books(cur, args.dry_run)
    if not args.dry_run: conn.commit()

    total += dedup_book_images(cur, args.dry_run)
    if not args.dry_run: conn.commit()

    total += dedup_book_categories(cur, args.dry_run)
    if not args.dry_run: conn.commit()

    # In thống kê sau
    if not args.dry_run:
        print("\n📊 THỐNG KÊ SAU KHI XỬ LÝ:")
        print_stats(cur)

    print(f"\n{'='*55}")
    if args.dry_run:
        print(f"🔍 DRY-RUN: Tìm thấy tổng cộng {total} bản ghi trùng.")
        print(f"   Chạy không có --dry-run để xóa thật sự.")
    else:
        print(f"🎉 HOÀN TẤT! Đã xóa tổng cộng {total} bản ghi trùng.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
