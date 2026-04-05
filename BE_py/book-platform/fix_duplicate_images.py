"""
Script giữ lại 1 ảnh chính (is_main=1) cho mỗi cuốn sách trong bảng book_images.
Nếu sách không có ảnh is_main=1 thì giữ ảnh có image_id nhỏ nhất.
"""
import mysql.connector
import sys

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "1900561275Nghia",
    "database": "bookstore",
}

def main():
    print("=== Kết nối MySQL... ===")
    conn = mysql.connector.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("✅ Kết nối thành công!\n")

    # ─── 1. Thống kê trước khi xóa ──────────────────────
    cur.execute("SELECT COUNT(*) FROM book_images")
    total_before = cur.fetchone()[0]
    print(f"📊 Tổng số dòng hiện tại       : {total_before}")

    cur.execute("SELECT COUNT(DISTINCT book_id) FROM book_images")
    total_books = cur.fetchone()[0]
    print(f"📚 Số sách có ảnh               : {total_books}")

    cur.execute("SELECT COUNT(*) FROM book_images WHERE is_main = 1")
    main_count = cur.fetchone()[0]
    print(f"🖼️  Số ảnh is_main=1             : {main_count}")

    cur.execute("SELECT COUNT(*) FROM book_images WHERE is_main = 0")
    sub_count = cur.fetchone()[0]
    print(f"🗑️  Số ảnh phụ is_main=0         : {sub_count}")

    # Sách không có is_main=1
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT book_id FROM book_images
            GROUP BY book_id
            HAVING SUM(is_main) = 0
        ) t
    """)
    no_main_books = cur.fetchone()[0]
    print(f"⚠️  Sách không có ảnh is_main=1  : {no_main_books}\n")

    # ─── 2. Tìm image_id cần GIỮ LẠI cho mỗi sách ──────
    # Ưu tiên: is_main=1 → nếu không có thì MIN(image_id)
    cur.execute("""
        SELECT
            CASE
                WHEN SUM(is_main) > 0
                    THEN MAX(CASE WHEN is_main = 1 THEN image_id END)
                ELSE MIN(image_id)
            END AS keep_id
        FROM book_images
        GROUP BY book_id
    """)
    keep_ids = [row[0] for row in cur.fetchall()]
    print(f"✅ Số ảnh sẽ giữ lại: {len(keep_ids)}\n")

    # ─── 3. Xóa tất cả dòng KHÔNG nằm trong keep_ids ────
    print("=== Bắt đầu xóa ảnh phụ... ===")

    # Chia thành batch để tránh query quá dài
    BATCH_SIZE = 1000
    total_deleted = 0
    for i in range(0, len(keep_ids), BATCH_SIZE):
        batch = keep_ids[i:i+BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(batch))
        cur.execute(f"""
            DELETE FROM book_images
            WHERE image_id NOT IN ({placeholders})
            AND book_id IN (
                SELECT DISTINCT book_id FROM (
                    SELECT book_id FROM book_images WHERE image_id IN ({placeholders})
                ) AS t
            )
        """, batch + batch)
        total_deleted += cur.rowcount
        conn.commit()
        print(f"  Batch {i//BATCH_SIZE + 1}: đã xóa {cur.rowcount} dòng")

    # ─── 4. Thống kê sau khi xóa ────────────────────────
    cur.execute("SELECT COUNT(*) FROM book_images")
    total_after = cur.fetchone()[0]

    print(f"\n{'='*50}")
    print(f"✅ Hoàn tất!")
    print(f"   Dòng trước: {total_before:,}")
    print(f"   Đã xóa    : {total_before - total_after:,}")
    print(f"   Còn lại   : {total_after:,}")
    print(f"{'='*50}")

    # ─── 5. Kiểm tra: mỗi sách chỉ còn 1 ảnh ───────────
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT book_id FROM book_images
            GROUP BY book_id HAVING COUNT(*) > 1
        ) t
    """)
    still_multi = cur.fetchone()[0]
    if still_multi == 0:
        print("✅ Mỗi sách bây giờ chỉ còn đúng 1 ảnh!")
    else:
        print(f"⚠️  Vẫn còn {still_multi} sách có nhiều hơn 1 ảnh")

    cur.execute("SELECT COUNT(DISTINCT book_id) FROM book_images")
    books_with_img = cur.fetchone()[0]
    print(f"📚 Số sách vẫn có ảnh: {books_with_img}/{total_books}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
