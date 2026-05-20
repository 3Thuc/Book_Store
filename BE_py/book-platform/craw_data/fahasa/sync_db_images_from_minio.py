"""
Cập nhật đúng extension ảnh (jpg, webp, png...) trong bảng book_images MySQL
dựa trên file thực tế đang được lưu trữ trên MinIO.

Cách chạy:
    python sync_db_images_from_minio.py
"""

import sys
import mysql.connector

try:
    from minio import Minio
except ImportError:
    print("Vui lòng cài đặt thư viện minio: pip install minio")
    sys.exit(1)

# CẤU HÌNH MINIO
MINIO_ENDPOINT  = "localhost:9000"
MINIO_ACCESS    = "admin"
MINIO_SECRET    = "admin123456789"
MINIO_BUCKET    = "bookstore"
MINIO_PREFIX    = "covers/books/"

# CẤU HÌNH MYSQL DB
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "1900561275Nghia",
    "database": "bookstore",
    "charset":  "utf8mb4",
}

def main():
    # 1. Kết nối MinIO
    print(f"🔗 Kết nối MinIO: {MINIO_ENDPOINT} ...")
    try:
        minio_client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS,
            secret_key=MINIO_SECRET,
            secure=False
        )
        
        # Duyệt qua tất cả các file có dạng covers/books/...
        objects = minio_client.list_objects(bucket_name=MINIO_BUCKET, prefix=MINIO_PREFIX, recursive=True)
        
        # Dictionary map book_id -> correct_image_url
        minio_images = {}
        for obj in objects:
            # obj.object_name VD: covers/books/1287/1287.webp
            path = obj.object_name
            # Trích xuất book_id từ path (đảm bảo path đúng format)
            parts = path.split('/')
            if len(parts) >= 4:
                book_id_str = parts[2]
                if book_id_str.isdigit():
                    book_id = int(book_id_str)
                    # Lưu lại với path này
                    minio_images[book_id] = path
                    
        print(f"📦 Đã tìm thấy {len(minio_images)} ảnh sách trên MinIO.")
    except Exception as e:
        print(f"❌ Lỗi MinIO: {e}")
        return

    # 2. Kết nối tới MySQL và cập nhật book_images
    print("🔗 Kết nối MySQL...")
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        update_count = 0
        skip_count = 0
        
        for book_id, correct_url in minio_images.items():
            # Cập nhật duy nhất những bìa chính hoặc tất cả cover mapping khớp vs ID
            cur.execute(
                "UPDATE book_images SET image_url=%s WHERE book_id=%s AND image_url LIKE %s",
                (correct_url, book_id, "covers/books/%")
            )
            if cur.rowcount > 0:
                update_count += cur.rowcount
            else:
                skip_count += 1
                
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"🎉 Hoàn tất quá trình đồng bộ!")
        print(f"   ✅ Đã sửa/cập nhật: {update_count} đường dẫn ảnh trong MySQL")
        
    except Exception as e:
        print(f"❌ Lỗi MySQL: {e}")

if __name__ == "__main__":
    main()
