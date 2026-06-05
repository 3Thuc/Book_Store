import os
import re
import mysql.connector
from minio import Minio

# MinIO configs
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456789")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bookstore")

# MySQL configs
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWORD = ''
MYSQL_DB = 'bookstore'

def get_minio_books():
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    objects = client.list_objects(bucket_name=MINIO_BUCKET, prefix="covers/books/", recursive=True)
    minio_files = 0
    minio_books = set()
    
    for obj in objects:
        minio_files += 1
        filename = obj.object_name
        # Tìm các con số trong đường dẫn file ảnh
        # VD: covers/books/6451/6451.jpg -> 6451
        m = re.search(r'(\d+)', filename.split('/')[-1])
        if not m:
            m = re.search(r'(\d+)', filename)
        if m:
            minio_books.add(int(m.group(1)))
            
    return minio_files, minio_books

def get_db_books():
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT book_id FROM books")
    db_books = set([row['book_id'] for row in cursor.fetchall()])
    
    # Phân loại ảnh local MinIO và ảnh CDN ngoại
    cursor.execute("SELECT book_id, image_url FROM book_images WHERE is_main = 1")
    db_minio_books = set()
    db_external_books = set()
    
    for row in cursor.fetchall():
        url = row['image_url'] or ""
        # Nếu url chứa http://cdn hoặc https://cdn hoặc fahasa
        if 'cdn' in url or 'fahasa' in url:
            db_external_books.add(row['book_id'])
        else:
            db_minio_books.add(row['book_id'])
            
    return db_books, db_minio_books, db_external_books

def main():
    report = []
    report.append("="*60)
    report.append("KIEM TRA DOI CHIEU DU LIEU TU MINIO VA MYSQL")
    report.append("="*60)
    
    report.append("\n1. Dang tai danh sach anh tu MinIO...")
    minio_files, minio_books = get_minio_books()
    report.append(f"   => Tong so file vat ly trong thu muc MinIO: {minio_files}")
    report.append(f"   => So luong sach co anh tren MinIO: {len(minio_books)}")
    
    report.append("\n2. Dang quet MySQL Database...")
    db_books, db_minio_books, db_external_books = get_db_books()
    report.append(f"   => Tong so sach trong Database: {len(db_books)}")
    report.append(f"   => Sach dung anh ngoai CDN: {len(db_external_books)}")
    report.append(f"   => Sach dung anh luu tren MinIO: {len(db_minio_books)}")
    
    report.append("\n" + "="*60)
    report.append("KET QUA DOI CHIEU")
    report.append("="*60)
    
    missing_in_minio = db_minio_books - minio_books
    report.append(f"⚠️  Sach khai bao co anh nhung FILE KHONG TON TAI: {len(missing_in_minio)}")
    if missing_in_minio:
        report.append(f"     => book_id bi thieu: {list(missing_in_minio)[:50]} ...")
        
    missing_in_db = minio_books - db_books
    report.append(f"\n🗑️  File anh rác (khong co trong db): {len(missing_in_db)}")
    if missing_in_db:
        report.append(f"     => book_id anh rac: {list(missing_in_db)[:50]} ...")
        
    report.append("\n=> TONG KET:")
    total_valid = len(db_external_books) + len(db_minio_books)
    report.append(f"   => Tong so sach hop le co anh: {total_valid} cuon.")
    
    with open('report.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

if __name__ == "__main__":
    main()
