"""
config.py – Cấu hình tập trung cho OCR Service.

Mọi giá trị đều có default hợp lý để chạy được local mà không cần file .env.
Khi deploy Docker, override bằng environment variables trong docker-compose.yml.
"""
import os
from dotenv import load_dotenv

# Load .env file nếu có (development local)
# Trong Docker, environment variables được inject trực tiếp → load_dotenv() không làm gì
load_dotenv()

# ── Search Service ─────────────────────────────────────────────────────────
# URL nội bộ Docker network: "http://fastapi:8000"
# URL local development:     "http://localhost:8000"
SEARCH_SERVICE_URL = os.getenv("SEARCH_SERVICE_URL", "http://localhost:8000")

# ── MySQL (Cho Background Hash Indexing lấy trọn 100% sách) ───────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "bookstore")

# ── Google Books API (optional) ────────────────────────────────────────────
# Nếu không có key → fallback này bị bỏ qua, chỉ dùng Search Service
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY", "")

# ── MinIO Object Storage (optional - lưu ảnh OCR) ─────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123456789")
MINIO_OCR_BUCKET = os.getenv("MINIO_OCR_BUCKET", "ocr-uploads")

# ── OCR Processing Settings ────────────────────────────────────────────────
# Giới hạn kích thước file upload (MB)
# Ảnh > 10MB thường là ảnh RAW camera, quá nặng để xử lý realtime
MAX_IMAGE_SIZE_MB = int(os.getenv("MAX_IMAGE_SIZE_MB", "10"))

# Ngưỡng confidence để chấp nhận kết quả EasyOCR
# 0.0 = chấp nhận mọi kết quả (có thể nhiều lỗi)
# 0.3 = loại bỏ kết quả quá kém (mốc hợp lý cho ảnh bìa sách đa dạng)
# 0.8 = chỉ chấp nhận kết quả rất tốt (quá khắt khe)
OCR_CONFIDENCE_THRESHOLD = float(os.getenv("OCR_CONFIDENCE_THRESHOLD", "0.3"))

# ── Admin Security ────────────────────────────────────────────────────────
# Key bảo vệ các endpoint quản trị (rebuild-index, force-cache-clear...)
# WARNING: Không để trống trong production!
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

# ── CORS ──────────────────────────────────────────────────────────────────
# Danh sách origins hợp lệ, cách nhau bởi dấu phẩy
# VD: "http://localhost:3000,https://bookstore.vn"
# Để trống hoặc "*" để cho phép tất cả (không khuyến khích production)
ALLOWED_ORIGINS_RAW = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000")
ALLOWED_ORIGINS = [o.strip() for o in ALLOWED_ORIGINS_RAW.split(",") if o.strip()]

# ── pHash Index Persistence ───────────────────────────────────────────────
# Đường dẫn file lưu pHash index khi build xong
# Khi restart: load từ file (~<1s) thay vì rebuild từ đầu (~30-60s)
PHASH_INDEX_PATH = os.getenv("PHASH_INDEX_PATH", "/tmp/ocr_phash_index.pkl")

# ── Derived Values ────────────────────────────────────────────────────────
MAX_IMAGE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024  # bytes
