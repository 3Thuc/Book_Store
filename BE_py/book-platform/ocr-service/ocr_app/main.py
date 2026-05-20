"""
main.py – FastAPI entry point cho OCR Service (Port 8005).

Khởi động (local):
    uvicorn ocr_app.main:app --host 0.0.0.0 --port 8005 --reload

Khởi động (Docker):
    CMD trong Dockerfile tự gọi lệnh này.

Điểm đặc biệt của file này:
1. lifespan context manager: pre-load EasyOCR model 1 lần duy nhất vào RAM
   → Request đầu tiên không bị chậm vì load model
2. Singleton pattern: biến _easyocr_reader trong ocr_engine.py
   → Tất cả request dùng chung 1 model instance đã load sẵn
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded
from ocr_app.middleware.rate_limiter import limiter, rate_limit_exceeded_handler
from ocr_app.config import ALLOWED_ORIGINS

# Cấu hình logging chuẩn cho production
# format: "2026-04-05 10:30:00 [INFO] ocr_app.main: Server starting..."
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════
# LIFESPAN – Startup & Shutdown
# ══════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager thay thế @app.on_event("startup") đã deprecated.

    Code TRƯỚC yield: chạy lúc server khởi động (trước khi nhận request đầu tiên)
    Code SAU  yield:  chạy lúc server tắt (cleanup)

    Tại sao pre-load EasyOCR tại đây?
    ──────────────────────────────────
    EasyOCR.Reader(['vi', 'en']) load 2 model vào RAM:
      - Detection model (CRAFT): ~45MB
      - Recognition model (CRNN vi): ~26MB
      - Recognition model (CRNN en): ~22MB
    Tổng: ~93MB model weights + ~1.2GB khi chạy (RAM cho tensor operations)

    Nếu load trong từng request:
      - Lần đầu: chậm 5-15 giây (load từ disk)
      - Request timeout 30s có thể bị lỗi
      - Mỗi request tốn RAM tạm thời để alloc/dealloc model

    Nếu load 1 lần trong lifespan:
      - Chỉ tốn 5-15s khi server khởi động (user chưa vào)
      - Mọi request sau đó: 0ms để load model, chỉ tốn thời gian inference
    """
    logger.info("=" * 60)
    logger.info("🚀 OCR Service đang khởi động (Port 8005)...")
    logger.info("   Engine: EasyOCR (vi+en, CPU) + Tesseract (fallback)")
    logger.info("=" * 60)

    # ── Pre-load EasyOCR Model ───────────────────────────────────
    logger.info("🧠 [1/2] Loading EasyOCR model (vi + en) vào RAM...")
    logger.info("         Lần đầu: 5-15 giây | Sau đó: 0 giây/request")
    try:
        from ocr_app.services.ocr_engine import get_easyocr_reader
        get_easyocr_reader()  # Trigger Singleton load
        logger.info("   ✅ EasyOCR model loaded thành công!")
    except Exception as e:
        # Non-critical: service vẫn chạy, model sẽ load lazy khi request đến
        logger.warning(f"   ⚠️  EasyOCR load thất bại: {e}")
        logger.warning("       Service vẫn khởi động, sẽ retry khi có request.")

    # ── Verify Tesseract ─────────────────────────────────────────
    logger.info("🔧 [2/2] Kiểm tra Tesseract + tiếng Việt...")
    try:
        import pytesseract
        langs = pytesseract.get_languages()
        if "vie" in langs:
            logger.info(f"   ✅ Tesseract OK – Languages: {langs}")
        else:
            logger.warning(f"   ⚠️  Tesseract OK nhưng THIẾU tiếng Việt! Langs: {langs}")
            logger.warning("       Fallback engine sẽ chỉ nhận diện tiếng Anh.")
    except Exception as e:
        logger.warning(f"   ⚠️  Tesseract không tìm thấy: {e}")
        logger.warning("       Fallback engine bị vô hiệu hóa.")

    # ── Ready ────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("✅ OCR Service sẵn sàng tại: http://localhost:8005")
    logger.info("📄 Swagger Docs:    http://localhost:8005/docs")
    logger.info("🔍 Health Check:    GET  /api/ocr/health")
    logger.info("🖼️  Tìm sách:       POST /api/ocr/search-by-cover")
    logger.info("📖 Trích xuất info: POST /api/ocr/extract-book-info")
    logger.info("🧾 Scan hóa đơn:   POST /api/ocr/scan-receipt")
    logger.info("=" * 60)

    # ── Pre-build Visual Hash Index (background) ─────────────────
    # Bước 1: Thử load từ file pickle trước (< 1s nếu còn hiệu lực)
    # Bước 2: Nếu không có → rebuild từ MinIO (30-60s, background)
    logger.info("[3/3] Khởi tạo visual hash index...")
    try:
        from ocr_app.services.image_similarity_engine import (
            build_hash_index,
            load_index_from_disk_async,
        )
        disk_loaded = await load_index_from_disk_async()
        if disk_loaded:
            logger.info("   ✅ pHash index loaded từ disk cache (nhanh hơn rebuild 30-60s)!")
        else:
            logger.info("   🔍 Không có cache → rebuild từ MinIO (background, 30-60s)...")
            # Chạy background task: không await để không block startup
            asyncio.ensure_future(build_hash_index())
    except Exception as e:
        logger.warning("Visual hash index không thể khởi động: %s", e)
        logger.warning("    Search-by-cover sẽ bỏ qua visual matching trong lần này.")

    # ── P2.2: Khoi dong OCR Async Queue Workers ──────────────────
    logger.info("[P2.2] Khoi dong 2 OCR Async Worker...")
    try:
        from ocr_app.queue.task_queue import start_workers
        start_workers(n=2)
        logger.info("   2 OCR Async Worker san sang!")
        logger.info("   Endpoint: POST /api/ocr/search-by-cover-async")
    except Exception as e:
        logger.warning("OCR Async Queue khoi dong that bai: %s", e)

    yield  # ← Server đang chạy và nhận request

    # ── Shutdown cleanup ─────────────────────────────────────────
    logger.info("🔴 OCR Service đang tắt – giải phóng RAM...")


# ══════════════════════════════════════════════════════════════════
# FASTAPI APPLICATION
# ══════════════════════════════════════════════════════════════════

app = FastAPI(
    title="BookStore OCR Service",
    description=(
        "Nhận dạng ký tự quang học (OCR) từ ảnh bìa sách, trang sách, hóa đơn.\n\n"
        "**Engine:** EasyOCR (CRAFT + CRNN) làm primary, Tesseract làm fallback.\n\n"
        "**Ngôn ngữ:** Tiếng Việt + Tiếng Anh.\n\n"
        "**Tích hợp:** Tự động gọi Search Service (:8000) để tìm sách sau khi OCR."
    ),
    version="1.0.0",
    lifespan=lifespan,
    # Docs có thể đổi path nếu cần (mặc định /docs và /redoc)
    docs_url="/docs",
    redoc_url="/redoc",
)

# -- Rate Limiter (slowapi) ──────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# ── CORS Middleware ─────────────────────────────────────────────
# Origins được kiểm soát qua env var ALLOWED_ORIGINS (xem config.py)
# Mặc định: ["http://localhost:3000"]
# Production: set ALLOWED_ORIGINS=https://bookstore.vn trong docker-compose
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Mount Routers ────────────────────────────────────────────────
from ocr_app.routers.ocr_router import router as ocr_router
from ocr_app.routers.async_ocr_router import router as async_ocr_router  # P2.2
app.include_router(ocr_router)
app.include_router(async_ocr_router)  # P2.2: Async OCR endpoints


# ══════════════════════════════════════════════════════════════════
# ROOT ENDPOINT
# ══════════════════════════════════════════════════════════════════

@app.get("/", tags=["Root"])
def root():
    """Thông tin tổng quan về OCR Service."""
    return {
        "service":  "BookStore OCR Service",
        "version":  "1.0.0",
        "status":   "running",
        "port":     8005,
        "endpoints": {
            "health":            "GET  /api/ocr/health",
            "search_by_cover":   "POST /api/ocr/search-by-cover",
            "extract_book_info": "POST /api/ocr/extract-book-info",
            "scan_receipt":      "POST /api/ocr/scan-receipt",
            "swagger_ui":        "GET  /docs",
        },
        "tech_stack": {
            "ocr_primary":  "EasyOCR 1.7+ (CRAFT + CRNN, CPU mode)",
            "ocr_fallback": "Tesseract 5 (vie+eng, PSM 6)",
            "preprocessing": "OpenCV 4.10 (deskew + CLAHE + bilateral filter)",
            "nlp":           "underthesea NER (PER/ORG entity extraction)",
            "framework":     "FastAPI 0.111 + Uvicorn",
        }
    }
