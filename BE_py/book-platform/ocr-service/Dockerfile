###############################################################################
#  Dockerfile – OCR Service (FastAPI port 8005)
#  Build context: source-code/BE_py/book-platform/ocr-service/
#
#  Engine 1 (Primary):  EasyOCR  – CRAFT detection + CRNN recognition
#  Engine 2 (Fallback): Tesseract – classical LSTM-based OCR
#  Ngôn ngữ hỗ trợ: Tiếng Việt + Tiếng Anh
#
#  Lưu ý quan trọng:
#  - torch CPU-only được cài trước (tách riêng layer để tận dụng Docker cache)
#  - EasyOCR model được pre-download ngay trong build (tránh download lúc runtime)
#  - gpu=False vì RTX 3050 đang bị Ollama chiếm VRAM
###############################################################################

# ── Base Image ──────────────────────────────────────────────────────────────
# python:3.11-slim: nhỏ gọn (~130MB), đủ build C extensions (gcc có sẵn)
FROM python:3.11-slim

WORKDIR /app

# ── System Dependencies ──────────────────────────────────────────────────────
# Lý do từng package:
#   tesseract-ocr        = Tesseract binary (Fallback OCR engine)
#   tesseract-ocr-vie    = Language pack tiếng Việt cho Tesseract
#   libglib2.0-0         = GLib (phụ thuộc của OpenCV)
#   libsm6 libxext6      = X11 session/extension (OpenCV cần dù headless)
#   libxrender-dev       = X rendering (OpenCV)
#   libgomp1             = GNU OpenMP: song song hóa tính toán numpy/OpenCV
#   gcc g++              = Compiler để build C extensions (underthesea, Pillow)
#   curl                 = Dùng trong HEALTHCHECK
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-vie \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*
#   ↑ Xóa apt cache sau khi cài để giảm kích thước image

# ── Python: Torch CPU-only (tách riêng để cache layer) ──────────────────────
# Tại sao tách riêng: torch ~700MB, nếu chung với requirements.txt thì
# mỗi lần thay đổi requirements nhỏ cũng phải tải lại torch = rất chậm.
# Với CPU-only index: giảm từ ~2.5GB (CUDA full) xuống ~700MB
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        torch==2.2.0 \
        --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -r requirements.txt

# ── Pre-download EasyOCR Models ─────────────────────────────────────────────
# Lý do: EasyOCR lần đầu chạy sẽ download model ~200MB từ Internet.
# Nếu download lúc runtime (khi user gửi request đầu tiên):
#   - Request đầu bị timeout
#   - Container cần Internet
# Giải pháp: download ngay trong BUILD, model nằm sẵn trong image.
# Dùng || echo để nếu download fail (no internet khi build) thì build không bị abort.
# Model sẽ được cache tại: /root/.EasyOCR/model/
RUN python -c "\
import easyocr; \
print('Downloading EasyOCR models (vi + en)...'); \
easyocr.Reader(['vi', 'en'], gpu=False, verbose=True); \
print('EasyOCR models ready!')" \
    || echo "WARNING: EasyOCR model pre-download failed, will retry at runtime"

# ── Copy Source Code ─────────────────────────────────────────────────────────
# Copy sau khi cài dependencies để tận dụng Docker layer cache:
# Nếu chỉ thay đổi code (không thay đổi requirements.txt),
# Docker sẽ dùng lại các layer pip install đã cache → build rất nhanh.
COPY . .

# ── Port ─────────────────────────────────────────────────────────────────────
EXPOSE 8005

# ── Health Check ─────────────────────────────────────────────────────────────
# Docker tự động check health mỗi 30s, nếu fail 3 lần → container = unhealthy
# start_period=90s: cho phép service khởi động (load EasyOCR model ~10-20s)
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8005/api/ocr/health || exit 1

# ── Startup Command ───────────────────────────────────────────────────────────
# --reload: Hot-reload khi code thay đổi (mount volume trong docker-compose)
# --workers 1: Chỉ 1 worker vì EasyOCR model Singleton không thread-safe
#              với nhiều process (cần dùng multiprocessing-safe approach nếu scale)
CMD ["uvicorn", "ocr_app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8005", \
     "--reload"]
