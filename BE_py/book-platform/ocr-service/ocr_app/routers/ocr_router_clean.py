"""
ocr_router.py – FastAPI Router cho OCR Service (v2 - Optimized)
====================================================================
v2 changes:
  - preprocess() giờ trả về tuple (img, image_type)
  - extract_text_async() nhận image_type để chọn PSM + tiling strategy
  - extract_book_info() nhận raw_ocr_results cho multi-pass title extraction
  - Compound confidence score thay cho raw OCR confidence
"""
import logging
import time
from typing import List, Optional

from fastapi import APIRouter, File, HTTPException, UploadFile, status

from ocr_app.config import MAX_IMAGE_BYTES
from ocr_app.models.schemas import (
    BookInfo,
    BookResult,
    HealthResponse,
    OCRResponse,
    ReceiptItem,
    ReceiptResponse,
)
from ocr_app.services.image_preprocessor import preprocess
from ocr_app.services.ocr_engine import extract_text_async, get_easyocr_reader
from ocr_app.services.text_extractor import extract_book_info
from ocr_app.services.search_integrator import integrate_search
from ocr_app.services.image_similarity_engine import (
    find_similar_book,
    assess_image_relevance,
    assess_ocr_result_quality,
    get_index_stats,
    build_hash_index,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ocr", tags=["OCR"])


# ══════════════════════════════════════════════════════════════════════════════
# HELPER – Validate file upload
# ══════════════════════════════════════════════════════════════════════════════

ALLOWED_CONTENT_TYPES = {
    "image/jpeg", "image/jpg", "image/png",
    "image/webp", "image/bmp", "image/tiff",
}


async def _validate_and_read(file: UploadFile) -> bytes:
    """
    Đọc và validate file ảnh upload.

    Kiểm tra:
    1. Content-type phải là image/* (không nhận PDF, video...)
    2. Kích thước ≤ MAX_IMAGE_SIZE_BYTES (mặc định 10MB)

    Tại sao đọc toàn bộ vào memory?
    - OpenCV/EasyOCR cần bytes[] hoặc numpy array
    - File ảnh bìa sách thường < 5MB → an toàn
    - Giới hạn 10MB ngăn memory abuse
    """
    content_type = file.content_type or ""
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Loại file không hỗ trợ: {content_type}. "
                   f"Chấp nhận: JPEG, PNG, WebP, BMP, TIFF.",
        )

    content = await file.read()

    if len(content) > MAX_IMAGE_BYTES:
        size_mb = len(content) / (1024 * 1024)
        max_mb = MAX_IMAGE_BYTES / (1024 * 1024)
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File quá lớn: {size_mb:.1f}MB (tối đa {max_mb:.0f}MB).",
        )

    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File rỗng. Vui lòng tải lên ảnh hợp lệ.",
        )

    return content


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 0: HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════════════

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Kiểm tra trạng thái OCR Service",
    description="Trả về trạng thái service và trạng thái EasyOCR model.",
)
def health_check() -> HealthResponse:
    """
    Health check endpoint – Docker healthcheck gọi endpoint này.

    Kiểm tra:
    - Service có đang chạy không (nếu endpoint trả về → có)
    - EasyOCR model đã được load vào RAM chưa
      (nếu chưa load → request đầu tiên sẽ chậm 5-15s)

    Tại sao sync (không phải async)?
    Health check không I/O → không cần async.
    Sync function đơn giản hơn và FastAPI vẫn xử lý tốt.
    """
    try:
        reader = get_easyocr_reader()
        easyocr_ready = reader is not None
    except Exception:
        easyocr_ready = False

    return HealthResponse(
        status="ok",
        service="ocr-service",
        version="2.0.0",
        port=8005,
        easyocr_ready=easyocr_ready,
        visual_index=get_index_stats(),
    )



# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 0.5: REBUILD VISUAL INDEX
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/rebuild-index",
    summary="Rebuild visual pHash index",
    description="Force rebuild perceptual hash index từ tất cả ảnh bìa sách trong MinIO.",
)
async def rebuild_visual_index():
    """Trigger rebuild pHash index (chạy background, không block request)."""
    import asyncio
    asyncio.ensure_future(build_hash_index(force=True))
    stats = get_index_stats()
    return {
        "message": "Đang rebuild visual index trong background...",
        "current_stats": stats,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 1: SEARCH BY COVER
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/search-by-cover",
    response_model=OCRResponse,
    summary="Tìm sách bằng ảnh bìa",
    description=(
        "Upload ảnh bìa sách → OCR nhận diện chữ → NLP trích xuất tên sách/tác giả "
        "→ Tìm sách trong DB → trả về danh sách kết quả.\n\n"
        "**Use-case:** Người dùng thấy bìa sách hay nhưng không nhớ tên chính xác."
    ),
)
async def search_by_cover(
    file: UploadFile = File(..., description="Ảnh bìa sách (JPEG/PNG/WebP, tối đa 10MB)"),
) -> OCRResponse:
    """
    Pipeline 3 tầng: Visual Match → OCR Fallback → Garbage Detection

    TẦNG 0 – Input Validation (Ảnh bậy lung tung?)
      Đánh giá nhanh: ảnh trắng đen, quá mờ, quá nhỏ → từ chối ngay.

    TẦNG 1 – Visual pHash Match (~0.5ms)
      Nếu ảnh upload giống hệt/gần giống một bìa sách trong hệ thống
      → Trả kết quả ngay lập tức (không chạy OCR tốn kém).

    TẦNG 2 – OCR Pipeline (~1-5s)
      Nếu không match visual → chạy OCR đầy đủ (preprocessing + EasyOCR + NLP).

    TẦNG 3 – Output Quality Check
      Sau OCR: nếu confidence thấp + 0 kết quả → thông báo "không tìm thấy".

    Thời gian xử lý dự kiến:
    - Visual match HIT:   ~0.5ms  (🚀 cực kỳ nhanh)
    - OCR normal:         ~1.5-5s
    - Garbage detection:  ~1ms (instant)
    """
    t_start = time.time()
    filename = file.filename or "unknown"
    logger.info("📸 [search-by-cover] Nhận file: %s", filename)

    # ── [1] Đọc & validate file ────────────────────────────────────────────
    try:
        image_bytes = await _validate_and_read(file)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Lỗi đọc file: %s", e)
        raise HTTPException(status_code=400, detail=f"Không đọc được file: {e}")

    # ══ TẦNG 0: Input Garbage Detection ══════════════════════════════════════
    # Kiểm tra nhanh ảnh có hợp lệ không (~1ms) TRƯỚC KHI làm bất kỳ thứ gì
    logger.info("🛡️  [0/4] Kiểm tra chất lượng ảnh...")
    relevance = assess_image_relevance(image_bytes)
    if not relevance["is_relevant"]:
        elapsed = int((time.time() - t_start) * 1000)
        logger.warning("❌ Ảnh bị từ chối (garbage): %s", relevance["reason"])
        return OCRResponse(
            success=False,
            processing_time_ms=elapsed,
            extracted_text="",
            book_info=BookInfo(confidence=0.0),
            search_results=[],
            total_results=0,
            engine_used=None,
            match_method="not_found",
            image_quality="irrelevant",
            error=relevance["reason"],
        )

    # ══ TẦNG 1: Visual pHash Match ═══════════════════════════════════════════
    # Cực kỳ nhanh: O(n) Hamming distance, ~0.5ms cho 2000 sách
    logger.info("🔎 [1/4] Visual hash matching...")
    visual_match = find_similar_book(image_bytes)

    if visual_match is not None:
        # 🎯 KHỚP! Trả kết quả ngay không cần OCR
        matched_book, similarity = visual_match
        elapsed = int((time.time() - t_start) * 1000)
        logger.info(
            "🎯 Visual MATCH: '%s' (sim=%.1f%%, %dms)",
            matched_book.title, similarity * 100, elapsed
        )
        return OCRResponse(
            success=True,
            processing_time_ms=elapsed,
            extracted_text=f"[Visual Match] {matched_book.title}",
            book_info=BookInfo(
                title=matched_book.title,
                authors=[matched_book.author_name] if matched_book.author_name else [],
                confidence=round(similarity, 3),
            ),
            search_results=[BookResult(
                book_id=matched_book.book_id,
                title=matched_book.title,
                author_name=matched_book.author_name,
                price=matched_book.price,
                image_url=matched_book.image_url,
                score=round(similarity * 100, 1),
            )],
            total_results=1,
            engine_used="visual_phash",
            match_method="visual_match",
            image_quality="good",
            error=None,
        )

    # ══ TẦNG 2: OCR Pipeline ═════════════════════════════════════════════════
    # Không khớp visual → chạy OCR đầy đủ
    logger.info("🖼️  [2/4] Preprocessing ảnh (auto-detect type)...")
    try:
        preprocessed_img, image_type = preprocess(image_bytes, aggressive=False)
    except Exception as e:
        logger.error("Preprocessing thất bại: %s", e)
        raise HTTPException(status_code=422, detail=f"Không xử lý được ảnh: {e}")

    logger.info("🔤 [3/4] Chạy OCR (image_type=%s)...", image_type)
    try:
        ocr_result = await extract_text_async(preprocessed_img, image_type=image_type)
    except Exception as e:
        logger.error("OCR thất bại: %s", e)
        raise HTTPException(status_code=500, detail=f"OCR engine lỗi: {e}")

    raw_text    = ocr_result.get("text", "")
    confidence  = ocr_result.get("confidence", 0.0)
    engine_used = ocr_result.get("engine_used", "unknown")
    raw_ocr_results = ocr_result.get("raw_results", [])

    logger.info("   Text: %s... (conf=%.3f, engine=%s)",
                raw_text[:80].replace("\n", " "), confidence, engine_used)

    # NLP Extraction
    logger.info("📝 NLP text extraction...")
    try:
        extracted = extract_book_info(
            raw_text,
            ocr_confidence=confidence,
            raw_ocr_results=raw_ocr_results,
        )
    except Exception as e:
        logger.error("Text extraction lỗi: %s", e)
        extracted = None

    compound_conf = extracted.confidence if extracted else confidence

    book_info_schema = BookInfo(
        title=extracted.title if extracted else None,
        authors=extracted.authors if extracted else [],
        isbn=extracted.isbn if extracted else None,
        publisher=extracted.publisher if extracted else None,
        confidence=round(compound_conf, 3),
    )

    # Search
    search_results: List[BookResult] = []
    if extracted and extracted.search_query:
        logger.info("🔍 [4/4] Tìm sách trong DB (query='%s')...", extracted.search_query[:40])
        try:
            search_results = await integrate_search(extracted)
        except Exception as e:
            logger.error("Search integration lỗi: %s", e)

    elapsed = int((time.time() - t_start) * 1000)

    # ══ TẦNG 3: Output Quality Assessment ════════════════════════════════════
    # Phát hiện "ảnh không liên quan đến sách" qua output (sau OCR)
    quality = assess_ocr_result_quality(
        ocr_confidence=confidence,
        search_results=search_results,
        extracted_text=raw_text,
        compound_confidence=compound_conf,
    )

    if quality["quality"] == "irrelevant":
        logger.warning("❌ Output assessment: IRRELEVANT (%dms)", elapsed)
        return OCRResponse(
            success=False,
            processing_time_ms=elapsed,
            extracted_text=raw_text,
            book_info=BookInfo(confidence=0.0),
            search_results=[],
            total_results=0,
            engine_used=engine_used,
            match_method="not_found",
            image_quality="irrelevant",
            error=quality["message"],
        )

    logger.info(
        "✅ [search-by-cover] Hoàn tất: %dms, %d kết quả, quality=%s",
        elapsed, len(search_results), quality["quality"]
    )

    return OCRResponse(
        success=True if search_results else len(raw_text.strip()) > 0,
        processing_time_ms=elapsed,
        extracted_text=raw_text,
        book_info=book_info_schema,
        search_results=search_results,
        total_results=len(search_results),
        engine_used=engine_used,
        match_method="ocr_search" if search_results else "not_found",
        image_quality=quality["quality"],
        error=quality["message"] if quality["quality"] == "low" and not search_results else None,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 2: EXTRACT BOOK INFO (Admin Auto-Fill)
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/extract-book-info",
    response_model=OCRResponse,
    summary="Trích xuất thông tin sách từ ảnh bìa (Admin)",
    description=(
        "Upload ảnh bìa hoặc mặt sau sách → OCR + NLP tự động điền "
        "Tiêu đề, Tác giả, ISBN, NXB cho form nhập sách mới.\n\n"
        "**Use-case:** Admin upload bìa sách khi thêm sách mới → form tự điền."
    ),
)
async def extract_book_info_endpoint(
    file: UploadFile = File(..., description="Ảnh bìa hoặc mặt sau sách"),
) -> OCRResponse:
    """
    So với search-by-cover, endpoint này:
    1. Chạy THÊM aggressive mode preprocessing (Adaptive Threshold cho mặt sau)
    2. So sánh kết quả normal vs aggressive → chọn confidence cao hơn
    3. Không cần tìm kiếm sách trong DB (chỉ trả về BookInfo để auto-fill form)

    Tại sao cần 2 preprocessing mode?
    - Bìa trước: Màu sắc, nền gradient → Normal mode (CLAHE + bilateral) tốt hơn
    - Mặt sau: Đen trắng, mật độ chữ cao → Aggressive mode (Adaptive threshold) tốt hơn
    """
    t_start = time.time()
    logger.info("📸 [extract-book-info] Nhận file: %s", file.filename or "unknown")

    # ── [1] Đọc & validate file ────────────────────────────────────────────
    try:
        image_bytes = await _validate_and_read(file)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Không đọc được file: {e}")

    # ── [2a] Preprocessing – Normal mode ──────────────────────────────────
    logger.info("🖼️  [1/3] Preprocessing normal + aggressive mode song song...")
    try:
        normal_img, _normal_type = preprocess(image_bytes, aggressive=False)  # FIX: unpack tuple
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Không xử lý được ảnh: {e}")

    # ── [2b] Preprocessing – Aggressive mode (Adaptive Threshold) ─────────
    try:
        aggressive_img, _ = preprocess(image_bytes, aggressive=True)
    except Exception as e:
        logger.warning("Aggressive preprocess thất bại, fallback sang normal: %s", e)
        aggressive_img = normal_img  # safe: normal_img is now an ndarray

    # ── [3] Chạy OCR với cả 2 mode – so sánh confidence ──────────────────
    logger.info("🔤 [2/3] Chạy OCR normal + aggressive...")
    try:
        normal_result     = await extract_text_async(normal_img,     image_type=_normal_type)
        aggressive_result = await extract_text_async(aggressive_img, image_type="document")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OCR engine lỗi: {e}")

    # So sánh confidence – chọn kết quả tốt hơn
    normal_conf     = normal_result.get("confidence", 0.0)
    aggressive_conf = aggressive_result.get("confidence", 0.0)

    if aggressive_conf > normal_conf and aggressive_result.get("text", "").strip():
        best_result = aggressive_result
        logger.info("   Aggressive mode thắng: %.3f > %.3f", aggressive_conf, normal_conf)
    else:
        best_result = normal_result
        logger.info("   Normal mode thắng: %.3f >= %.3f", normal_conf, aggressive_conf)

    raw_text    = best_result.get("text", "")
    confidence  = best_result.get("confidence", 0.0)
    engine_used = best_result.get("engine_used", "unknown")

    if not raw_text.strip():
        elapsed = int((time.time() - t_start) * 1000)
        return OCRResponse(
            success=False,
            processing_time_ms=elapsed,
            extracted_text="",
            book_info=BookInfo(confidence=0.0),
            search_results=[],
            total_results=0,
            engine_used=engine_used,
            error="Không nhận diện được chữ. Thử ảnh rõ hơn.",
        )

    # ── [4] NLP Extraction ─────────────────────────────────────────────────
    logger.info("📝 [3/3] Trích xuất metadata sách...")
    raw_ocr_results = best_result.get("raw_results", [])
    try:
        extracted = extract_book_info(
            raw_text,
            ocr_confidence=confidence,
            raw_ocr_results=raw_ocr_results,
        )
    except Exception as e:
        logger.error("NLP extraction lỗi: %s", e)
        extracted = None

    book_info_schema = BookInfo(
        title=extracted.title if extracted else None,
        authors=extracted.authors if extracted else [],
        isbn=extracted.isbn if extracted else None,
        publisher=extracted.publisher if extracted else None,
        confidence=round(confidence, 3),
    )

    elapsed = int((time.time() - t_start) * 1000)
    logger.info("✅ [extract-book-info] Hoàn tất: %dms | title=%s | isbn=%s",
                elapsed,
                extracted.title if extracted else "N/A",
                extracted.isbn if extracted else "N/A")

    # extract-book-info không cần search_results (chỉ dùng cho auto-fill form)
    return OCRResponse(
        success=True,
        processing_time_ms=elapsed,
        extracted_text=raw_text,
        book_info=book_info_schema,
        search_results=[],
        total_results=0,
        engine_used=engine_used,
        error=None,
    )


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 3: SCAN RECEIPT
# ══════════════════════════════════════════════════════════════════════════════

@router.post(
    "/scan-receipt",
    response_model=ReceiptResponse,
    summary="Scan hóa đơn sách",
    description=(
        "Upload ảnh hóa đơn sách → OCR nhận diện → Phân tích danh sách sách + giá.\n\n"
        "**Use-case:** Khách có hóa đơn cũ muốn mua lại cùng danh sách sách."
    ),
)
async def scan_receipt(
    file: UploadFile = File(..., description="Ảnh hóa đơn sách (in máy hoặc viết tay)"),
) -> ReceiptResponse:
    """
    Scan hóa đơn sách.

    Hóa đơn in máy thường là văn bản đen trắng, thẳng hàng → Tesseract (PSM 6) rất tốt.
    Hóa đơn viết tay → EasyOCR tốt hơn.

    Preprocessing: Aggressive mode (Adaptive Threshold) để nhị phân hóa
    văn bản trên nền trắng/kem của hóa đơn in.

    Phân tích hóa đơn:
    - Tìm các dòng có pattern: [Tên sách] ... [Số lượng] ... [Giá]
    - Tính tổng tiền nếu tìm thấy đủ dữ liệu
    """
    t_start = time.time()
    logger.info("🧾 [scan-receipt] Nhận file: %s", file.filename or "unknown")

    # ── [1] Đọc & validate file ────────────────────────────────────────────
    try:
        image_bytes = await _validate_and_read(file)
    except HTTPException:
        raise

    # ── [2] Preprocessing – Aggressive mode tốt cho hóa đơn in ───────────
    logger.info("🖼️  Preprocessing hóa đơn (aggressive mode)...")
    try:
        preprocessed_img = preprocess(image_bytes, aggressive=True)
    except Exception:
        # Fallback sang normal nếu aggressive thất bại
        try:
            preprocessed_img = preprocess(image_bytes, aggressive=False)
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Không xử lý được ảnh: {e}")

    # ── [3] OCR – Ưu tiên Tesseract cho hóa đơn in thẳng hàng ───────────
    logger.info("🔤 Chạy OCR (hóa đơn)...")
    try:
        # force_tesseract=False: vẫn ưu tiên EasyOCR, Tesseract là fallback
        # Nếu hóa đơn in thẳng hàng, EasyOCR thường có confidence cao
        ocr_result = await extract_text_async(preprocessed_img, force_tesseract=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OCR lỗi: {e}")

    raw_text    = ocr_result.get("text", "")
    confidence  = ocr_result.get("confidence", 0.0)
    engine_used = ocr_result.get("engine_used", "unknown")

    if not raw_text.strip():
        elapsed = int((time.time() - t_start) * 1000)
        return ReceiptResponse(
            success=False,
            processing_time_ms=elapsed,
            extracted_text="",
            items=[],
            total_amount=None,
            error="Không nhận diện được chữ trong hóa đơn.",
        )

    # ── [4] Phân tích hóa đơn – Tìm danh sách sách + giá ─────────────────
    logger.info("🧾 Phân tích nội dung hóa đơn...")
    items, total_amount = _parse_receipt_lines(raw_text)

    elapsed = int((time.time() - t_start) * 1000)
    logger.info("✅ [scan-receipt] Hoàn tất: %dms | %d items | total=%s",
                elapsed, len(items), total_amount)

    return ReceiptResponse(
        success=True,
        processing_time_ms=elapsed,
        extracted_text=raw_text,
        items=items,
        total_amount=total_amount,
        error=None,
    )


# ══════════════════════════════════════════════════════════════════════════════
# HELPER – Phân tích hóa đơn
# ══════════════════════════════════════════════════════════════════════════════

import re as _re

# Pattern nhận diện giá tiền VNĐ: 79.000, 79000, 79,000đ, 79.000 VNĐ
_PRICE_PATTERN = _re.compile(
    r"(\d{1,3}(?:[.,]\d{3})+|\d{4,7})"   # Số: 79.000 hoặc 79000
    r"\s*(?:đ|đồng|vnd|vnđ)?",            # Đơn vị (tùy chọn)
    _re.IGNORECASE
)

# Pattern nhận diện số lượng: "x2", "2x", "sl: 2", "SL 2"
_QUANTITY_PATTERN = _re.compile(
    r"(?:x\s*(\d+)|(\d+)\s*x|sl\s*:?\s*(\d+)|số\s*lượng\s*:?\s*(\d+))",
    _re.IGNORECASE
)

# Dòng không phải sách: cột tiêu đề, tổng cộng, mã, ngày tháng
_SKIP_KEYWORDS = {
    "tổng", "total", "cộng", "subtotal", "thuế", "vat", "tax",
    "ngày", "date", "mã", "code", "stt", "số thứ tự", "ghi chú",
    "note", "đơn vị tính", "đvt", "cảm ơn", "thank", "hotline",
    "địa chỉ", "address", "điện thoại", "tel", "website", "email",
}


def _parse_receipt_lines(raw_text: str) -> tuple:
    """
    Phân tích text hóa đơn để tìm danh sách sách và giá.

    Heuristic đơn giản:
    1. Tách từng dòng
    2. Bỏ qua dòng có từ khóa không phải sách (tổng, ngày, địa chỉ...)
    3. Bỏ qua dòng quá ngắn (< 5 ký tự) hoặc chỉ là số
    4. Tìm pattern giá tiền trong dòng
    5. Tìm pattern số lượng trong dòng
    6. Phần còn lại là tên sách

    Returns:
        (List[ReceiptItem], Optional[float])   ← items + total_amount
    """
    lines = [ln.strip() for ln in raw_text.split("\n") if ln.strip()]
    items = []
    prices_found = []

    for line in lines:
        line_lower = line.lower()

        # Bỏ qua dòng chứa từ khóa "không phải sách"
        if any(kw in line_lower for kw in _SKIP_KEYWORDS):
            # Nếu là dòng tổng cộng → lấy giá tổng
            if any(kw in line_lower for kw in ("tổng", "total", "cộng")):
                price_match = _PRICE_PATTERN.search(line)
                if price_match:
                    try:
                        total_str = price_match.group(1).replace(".", "").replace(",", "")
                        prices_found.append(("total", float(total_str)))
                    except ValueError:
                        pass
            continue

        # Bỏ qua dòng quá ngắn hoặc chỉ toàn số/ký tự đặc biệt
        if len(line) < 5:
            continue
        if _re.match(r"^[\d\s.,\-|]+$", line):
            continue

        # Tìm giá trong dòng
        price_val: Optional[float] = None
        price_match = _PRICE_PATTERN.search(line)
        if price_match:
            try:
                price_str = price_match.group(1).replace(".", "").replace(",", "")
                candidate = float(price_str)
                # Giá sách VNĐ thường từ 10.000 đến 1.000.000
                if 10_000 <= candidate <= 1_000_000:
                    price_val = candidate
                    prices_found.append(("item", candidate))
            except ValueError:
                pass

        # Tìm số lượng
        qty_val: Optional[int] = None
        qty_match = _QUANTITY_PATTERN.search(line)
        if qty_match:
            for g in qty_match.groups():
                if g:
                    try:
                        qty_val = int(g)
                    except ValueError:
                        pass
                    break

        # Phần còn lại = tên sách
        # Xóa giá và số lượng khỏi dòng để lấy tên sách
        title_text = line
        if price_match:
            title_text = title_text[:price_match.start()].strip()
        if qty_match:
            title_text = _QUANTITY_PATTERN.sub("", title_text).strip()

        # Làm sạch tên sách: bỏ ký tự phân cách ở đầu/cuối
        title_text = _re.sub(r"^[\d\.\-\|\s]+", "", title_text).strip()
        title_text = _re.sub(r"[\.\-\|\s]+$", "", title_text).strip()

        if len(title_text) >= 3:
            items.append(ReceiptItem(
                title=title_text,
                quantity=qty_val or 1,
                price=price_val,
            ))

    # Tính tổng tiền
    total_amount: Optional[float] = None
    # Ưu tiên tổng đã in trên hóa đơn
    total_from_label = [v for label, v in prices_found if label == "total"]
    if total_from_label:
        total_amount = total_from_label[-1]  # Lấy số cuối cùng (thường là TOTAL)
    elif items:
        # Tính tổng từ items nếu không tìm thấy dòng tổng
        calculable = [
            (it.price * (it.quantity or 1))
            for it in items
            if it.price is not None
        ]
        if calculable:
            total_amount = sum(calculable)

    return items, total_amount
