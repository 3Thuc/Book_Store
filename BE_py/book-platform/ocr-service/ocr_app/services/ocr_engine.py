"""
ocr_engine.py – OCR Engine Module (v3 - Always-Rotate180)
====================================================
Cải tiến v3 (so với v2):
  - LUÔN thử rotate180° song song cho cover images (không chỉ khi confidence thấp)
  - Lý do: ảnh lật ngược 180° đôi khi tạo Vietnamese "hợp lệ" nhưng sai
    (VD: "Tâm Hồn Treo Ngược" upside-down → OCR reads "Blog Cho Tâm Hồn 2")
    → không phát hiện được qua confidence hay garbled detector
  - Horizontal flip vẫn chỉ thử khi confidence thấp hoặc text garbled (mirror images)

v2 features (giữ nguyên):
  1. EasyOCR beamsearch decoder + adjust_contrast
  2. Tesseract adaptive PSM theo image_type
  3. Ensemble voting thông minh (real-word scoring)
  4. Image tiling (top/middle/bottom)
  5. Image hash cache với TTL
  6. ThreadPoolExecutor 3 workers

Pattern: Singleton – Model load 1 lần vào RAM.
Concurrency: run_in_executor – OCR không block FastAPI event loop.
"""
import asyncio
import hashlib
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Optional

import numpy as np
import pytesseract

from ocr_app.config import OCR_CONFIDENCE_THRESHOLD
from ocr_app.services.image_preprocessor import detect_text_regions_for_ocr

logger = logging.getLogger(__name__)

ImageType = Literal["cover", "document", "receipt"]

# ── Singleton: EasyOCR Reader ──────────────────────────────────────────────
_easyocr_reader = None

# ── ThreadPoolExecutor (tăng lên 3 workers) ────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="ocr_worker")

# ── Image Hash Cache ────────────────────────────────────────────────────────
_ocr_cache: dict[str, tuple[dict, float]] = {}  # {md5: (result, timestamp)}
_CACHE_TTL = 3600.0  # 1 giờ

# ── Tesseract PSM theo image_type ──────────────────────────────────────────
_TESSERACT_PSM = {
    "cover":    3,   # Auto page segmentation (bìa sách có nhiều layout)
    "document": 6,   # Single uniform block of text
    "receipt":  4,   # Single column of text
}


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON LOADER
# ══════════════════════════════════════════════════════════════════════════════

def get_easyocr_reader():
    """
    Trả về EasyOCR Reader instance, tạo mới nếu chưa (lazy init).
    Singleton tránh OOM khi load model nhiều lần (~1.2GB RAM).
    gpu=False: bảo vệ VRAM cho Ollama (chatbot service).
    """
    global _easyocr_reader
    if _easyocr_reader is None:
        logger.info("🧠 Đang load EasyOCR model (vi + en, CPU)...")
        try:
            import easyocr
            _easyocr_reader = easyocr.Reader(
                lang_list=['vi', 'en'],
                gpu=False,
                verbose=False,
                model_storage_directory=None,
                download_enabled=True,
            )
            logger.info("✅ EasyOCR ready! (vi + en, CPU mode)")
        except Exception as e:
            logger.error("❌ Không thể load EasyOCR: %s", e)
            raise RuntimeError(f"EasyOCR load thất bại: {e}") from e
    return _easyocr_reader


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: Real-word scoring
# ══════════════════════════════════════════════════════════════════════════════

def _count_real_words(text: str) -> int:
    """
    Đếm số 'từ thật' trong text – ít nhất 2 ký tự alpha liên tiếp.
    Dùng để so sánh chất lượng kết quả OCR tốt hơn là confidence đơn thuần.

    Ví dụ:
      "Đắc Nhân Tâm" → 3 real words
      "z9.x1 k;="    → 0 real words
      "123 456 789"  → 0 real words
    """
    words = re.findall(r'[A-Za-zÀ-ɏḀ-ỿ\u0300-\u036f]{2,}', text, re.UNICODE)
    return len(words)


def _score_result(result: dict) -> float:
    """
    Tính điểm tổng hợp cho kết quả OCR:
    score = 0.5 * confidence + 0.5 * (real_words / max_expected_words)

    Tại sao kết hợp?
    - Tesseract đôi khi confidence thấp nhưng text đúng hơn EasyOCR
    - Đếm real words giúp phân biệt text thật vs gibberish
    """
    confidence = result.get("confidence", 0.0)
    text = result.get("text", "")
    real_words = _count_real_words(text)
    # Normalize: 10 words trở lên = max score phần real_words
    word_score = min(real_words / 10.0, 1.0)
    return 0.5 * confidence + 0.5 * word_score


# ── Ngưỡng confidence để thử horizontal flip ──────────────────────────────
_FLIP_TRIGGER_CONF  = 0.35   # confidence thấp hơn ngưỡng này → thử flip
_FLIP_IMPROVE_RATIO = 0.20   # conf phải cải thiện ≥ 20% mới chấp nhận flip

# Tập hợp ký tự tiếng Việt có dấu hợp lệ (để kiểm tra text có phải tiếng Việt thật)
_VALID_VN_CHARS = set(
    'àáâãèéêìíòóôõùúý'
    'ạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỷỹỵ'
    'đơư'
)


def _is_garbled_vietnamese(text: str) -> bool:
    """
    Phát hiện text là tiếng Việt bị đọc ngược / xoay (garbled Vietnamese).

    Khi ảnh bìa sách bị lật 180°, EasyOCR vẫn đọc được một số ký tự vì
    Latin có ký tự đối xứng. Kết quả: các từ lạ (Xow, Unong, gên, boc,
    Cluen...) xen kẽ với từ Việt đọc ngược (HÔN TÂM = TÂM HỒN reversed).

    Returns True nếu nghi ngờ text bị xoay ngược.
    """
    if not text or len(text) < 5:
        return False

    lower = text.lower()
    total_alpha = sum(1 for c in lower if c.isalpha())
    if total_alpha == 0:
        return False

    # Tỷ lệ ký tự tiếng Việt có dấu
    vn_accented = sum(1 for c in lower if c in _VALID_VN_CHARS)
    vn_ratio = vn_accented / total_alpha

    # Phân tích từng từ
    words = re.findall(r'[A-Za-z\u00C0-\u1EF9]{2,}', lower, re.UNICODE)
    if not words:
        return False

    _COMMON_EN = {
        'the', 'and', 'for', 'not', 'but', 'with', 'from', 'have',
        'this', 'that', 'they', 'were', 'will', 'your', 'you', 'our',
        'her', 'his', 'its', 'are', 'was', 'has', 'had', 'can', 'may',
        'all', 'one', 'two', 'him', 'she', 'we', 'it', 'be', 'do',
        'if', 'an', 'as', 'at', 'by', 'in', 'is', 'of', 'on', 'or',
        'so', 'to', 'up'
    }
    garbled_count = 0
    for w in words:
        # Garbled: thuần ASCII, 3-7 ký tự, không phải từ tiếng Anh thông dụng
        if (
            all(ord(c) < 128 for c in w)
            and 3 <= len(w) <= 7
            and w not in _COMMON_EN
        ):
            garbled_count += 1

    garbled_ratio = garbled_count / len(words)

    # Cảnh báo garbled nếu > 35% từ là ASCII lạ VÀ < 15% ký tự VN có dấu
    is_garbled = garbled_ratio > 0.35 and vn_ratio < 0.15
    if is_garbled:
        logger.info(
            "🛑 Garbled Vietnamese: garbled=%.0f%%, vn_chars=%.0f%%, text='%s...'",
            garbled_ratio * 100, vn_ratio * 100, text[:40]
        )
    return is_garbled


def _run_easyocr_flipped(preprocessed_img: np.ndarray) -> dict:
    """
    Thử chạy EasyOCR trên ảnh bị lật ngang (horizontal flip).

    Mục đích: Phát hiện ảnh bị gương (mirror) – trường hợp phổ biến khi:
    - Chụp bìa sách qua gương hoặc kính cửa hàng
    - Camera selfie lật ngang mặc định
    - Scan hai mặt bị nhầm chiều
    """
    import cv2
    flipped = cv2.flip(preprocessed_img, 1)  # 1 = horizontal flip
    return _run_easyocr_sync(flipped, use_tiling=True)


def _run_easyocr_rotated180(preprocessed_img: np.ndarray) -> dict:
    """
    Thử chạy EasyOCR trên ảnh bị xoay 180° (upside-down rotation).

    Trường hợp phổ biến:
    - Người dùng chụp ảnh sách khi sách đang nằm ngược
    - Ảnh được upload xoay ngược từ máy tính
    - Tesseract OSD không khả dụng để detect trước

    Khác với horizontal flip (gương): xoay 180° = lật cả ngang lẫn dọc.
    """
    import cv2
    rotated = cv2.rotate(preprocessed_img, cv2.ROTATE_180)
    return _run_easyocr_sync(rotated, use_tiling=True)


def _run_easyocr_rotated90cw(preprocessed_img: np.ndarray) -> dict:
    """
    Thử chạy EasyOCR trên ảnh bị xoay 90° theo chiều kim đồng hồ (CW).

    Dùng cho:
    - Ảnh bìa sách chụp theo chiều ngang (landscape)
    - Chữ trên bìa chạy dọc từ trên xuống dưới
    - Sach có layout dọc như "Nâng Cao Tiếng Việt", "Giải Toán"
    """
    import cv2
    rotated = cv2.rotate(preprocessed_img, cv2.ROTATE_90_CLOCKWISE)
    return _run_easyocr_sync(rotated, use_tiling=True)


def _run_easyocr_rotated90ccw(preprocessed_img: np.ndarray) -> dict:
    """
    Thử chạy EasyOCR trên ảnh bị xoay 90° ngược chiều kim đồng hồ (CCW).

    Dùng cho:
    - Ảnh bìa sách chụp theo chiều ngang ngược lại
    - Chữ trên bìa chạy dọc từ dưới lên trên
    """
    import cv2
    rotated = cv2.rotate(preprocessed_img, cv2.ROTATE_90_COUNTERCLOCKWISE)
    return _run_easyocr_sync(rotated, use_tiling=True)


# ══════════════════════════════════════════════════════════════════════════════
# EASYOCR ENGINE (Primary) – v2 với beamsearch
# ══════════════════════════════════════════════════════════════════════════════

def _run_easyocr_sync(
    preprocessed_img: np.ndarray,
    use_tiling: bool = False,
) -> dict:
    """
    Chạy EasyOCR đồng bộ (gọi trong ThreadPoolExecutor).

    v2 improvements:
    1. decoder='beamsearch' + beamWidth=5: chính xác hơn ~5-10% so với greedy
    2. adjust_contrast=0.5: EasyOCR tự chuẩn hóa tương phản trước OCR
    3. contrast_ths=0.1: bỏ qua vùng ảnh tương phản quá thấp
    4. Image tiling: chia 3 vùng (top/mid/bot) để không bỏ sót chữ ở góc

    Trade-off:
    - beamsearch chậm hơn greedy ~20-25% nhưng accuracy cao hơn đáng kể
    - tiling tăng latency ~50% nhưng recall tốt hơn nhiều cho bìa sách
    """
    reader = get_easyocr_reader()

    def _ocr_single(img_part: np.ndarray) -> list:
        """OCR một phần ảnh."""
        res = reader.readtext(
            img_part,
            detail=True,
            paragraph=False,
            decoder='beamsearch',       # v2: beamsearch thay greedy
            beamWidth=5,                # v2: beam width
            batch_size=4,               # v2: batch processing
            contrast_ths=0.1,           # v2: bỏ vùng tương phản thấp
            adjust_contrast=0.5,        # v2: auto contrast normalization
        )
        return [(bbox, text, conf) for bbox, text, conf in res if conf >= 0.1]

    if use_tiling:
        h = preprocessed_img.shape[0]
        w = preprocessed_img.shape[1]

        # ── Intelligent Text-Region Tiling (TRD) ───────────────────────────
        # Ưu tiên dùng variance-based zones thay vì fixed 3 dải
        # Lợi ích: tránh noise từ vùng minh họa; OCR focus hoàn toàn vào text
        trd_zones = detect_text_regions_for_ocr(preprocessed_img)

        if len(trd_zones) >= 2:
            # TRD tìm được ≥ 2 vùng → dùng detected zones
            tiles = [(f"zone_{i}", zone) for i, zone in enumerate(trd_zones)]
            logger.debug("TRD tiling: %d zones detected", len(trd_zones))
        else:
            # Fallback: 3-band cố định (behavior cũ)
            tiles = [
                ("top",    preprocessed_img[:int(h * 0.45), :]),
                ("middle", preprocessed_img[int(h * 0.3):int(h * 0.75), :]),
                ("bottom", preprocessed_img[int(h * 0.6):, :]),
            ]
            logger.debug("TRD fallback: using fixed 3-band tiling")

        all_results: list[tuple] = []
        seen_texts: set[str] = set()

        for part_name, part in tiles:
            if part.shape[0] < 15 or part.shape[1] < 15:
                continue
            part_results = _ocr_single(part)
            for bbox, text, conf in part_results:
                t = text.strip().lower()
                if t and t not in seen_texts:
                    seen_texts.add(t)
                    all_results.append((bbox, text, conf))

        # Nếu TRD cho ít text (<3 từ), bổ sung full-image OCR làm safety net
        total_words = sum(
            len(text.strip().split())
            for _, text, _ in all_results
            if text.strip()
        )
        if len(trd_zones) >= 2 and total_words < 3:
            logger.debug("TRD gave few words (%d), running full-image OCR as backup", total_words)
            full_results = _ocr_single(preprocessed_img)
            for bbox, text, conf in full_results:
                t = text.strip().lower()
                if t and t not in seen_texts:
                    seen_texts.add(t)
                    all_results.append((bbox, text, conf))

        filtered = all_results
    else:
        filtered = _ocr_single(preprocessed_img)

    if not filtered:
        return {"text": "", "confidence": 0.0, "raw_results": []}

    texts = [text.strip() for _, text, _ in filtered if text.strip()]
    full_text = "\n".join(texts)
    avg_confidence = sum(conf for _, _, conf in filtered) / len(filtered)

    return {
        "text": full_text,
        "confidence": avg_confidence,
        "raw_results": filtered,
    }


# ══════════════════════════════════════════════════════════════════════════════
# TESSERACT ENGINE (Fallback) – v2 với adaptive PSM
# ══════════════════════════════════════════════════════════════════════════════

def _run_tesseract_sync(
    preprocessed_img: np.ndarray,
    image_type: ImageType = "cover",
) -> dict:
    """
    Chạy Tesseract đồng bộ (gọi trong ThreadPoolExecutor).

    v2: PSM adaptive theo image_type thay vì cứng PSM=6.
      cover    → PSM 3 (auto page segmentation)
      document → PSM 6 (uniform block)
      receipt  → PSM 4 (single column)
    """
    psm = _TESSERACT_PSM.get(image_type, 6)
    try:
        config = f"--psm {psm} --oem 1"
        data = pytesseract.image_to_data(
            preprocessed_img,
            lang="vie+eng",
            config=config,
            output_type=pytesseract.Output.DICT,
        )
        words_with_conf = [
            (int(data["conf"][i]), data["text"][i])
            for i in range(len(data["text"]))
            if int(data["conf"][i]) > 0 and data["text"][i].strip()
        ]
        if not words_with_conf:
            return {"text": "", "confidence": 0.0}
        full_text = " ".join(w for _, w in words_with_conf)
        avg_conf  = sum(c for c, _ in words_with_conf) / (len(words_with_conf) * 100)
        return {"text": full_text, "confidence": avg_conf}
    except Exception as e:
        logger.warning("Tesseract lỗi (PSM=%s): %s", psm, e)
        return {"text": "", "confidence": 0.0}


# ══════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _cache_key(img: np.ndarray) -> str:
    """MD5 hash của numpy array bytes – dùng làm cache key."""
    return hashlib.md5(img.tobytes()).hexdigest()


def _cache_get(key: str) -> Optional[dict]:
    """Lấy từ cache nếu còn hiệu lực."""
    if key not in _ocr_cache:
        return None
    result, ts = _ocr_cache[key]
    if time.time() - ts > _CACHE_TTL:
        del _ocr_cache[key]
        return None
    logger.info("🔑 Cache HIT: %s", key[:12])
    return result


def _cache_set(key: str, result: dict) -> None:
    """Lưu vào cache với timestamp hiện tại."""
    _ocr_cache[key] = (result, time.time())
    # Dọn cache quá cũ nếu có > 100 entries
    if len(_ocr_cache) > 100:
        now = time.time()
        expired = [k for k, (_, ts) in _ocr_cache.items() if now - ts > _CACHE_TTL]
        for k in expired:
            del _ocr_cache[k]


# ══════════════════════════════════════════════════════════════════════════════
# ASYNC WRAPPER – v3: always-rotate180 + conditional flip + ensemble voting
# ══════════════════════════════════════════════════════════════════════════════

async def extract_text_async(
    preprocessed_img: np.ndarray,
    force_tesseract: bool = False,
    image_type: ImageType = "cover",
) -> dict:
    """
    Async wrapper để chạy OCR mà không block FastAPI event loop.

    v3 Logic (cover images):
    1. Kiểm tra cache (MD5 hash)
    2. Chạy EasyOCR (với tiling)
    3. LUÔN chạy rotate180° song song → chọn orientation tốt hơn
       (ảnh lật 180° đôi khi tạo Vietnamese "hợp lệ" giả → không detect được)
    4. Conditional flip ngang khi conf thấp hoặc text garbled
    5. Nếu EasyOCR confidence vẫn thấp → Tesseract fallback + ensemble voting
    6. Lưu cache

    Args:
        preprocessed_img:  Ảnh đã qua preprocessing
        force_tesseract:   Bỏ qua EasyOCR
        image_type:        Loại ảnh (ảnh hưởng Tesseract PSM, tiling strategy)

    Returns:
        {"text", "confidence", "engine_used"}
    """
    loop = asyncio.get_event_loop()
    t0 = time.perf_counter()

    # ── Cache check ──────────────────────────────────────────────────────────
    cache_key = _cache_key(preprocessed_img)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # ── Tesseract Only mode ──────────────────────────────────────────────────
    if force_tesseract:
        logger.info("Mode: Tesseract only (forced), PSM=%d", _TESSERACT_PSM.get(image_type, 6))
        result = await loop.run_in_executor(
            _executor, _run_tesseract_sync, preprocessed_img, image_type
        )
        final = {**result, "engine_used": "tesseract"}
        _cache_set(cache_key, final)
        return final

    # ── EasyOCR: dùng tiling cho bìa sách (nhiều vùng chữ khác nhau) ────────
    use_tiling = (image_type == "cover")
    logger.info("▶ EasyOCR (tiling=%s, image_type=%s)...", use_tiling, image_type)

    easy_result = await loop.run_in_executor(
        _executor,
        lambda: _run_easyocr_sync(preprocessed_img, use_tiling=use_tiling)
    )
    easy_conf = easy_result.get("confidence", 0.0)
    easy_text = easy_result.get("text", "")
    easy_score = _score_result(easy_result)

    logger.info(
        "EasyOCR – conf=%.3f, words=%d, score=%.3f",
        easy_conf, _count_real_words(easy_text), easy_score
    )

    # ── BƯỚC 0: Phát hiện sớm ảnh xoay 90° (landscape) ─────────────────────
    # Tối ưu: thử CW trước, chỉ thử CCW nếu CW chưa đủ tốt (giảm latency)
    LANDSCAPE_CONF_THRESHOLD = 0.3
    if image_type == "cover" and (
        easy_conf < LANDSCAPE_CONF_THRESHOLD or not easy_text.strip()
    ):
        orig_eff = easy_score * (0.5 if _is_garbled_vietnamese(easy_text) else 1.0)
        logger.info(
            "🔄 Confidence thấp (%.3f), thử 90°CW trước...", easy_conf
        )

        # Thử 90°CW trước
        r90cw_result = await loop.run_in_executor(
            _executor, _run_easyocr_rotated90cw, preprocessed_img
        )
        r90cw_score = _score_result(r90cw_result)
        r90cw_text  = r90cw_result.get("text", "")
        r90cw_eff   = r90cw_score * (0.5 if _is_garbled_vietnamese(r90cw_text) else 1.0)

        if r90cw_eff > orig_eff * 1.1:
            logger.info(
                "✅ 90°CW thắng (eff %.3f → %.3f): '%s...'",
                orig_eff, r90cw_eff, r90cw_text[:50]
            )
            easy_result = r90cw_result
            easy_conf   = r90cw_result.get("confidence", 0.0)
            easy_text   = r90cw_text
            easy_score  = r90cw_score
        else:
            # CW chưa đủ → thử CCW (fallback)
            logger.info("↩️  90°CW chưa đủ (%.3f), thử 90°CCW...", r90cw_eff)
            r90ccw_result = await loop.run_in_executor(
                _executor, _run_easyocr_rotated90ccw, preprocessed_img
            )
            r90ccw_score = _score_result(r90ccw_result)
            r90ccw_text  = r90ccw_result.get("text", "")
            r90ccw_eff   = r90ccw_score * (0.5 if _is_garbled_vietnamese(r90ccw_text) else 1.0)

            if r90ccw_eff > orig_eff * 1.1:
                logger.info(
                    "✅ 90°CCW thắng (eff %.3f → %.3f): '%s...'",
                    orig_eff, r90ccw_eff, r90ccw_text[:50]
                )
                easy_result = r90ccw_result
                easy_conf   = r90ccw_result.get("confidence", 0.0)
                easy_text   = r90ccw_text
                easy_score  = r90ccw_score
            else:
                logger.info(
                    "↩️  Xoay 90° không cải thiện (orig=%.3f, cw=%.3f, ccw=%.3f)",
                    orig_eff, r90cw_eff, r90ccw_eff
                )


    # ── BƯỚC 1: Luôn thử rotate180° song song cho cover images ────────────────
    # Tại sao LUÔN chạy (không chỉ khi confidence thấp)?
    # Vì ảnh bìa lật 180° đôi khi đọc thành Vietnamese "hợp lệ" nhưng sai.
    # VD: "Tâm Hồn Treo Ngược" upside-down → OCR đọc "Blog Cho Tâm Hồn 2"
    # Không phát hiện được qua confidence hay garbled detector.
    # Chạy song song với EasyOCR gốc → latency tăng tối thiểu.
    if image_type == "cover":
        rotated_result = await loop.run_in_executor(
            _executor, _run_easyocr_rotated180, preprocessed_img
        )
        rotated_score_raw = _score_result(rotated_result)
        rotated_text = rotated_result.get("text", "")

        orig_effective   = easy_score       * (0.5 if _is_garbled_vietnamese(easy_text) else 1.0)
        rotate_effective = rotated_score_raw * (0.5 if _is_garbled_vietnamese(rotated_text) else 1.0)

        # Chuyển sang rotated nếu cải thiện ≥ 15% (tránh false-positive)
        if rotated_text.strip() and rotate_effective > orig_effective * 1.15:
            logger.info(
                "✅ rotate180 thắng (eff %.3f → %.3f): '%s...'",
                orig_effective, rotate_effective, rotated_text[:50]
            )
            easy_result = rotated_result
            easy_conf   = rotated_result.get("confidence", 0.0)
            easy_text   = rotated_text
            easy_score  = rotated_score_raw
        else:
            logger.info(
                "↩️  Original giữ (orig_eff=%.3f, rot_eff=%.3f)",
                orig_effective, rotate_effective
            )

    # ── BƯỚC 2: Horizontal Flip – chỉ khi conf thấp hoặc garbled ────────────
    # Xử lý ảnh bị gương (mirror) – ít gặp hơn ảnh lật ngược
    should_try_flip = (
        image_type == "cover"
        and (easy_conf < _FLIP_TRIGGER_CONF or _is_garbled_vietnamese(easy_text))
    )
    if should_try_flip:
        logger.info(
            "🔄 Thử horizontal flip (conf=%.3f, garbled=%s)...",
            easy_conf, _is_garbled_vietnamese(easy_text)
        )
        flipped_result = await loop.run_in_executor(
            _executor, _run_easyocr_flipped, preprocessed_img
        )
        flipped_score = _score_result(flipped_result)
        flipped_text  = flipped_result.get("text", "")

        flip_effective = flipped_score * (0.5 if _is_garbled_vietnamese(flipped_text) else 1.0)
        orig_effective = easy_score    * (0.5 if _is_garbled_vietnamese(easy_text) else 1.0)

        if flipped_text.strip() and flip_effective > orig_effective * 1.10:
            logger.info(
                "✅ Flip thắng: eff %.3f → %.3f: '%s...'",
                orig_effective, flip_effective, flipped_text[:50]
            )
            easy_result = flipped_result
            easy_conf   = flipped_result.get("confidence", 0.0)
            easy_text   = flipped_text
            easy_score  = flipped_score
        else:
            logger.info("↩️  Flip không cải thiện, giữ original")

    # ── EasyOCR đủ tốt → trả về ngay ────────────────────────────────────────
    if easy_conf >= OCR_CONFIDENCE_THRESHOLD and easy_text.strip():
        elapsed = (time.perf_counter() - t0) * 1000
        logger.info("✅ EasyOCR đủ tốt (conf=%.3f, %.0fms)", easy_conf, elapsed)
        final = {
            "text": easy_text,
            "confidence": easy_conf,
            "engine_used": "easyocr",
            "raw_results": easy_result.get("raw_results", []),
        }
        _cache_set(cache_key, final)
        return final

    # ── EasyOCR yếu → chạy Tesseract để so sánh ─────────────────────────────
    logger.info("⚠️  EasyOCR confidence thấp (%.3f), thử Tesseract...", easy_conf)
    tess_result = await loop.run_in_executor(
        _executor, _run_tesseract_sync, preprocessed_img, image_type
    )
    tess_conf  = tess_result.get("confidence", 0.0)
    tess_text  = tess_result.get("text", "")
    tess_score = _score_result(tess_result)

    logger.info(
        "Tesseract – conf=%.3f, words=%d, score=%.3f",
        tess_conf, _count_real_words(tess_text), tess_score
    )

    # ── Ensemble voting: chọn kết quả composite score cao hơn ───────────────
    elapsed = (time.perf_counter() - t0) * 1000

    if tess_score > easy_score and tess_text.strip():
        logger.info("✅ Tesseract thắng (score %.3f > %.3f, %.0fms)", tess_score, easy_score, elapsed)
        final = {
            "text": tess_text,
            "confidence": tess_conf,
            "engine_used": "tesseract",
        }
    elif easy_text.strip():
        logger.info("✅ EasyOCR thắng (score %.3f >= %.3f, %.0fms)", easy_score, tess_score, elapsed)
        final = {
            "text": easy_text,
            "confidence": easy_conf,
            "engine_used": "easyocr+tesseract",
        }
    else:
        # Cả 2 đều thất bại
        logger.warning("❌ Cả EasyOCR lẫn Tesseract đều không đọc được (%.0fms)", elapsed)
        final = {
            "text": tess_text or easy_text,
            "confidence": max(easy_conf, tess_conf),
            "engine_used": "easyocr+tesseract",
        }

    _cache_set(cache_key, final)
    return final
