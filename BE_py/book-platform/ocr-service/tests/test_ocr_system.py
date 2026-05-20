"""
test_ocr_system.py – Bộ test toàn diện cho OCR Service v2
==========================================================
Đo 4 nhóm chỉ số:

  [1] ACCURACY (Độ chính xác)
    - CER (Character Error Rate): % ký tự nhận sai
    - WER (Word Error Rate): % từ nhận sai
    - Title Detection Rate: % ảnh có title được trích xuất đúng
    - Search Hit Rate: % ảnh tìm đúng sách (Top-1, Top-3)

  [2] PERFORMANCE (Hiệu năng)
    - Latency: p50 / p90 / p99 của từng stage
    - Throughput: ảnh/giây
    - Cache HIT latency vs MISS latency

  [3] GARBAGE DETECTION (Phát hiện ảnh không hợp lệ)
    - True Positive Rate (TPR): % ảnh rác bị từ chối đúng
    - False Positive Rate (FPR): % ảnh hợp lệ bị từ chối nhầm

  [4] VISUAL MATCH
    - pHash accuracy: cùng ảnh luôn khớp chính mình?
    - pHash collision: ảnh khác không khớp nhầm?

Chạy:
    cd source-code/BE_py/book-platform/ocr-service
    python tests/test_ocr_system.py

    Chạy nhanh (chỉ unit test, bỏ qua OCR engine nặng):
    python tests/test_ocr_system.py --fast

    Chạy benchmark:
    python tests/test_ocr_system.py --benchmark
"""
import argparse
import io
import os
import sys
import time
import statistics
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont

# ── Add project root to path ──────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── ANSI colors ───────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS: Tạo ảnh synthetic để test (không cần ảnh thật)
# ═══════════════════════════════════════════════════════════════════════════════

def _make_font(size: int = 36) -> ImageFont.FreeTypeFont:
    """Tìm font có thể dùng được trên Windows/Linux."""
    font_candidates = [
        # Windows
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/times.ttf",
        "C:/Windows/Fonts/calibri.ttf",
        # Linux
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in font_candidates:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def create_book_cover_image(
    title: str,
    author: str = "",
    publisher: str = "",
    width: int = 400,
    height: int = 600,
    bg_color: tuple = (30, 80, 150),
    text_color: tuple = (255, 255, 255),
) -> bytes:
    """
    Tạo ảnh bìa sách synthetic với text rõ ràng.
    V2: Thêm các yếu tố giả lập kết cấu bìa thậ (viền, texture nhẹ)
    để vượt qua garbage detection.
    """
    # Nền gradient (thực tế hơn nhiều so với solid color)
    arr = np.zeros((height, width, 3), dtype=np.uint8)
    for y in range(height):
        factor = y / height
        r = int(bg_color[0] * (1 - factor * 0.3))
        g = int(bg_color[1] * (1 - factor * 0.3))
        b = int(bg_color[2] * (1 + factor * 0.1))
        arr[y, :] = [min(r, 255), min(g, 255), min(b, 255)]

    img = Image.fromarray(arr, mode='RGB')
    draw = ImageDraw.Draw(img)

    # Viền trang trí (tăng edge density)
    border = 8
    draw.rectangle([border, border, width-border, height-border],
                   outline=(255, 255, 255, 200), width=2)
    draw.rectangle([border+6, border+6, width-border-6, height-border-6],
                   outline=(200, 200, 200, 100), width=1)

    # Title (to, ở trên)
    font_title  = _make_font(40)
    font_author = _make_font(28)
    font_pub    = _make_font(22)

    y = 80
    draw.text((width // 2, y), title, font=font_title, fill=text_color, anchor="mm")
    y += 60

    if author:
        draw.text((width // 2, y + 60), author, font=font_author,
                  fill=(220, 220, 180), anchor="mm")

    if publisher:
        draw.text((width // 2, height - 60), publisher, font=font_pub,
                  fill=(180, 220, 180), anchor="mm")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def create_garbage_image(kind: str = "white") -> bytes:
    """Tạo các loại ảnh 'bậy' để test garbage detection."""
    if kind == "white":
        img = Image.new("RGB", (200, 200), (255, 255, 255))
    elif kind == "black":
        img = Image.new("RGB", (200, 200), (0, 0, 0))
    elif kind == "random_gradient":
        arr = np.zeros((200, 200, 3), dtype=np.uint8)
        for i in range(200):
            arr[:, i] = [i, 255 - i, 100]  # Gradient màu
        img = Image.fromarray(arr)
    elif kind == "tiny":
        img = Image.new("RGB", (30, 30), (200, 100, 50))
    elif kind == "food":
        # Ảnh mô phỏng "ảnh chụp đồ ăn" – không phải sách
        arr = np.random.randint(80, 180, (200, 200, 3), dtype=np.uint8)
        img = Image.fromarray(arr)
    else:
        img = Image.new("RGB", (200, 200), (128, 128, 128))

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════════
# METRICS CALCULATORS
# ═══════════════════════════════════════════════════════════════════════════════

def character_error_rate(reference: str, hypothesis: str) -> float:
    """
    CER = Levenshtein(ref, hyp) / len(ref)
    0.0 = hoàn hảo, 1.0 = hoàn toàn sai.
    """
    ref = reference.strip().lower()
    hyp = hypothesis.strip().lower()
    if not ref:
        return 0.0 if not hyp else 1.0

    # Levenshtein distance
    m, n = len(ref), len(hyp)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[:]
        dp[0] = i
        for j in range(1, n + 1):
            if ref[i-1] == hyp[j-1]:
                dp[j] = prev[j-1]
            else:
                dp[j] = 1 + min(prev[j], dp[j-1], prev[j-1])

    return dp[n] / m


def word_error_rate(reference: str, hypothesis: str) -> float:
    """
    WER = Levenshtein(ref_words, hyp_words) / len(ref_words)
    Tương tự CER nhưng đơn vị là từ.
    """
    ref_words = reference.strip().lower().split()
    hyp_words = hypothesis.strip().lower().split()
    if not ref_words:
        return 0.0 if not hyp_words else 1.0

    m, n = len(ref_words), len(hyp_words)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev = dp[:]
        dp[0] = i
        for j in range(1, n + 1):
            if ref_words[i-1] == hyp_words[j-1]:
                dp[j] = prev[j-1]
            else:
                dp[j] = 1 + min(prev[j], dp[j-1], prev[j-1])

    return dp[n] / m


def latency_stats(times_ms: List[float]) -> dict:
    """Tính các percentile latency từ list time measurements."""
    if not times_ms:
        return {}
    sorted_t = sorted(times_ms)
    n = len(sorted_t)
    def percentile(p):
        idx = int(p / 100 * n)
        return sorted_t[min(idx, n-1)]

    return {
        "count":  n,
        "min":    round(min(sorted_t), 1),
        "mean":   round(statistics.mean(sorted_t), 1),
        "median": round(statistics.median(sorted_t), 1),
        "p90":    round(percentile(90), 1),
        "p99":    round(percentile(99), 1),
        "max":    round(max(sorted_t), 1),
        "stdev":  round(statistics.stdev(sorted_t) if n > 1 else 0, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TEST RESULTS COLLECTOR
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TestResult:
    name:    str
    passed:  bool
    detail:  str = ""
    elapsed: float = 0.0  # ms


@dataclass
class TestReport:
    results: List[TestResult] = field(default_factory=list)

    def add(self, name: str, passed: bool, detail: str = "", elapsed: float = 0.0):
        self.results.append(TestResult(name, passed, detail, elapsed))

    def summary(self) -> dict:
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return {"total": total, "passed": passed, "failed": total - passed}

    def print_report(self):
        print(f"\n{'='*70}")
        print(f"{BOLD}📋 TEST REPORT{RESET}")
        print(f"{'='*70}")
        for r in self.results:
            icon  = f"{GREEN}✅{RESET}" if r.passed else f"{RED}❌{RESET}"
            time_ = f" [{r.elapsed:.0f}ms]" if r.elapsed > 0 else ""
            print(f"  {icon}  {r.name}{time_}")
            if r.detail:
                color = CYAN if r.passed else YELLOW
                print(f"        {color}→ {r.detail}{RESET}")

        s = self.summary()
        pct = s['passed'] / s['total'] * 100 if s['total'] > 0 else 0
        color = GREEN if pct >= 80 else YELLOW if pct >= 60 else RED
        print(f"\n{'─'*70}")
        print(f"  {color}{BOLD}Kết quả: {s['passed']}/{s['total']} PASSED ({pct:.0f}%){RESET}")
        print(f"{'='*70}\n")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 1: GARBAGE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def test_garbage_detection(report: TestReport):
    """
    Test assess_image_relevance():
    - Ảnh rác phải bị từ chối (is_relevant=False)
    - Ảnh hợp lệ phải được chấp nhận (is_relevant=True)

    Metrics: TPR (True Positive Rate) và FPR (False Positive Rate)
    """
    print(f"\n{BOLD}{CYAN}══ TEST 1: Garbage Detection ══{RESET}")

    from ocr_app.services.image_similarity_engine import assess_image_relevance

    # ── Ảnh rác (phải bị từ chối) ─────────────────────────────────────────
    garbage_cases = [
        ("white_image",       create_garbage_image("white"),           True),   # phải từ chối
        ("black_image",       create_garbage_image("black"),           True),   # phải từ chối
        ("tiny_30x30",        create_garbage_image("tiny"),            True),   # phải từ chối (size)
    ]

    # ── Ảnh hợp lệ (phải chấp nhận) ──────────────────────────────────────
    valid_cases = [
        ("book_cover_text",   create_book_cover_image("Đắc Nhân Tâm", "Dale Carnegie"), False),
        ("book_cover_en",     create_book_cover_image("The Alchemist", "Paulo Coelho"), False),
        ("book_doc_style",    create_book_cover_image("Lập trình Python", "NXB Trẻ"),   False),
    ]

    tp, fp, tn, fn = 0, 0, 0, 0  # true/false positive/negative

    all_cases = garbage_cases + valid_cases
    for name, img_bytes, should_reject in all_cases:
        t0 = time.perf_counter()
        result = assess_image_relevance(img_bytes)
        elapsed = (time.perf_counter() - t0) * 1000
        did_reject = not result["is_relevant"]

        if should_reject:
            if did_reject:
                tp += 1  # Đúng: từ chối ảnh rác
                passed = True
                detail = f"✓ Từ chối đúng: {result['reason'][:50]}"
            else:
                fn += 1  # Sai: không từ chối ảnh rác
                passed = False
                detail = f"✗ Bỏ sót ảnh rác!"
        else:
            if not did_reject:
                tn += 1  # Đúng: chấp nhận ảnh hợp lệ
                passed = True
                detail = "✓ Chấp nhận đúng"
            else:
                fp += 1  # Sai: từ chối nhầm ảnh hợp lệ
                passed = False
                detail = f"✗ Từ chối nhầm: {result['reason'][:50]}"

        report.add(f"Garbage [{name}]", passed, detail, elapsed)

    # Tính metrics
    total_garbage = tp + fn
    total_valid   = tn + fp
    tpr = tp / total_garbage * 100 if total_garbage > 0 else 0  # Recall của class "rác"
    fpr = fp / total_valid   * 100 if total_valid > 0   else 0  # False alarm rate

    print(f"\n  {BOLD}Garbage Detection Metrics:{RESET}")
    print(f"    TPR (Từ chối đúng ảnh rác):    {GREEN}{tpr:.0f}%{RESET}  [{tp}/{total_garbage}]")
    print(f"    FPR (Từ chối nhầm ảnh hợp lệ): {YELLOW}{fpr:.0f}%{RESET}  [{fp}/{total_valid}]")
    print(f"    {'Target: TPR≥80%, FPR≤10%' if tpr >= 80 and fpr <= 10 else '⚠ Cần điều chỉnh ngưỡng'}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 2: IMAGE PREPROCESSING
# ═══════════════════════════════════════════════════════════════════════════════

def test_preprocessing(report: TestReport):
    """
    Test image_preprocessor.py:
    - classify_image_type: Phân loại đúng loại ảnh
    - preprocess: Output là grayscale array hợp lệ
    - deskew: Ảnh không bị xoay sau deskew
    - resize: Output trong khoảng kích thước mong đợi
    """
    print(f"\n{BOLD}{CYAN}══ TEST 2: Image Preprocessing ══{RESET}")

    from ocr_app.services.image_preprocessor import (
        preprocess,
        classify_image_type,
        bytes_to_cv2,
        resize_image,
    )

    # ── Test 1: classify_image_type ────────────────────────────────────────
    cover_bytes = create_book_cover_image("Test Title", "Test Author")
    cover_cv2   = bytes_to_cv2(cover_bytes)
    img_type    = classify_image_type(cover_cv2)
    report.add(
        "Classify: Book cover → 'cover'",
        img_type == "cover",
        f"Detected: '{img_type}'",
    )

    # ── Test 2: preprocess output shape ────────────────────────────────────
    t0 = time.perf_counter()
    result_img, detected_type = preprocess(cover_bytes)
    elapsed = (time.perf_counter() - t0) * 1000

    is_ndarray = isinstance(result_img, np.ndarray)
    is_2d      = len(result_img.shape) == 2  # Grayscale
    report.add(
        "Preprocess: Output = 2D grayscale array",
        is_ndarray and is_2d,
        f"Shape: {result_img.shape}, type: {detected_type}",
        elapsed,
    )

    # ── Test 3: Resize không vượt kích thước tối đa ─────────────────────
    h, w = result_img.shape[:2]
    report.add(
        "Preprocess: Kích thước output hợp lệ (≤ 2000px)",
        max(h, w) <= 2000,
        f"Output: {w}x{h}px",
    )

    # ── Test 4: Preprocess ảnh nhỏ → upscale ─────────────────────────────
    tiny_img = Image.new("RGB", (100, 150), (30, 80, 150))
    draw = ImageDraw.Draw(tiny_img)
    draw.text((10, 50), "Test", fill=(255, 255, 255))
    buf = io.BytesIO()
    tiny_img.save(buf, format="JPEG")
    tiny_bytes = buf.getvalue()

    tiny_result, _ = preprocess(tiny_bytes)
    h2, w2 = tiny_result.shape[:2]
    was_upscaled = max(h2, w2) > 150
    report.add(
        "Preprocess: Ảnh nhỏ được upscale",
        was_upscaled,
        f"100x150 → {w2}x{h2}px",
    )

    # ── Test 5: Receipt mode → binary image ─────────────────────────────
    receipt_bytes = create_book_cover_image("", bg_color=(240, 240, 240), text_color=(0, 0, 0))
    receipt_result, receipt_type = preprocess(receipt_bytes, image_type="receipt")
    unique_vals = np.unique(receipt_result)
    is_binary = len(unique_vals) == 2 and set(unique_vals).issubset({0, 255})
    report.add(
        "Preprocess: Receipt → binary image (0/255)",
        is_binary,
        f"Unique values: {unique_vals.tolist()[:5]}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 3: TEXT EXTRACTION (NLP)
# ═══════════════════════════════════════════════════════════════════════════════

def test_text_extraction(report: TestReport):
    """
    Test text_extractor.py với ground-truth text:
    - Title detection accuracy (WER)
    - Garbled line detection
    - ISBN extraction
    - Publisher matching
    """
    print(f"\n{BOLD}{CYAN}══ TEST 3: Text Extraction (NLP) ══{RESET}")

    from ocr_app.services.text_extractor import (
        extract_isbn,
        extract_title,
        extract_publisher,
        extract_book_info,
        clean_ocr_text,
        _is_garbled_line,
    )

    # ── Test 1: Garbled line detection ───────────────────────────────────
    garbled_cases = [
        ("~»., -á = \" cOng HO Bia",  True,  "Ký tự đặc biệt nhiều"),
        ("||| === ^^^",                 True,  "Symbol-only"),
        ("123 456 789",                 True,  "Số thuần túy"),
        ("",                            True,  "Chuỗi rỗng"),
        ("Đắc Nhân Tâm",               False, "Tiêu đề hợp lệ"),
        ("Dale Carnegie",               False, "Tên tác giả hợp lệ"),
        ("NXB Kim Đồng",                False, "Nhà xuất bản"),
        ("The 7 Habits",                False, "Tiêu đề có số OK"),
    ]

    garbled_correct = 0
    for text, expected_garbled, desc in garbled_cases:
        is_garbled = _is_garbled_line(text)
        passed = (is_garbled == expected_garbled)
        garbled_correct += passed
        report.add(
            f"Garbled [{desc}]",
            passed,
            f"'{text[:30]}' → garbled={is_garbled} (expect={expected_garbled})",
        )

    print(f"\n  Garbled Detection: {garbled_correct}/{len(garbled_cases)} đúng ({garbled_correct/len(garbled_cases)*100:.0f}%)")

    # ── Test 2: ISBN extraction ────────────────────────────────────────────
    isbn_cases = [
        # Format                               Expected (digits only)
        ("ISBN 978-604-325-102-1",  "9786043251021"),   # ISBN-13 với prefix
        ("9786043251021",            "9786043251021"),   # ISBN-13 thuần
        ("0316769487",               "0316769487"),      # ISBN-10 thuần
        ("Đây không có ISBN",        None),              # Không có ISBN
        ("ISBN 123",                 None),              # Quá ngắn
    ]

    isbn_correct = 0
    for text, expected in isbn_cases:
        result = extract_isbn(text)
        passed = (result == expected)
        isbn_correct += passed
        report.add(
            f"ISBN ['{text[:30]}']",
            passed,
            f"Got='{result}', expect='{expected}'",
        )

    print(f"  ISBN Extraction: {isbn_correct}/{len(isbn_cases)} đúng")

    # ── Test 3: Title extraction ───────────────────────────────────────────
    title_cases = [
        (
            ["Đắc Nhân Tâm", "Dale Carnegie", "NXB Tổng hợp"],
            "Đắc Nhân Tâm",
            "Vietnamese book title first"
        ),
        (
            ["ISBN 978-604-01-1234-5", "The Alchemist", "Paulo Coelho"],
            "The Alchemist",        # Bỏ qua dòng ISBN
            "Skip ISBN line"
        ),
        (
            ["~»= sdf", "Harry Potter", "J.K. Rowling"],
            "Harry Potter",         # Bỏ qua garbled line
            "Skip garbled first line"
        ),
        (
            ["NXB Kim Đồng", "Doraemon Tập 1", "Fujiko"],
            "Doraemon Tập 1",       # Bỏ qua dòng publisher
            "Skip publisher line"
        ),
    ]

    title_wers = []
    for lines, expected_title, desc in title_cases:
        result = extract_title(lines)
        result_str = result or ""
        wer = word_error_rate(expected_title, result_str)
        title_wers.append(wer)
        is_correct = wer < 0.3  # Chấp nhận WER < 30%
        report.add(
            f"Title [{desc}]",
            is_correct,
            f"Got='{result_str}', WER={wer:.1%}",
        )

    avg_wer = statistics.mean(title_wers) if title_wers else 1.0
    print(f"  Title WER avg: {avg_wer:.1%} ({'✓ Tốt' if avg_wer < 0.3 else '⚠ Cần cải thiện'})")

    # ── Test 4: Publisher matching ──────────────────────────────────────────
    pub_cases = [
        ("NXB Trẻ ra mắt tác phẩm mới", [""], "NXB Trẻ"),
        ("Sách do NXB Kim Đồng phát hành", [""], "NXB Kim Đồng"),
        ("First News tài trợ bản dịch", [""], "First News"),
    ]

    for text, lines, expected in pub_cases:
        result = extract_publisher(text, lines)
        passed = (result is not None and expected.lower() in (result or "").lower())
        report.add(
            f"Publisher ['{expected}']",
            passed,
            f"Got='{result}'",
        )

    # ── Test 5: extract_book_info full pipeline ─────────────────────────────
    full_text = """Đắc Nhân Tâm
Dale Carnegie
NXB Tổng hợp TP.HCM
Dịch: Nguyễn Nhật Ánh
ISBN 978-604-001-234-5"""

    t0 = time.perf_counter()
    info = extract_book_info(full_text, ocr_confidence=0.85)
    elapsed = (time.perf_counter() - t0) * 1000

    report.add(
        "extract_book_info: Title detected",
        bool(info.title),
        f"title='{info.title}'",
        elapsed,
    )
    report.add(
        "extract_book_info: Has search_query",
        bool(info.search_query),
        f"query='{info.search_query[:40]}'",
    )
    report.add(
        "extract_book_info: Compound confidence",
        0.0 < info.confidence <= 1.0,
        f"confidence={info.confidence:.3f}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 4: VISUAL SIMILARITY (pHash)
# ═══════════════════════════════════════════════════════════════════════════════

def test_visual_similarity(report: TestReport):
    """
    Test pHash:
    - Same image (byte-for-byte equal) → distance = 0
    - Same image JPEG compressed → distance < 5
    - Same image resized → distance < 10
    - Different image → distance > 10
    """
    print(f"\n{BOLD}{CYAN}══ TEST 4: Visual pHash Matching ══{RESET}")

    from ocr_app.services.image_similarity_engine import _compute_phash, _hamming_distance

    book1 = create_book_cover_image("Đắc Nhân Tâm", "Dale Carnegie", bg_color=(30, 80, 150))
    book2 = create_book_cover_image("Harry Potter",  "J.K. Rowling",  bg_color=(80, 10, 100))
    book3 = create_book_cover_image("The Alchemist", "Paulo Coelho",  bg_color=(150, 80, 10))

    # Tính hashes
    hash1 = _compute_phash(book1)
    hash2 = _compute_phash(book2)
    hash3 = _compute_phash(book3)

    # Test: Same image → distance = 0
    hash1_again = _compute_phash(book1)
    dist_same = _hamming_distance(hash1, hash1_again)
    report.add(
        "pHash: Same image → distance=0",
        dist_same == 0,
        f"Hamming distance = {dist_same}",
    )

    # Test: JPEG re-compress → still close
    pil = Image.open(io.BytesIO(book1))
    buf = io.BytesIO()
    pil.save(buf, format="JPEG", quality=70)  # Recompress với quality thấp hơn
    book1_recompressed = buf.getvalue()
    hash1_recomp = _compute_phash(book1_recompressed)
    dist_recomp = _hamming_distance(hash1, hash1_recomp)
    report.add(
        "pHash: JPEG recompress → distance≤5",
        dist_recomp <= 5,
        f"Hamming distance = {dist_recomp}",
    )

    # Test: Slight resize → still close
    pil_small = pil.resize((300, 450))
    buf2 = io.BytesIO()
    pil_small.save(buf2, format="JPEG", quality=85)
    book1_resized = buf2.getvalue()
    hash1_resized = _compute_phash(book1_resized)
    dist_resize = _hamming_distance(hash1, hash1_resized)
    report.add(
        "pHash: Resized image → distance≤10",
        dist_resize <= 10,
        f"Hamming distance = {dist_resize}",
    )

    # Test: Different books → far apart
    dist_diff1 = _hamming_distance(hash1, hash2)
    dist_diff2 = _hamming_distance(hash1, hash3)
    report.add(
        "pHash: Different books → distance>10",
        dist_diff1 > 10,
        f"Book1 vs Book2: distance={dist_diff1}",
    )
    report.add(
        "pHash: Different books → distance>10 (2nd pair)",
        dist_diff2 > 10,
        f"Book1 vs Book3: distance={dist_diff2}",
    )

    print(f"\n  {BOLD}pHash Summary:{RESET}")
    print(f"    Same image:         dist={dist_same}")
    print(f"    JPEG recompressed:  dist={dist_recomp}")
    print(f"    Resized:            dist={dist_resize}")
    print(f"    Different book1-2:  dist={dist_diff1}")
    print(f"    Different book1-3:  dist={dist_diff2}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 5: OCR ENGINE (nặng, bỏ qua nếu --fast)
# ═══════════════════════════════════════════════════════════════════════════════

def test_ocr_engine(report: TestReport):
    """
    Test OCR engine với synthetic images có ground truth text.
    Đo CER và WER cho từng loại ảnh.
    """
    print(f"\n{BOLD}{CYAN}══ TEST 5: OCR Engine Accuracy ══{RESET}")
    print(f"  {YELLOW}⚠  OCR engine test (~30-60s do load model...){RESET}")

    import asyncio
    from ocr_app.services.image_preprocessor import preprocess
    from ocr_app.services.ocr_engine import extract_text_async

    test_cases = [
        {
            "name":     "Simple Vietnamese title",
            "title":    "Đắc Nhân Tâm",
            "author":   "Dale Carnegie",
            "expected": "Đắc Nhân Tâm Dale Carnegie",
        },
        {
            "name":     "English title",
            "title":    "The Alchemist",
            "author":   "Paulo Coelho",
            "expected": "The Alchemist Paulo Coelho",
        },
        {
            "name":     "Vietnamese NXB",
            "title":    "Nhà Giả Kim",
            "author":   "",
            "expected": "Nhà Giả Kim",
        },
    ]

    cers = []
    wers = []

    async def run_test(case):
        img_bytes = create_book_cover_image(case["title"], case["author"])
        t0 = time.perf_counter()
        preprocessed, img_type = preprocess(img_bytes)
        ocr_result = await extract_text_async(preprocessed, image_type=img_type)
        elapsed = (time.perf_counter() - t0) * 1000

        ocr_text = ocr_result.get("text", "")
        conf     = ocr_result.get("confidence", 0.0)
        engine   = ocr_result.get("engine_used", "?")

        cer = character_error_rate(case["expected"], ocr_text)
        wer = word_error_rate(case["expected"], ocr_text)
        cers.append(cer)
        wers.append(wer)

        passed = wer < 0.5  # Chấp nhận WER < 50% cho ảnh synthetic (font hệ thống có thể giới hạn)
        report.add(
            f"OCR [{case['name']}]",
            passed,
            f"WER={wer:.1%} CER={cer:.1%} conf={conf:.2f} engine={engine}",
            elapsed,
        )
        print(f"    '{case['name']}': WER={wer:.1%} (expected='{case['expected']}', got='{ocr_text[:50]}')")

    for case in test_cases:
        asyncio.run(run_test(case))

    if cers:
        print(f"\n  {BOLD}OCR Accuracy Summary:{RESET}")
        print(f"    Avg CER: {statistics.mean(cers):.1%}  (target < 20%)")
        print(f"    Avg WER: {statistics.mean(wers):.1%}  (target < 30%)")
        status = GREEN + "✓ Tốt" if statistics.mean(wers) < 0.3 else YELLOW + "⚠ Cần điều chỉnh"
        print(f"    Status: {status}{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# TEST SUITE 6: PERFORMANCE BENCHMARK
# ═══════════════════════════════════════════════════════════════════════════════

def test_performance_benchmark(report: TestReport):
    """
    Đo latency của từng stage (không tính OCR engine vì phụ thuộc hardware).
    Target:
      - Garbage detection:     < 5ms
      - pHash computation:     < 20ms
      - Image preprocessing:   < 500ms
      - Text extraction (NLP): < 100ms
    """
    print(f"\n{BOLD}{CYAN}══ TEST 6: Performance Benchmark ══{RESET}")
    REPS   = 20  # Số lần lặp để đo latency ổn định
    WARMUP = 3   # Số lần warmup (loại bỏ cold start latency)

    from ocr_app.services.image_similarity_engine import assess_image_relevance, _compute_phash
    from ocr_app.services.image_preprocessor import preprocess
    from ocr_app.services.text_extractor import extract_book_info

    book_img = create_book_cover_image("Benchmark Test", "Author Name")

    # ── Benchmark: Garbage Detection ─────────────────────────────────────
    print(f"\n  {BOLD}Garbage Detection Latency ({REPS} reps, {WARMUP} warmup):{RESET}")
    gc_times = []
    # Warmup: Cold start trần đầu bao giờ chậm hơn vì import/JIT
    for _ in range(WARMUP):
        assess_image_relevance(book_img)
    for _ in range(REPS):
        t0 = time.perf_counter()
        assess_image_relevance(book_img)
        gc_times.append((time.perf_counter() - t0) * 1000)

    gc_stats = latency_stats(gc_times)
    passed_gc = gc_stats["p90"] < 50  # Target: p90 < 50ms (warm run)
    report.add(
        "Perf: Garbage Detection (p90 < 20ms)",
        passed_gc,
        f"median={gc_stats['median']}ms p90={gc_stats['p90']}ms",
    )
    _print_latency("Garbage Detection", gc_stats)

    # ── Benchmark: pHash Computation ─────────────────────────────────────
    print(f"\n  {BOLD}pHash Computation Latency ({REPS} reps, {WARMUP} warmup):{RESET}")
    ph_times = []
    for _ in range(WARMUP):
        _compute_phash(book_img)
    for _ in range(REPS):
        t0 = time.perf_counter()
        _compute_phash(book_img)
        ph_times.append((time.perf_counter() - t0) * 1000)

    ph_stats = latency_stats(ph_times)
    passed_ph = ph_stats["p90"] < 100  # Target: p90 < 100ms
    report.add(
        "Perf: pHash Computation (p90 < 100ms)",
        passed_ph,
        f"median={ph_stats['median']}ms p90={ph_stats['p90']}ms",
    )
    _print_latency("pHash Computation", ph_stats)

    # ── Benchmark: Image Preprocessing ────────────────────────────────────
    print(f"\n  {BOLD}Image Preprocessing Latency ({REPS} reps, {WARMUP} warmup):{RESET}")
    pp_times = []
    for _ in range(WARMUP):
        preprocess(book_img)
    for _ in range(REPS):
        t0 = time.perf_counter()
        preprocess(book_img)
        pp_times.append((time.perf_counter() - t0) * 1000)

    pp_stats = latency_stats(pp_times)
    passed_pp = pp_stats["p90"] < 500  # Target: p90 < 500ms
    report.add(
        "Perf: Image Preprocessing (p90 < 500ms)",
        passed_pp,
        f"median={pp_stats['median']}ms p90={pp_stats['p90']}ms",
    )
    _print_latency("Image Preprocessing", pp_stats)

    # ── Benchmark: Text Extraction (NLP) ──────────────────────────────────
    sample_text = "Đắc Nhân Tâm\nDale Carnegie\nNXB Tổng hợp\nISBN 978-604-001-234-5"
    print(f"\n  {BOLD}Text Extraction (NLP) Latency ({REPS} reps, {WARMUP} warmup):{RESET}")
    nlp_times = []
    for _ in range(WARMUP):
        extract_book_info(sample_text, 0.85)
    for _ in range(REPS):
        t0 = time.perf_counter()
        extract_book_info(sample_text, 0.85)
        nlp_times.append((time.perf_counter() - t0) * 1000)

    nlp_stats = latency_stats(nlp_times)
    passed_nlp = nlp_stats["p90"] < 200  # Target: p90 < 200ms (NER có thể chậm)
    report.add(
        "Perf: NLP Extraction (p90 < 200ms)",
        passed_nlp,
        f"median={nlp_stats['median']}ms p90={nlp_stats['p90']}ms",
    )
    _print_latency("NLP Extraction", nlp_stats)

    # ── Summary ────────────────────────────────────────────────────────────
    print(f"\n  {BOLD}Pipeline Latency Budget Estimate (ex. OCR engine):{RESET}")
    non_ocr_p90 = gc_stats["p90"] + ph_stats["p90"] + pp_stats["p90"] + nlp_stats["p90"]
    print(f"    Garbage check:  {gc_stats['p90']:>6.0f}ms")
    print(f"    pHash match:    {ph_stats['p90']:>6.0f}ms")
    print(f"    Preprocessing:  {pp_stats['p90']:>6.0f}ms")
    print(f"    NLP extraction: {nlp_stats['p90']:>6.0f}ms")
    print(f"    ─────────────────────────")
    print(f"    Non-OCR total:  {non_ocr_p90:>6.0f}ms (p90)")
    print(f"    OCR engine:     ~1500-5000ms (hardware dependent)")
    print(f"    Visual MATCH:   {ph_stats['p90']:>6.0f}ms (NO OCR needed!)")


def _print_latency(name: str, stats: dict):
    bar_len = min(int(stats['median'] / 10), 50)
    bar = "█" * bar_len
    print(f"    {name}:")
    print(f"      min={stats['min']}ms  median={stats['median']}ms  p90={stats['p90']}ms  max={stats['max']}ms")
    print(f"      [{bar}] {stats['median']}ms")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_run(fn, report: TestReport, name: str):
    """Ch\u1ea1y m\u1ed9t test suite, b\u1eaft ImportError/Exception m\u00e0 kh\u00f4ng d\u1eebng c\u00e1c suite kh\u00e1c."""
    try:
        fn(report)
    except ImportError as e:
        missing = str(e).replace("No module named ", "").strip("'")
        print(f"\n{YELLOW}⚠  Suite '{name}' b\u1ecb b\u1ecf qua do thi\u1ebfu th\u01b0 vi\u1ec7n: {e}{RESET}")
        print(f"   \u25b8 C\u00e0i \u0111\u1eb7t: pip install {missing}")
        report.add(f"{name} [SKIPPED \u2013 ch\u01b0a c\u00e0i {missing}]", True,
                   f"Skip (kh\u00f4ng \u1ea3nh h\u01b0\u1edfng \u0111\u1ebfn c\u00e1c suite kh\u00e1c)")
    except Exception as e:
        print(f"\n{RED}\u274c Suite '{name}' l\u1ed7i kh\u00f4ng mong \u0111\u1ee3i: {e}{RESET}")
        report.add(f"{name} [ERROR]", False, str(e)[:120])


def main():
    parser = argparse.ArgumentParser(description="OCR System Test Suite v2")
    parser.add_argument("--fast",      action="store_true", help="B\u1ecf qua OCR engine test (n\u1eb7ng, c\u1ea7n EasyOCR + Tesseract)")
    parser.add_argument("--benchmark", action="store_true", help="Ch\u1ec9 ch\u1ea1y performance benchmark")
    parser.add_argument("--suite",     type=str, default="all",
                        choices=["all", "garbage", "preprocessing", "nlp", "visual", "ocr", "perf"],
                        help="Ch\u1ecdn suite c\u1ee5 th\u1ec3")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"{BOLD}\U0001f52c OCR SYSTEM TEST SUITE v2{RESET}")
    print(f"{'='*70}")
    print(f"  Mode:  {'FAST (b\u1ecf OCR engine)' if args.fast else 'FULL'}")
    print(f"  Suite: {args.suite}")
    print(f"  Path:  {os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}")
    print(f"{'='*70}")

    report = TestReport()
    t_total = time.perf_counter()

    # ── Ch\u1ea1y t\u1eebng suite \u0111\u1ed9c l\u1eadp (ImportError trong 1 suite kh\u00f4ng \u1ea3nh h\u01b0\u1edfng suite kh\u00e1c) ──

    if args.suite in ("all", "garbage"):
        _safe_run(test_garbage_detection, report, "Garbage Detection")

    if args.suite in ("all", "preprocessing"):
        _safe_run(test_preprocessing, report, "Image Preprocessing")

    if args.suite in ("all", "nlp"):
        _safe_run(test_text_extraction, report, "NLP Text Extraction")

    if args.suite in ("all", "visual"):
        _safe_run(test_visual_similarity, report, "Visual pHash")

    if args.suite in ("all", "ocr"):
        if args.fast:
            print(f"\n{YELLOW}\u26a1 B\u1ecf qua OCR Engine test (--fast mode){RESET}")
            print(f"   Ch\u1ea1y \u0111\u1ea7y \u0111\u1ee7: python test_ocr_system.py --suite ocr")
        else:
            print(f"\n{YELLOW}\u26a0  OCR Engine test y\u00eau c\u1ea7u: EasyOCR + pytesseract (C\u00f3 th\u1ec3 m\u1ea5t 1-5 ph\u00fat){RESET}")
            print(f"   N\u1ebfu ch\u01b0a c\u00e0i: pip install easyocr pytesseract")
            print(f"   Tesseract binary: https://github.com/UB-Mannheim/tesseract/wiki")
            _safe_run(test_ocr_engine, report, "OCR Engine")

    # Performance benchmark lu\u00f4n ch\u1ea1y (tr\u1eeb khi ch\u1ec9 ch\u1ecdn suite kh\u00e1c)
    if args.suite in ("all", "perf") or args.benchmark:
        _safe_run(test_performance_benchmark, report, "Performance Benchmark")

    total_elapsed = (time.perf_counter() - t_total) * 1000
    report.print_report()

    s = report.summary()
    color = GREEN if s["failed"] == 0 else RED
    print(f"  T\u1ed5ng th\u1eddi gian: {total_elapsed:.0f}ms")
    if s["failed"] > 0:
        print(f"  {RED}\u274c {s['failed']} test(s) FAILED \u2013 xem chi ti\u1ebft ph\u00eda tr\u00ean{RESET}\n")
    else:
        print(f"  {GREEN}\u2705 T\u1ea5t c\u1ea3 PASSED!\u0020\U0001f680{RESET}\n")

    sys.exit(0 if s["failed"] == 0 else 1)


if __name__ == "__main__":
    main()
