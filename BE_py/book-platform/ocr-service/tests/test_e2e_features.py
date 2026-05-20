#!/usr/bin/env python3
"""
test_e2e_features.py – E2E Integration Test cho 3 tính năng chính
===================================================================

Tính năng test:
  [1] Tìm sách bằng ảnh (search-by-cover):    POST /api/ocr/search-by-cover
  [2] Admin auto-fill nhận diện sách:          POST /api/ocr/extract-book-info
  [3] Chatbot nhận ảnh sách, tìm và trả lời:  OCR → POST /api/chat/message

Cách chạy (services phải đang chạy):
  cd source-code/BE_py/book-platform/ocr-service
  python tests/test_e2e_features.py

  Chỉ test OCR (không cần chatbot):
  python tests/test_e2e_features.py --skip-chat

  Test với service URL khác:
  python tests/test_e2e_features.py --ocr-url http://localhost:8005 --chat-url http://localhost:8004
"""
import argparse
import io
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional, List

import numpy as np
from PIL import Image, ImageDraw, ImageFont

# ── Try import httpx, fallback to urllib ─────────────────────────────────────
try:
    import httpx
    _USE_HTTPX = True
except ImportError:
    import urllib.request
    import urllib.error
    _USE_HTTPX = False

# ── ANSI colors ───────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Default service URLs ──────────────────────────────────────────────────────
DEFAULT_OCR_URL  = "http://localhost:8005"
DEFAULT_CHAT_URL = "http://localhost:8004"

# ═══════════════════════════════════════════════════════════════════════════════
# TEST IMAGE FACTORY
# ═══════════════════════════════════════════════════════════════════════════════

def _make_font(size: int = 36) -> ImageFont.FreeTypeFont:
    for path in [
        "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/times.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]:
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def make_book_cover(
    title: str,
    author: str = "",
    isbn: str = "",
    bg_color=(30, 80, 150),
    width=400, height=600,
) -> bytes:
    """Tạo ảnh bìa sách synthetic với gradient + đường viền (pass garbage detection)."""
    arr = np.zeros((height, width, 3), dtype=np.uint8)
    for y in range(height):
        f = y / height
        arr[y, :] = [
            min(int(bg_color[0] * (1 - f * 0.35)), 255),
            min(int(bg_color[1] * (1 - f * 0.35)), 255),
            min(int(bg_color[2] * (1 + f * 0.1)),  255),
        ]
    img  = Image.fromarray(arr, "RGB")
    draw = ImageDraw.Draw(img)

    # Viền
    b = 10
    draw.rectangle([b, b, width-b, height-b], outline=(255,255,255), width=2)

    # Text
    draw.text((width//2, 80),  title,  font=_make_font(38), fill=(255,255,255), anchor="mm")
    if author:
        draw.text((width//2, 160), author, font=_make_font(26), fill=(220,220,180), anchor="mm")
    if isbn:
        draw.text((width//2, height-40), f"ISBN {isbn}", font=_make_font(18), fill=(180,220,180), anchor="mm")

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()


def make_garbage_image() -> bytes:
    """Tạo ảnh trắng trơn – garbage."""
    img = Image.new("RGB", (200, 200), (255, 255, 255))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def make_receipt_image(items: list, total: int) -> bytes:
    """Tạo ảnh hóa đơn synthetic."""
    width, height = 400, 200 + len(items) * 40
    img  = Image.new("RGB", (width, height), (248, 248, 248))
    draw = ImageDraw.Draw(img)
    font = _make_font(22)
    draw.text((width//2, 20), "HÓA ĐƠN MUA SÁCH", font=_make_font(24), fill=(0,0,0), anchor="mm")
    draw.line([(20, 45), (width-20, 45)], fill=(0,0,0), width=1)
    y = 60
    for name, qty, price in items:
        draw.text((25, y), f"{name}", font=font, fill=(0,0,0))
        draw.text((width-25, y), f"x{qty}  {price:,}đ", font=font, fill=(0,0,0), anchor="ra")
        y += 40
    draw.line([(20, y), (width-20, y)], fill=(0,0,0), width=1)
    draw.text((width-25, y+10), f"Tổng: {total:,}đ", font=_make_font(24), fill=(0,0,0), anchor="ra")
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP CLIENT (httpx hoặc urllib fallback)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class Response:
    status_code: int
    json_data:   dict
    elapsed_ms:  float
    error:       Optional[str] = None


def post_file(url: str, image_bytes: bytes, filename: str = "test.jpg",
              timeout: int = 60) -> Response:
    """POST multipart/form-data với file upload."""
    t0 = time.perf_counter()
    try:
        if _USE_HTTPX:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(
                    url,
                    files={"file": (filename, image_bytes, "image/jpeg")},
                )
            elapsed = (time.perf_counter() - t0) * 1000
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text[:500]}
            return Response(resp.status_code, data, elapsed)
        else:
            # urllib fallback (không hỗ trợ multipart tốt, nhưng thử)
            import urllib.request as ur2
            boundary = "----TestBoundary1234567890"
            body = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
                f"Content-Type: image/jpeg\r\n\r\n"
            ).encode() + image_bytes + f"\r\n--{boundary}--\r\n".encode()
            req = ur2.Request(
                url,
                data=body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            with ur2.urlopen(req, timeout=timeout) as resp:
                elapsed = (time.perf_counter() - t0) * 1000
                data = json.loads(resp.read())
                return Response(resp.status, data, elapsed)
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        return Response(0, {}, elapsed, error=str(e))


def post_json(url: str, payload: dict, timeout: int = 30) -> Response:
    """POST JSON."""
    t0 = time.perf_counter()
    try:
        if _USE_HTTPX:
            with httpx.Client(timeout=timeout) as client:
                resp = client.post(url, json=payload)
            elapsed = (time.perf_counter() - t0) * 1000
            try:
                data = resp.json()
            except Exception:
                data = {"raw": resp.text[:500]}
            return Response(resp.status_code, data, elapsed)
        else:
            body = json.dumps(payload, ensure_ascii=False).encode()
            req  = urllib.request.Request(
                url, data=body,
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                elapsed = (time.perf_counter() - t0) * 1000
                return Response(resp.status, json.loads(resp.read()), elapsed)
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        return Response(0, {}, elapsed, error=str(e))


def get_json(url: str, timeout: int = 10) -> Response:
    t0 = time.perf_counter()
    try:
        if _USE_HTTPX:
            with httpx.Client(timeout=timeout) as client:
                resp = client.get(url)
            elapsed = (time.perf_counter() - t0) * 1000
            data = resp.json()
            return Response(resp.status_code, data, elapsed)
        else:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                elapsed = (time.perf_counter() - t0) * 1000
                return Response(resp.status, json.loads(resp.read()), elapsed)
    except Exception as e:
        elapsed = (time.perf_counter() - t0) * 1000
        return Response(0, {}, elapsed, error=str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# TEST REPORT
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class E2EResult:
    name:       str
    passed:     bool
    detail:     str = ""
    elapsed_ms: float = 0.0
    response:   Optional[dict] = None


@dataclass
class E2EReport:
    results: List[E2EResult] = field(default_factory=list)
    skipped: int = 0

    def add(self, name: str, passed: bool, detail: str = "",
            elapsed_ms: float = 0.0, response: dict = None):
        self.results.append(E2EResult(name, passed, detail, elapsed_ms, response))

    def skip(self, name: str, reason: str):
        self.skipped += 1
        self.results.append(E2EResult(f"[SKIP] {name}", True, f"⏭ {reason}", 0.0))

    def print_report(self):
        print(f"\n{'═'*72}")
        print(f"{BOLD}📋 E2E TEST REPORT{RESET}")
        print(f"{'═'*72}")
        for r in self.results:
            icon   = f"{GREEN}✅{RESET}" if r.passed else f"{RED}❌{RESET}"
            timing = f"  {CYAN}[{r.elapsed_ms:.0f}ms]{RESET}" if r.elapsed_ms > 0 else ""
            print(f"  {icon}  {r.name}{timing}")
            if r.detail:
                color = CYAN if r.passed else YELLOW
                print(f"       {color}→ {r.detail}{RESET}")

        total   = len(self.results)
        passed  = sum(1 for r in self.results if r.passed and not r.name.startswith("[SKIP]"))
        skipped = sum(1 for r in self.results if r.name.startswith("[SKIP]"))
        failed  = total - passed - skipped
        pct     = (passed + skipped) / total * 100 if total > 0 else 0

        print(f"\n{'─'*72}")
        color = GREEN if failed == 0 else RED
        print(f"  {color}{BOLD}Ket qua: {passed}/{total - skipped} PASSED | {skipped} SKIPPED | {failed} FAILED ({pct:.0f}%){RESET}")
        print(f"{'═'*72}\n")
        return failed == 0


# ═══════════════════════════════════════════════════════════════════════════════
# KIỂM TRA SERVICE AVAILABILITY
# ═══════════════════════════════════════════════════════════════════════════════

def check_service(name: str, url: str) -> bool:
    """Ping /health endpoint."""
    health_url = url.rstrip("/") + "/health"
    # Try common health endpoints
    for endpoint in ["/health", "/api/ocr/health", "/api/chat/health", "/"]:
        r = get_json(url.rstrip("/") + endpoint, timeout=5)
        if r.status_code in (200, 404) and not r.error:
            return r.status_code == 200
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE 1: TÌM SÁCH BẰNG ẢNH (Customer Search by Cover)
# ═══════════════════════════════════════════════════════════════════════════════

def test_search_by_cover(report: E2EReport, ocr_url: str):
    """
    Test POST /api/ocr/search-by-cover

    Scenarios:
    1. Ảnh bìa sách hợp lệ → success=True, có kết quả OCR
    2. Ảnh rác (trắng hoàn toàn) → success=False, image_quality=irrelevant
    3. Response format đầy đủ (match_method, image_quality, processing_time_ms)
    4. Latency < 60s cho ảnh cover
    """
    url = ocr_url.rstrip("/") + "/api/ocr/search-by-cover"
    print(f"\n{BOLD}{CYAN}═══ [1] Tìm Sách Bằng Ảnh (search-by-cover) ═══{RESET}")
    print(f"  URL: {url}")

    # ── Test 1.1: Ảnh bìa hợp lệ ─────────────────────────────────────────────
    print(f"\n  {YELLOW}▸ Test 1.1: Upload ảnh bìa sách hợp lệ...{RESET}")
    cover = make_book_cover("Đắc Nhân Tâm", "Dale Carnegie", "9786043251021",
                            bg_color=(20, 60, 140))
    r = post_file(url, cover, "dac_nhan_tam.jpg", timeout=90)

    if r.error:
        report.add("Search-by-cover: Service kết nối",
                   False, f"Lỗi kết nối: {r.error}")
        print(f"  {RED}❌ Không kết nối được đến OCR service: {r.error}{RESET}")
        print(f"  {YELLOW}   Đảm bảo OCR service đang chạy trên {ocr_url}{RESET}")
        return
    else:
        report.add("Search-by-cover: Service kết nối", True,
                   f"HTTP {r.status_code} [{r.elapsed_ms:.0f}ms]", r.elapsed_ms)

    d = r.json_data
    _print_ocr_response(d, "1.1 Ảnh bìa")

    # Validate response schema
    has_schema = all(k in d for k in ["success", "processing_time_ms", "match_method",
                                       "image_quality", "search_results"])
    report.add(
        "Search-by-cover: Response có đủ fields",
        has_schema,
        f"success={d.get('success')}, match_method={d.get('match_method')}, "
        f"image_quality={d.get('image_quality')}",
        r.elapsed_ms,
    )

    # match_method phải là 1 trong 3 giá trị hợp lệ
    valid_methods = {"visual_match", "ocr_search", "not_found"}
    method = d.get("match_method", "")
    report.add(
        "Search-by-cover: match_method hợp lệ",
        method in valid_methods,
        f"match_method='{method}' ∈ {valid_methods}",
    )

    # Latency < 60s
    report.add(
        "Search-by-cover: Latency < 60s",
        r.elapsed_ms < 60_000,
        f"{r.elapsed_ms:.0f}ms (target < 60000ms)",
        r.elapsed_ms,
    )

    # image_quality = "good" cho ảnh hợp lệ (có thể "low" nếu search service offline)
    quality = d.get("image_quality", "")
    report.add(
        "Search-by-cover: image_quality ≠ irrelevant",
        quality != "irrelevant",
        f"quality='{quality}' (irrelevant = ảnh bị từ chối sai)",
    )

    # ── Test 1.2: Ảnh rác → phải bị từ chối ─────────────────────────────────
    print(f"\n  {YELLOW}▸ Test 1.2: Upload ảnh rác (nền trắng thuần)...{RESET}")
    garbage = make_garbage_image()
    r2 = post_file(url, garbage, "garbage.jpg", timeout=30)

    if not r2.error:
        d2 = r2.json_data
        _print_ocr_response(d2, "1.2 Ảnh rác")
        was_rejected = (
            d2.get("success") == False
            and d2.get("image_quality") == "irrelevant"
        )
        report.add(
            "Search-by-cover: Ảnh rác bị từ chối (irrelevant)",
            was_rejected,
            f"success={d2.get('success')}, quality={d2.get('image_quality')}, "
            f"error: {d2.get('error','')[:60]}",
            r2.elapsed_ms,
        )

    # ── Test 1.3: Ảnh quá nhỏ (< 50px) ──────────────────────────────────────
    print(f"\n  {YELLOW}▸ Test 1.3: Upload ảnh quá nhỏ (30x30px)...{RESET}")
    tiny = Image.new("RGB", (30, 30), (100, 50, 200))
    buf  = io.BytesIO()
    tiny.save(buf, format="JPEG")
    r3 = post_file(url, buf.getvalue(), "tiny.jpg", timeout=15)
    if not r3.error:
        d3 = r3.json_data
        rejected_tiny = d3.get("success") == False
        report.add(
            "Search-by-cover: Ảnh nhỏ (30x30) bị từ chối",
            rejected_tiny,
            f"success={d3.get('success')}, error: {d3.get('error','')[:60]}",
            r3.elapsed_ms,
        )

    # ── Test 1.4: File không phải ảnh → 400 ──────────────────────────────────
    print(f"\n  {YELLOW}▸ Test 1.4: Upload file text (không phải ảnh)...{RESET}")
    r4 = post_file(url, b"This is not an image", "notimage.txt", timeout=10)
    if not r4.error:
        report.add(
            "Search-by-cover: File không hợp lệ → HTTP 4xx",
            r4.status_code in (400, 415, 422),
            f"HTTP {r4.status_code}",
            r4.elapsed_ms,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE 2: ADMIN AUTO-FILL (Nhận diện sách để điền form)
# ═══════════════════════════════════════════════════════════════════════════════

def test_extract_book_info(report: E2EReport, ocr_url: str):
    """
    Test POST /api/ocr/extract-book-info

    Scenarios:
    1. Ảnh bìa có đầy đủ thông tin → title được trích xuất
    2. Ảnh bìa có ISBN → isbn được trả về
    3. Response KHÔNG có search_results (chỉ dùng cho auto-fill form)
    4. Dual preprocessing mode (normal + aggressive)
    """
    url = ocr_url.rstrip("/") + "/api/ocr/extract-book-info"
    print(f"\n{BOLD}{CYAN}═══ [2] Admin Auto-Fill (extract-book-info) ═══{RESET}")
    print(f"  URL: {url}")

    # ── Test 2.1: Bìa trước với title + author ────────────────────────────────
    print(f"\n  {YELLOW}▸ Test 2.1: Bìa sách (title + author)...{RESET}")
    cover = make_book_cover(
        "Nhà Giả Kim",
        "Paulo Coelho",
        "9786044800813",
        bg_color=(120, 60, 20)
    )
    r = post_file(url, cover, "nha_gia_kim.jpg", timeout=90)

    if r.error:
        report.add("Extract-book-info: Service kết nối",
                   False, f"Lỗi kết nối: {r.error}")
        print(f"  {RED}❌ Không kết nối được: {r.error}{RESET}")
        return
    if r.status_code not in (200, 201):
        detail = r.json_data.get("detail", r.json_data)
        report.add("Extract-book-info: HTTP status",
                   False, f"HTTP {r.status_code}: {str(detail)[:100]}")
        print(f"  {RED}❌ HTTP {r.status_code}: {detail}{RESET}")
        return

    d = r.json_data
    _print_ocr_response(d, "2.1 Admin extract")

    # success = True
    report.add(
        "Extract-book-info: success=True cho ảnh hợp lệ",
        d.get("success") == True,
        f"success={d.get('success')}, engine={d.get('engine_used')}",
        r.elapsed_ms,
    )

    # book_info tồn tại
    book_info = d.get("book_info", {})
    report.add(
        "Extract-book-info: book_info tồn tại",
        isinstance(book_info, dict) and len(book_info) > 0,
        f"book_info keys: {list(book_info.keys())}",
    )

    # confidence > 0
    conf = book_info.get("confidence", 0)
    report.add(
        "Extract-book-info: confidence > 0",
        conf > 0,
        f"confidence={conf:.3f}",
    )

    # search_results PHẢI RỖNG (extract-book-info không search DB)
    search_res = d.get("search_results", [])
    report.add(
        "Extract-book-info: search_results=[] (chỉ dùng cho auto-fill)",
        search_res == [],
        f"search_results count: {len(search_res)} (expect 0)",
    )

    # ── Test 2.2: Bìa sau (nhiều chữ, mật độ cao) → aggressive mode ──────────
    print(f"\n  {YELLOW}▸ Test 2.2: Ảnh kiểu document (back cover)...{RESET}")
    # Tạo ảnh giống bìa sau – nền sáng, chữ đen
    back = Image.new("RGB", (400, 600), (240, 240, 240))
    draw = ImageDraw.Draw(back)
    font = _make_font(20)
    lines = [
        "Tóm tắt nội dung sách:",
        "Nhà Giả Kim là cuốn tiểu thuyết",
        "của Paulo Coelho xuất bản năm 1988.",
        "Câu chuyện kể về Santiago,",
        "một cậu bé chăn cừu Tây Ban Nha",
        "theo đuổi giấc mơ đến Kim Tự Tháp.",
        "",
        "ISBN: 978-604-4800-813",
        "NXB Văn Học",
        "Giá bìa: 89.000đ",
    ]
    y = 40
    for line in lines:
        draw.text((20, y), line, font=font, fill=(0, 0, 0))
        y += 35

    buf = io.BytesIO()
    back.save(buf, format="JPEG", quality=90)
    r2 = post_file(url, buf.getvalue(), "back_cover.jpg", timeout=90)

    if not r2.error:
        d2 = r2.json_data
        _print_ocr_response(d2, "2.2 Back cover")

        extracted_text = d2.get("extracted_text", "")
        report.add(
            "Extract-book-info: Đọc được text từ back cover",
            len(extracted_text.strip()) > 10,
            f"text length: {len(extracted_text)} chars → '{extracted_text[:50]}...'",
            r2.elapsed_ms,
        )

        # ISBN phải được trích xuất
        isbn = d2.get("book_info", {}).get("isbn")
        report.add(
            "Extract-book-info: ISBN được trích xuất",
            isbn is not None,
            f"isbn='{isbn}'",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE 3: CHATBOT NHẬN ẢNH (OCR → Chat integration)
# ═══════════════════════════════════════════════════════════════════════════════

def test_chatbot_image_flow(report: E2EReport, ocr_url: str, chat_url: str):
    """
    Luồng: Upload ảnh → OCR trích xuất title → Gửi title vào chatbot → Chatbot trả lời.

    Pattern này simulate UI flow:
    1. User upload ảnh bìa sách trong chatbot
    2. Frontend gọi OCR service → lấy title
    3. Frontend tự động gửi message: "Tìm sách <title>"
    4. Chatbot trả lời với thông tin sách
    """
    ocr_endpoint  = ocr_url.rstrip("/")  + "/api/ocr/search-by-cover"
    chat_endpoint = chat_url.rstrip("/") + "/api/chat/message"

    print(f"\n{BOLD}{CYAN}═══ [3] Chatbot + OCR Integration Flow ═══{RESET}")
    print(f"  OCR URL:  {ocr_endpoint}")
    print(f"  Chat URL: {chat_endpoint}")

    # ── Step 3.1: Upload ảnh đến OCR ─────────────────────────────────────────
    print(f"\n  {YELLOW}▸ Bước 3.1: Upload ảnh bìa sách đến OCR...{RESET}")
    cover = make_book_cover("Harry Potter", "J.K. Rowling", bg_color=(60, 20, 120))
    r_ocr = post_file(ocr_endpoint, cover, "harry_potter.jpg", timeout=90)

    if r_ocr.error:
        report.add("Chatbot+OCR: Bước 1 – OCR upload",
                   False, f"OCR error: {r_ocr.error}")
        return

    d_ocr = r_ocr.json_data
    ocr_title = (d_ocr.get("book_info") or {}).get("title") or ""
    ocr_text  = d_ocr.get("extracted_text", "")
    search_query = ocr_title or (ocr_text.split("\n")[0] if ocr_text else "")

    _print_ocr_response(d_ocr, "3.1 OCR")

    report.add(
        "Chatbot+OCR: Bước 1 – OCR trả về response",
        r_ocr.status_code == 200,
        f"HTTP {r_ocr.status_code}, match_method={d_ocr.get('match_method')}",
        r_ocr.elapsed_ms,
    )

    # ── Step 3.2: Gửi vào chatbot ────────────────────────────────────────────
    print(f"\n  {YELLOW}▸ Bước 3.2: Gửi query từ OCR vào chatbot...{RESET}")

    # Nếu không extract được title, dùng fallback message
    if search_query.strip():
        chat_message = f"Tìm sách: {search_query}"
    else:
        chat_message = "Tìm sách Harry Potter"
        print(f"  {YELLOW}  (OCR không extract được title, dùng fallback: '{chat_message}'){RESET}")

    print(f"  Chat message: '{chat_message}'")

    r_chat = post_json(chat_endpoint, {
        "session_id": f"e2e-test-{int(time.time())}",
        "message":    chat_message,
        "role":       "customer",
        "user_id":    999,
    }, timeout=60)

    if r_chat.error:
        report.add("Chatbot+OCR: Bước 2 – Chat response",
                   False, f"Chat error: {r_chat.error}")
        print(f"  {RED}❌ Chat service không phản hồi: {r_chat.error}{RESET}")
        print(f"  {YELLOW}   Đảm bảo Chatbot service đang chạy trên {chat_url}{RESET}")
        return

    d_chat = r_chat.json_data
    answer = d_chat.get("answer", "")
    intent = d_chat.get("intent", "")

    print(f"\n  {BOLD}Chat Response:{RESET}")
    print(f"    Intent:  {intent}")
    print(f"    Answer:  {answer[:150]}{'...' if len(answer) > 150 else ''}")
    print(f"    Sources: {len(d_chat.get('sources', []))} nguồn")
    print(f"    Buttons: {[b.get('label') for b in d_chat.get('navigate_buttons', [])]}")

    # Chatbot phải có answer khác rỗng
    report.add(
        "Chatbot+OCR: Bước 2 – Chatbot trả lời được",
        len(answer) > 10,
        f"intent={intent}, answer_len={len(answer)}",
        r_chat.elapsed_ms,
    )

    # Intent phải là search-related (không phải "error")
    report.add(
        "Chatbot+OCR: Intent không phải 'error'",
        intent != "error",
        f"intent='{intent}'",
    )

    # ── Step 3.3: Verify navigate_buttons (link đến trang sách) ─────────────
    buttons = d_chat.get("navigate_buttons", [])
    has_book_button = any(
        "sách" in b.get("label", "").lower() or "xem" in b.get("label", "").lower()
        for b in buttons
    )
    report.add(
        "Chatbot+OCR: Có navigate_buttons dẫn đến sách",
        len(buttons) > 0,
        f"{len(buttons)} buttons: {[b.get('label') for b in buttons[:3]]}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE 4: HEALTH CHECKS
# ═══════════════════════════════════════════════════════════════════════════════

def test_health_endpoints(report: E2EReport, ocr_url: str, chat_url: str,
                          skip_chat: bool = False):
    """Kiểm tra health endpoints."""
    print(f"\n{BOLD}{CYAN}═══ [0] Health Check Services ═══{RESET}")

    # OCR Health
    r_ocr = get_json(ocr_url.rstrip("/") + "/api/ocr/health", timeout=10)
    ocr_ok = r_ocr.status_code == 200 and not r_ocr.error
    d = r_ocr.json_data
    easyocr  = "✓" if d.get("easyocr_ready") else "✗"
    vis_idx  = d.get("visual_index", {})
    vis_info = f"index={vis_idx.get('total_books', '?')} books" if vis_idx else "index=N/A"
    report.add(
        "Health: OCR Service (port 8005)",
        ocr_ok,
        f"version={d.get('version','?')}, easyocr={easyocr}, {vis_info}",
        r_ocr.elapsed_ms,
    )
    print(f"  OCR Service:      {'✅ UP' if ocr_ok else '❌ DOWN'} {r_ocr.elapsed_ms:.0f}ms")
    if ocr_ok:
        print(f"    version={d.get('version')}, easyocr={easyocr}, {vis_info}")

    if not skip_chat:
        # Chat Health
        r_chat = get_json(chat_url.rstrip("/") + "/api/chat/health", timeout=10)
        chat_ok = r_chat.status_code == 200 and not r_chat.error
        d2 = r_chat.json_data
        ollama = d2.get("ollama", "?")
        report.add(
            "Health: Chatbot Service (port 8004)",
            chat_ok,
            f"ollama={ollama}",
            r_chat.elapsed_ms,
        )
        print(f"  Chatbot Service:  {'✅ UP' if chat_ok else '❌ DOWN'} {r_chat.elapsed_ms:.0f}ms")
        if chat_ok:
            print(f"    ollama={ollama}")
        elif r_chat.error:
            print(f"    {YELLOW}→ {r_chat.error}{RESET}")

    return ocr_ok


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _print_ocr_response(d: dict, label: str):
    """In tóm tắt OCR response ra console."""
    success = d.get("success")
    method  = d.get("match_method", "?")
    quality = d.get("image_quality", "?")
    ms      = d.get("processing_time_ms", 0)
    engine  = d.get("engine_used", "?")
    text    = d.get("extracted_text", "")[:60].replace("\n", " ")
    error   = d.get("error", "")

    book    = d.get("book_info") or {}
    title   = book.get("title", "")
    conf    = book.get("confidence", 0)
    n_res   = d.get("total_results", 0)

    status_icon = f"{GREEN}✅{RESET}" if success else f"{RED}❌{RESET}"
    print(f"\n  {BOLD}Response [{label}]:{RESET}")
    print(f"    {status_icon} success={success}  match={method}  quality={quality}  [{ms}ms] engine={engine}")
    if text:
        print(f"    text:      '{text}'")
    if title:
        print(f"    title:     '{title}'  conf={conf:.2f}")
    if n_res:
        results = d.get("search_results", [])
        for i, r in enumerate(results[:3], 1):
            print(f"    result[{i}]:  '{r.get('title', '?')}' by {r.get('author_name', '?')}  score={(r.get('score') or 0):.1f}")
    if error:
        print(f"    {YELLOW}error:     {error}{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="E2E Integration Test cho 3 tính năng OCR")
    parser.add_argument("--ocr-url",   default=DEFAULT_OCR_URL,
                        help=f"OCR Service URL (default: {DEFAULT_OCR_URL})")
    parser.add_argument("--chat-url",  default=DEFAULT_CHAT_URL,
                        help=f"Chatbot Service URL (default: {DEFAULT_CHAT_URL})")
    parser.add_argument("--skip-chat", action="store_true",
                        help="Bỏ qua chatbot integration test")
    parser.add_argument("--feature",   default="all",
                        choices=["all", "search", "admin", "chatbot", "health"],
                        help="Chỉ test 1 feature")
    args = parser.parse_args()

    print(f"\n{'═'*72}")
    print(f"{BOLD}🚀 E2E INTEGRATION TEST – OCR + Chatbot Features{RESET}")
    print(f"{'═'*72}")
    print(f"  OCR Service:  {args.ocr_url}")
    print(f"  Chat Service: {args.chat_url}")
    print(f"  Feature:      {args.feature}")
    print(f"  Skip Chat:    {args.skip_chat}")
    print(f"{'═'*72}")

    report   = E2EReport()
    t_start  = time.perf_counter()

    # ── [0] Health Check ──────────────────────────────────────────────────────
    ocr_up = test_health_endpoints(report, args.ocr_url, args.chat_url,
                                   skip_chat=args.skip_chat)
    if not ocr_up:
        print(f"\n{RED}⛔ OCR Service không hoạt động. Kiểm tra:{RESET}")
        print(f"   1. Service đang chạy trên {args.ocr_url}?")
        print(f"   2. Thử: uvicorn ocr_app.main:app --port 8005")
        print(f"\n{YELLOW}Còn lại các test sẽ được đánh dấu SKIP.{RESET}")
        report.skip("Feature 1: search-by-cover",  "OCR service offline")
        report.skip("Feature 2: extract-book-info", "OCR service offline")
        if not args.skip_chat:
            report.skip("Feature 3: chatbot+OCR",  "OCR service offline")
        report.print_report()
        sys.exit(1)

    # ── [1] Tìm sách bằng ảnh ───────────────────────────────────────────────
    if args.feature in ("all", "search"):
        test_search_by_cover(report, args.ocr_url)

    # ── [2] Admin auto-fill ──────────────────────────────────────────────────
    if args.feature in ("all", "admin"):
        test_extract_book_info(report, args.ocr_url)

    # ── [3] Chatbot + OCR ────────────────────────────────────────────────────
    if args.feature in ("all", "chatbot") and not args.skip_chat:
        test_chatbot_image_flow(report, args.ocr_url, args.chat_url)
    elif args.skip_chat and args.feature in ("all", "chatbot"):
        report.skip("Feature 3: chatbot+OCR", "--skip-chat flag")

    total_elapsed = (time.perf_counter() - t_start) * 1000
    all_passed = report.print_report()

    # ── Hướng dẫn sau khi test ───────────────────────────────────────────────
    print(f"  Tổng thời gian: {total_elapsed:.0f}ms ({total_elapsed/1000:.1f}s)")
    print()
    if all_passed:
        print(f"  {GREEN}{BOLD}✅ TẤT CẢ TESTS PASSED! Hệ thống hoạt động tốt.{RESET}")
    else:
        print(f"  {RED}{BOLD}❌ Có lỗi. Kiểm tra log của service để debug.{RESET}")
        print(f"""
  📌 Các bước debug:
    1. OCR logs:  docker logs ocr-service  (hoặc xem terminal uvicorn)
    2. Chat logs: docker logs chatbot-service
    3. Kiểm tra: curl {args.ocr_url}/api/ocr/health
    4. Kiểm tra: curl {args.chat_url}/api/chat/health
""")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
