"""
test_chatbot_e2e.py – Bộ E2E Test 10 Sets × 8 Câu cho Chatbot + OCR
=====================================================================

Cách chạy:
    pip install requests colorama
    python tests/test_chatbot_e2e.py

Yêu cầu:
    - Chatbot service đang chạy tại CHATBOT_URL (port 8003)
    - OCR service đang chạy tại OCR_URL (port 8005)
    - Ảnh test có tại IMAGES_DIR

Kết quả: In ra màn hình + lưu file tests/results/report_<timestamp>.txt
"""

import os
import sys
import json
import time
import uuid
import textwrap
import requests
from datetime import datetime
from colorama import Fore, Style, init

# Fix UnicodeEncodeError trên Windows (cp1252 không hỗ trợ emoji)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

init(autoreset=True)

# ══════════════════════════════════════════════════════════════════════════════
# CẤU HÌNH — chỉnh sửa nếu port/token khác
# ══════════════════════════════════════════════════════════════════════════════
CHATBOT_URL  = "http://localhost:8004"
OCR_URL      = "http://localhost:8005"
IMAGES_DIR   = r"D:\craw_demo\scraped_images"

# JWT tokens — thay bằng token thực từ Spring Boot login
STAFF_JWT  = os.environ.get("STAFF_JWT",  "YOUR_STAFF_JWT_TOKEN_HERE")
ADMIN_JWT  = os.environ.get("ADMIN_JWT",  "YOUR_ADMIN_JWT_TOKEN_HERE")

# User ID member đã đăng nhập (thay bằng ID thực trong DB)
MEMBER_USER_ID = int(os.environ.get("MEMBER_USER_ID", "1"))

TIMEOUT = 60   # giây — LLM có thể chậm
PASS_THRESHOLD = 6   # ≥6/8 để pass mỗi bộ

# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def new_session() -> str:
    """Tạo session_id mới duy nhất cho mỗi bộ test."""
    return f"e2e-{uuid.uuid4().hex[:12]}"


def chat(message: str, session_id: str, user_id: int = None,
         role: str = "customer", jwt_token: str = None) -> dict:
    """Gọi POST /api/chat/message hoặc /api/staff|admin/chat/message."""
    headers = {"Content-Type": "application/json"}
    if jwt_token:
        headers["Authorization"] = f"Bearer {jwt_token}"

    if role == "staff":
        url  = f"{CHATBOT_URL}/api/staff/chat/message"
    elif role == "admin":
        url  = f"{CHATBOT_URL}/api/admin/chat/message"
    else:
        url = f"{CHATBOT_URL}/api/chat/message"

    payload = {
        "message":    message,
        "session_id": session_id,
        "role":       role,
    }
    if user_id:
        payload["user_id"] = user_id

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        return {"error": str(e), "status_code": resp.status_code, "detail": resp.text[:300]}
    except Exception as e:
        return {"error": str(e)}


def ocr_upload(image_path: str, session_id: str) -> dict:
    """Upload ảnh trực tiếp lên OCR service, trả về kết quả JSON."""
    if not os.path.exists(image_path):
        return {"error": f"Image not found: {image_path}"}
    _OCR_ENDPOINTS = [
        f"{OCR_URL}/api/ocr/search-by-cover",
        f"{OCR_URL}/api/ocr/find-by-image",
        f"{OCR_URL}/api/ocr/search-by-image",
    ]
    for _endpoint in _OCR_ENDPOINTS:
        try:
            with open(image_path, "rb") as f:
                resp = requests.post(
                    _endpoint,
                    files={"file": (os.path.basename(image_path), f, "image/jpeg")},
                    data={"session_id": session_id},
                    timeout=TIMEOUT,
                )
            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
            return resp.json()
        except Exception as e:
            return {"error": str(e)}
    return {"error": f"OCR endpoint not found. Tried: {_OCR_ENDPOINTS}"}


def _extract_title_from_ocr(ocr_result: dict) -> str:
    """
    Trích xuất tên sách từ JSON của OCR service.
    Hỗ trợ nhiều cấu trúc response khác nhau:
      - /search-by-cover: {success, book_info: {title}, search_results: [{title}]}
      - legacy:           {best_match: {title}} | {results: [{title}]}
    """
    if not isinstance(ocr_result, dict):
        return ""
    # Cấu trúc mới (/search-by-cover)
    book_info = ocr_result.get("book_info") or {}
    title = book_info.get("title", "")
    if not title:
        # search_results là list sach khớp trong DB
        sr = ocr_result.get("search_results") or []
        if sr:
            title = sr[0].get("title", "") or sr[0].get("book_title", "")
    # Legacy keys
    if not title:
        title = (ocr_result.get("best_match") or {}).get("title", "")
    if not title:
        title = ocr_result.get("title", "") or ocr_result.get("book_name", "")
    if not title:
        results = ocr_result.get("results") or []
        if results:
            title = results[0].get("title", "")
    return title.strip()


def ocr_then_chat(image_path: str, question: str, session_id: str,
                  user_id: int = None) -> dict:
    """
    ưu tiên: gọi /api/chat/upload-image (chatbot xử lý toàn bộ pipeline OCR).
    Fallback: gọi OCR trực tiếp rồi inject tiêu đề vào chatbot message.
    """
    # Phương án 1: Gọi chatbot's upload-image endpoint (luồng chuẩn)
    _upload_url = f"{CHATBOT_URL}/api/chat/upload-image"
    try:
        with open(image_path, "rb") as f:
            data = {"session_id": session_id, "message": question, "role": "customer"}
            if user_id:
                data["user_id"] = str(user_id)
            resp = requests.post(
                _upload_url,
                files={"file": (os.path.basename(image_path), f, "image/jpeg")},
                data=data,
                timeout=TIMEOUT,
            )
        if resp.status_code == 200:
            # /upload-image trả SSE stream, cần parse token events
            raw = resp.text  # text/event-stream
            _answer = ""
            _btns   = []
            for line in raw.splitlines():
                if not line.startswith("data:"):
                    continue
                try:
                    payload = json.loads(line[5:].strip())
                    if payload.get("type") == "token":
                        _answer = payload.get("content", "")
                    elif payload.get("type") == "done":
                        _btns = payload.get("btns", [])
                except Exception:
                    pass
            if _answer and _answer != "🔍 Đang nhận dạng ảnh bìa sách...":
                return {"answer": _answer, "navigate_buttons": _btns}
    except Exception:
        pass  # Fallback về phương án 2

    # Phương án 2: Gọi OCR service trực tiếp rồi inject têm sách vào chatbot
    ocr_result = ocr_upload(image_path, session_id)
    if "error" in ocr_result:
        return ocr_result
    title = _extract_title_from_ocr(ocr_result)
    ocr_message = f"[Ảnh OCR: {title}] {question}" if title else question
    return chat(ocr_message, session_id, user_id=user_id)


def img(filename: str) -> str:
    """Trả về path đầy đủ của ảnh test."""
    return os.path.join(IMAGES_DIR, filename)


# ══════════════════════════════════════════════════════════════════════════════
# KẾT QUẢ & REPORT
# ══════════════════════════════════════════════════════════════════════════════

class TestResult:
    def __init__(self, set_id: int, set_name: str, role: str):
        self.set_id   = set_id
        self.set_name = set_name
        self.role     = role
        self.turns: list[dict] = []
        self.score    = 0
        self.total    = 0
        self.start_ts = time.time()

    def add_turn(self, q: str, expected_intent: str, expected_keywords: list[str],
                 response: dict, manual_note: str = ""):
        self.total += 1
        # Dùng 'or' chain thay vì .get() với default: tránh trường hợp
        # API trả {"answer": null} —— dict.get(key, default) không dùng default
        # khi key THỰC SỰ tồn tại nhưng có giá trị None
        raw_answer = response.get("answer") or response.get("error") or "[NO RESPONSE]"
        answer   = str(raw_answer)  # đảm bảo luôn là string
        intent   = (response.get("debug") or {}).get("intent", "")
        latency  = response.get("latency_ms", 0)
        has_err  = ("error" in response) or (response.get("answer") is None
                                              and not response.get("error"))

        # Auto-score: kiểm tra keyword xuất hiện trong response
        kw_match  = sum(1 for kw in expected_keywords if kw.lower() in answer.lower())
        auto_pass = (kw_match >= max(1, len(expected_keywords) // 2)) and not has_err
        auto_score = 1 if auto_pass else 0.5 if (kw_match > 0 and not has_err) else 0
        self.score += auto_score

        self.turns.append({
            "q":               q,
            "expected_intent": expected_intent,
            "actual_intent":   intent,
            "expected_kw":     expected_keywords,
            "kw_match":        kw_match,
            "answer":          answer[:200] + ("…" if len(answer) > 200 else ""),
            "latency_ms":      latency,
            "auto_score":      auto_score,
            "has_error":       has_err,
            "note":            manual_note,
        })
        return auto_score, answer

    def passed(self) -> bool:
        return self.score >= PASS_THRESHOLD

    def elapsed(self) -> float:
        return round(time.time() - self.start_ts, 1)


# ══════════════════════════════════════════════════════════════════════════════
# PRINT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def print_header(text: str):
    print(f"\n{Fore.CYAN}{'═'*70}")
    print(f"  {text}")
    print(f"{'═'*70}{Style.RESET_ALL}")


def print_turn(idx: int, result: TestResult, score: float, answer: str, q: str):
    icon  = "✅" if score >= 1 else "⚠️" if score >= 0.5 else "❌"
    color = Fore.GREEN if score >= 1 else Fore.YELLOW if score >= 0.5 else Fore.RED
    q_short = textwrap.shorten(q, 60)
    a_short = textwrap.shorten(answer, 80)
    print(f"  {color}{icon} [{idx}] {q_short}")
    print(f"       ↳ {a_short}{Style.RESET_ALL}")


def print_set_summary(result: TestResult):
    pct   = result.score / result.total * 100 if result.total else 0
    color = Fore.GREEN if result.passed() else Fore.RED
    print(f"\n  {color}📊 Bộ {result.set_id:02d} ({result.set_name}): "
          f"{result.score:.1f}/{result.total} = {pct:.0f}% "
          f"{'✅ PASS' if result.passed() else '❌ FAIL'} "
          f"({result.elapsed()}s){Style.RESET_ALL}")


# ══════════════════════════════════════════════════════════════════════════════
# 10 BỘ TEST CASES
# ══════════════════════════════════════════════════════════════════════════════

def run_set_01() -> TestResult:
    """Bộ 01 — Guest: Tìm sách + Follow-up + Coref"""
    r = TestResult(1, "Guest: Tìm sách + Coref", "guest")
    sid = new_session()
    print_header("BỘ 01 — Guest: Tìm sách + Follow-up + Coref (8 turns)")

    pairs = [
        ("Xin chào, tôi muốn tìm sách đắc nhân tâm",
         "book_search", ["Đắc Nhân Tâm", "đ", "tìm thấy"]),
        ("Cuốn đó giá bao nhiêu vậy",
         "book_detail", ["đ", "giá"]),
        ("Còn hàng không",
         "book_availability", ["hàng", "còn", "tồn kho"]),
        ("Cho tôi xem thêm sách kỹ năng sống",
         "recommend_category", ["kỹ năng", "sách"]),
        ("Cuốn nào rẻ nhất trong mấy cuốn vừa gợi ý",
         "book_compare", ["rẻ nhất", "đ"]),
        ("Tôi muốn mua cuốn rẻ nhất đó",
         "cart_help", ["giỏ hàng", "nhấn", "trang sản phẩm"]),
        ("Chính sách đổi trả thế nào",
         "return_policy", ["7 ngày", "đổi", "trả", "nguyên"]),
        ("Tạm biệt",
         "farewell", ["cảm ơn", "hẹn", "vui vẻ"]),
    ]

    for i, (q, intent, kws) in enumerate(pairs, 1):
        resp = chat(q, sid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_02() -> TestResult:
    """Bộ 02 — Guest: OCR 2 ảnh + Ghi nhớ + So sánh"""
    r = TestResult(2, "Guest: OCR 2 ảnh + So sánh", "guest")
    sid = new_session()
    print_header("BỘ 02 — Guest: OCR 2 ảnh + Ghi nhớ (8 turns)")

    img1 = img("1-cm-giua-anh-va-em.jpg")
    img2 = img("10-hanh-phuc-hon-10-happier.jpg")

    # Turn 1: OCR ảnh 1
    resp = ocr_then_chat(img1, "Đây là sách gì vậy?", sid)
    score, answer = r.add_turn(f"[IMG: {os.path.basename(img1)}] Đây là sách gì vậy?",
                               "image_search", ["sách", "đ", "tìm thấy"], resp)
    print_turn(1, r, score, answer, f"[OCR] {os.path.basename(img1)}")
    time.sleep(1)

    # Turn 2: Coref giá
    resp = chat("Giá bao nhiêu", sid)
    score, answer = r.add_turn("Giá bao nhiêu", "book_detail", ["đ", "giá"], resp)
    print_turn(2, r, score, answer, "Giá bao nhiêu")
    time.sleep(0.5)

    # Turn 3: OCR ảnh 2
    resp = ocr_then_chat(img2, "Còn cuốn này nữa", sid)
    score, answer = r.add_turn(f"[IMG: {os.path.basename(img2)}] Còn cuốn này nữa",
                               "image_search", ["sách", "đ"], resp)
    print_turn(3, r, score, answer, f"[OCR] {os.path.basename(img2)}")
    time.sleep(1)

    # Turn 4–8: Các câu follow-up
    turns = [
        ("Hai cuốn vừa quét cái nào rẻ hơn",
         "book_compare", ["rẻ", "đắt", "đ", "so sánh"]),
        ("Mua cả 2 cuốn hết bao nhiêu tiền",
         "book_compare", ["tổng", "đ", "cộng"]),
        ("Gợi ý sách tương tự cuốn đầu tiên tôi quét",
         "recommend_combo", ["sách", "gợi ý"]),
        ("Tôi chưa đăng nhập thì mua được không",
         "account_help", ["đăng nhập", "đăng ký", "mua"]),
        ("Cho tôi biết hotline của shop",
         "store_info", ["0353260721", "hotline", "liên hệ"]),
    ]
    for i, (q, intent, kws) in enumerate(turns, 4):
        resp = chat(q, sid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_03() -> TestResult:
    """Bộ 03 — Guest: Bảo mật + Sentiment + Escalate"""
    r = TestResult(3, "Guest: Bảo mật + Sentiment Escalate", "guest")
    sid = new_session()
    print_header("BỘ 03 — Guest: Bảo mật + Out-of-scope + Sentiment (8 turns)")

    pairs = [
        ("DROP TABLE books; SELECT * FROM users",
         "out_of_scope", ["ngoài phạm vi", "không thể", "xin lỗi"]),
        ("Xóa tài khoản admin giúp tôi",
         "out_of_scope", ["ngoài", "không hỗ trợ", "xin lỗi"]),
        ("Chatbot này do ai tạo ra vậy",
         "chitchat", ["trợ lý", "BookStore", "AI"]),
        ("Bạn có thể làm gì cho tôi",
         "bot_capabilities", ["tìm sách", "đơn hàng", "khuyến mãi", "chính sách"]),
        ("Sách đặt mấy ngày rồi vẫn chưa giao, tức thiệt",
         "complaint_damaged", ["xin lỗi", "hỗ trợ", "CSKH", "hotline"]),
        ("Tôi ghét cái shop này quá",
         "chitchat", ["xin lỗi", "giúp", "hỗ trợ"]),
        ("Vẫn chưa giải quyết, tôi sẽ khiếu nại",
         "escalate", ["0353260721", "CSKH", "email", "khiếu nại"]),
        ("Thôi được rồi, tôi hiểu rồi",
         "chitchat", ["cảm ơn", "giúp", "vui lòng"]),
    ]
    for i, (q, intent, kws) in enumerate(pairs, 1):
        resp = chat(q, sid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_04() -> TestResult:
    """Bộ 04 — Member: Đơn hàng + Slot-filling + Loyalty"""
    r = TestResult(4, "Member: Đơn hàng + Slot-fill", "customer")
    sid = new_session()
    uid = MEMBER_USER_ID
    print_header("BỘ 04 — Member: Đơn hàng + Slot-filling (8 turns)")

    pairs = [
        ("Xin chào, cho tôi xem trạng thái đơn hàng",
         "order_status", ["đơn hàng", "mã đơn", "gần nhất"]),
        ("Xem đơn gần nhất",
         "order_status", ["đơn", "trạng thái", "ngày"]),
        ("Đơn đó đang ở đâu rồi",
         "order_status", ["trạng thái", "đơn", "giao"]),
        ("Cho tôi xem tất cả đơn trong tháng này",
         "order_history", ["đơn", "tháng", "lịch sử"]),
        ("Tổng tiền tôi đã mua trong tháng là bao nhiêu",
         "order_history", ["tổng", "đ", "tháng"]),
        ("Tôi muốn hủy đơn gần nhất",
         "order_cancel", ["xác nhận", "hủy", "đơn"]),
        ("Có",
         "confirmation_yes", ["hủy", "đã", "xác nhận"]),
        ("Điểm tích lũy của tôi là bao nhiêu",
         "loyalty_points", ["điểm", "tích lũy", "thành viên"]),
    ]
    for i, (q, intent, kws) in enumerate(pairs, 1):
        resp = chat(q, sid, user_id=uid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_05() -> TestResult:
    """Bộ 05 — Member: Gợi ý cá nhân + Ordinal + Cart guard"""
    r = TestResult(5, "Member: Gợi ý cá nhân + Ordinal ref", "customer")
    sid = new_session()
    uid = MEMBER_USER_ID
    print_header("BỘ 05 — Member: Gợi ý cá nhân + Ordinal reference (8 turns)")

    pairs = [
        ("Gợi ý sách phù hợp với tôi",
         "recommend_personal", ["sách", "gợi ý", "bạn"]),
        ("Cuốn thứ 2 trong danh sách đó thông tin thế nào",
         "book_detail", ["sách", "tác giả", "giá"]),
        ("Tác giả cuốn đó là ai",
         "book_detail", ["tác giả"]),
        ("Cho tôi xem sách cùng tác giả",
         "recommend_combo", ["sách", "tác giả", "cùng"]),
        ("Thanh toán bằng ví điện tử được không",
         "payment_method", ["thanh toán", "ví", "MoMo", "ZaloPay"]),
        ("Có mã giảm giá không",
         "promotion_current", ["khuyến mãi", "giảm giá", "mã"]),
        ("Nhập mã BOOK20 được giảm bao nhiêu",
         "voucher_apply", ["mã", "BOOK20", "giảm"]),
        ("Thêm cuốn thứ 2 vào giỏ giúp tôi",
         "cart_help", ["không thể", "giỏ hàng", "nhấn", "trang"]),
    ]
    for i, (q, intent, kws) in enumerate(pairs, 1):
        resp = chat(q, sid, user_id=uid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_06() -> TestResult:
    """Bộ 06 — Member: OCR 3 ảnh + Tổng tiền + Shipping"""
    r = TestResult(6, "Member: OCR 3 ảnh + Tính tổng", "customer")
    sid = new_session()
    uid = MEMBER_USER_ID
    print_header("BỘ 06 — Member: OCR 3 ảnh liên tiếp + Tổng tiền (8 turns)")

    imgs = [
        ("10-buoc-den-thanh-cong.jpg", "Quét ảnh này"),
        ("1-ngay-bang-48-gio.jpg",     "Còn cuốn này"),
        ("10-hanh-phuc-hon-10-happier.jpg", "Và cuốn này nữa"),
    ]

    for i, (fn, q) in enumerate(imgs, 1):
        resp = ocr_then_chat(img(fn), q, sid, user_id=uid)
        score, answer = r.add_turn(f"[IMG] {fn}: {q}",
                                   "image_search", ["sách", "đ", "tìm thấy"], resp)
        print_turn(i, r, score, answer, f"[OCR] {fn}")
        time.sleep(1)

    # Turns 4–8
    turns = [
        ("3 cuốn vừa quét tổng tiền hết bao nhiêu",
         "book_compare", ["tổng", "đ", "cộng", "cuốn"]),
        ("Cuốn nào đắt nhất",
         "book_compare", ["đắt nhất", "đ"]),
        ("Gợi ý sách tương tự cuốn đầu tiên tôi quét hôm nay",
         "recommend_combo", ["sách", "gợi ý"]),
        ("Mua cuốn rẻ nhất đó",
         "cart_help", ["nhấn", "trang sản phẩm", "giỏ hàng"]),
        ("Sau khi mua, bao nhiêu ngày thì nhận được hàng",
         "store_info", ["ngày", "giao hàng", "vận chuyển"]),
    ]
    for i, (q, intent, kws) in enumerate(turns, 4):
        resp = chat(q, sid, user_id=uid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_07() -> TestResult:
    """Bộ 07 — Staff: Tra kho + Báo cáo + Security block"""
    r = TestResult(7, "Staff: Tra kho + Admin block", "staff")
    sid = new_session()
    print_header("BỘ 07 — Staff: Tra kho + Báo cáo + Security (8 turns)")

    if STAFF_JWT == "YOUR_STAFF_JWT_TOKEN_HERE":
        print(f"  {Fore.YELLOW}⚠️  STAFF_JWT chưa cấu hình → Skip bộ 07{Style.RESET_ALL}")
        # Dùng turn dict đầy đủ keys để tránh KeyError trong save_report
        _skip_turn = {"q": "[SKIPPED]", "expected_intent": "", "actual_intent": "",
                      "expected_kw": [], "kw_match": 0, "answer": "[JWT not configured]",
                      "latency_ms": 0, "auto_score": 0, "has_error": True, "note": "skipped"}
        r.turns = [_skip_turn] * 8
        r.total = 0   # Không tính vào tổng
        return r

    pairs = [
        ("Xin chào, hôm nay có bao nhiêu đơn hàng chờ xử lý",
         "staff_order_list_pending", ["đơn", "chờ", "pending"]),
        ("Sách Đắc Nhân Tâm còn bao nhiêu cuốn trong kho",
         "book_availability", ["tồn kho", "cuốn", "Đắc Nhân Tâm"]),
        ("Khách hàng abc@gmail.com đặt đơn nào gần nhất",
         "order_history", ["đơn", "khách"]),
        ("Cập nhật tồn kho sách Python lên 50",
         "out_of_scope", ["không thể", "admin", "quyền"]),
        ("Xóa tài khoản khách này đi",
         "out_of_scope", ["không thể", "xin lỗi"]),
        ("Khách phàn nàn nhận được sách bị rách trang",
         "complaint_damaged", ["xin lỗi", "CSKH", "hỗ trợ", "hotline"]),
        ("Cho tôi xem top 5 sách bán chạy nhất tháng",
         "recommend_trending", ["sách", "bán chạy", "top"]),
        ("Tôi cần thêm vào đơn hàng #12345 một cuốn sách",
         "out_of_scope", ["không thể", "sửa đơn", "tạo đơn mới"]),
    ]
    for i, (q, intent, kws) in enumerate(pairs, 1):
        resp = chat(q, sid, role="staff", jwt_token=STAFF_JWT)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_08() -> TestResult:
    """Bộ 08 — Staff: OCR kiểm kho"""
    r = TestResult(8, "Staff: OCR kiểm kho", "staff")
    sid = new_session()
    print_header("BỘ 08 — Staff: OCR kiểm tra hàng nhập kho (8 turns)")

    if STAFF_JWT == "YOUR_STAFF_JWT_TOKEN_HERE":
        print(f"  {Fore.YELLOW}⚠️  STAFF_JWT chưa cấu hình → Skip bộ 08{Style.RESET_ALL}")
        _skip_turn = {"q": "[SKIPPED]", "expected_intent": "", "actual_intent": "",
                      "expected_kw": [], "kw_match": 0, "answer": "[JWT not configured]",
                      "latency_ms": 0, "auto_score": 0, "has_error": True, "note": "skipped"}
        r.turns = [_skip_turn] * 8
        r.total = 0
        return r

    img1 = img("1-phut-noi-tieng-anh-nhu-gio.jpg")
    img2 = img("1-2-4-15-6565-6612.jpg")

    # OCR turn 1
    resp = ocr_then_chat(img1, "Cuốn này trong hệ thống chưa", sid)
    score, answer = r.add_turn(f"[IMG] {os.path.basename(img1)}",
                               "image_search", ["sách", "hệ thống", "đ"], resp)
    print_turn(1, r, score, answer, f"[OCR] {os.path.basename(img1)}")
    time.sleep(1)

    staff_turns = [
        ("Giá nhập và giá bán hiện tại là bao nhiêu", "book_detail", ["đ", "giá"]),
        ("Số lượng tồn kho hiện tại", "book_availability", ["tồn kho", "cuốn"]),
    ]
    for i, (q, intent, kws) in enumerate(staff_turns, 2):
        resp = chat(q, sid, role="staff", jwt_token=STAFF_JWT)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    # OCR turn 4
    resp = ocr_then_chat(img2, "Và cuốn này", sid)
    score, answer = r.add_turn(f"[IMG] {os.path.basename(img2)} Và cuốn này",
                               "image_search", ["sách", "đ"], resp)
    print_turn(4, r, score, answer, f"[OCR] {os.path.basename(img2)}")
    time.sleep(1)

    end_turns = [
        ("2 cuốn vừa check, cuốn nào bán chạy hơn",
         "book_compare", ["so sánh", "rating", "bán chạy"]),
        ("Khách hàng thường mua 2 cuốn này cùng với gì",
         "recommend_combo", ["sách", "cùng", "gợi ý"]),
        ("Xuất báo cáo kho",
         "out_of_scope", ["không thể", "admin", "dashboard"]),
        ("Phiên làm việc hôm nay tôi đã check bao nhiêu sách",
         "chitchat", ["2", "sách", "kiểm tra"]),
    ]
    for i, (q, intent, kws) in enumerate(end_turns, 5):
        resp = chat(q, sid, role="staff", jwt_token=STAFF_JWT)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_09() -> TestResult:
    """Bộ 09 — Admin: Analytics + Reload KB"""
    r = TestResult(9, "Admin: Analytics + Reload KB", "admin")
    sid = new_session()
    print_header("BỘ 09 — Admin: Analytics + Quản trị (8 turns)")

    if ADMIN_JWT == "YOUR_ADMIN_JWT_TOKEN_HERE":
        print(f"  {Fore.YELLOW}⚠️  ADMIN_JWT chưa cấu hình → Skip bộ 09{Style.RESET_ALL}")
        _skip_turn = {"q": "[SKIPPED]", "expected_intent": "", "actual_intent": "",
                      "expected_kw": [], "kw_match": 0, "answer": "[JWT not configured]",
                      "latency_ms": 0, "auto_score": 0, "has_error": True, "note": "skipped"}
        r.turns = [_skip_turn] * 8
        r.total = 0
        return r

    pairs = [
        ("Xin chào, thống kê chatbot hôm nay thế nào",
         "admin_dashboard", ["session", "tin nhắn", "hôm nay"]),
        ("Có bao nhiêu khách đang chat với bot",
         "admin_user_stats", ["session", "active", "đang"]),
        ("Top 5 câu hỏi phổ biến nhất của khách",
         "admin_top_books", ["intent", "câu hỏi", "phổ biến"]),
        ("Reload knowledge base",
         "admin_reload_kb", ["reload", "KB", "thành công", "đang"]),
        ("Xem session của user có ID 42",
         "admin_dashboard", ["session", "user", "42"]),
        ("Intent nào đang bị trả lời sai nhiều nhất",
         "admin_dashboard", ["intent", "confidence", "thấp"]),
        ("Blacklist IP 192.168.1.100",
         "out_of_scope", ["không thể", "không hỗ trợ", "tính năng"]),
        ("Xuất log chatbot hôm nay ra file",
         "out_of_scope", ["không thể", "Docker", "dashboard", "log"]),
    ]
    for i, (q, intent, kws) in enumerate(pairs, 1):
        resp = chat(q, sid, role="admin", jwt_token=ADMIN_JWT)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


def run_set_10() -> TestResult:
    """Bộ 10 — Stress Test: Gift rec + OCR + Coref phức tạp (8 turns)"""
    r = TestResult(10, "Stress Test: Gift + OCR + Coref chuỗi", "customer")
    sid = new_session()
    uid = MEMBER_USER_ID
    print_header("BỘ 10 — Stress Test: Gift rec + OCR + Coref 8 turns liên tiếp")

    img_path = img("10-dieu-ran-lanh-dao-toi-uu-nhat-the-gioi.jpg")

    turns_text = [
        ("Tôi cần quà tặng cho bạn gái, cô ấy thích sách văn học lãng mạn",
         "recommend_gift", ["sách", "quà", "gợi ý", "lãng mạn"]),
        ("Ngân sách khoảng 200 ngàn thôi",
         "recommend_gift", ["200", "sách", "trong tầm"]),
        ("Cuốn thứ nhất bạn gợi ý có đánh giá mấy sao",
         "book_detail", ["sao", "đánh giá", "rating"]),
    ]
    for i, (q, intent, kws) in enumerate(turns_text, 1):
        resp = chat(q, sid, user_id=uid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    # Turn 4: OCR
    resp = ocr_then_chat(img_path, "Bạn gái tôi đang đọc cuốn này, gợi ý sách đọc kèm",
                         sid, user_id=uid)
    score, answer = r.add_turn(f"[IMG] {os.path.basename(img_path)}",
                               "image_search+recommend", ["gợi ý", "sách", "cùng chủ đề"], resp)
    print_turn(4, r, score, answer, "[OCR + gợi ý đọc kèm]")
    time.sleep(1)

    final_turns = [
        ("So sánh cuốn trong ảnh với cuốn đầu tiên bạn gợi ý lúc nãy",
         "book_compare", ["so sánh", "đ", "cuốn"]),
        ("Mua cả hai tặng cô ấy, ship về Đà Nẵng mất mấy ngày",
         "store_info", ["ngày", "Đà Nẵng", "3-5", "giao hàng"]),
        ("Nếu cô ấy không thích thì đổi được không",
         "return_policy", ["7 ngày", "đổi", "nguyên trạng"]),
        ("Tổng ngân sách tôi cần chi cho 2 cuốn là bao nhiêu",
         "book_compare", ["tổng", "đ", "2 cuốn"]),
    ]
    for i, (q, intent, kws) in enumerate(final_turns, 5):
        resp = chat(q, sid, user_id=uid)
        score, answer = r.add_turn(q, intent, kws, resp)
        print_turn(i, r, score, answer, q)
        time.sleep(0.5)

    print_set_summary(r)
    return r


# ══════════════════════════════════════════════════════════════════════════════
# BÁO CÁO CUỐI — Xuất .txt (đọc được) + .json (dữ liệu đầy đủ)
# ══════════════════════════════════════════════════════════════════════════════

# Luôn dùng đường dẫn tuyệt đối relative với file script → tránh save lạc chỗ
_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_RESULTS_DIR = os.path.join(_SCRIPT_DIR, "results")


def save_report(results: list["TestResult"]) -> tuple[str, str]:
    """Lưu báo cáo dạng .txt và .json, trả về (txt_path, json_path)."""
    os.makedirs(_RESULTS_DIR, exist_ok=True)
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path  = os.path.join(_RESULTS_DIR, f"report_{ts}.txt")
    json_path = os.path.join(_RESULTS_DIR, f"report_{ts}.json")

    # ── TXT ──────────────────────────────────────────────────────────────────
    total_score = total_max = 0
    lines = [
        "=" * 72,
        "  BÁO CÁO E2E TEST — BookStore Chatbot + OCR",
        f"  Ngày: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        f"  Chatbot: {CHATBOT_URL}  |  OCR: {OCR_URL}",
        "=" * 72, "",
    ]
    json_data = {
        "meta": {
            "generated_at": datetime.now().isoformat(),
            "chatbot_url":  CHATBOT_URL,
            "ocr_url":      OCR_URL,
        },
        "sets":    [],
        "summary": {},
    }

    for r in results:
        pct_r  = (r.score / r.total * 100) if r.total else 0
        status = "SKIPPED" if r.total == 0 else ("PASS" if r.passed() else "FAIL")

        # --- JSON record cho set này ---
        json_set = {
            "set_id":    r.set_id,
            "set_name":  r.set_name,
            "role":      r.role,
            "score":     r.score,
            "total":     r.total,
            "pct":       round(pct_r, 1),
            "status":    status,
            "elapsed_s": r.elapsed(),
            "turns":     [],
        }
        for i, t in enumerate(r.turns, 1):
            json_set["turns"].append({
                "turn": i,
                "question":        t.get("q", "[N/A]"),
                "expected_intent": t.get("expected_intent", ""),
                "actual_intent":   t.get("actual_intent",   ""),
                "expected_kw":     t.get("expected_kw",     []),
                "kw_match":        t.get("kw_match",        0),
                "auto_score":      t.get("auto_score",      0),
                "has_error":       t.get("has_error",       False),
                "answer":          t.get("answer",          ""),
                "latency_ms":      t.get("latency_ms",      0),
                "note":            t.get("note",            ""),
            })
        json_data["sets"].append(json_set)

        # --- TXT record cho set này ---
        if r.total == 0:
            lines.append(f"Bộ {r.set_id:02d} [{r.role.upper():8s}] {r.set_name}  ⏭  SKIPPED")
            lines.append("")
            continue

        icon_set = "✅" if r.passed() else "❌"
        lines.append(f"{'─'*72}")
        lines.append(f"Bộ {r.set_id:02d} [{r.role.upper():8s}] {r.set_name}")
        lines.append(f"  Điểm: {r.score:.1f}/{r.total} = {pct_r:.0f}%  {icon_set} {status}  ({r.elapsed():.1f}s)")
        lines.append("")
        for i, t in enumerate(r.turns, 1):
            sc  = t.get("auto_score", 0)
            ico = "✅" if sc >= 1 else "⚠" if sc >= 0.5 else "❌"
            q   = t.get("q",      "[N/A]")[:70]
            a   = t.get("answer", "[N/A]")[:120]
            lines.append(f"  {ico} [{i:02d}] Q: {q}")
            lines.append(f"         A: {a}")
            if t.get("note"):
                lines.append(f"         📝 {t['note']}")
        lines.append("")
        total_score += r.score
        total_max   += r.total

    pct_total = total_score / total_max * 100 if total_max else 0
    overall   = "✅ OVERALL PASS (≥75%)" if pct_total >= 75 else "❌ OVERALL FAIL (<75%)"
    lines += [
        "=" * 72,
        f"  TỔNG KẾT: {total_score:.1f}/{total_max} = {pct_total:.1f}%",
        f"  {overall}",
        "=" * 72,
    ]
    json_data["summary"] = {
        "total_score":  total_score,
        "total_max":    total_max,
        "pct":          round(pct_total, 1),
        "overall_pass": pct_total >= 75,
        "pass_threshold_per_set": PASS_THRESHOLD,
    }

    # Ghi file TXT (UTF-8)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Ghi file JSON (UTF-8, không escape unicode)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    print(f"\n{Fore.CYAN}📄 TXT : {txt_path}")
    print(f"📊 JSON: {json_path}{Style.RESET_ALL}")
    return txt_path, json_path


def print_final_summary(results: list[TestResult]):
    print(f"\n{Fore.CYAN}{'═'*70}")
    print("  📊 TỔNG KẾT TOÀN BỘ TEST")
    print(f"{'═'*70}{Style.RESET_ALL}")
    total_score = total_max = 0
    for r in results:
        if r.total == 0:  # Bộ bị skip
            print(f"  {Fore.YELLOW}Bộ {r.set_id:02d} | {r.set_name:40s} | ⏭️  SKIPPED{Style.RESET_ALL}")
            continue
        pct   = r.score / r.total * 100
        color = Fore.GREEN if r.passed() else Fore.RED
        print(f"  {color}Bộ {r.set_id:02d} | {r.set_name:40s} | "
              f"{r.score:.1f}/{r.total} = {pct:.0f}% | "
              f"{'PASS' if r.passed() else 'FAIL'}{Style.RESET_ALL}")
        total_score += r.score
        total_max   += r.total

    pct = total_score / total_max * 100 if total_max else 0
    color = Fore.GREEN if pct >= 75 else Fore.RED
    print(f"\n  {color}{'═'*58}")
    print(f"  TỔNG: {total_score:.1f}/{total_max} = {pct:.1f}% — "
          f"{'✅ OVERALL PASS' if pct >= 75 else '❌ OVERALL FAIL'}")
    print(f"  {'═'*58}{Style.RESET_ALL}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

ALL_SETS = [
    run_set_01,  # Guest: Tìm sách + Coref
    run_set_02,  # Guest: OCR 2 ảnh
    run_set_03,  # Guest: Bảo mật + Sentiment
    run_set_04,  # Member: Đơn hàng
    run_set_05,  # Member: Gợi ý + Ordinal
    run_set_06,  # Member: OCR 3 ảnh
    run_set_07,  # Staff: Tra kho
    run_set_08,  # Staff: OCR kiểm kho
    run_set_09,  # Admin: Analytics
    run_set_10,  # Stress Test
]


def main():
    # Parse args: python test_chatbot_e2e.py [set_numbers...]
    # Ví dụ: python test_chatbot_e2e.py 1 2 3  → chỉ chạy bộ 1, 2, 3
    args = [int(a) for a in sys.argv[1:] if a.isdigit()]
    sets_to_run = [ALL_SETS[i-1] for i in args if 1 <= i <= 10] if args else ALL_SETS

    print(f"\n{Fore.CYAN}🤖 BookStore Chatbot + OCR — E2E Test Suite")
    print(f"   Chạy {len(sets_to_run)}/10 bộ | {len(sets_to_run)*8} turns tổng cộng")
    print(f"   Chatbot: {CHATBOT_URL} | OCR: {OCR_URL}")
    print(f"   Ảnh:     {IMAGES_DIR}{Style.RESET_ALL}\n")

    # Health check — thử nhiều endpoint khác nhau
    _health_endpoints = ["/health", "/api/health", "/api/chat/health", "/"]
    _chatbot_alive = False
    for _ep in _health_endpoints:
        try:
            hc = requests.get(f"{CHATBOT_URL}{_ep}", timeout=5)
            if hc.status_code < 500:  # 200, 404 đều OK — có server đang chạy
                print(f"  ✅ Chatbot alive ({_ep}): HTTP {hc.status_code}")
                _chatbot_alive = True
                break
        except Exception:
            continue
    if not _chatbot_alive:
        print(f"  {Fore.RED}❌ Chatbot unreachable tại {CHATBOT_URL}{Style.RESET_ALL}")
        print("  → Kiểm tra Docker: docker compose ps | grep chatbot")
        sys.exit(1)

    try:
        hc2 = requests.get(f"{OCR_URL}/api/ocr/health", timeout=5)
        print(f"  ✅ OCR health: {hc2.status_code}")
    except Exception as e:
        print(f"  {Fore.YELLOW}⚠️  OCR unreachable: {e} (Bộ OCR sẽ bị skip){Style.RESET_ALL}")

    results = []
    for fn in sets_to_run:
        result = fn()
        results.append(result)
        time.sleep(1)  # nghỉ giữa các bộ

    print_final_summary(results)
    save_report(results)


if __name__ == "__main__":
    main()
