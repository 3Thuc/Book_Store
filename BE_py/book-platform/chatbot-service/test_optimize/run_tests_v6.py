# -*- coding: utf-8 -*-
"""
run_tests_v6.py – Bộ test E2E TOÀN DIỆN cho Chatbot + OCR
============================================================
V6: 10 bộ test (mỗi bộ 8 câu liên tục) bao phủ toàn bộ luồng
Guest, Member, Staff, Admin. Sử dụng format của V5.

Chạy:
  python run_tests_v6.py              -- toàn bộ 10 suites
  python run_tests_v6.py S01          -- chạy cụ thể suite S01
"""
import json, time, uuid, sys, re, unicodedata
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

# ─── Cấu hình ──────────────────────────────────────────────────────────────────
CUSTOMER_STREAM_URL = "http://localhost:8004/api/chat/stream"
STAFF_MSG_URL       = "http://localhost:8004/api/staff/chat/message"
ADMIN_MSG_URL       = "http://localhost:8004/api/admin/chat/message"
UPLOAD_URL          = "http://localhost:8004/api/chat/upload-image"

IMG_BASE = Path(r"D:\craw_demo\scraped_images")
OUT_DIR  = Path(r"results")
OUT_DIR.mkdir(parents=True, exist_ok=True)

V = "V6"
TXT_PATH  = OUT_DIR / f"test_results_{V}.txt"
JSON_PATH = OUT_DIR / f"test_results_{V}.json"
_LOG_FILE = None


# ─── Logging ───────────────────────────────────────────────────────────────────
def _open_log():
    global _LOG_FILE
    _LOG_FILE = open(TXT_PATH, "w", encoding="utf-8", buffering=1)

def log(*args, end="\n"):
    text = " ".join(str(a) for a in args) + end
    sys.stdout.write(text); sys.stdout.flush()
    if _LOG_FILE:
        _LOG_FILE.write(text); _LOG_FILE.flush()


# ─── Helpers ───────────────────────────────────────────────────────────────────
def _norm(s: str) -> str:
    """Normalize: lowercase + strip Vietnamese diacritics (đ→d)."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower().replace("đ", "d"))
        if unicodedata.category(c) != "Mn"
    )

def _check_kw(answer: str, keywords: list) -> tuple[bool, list, list]:
    """(all_pass, passed_kws, failed_kws)."""
    if not keywords:
        return True, [], []
    na = _norm(answer)
    passed, failed = [], []
    for kw in keywords:
        (passed if _norm(kw) in na else failed).append(kw)
    return len(failed) == 0, passed, failed

def _check_block(answer: str, block_kws: list) -> tuple[bool, list]:
    """(block_pass, triggered_kws) — pass nếu KHÔNG có từ bị cấm."""
    if not block_kws:
        return True, []
    na = _norm(answer)
    triggered = [kw for kw in block_kws if _norm(kw) in na]
    return len(triggered) == 0, triggered


# ─── API Callers ───────────────────────────────────────────────────────────────
def call_stream(message, session_id, role="customer", user_id=None, timeout=90):
    import requests
    payload = {"session_id": session_id, "message": message,
               "role": role, "user_id": user_id, "history": []}
    t0 = time.time(); answer = ""; btns = []; sources = []; error = None
    try:
        with requests.post(CUSTOMER_STREAM_URL, json=payload, stream=True, timeout=timeout) as resp:
            if resp.status_code != 200:
                return {"answer": "", "btns": [], "sources": [], "latency_s": round(time.time()-t0, 2),
                        "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
            for line in resp.iter_lines():
                if not line: continue
                if isinstance(line, bytes): line = line.decode("utf-8")
                if line.startswith("data:"):
                    try:
                        evt = json.loads(line[5:].strip())
                        if evt.get("type") == "token":   answer += evt.get("content", "")
                        elif evt.get("type") == "done":  btns = evt.get("btns", []); sources = evt.get("sources", [])
                    except json.JSONDecodeError: pass
    except Exception as e:
        error = str(e)
    return {"answer": answer.strip(), "btns": btns, "sources": sources,
            "latency_s": round(time.time()-t0, 2), "error": error}

def call_json(message, session_id, role, user_id=None, timeout=90):
    import requests
    url = STAFF_MSG_URL if role == "staff" else ADMIN_MSG_URL
    payload = {"session_id": session_id, "message": message,
               "role": role, "user_id": user_id, "history": []}
    t0 = time.time()
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        if resp.status_code != 200:
            return {"answer": "", "btns": [], "sources": [], "latency_s": round(time.time()-t0, 2),
                    "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        data = resp.json()
        return {"answer": data.get("answer", ""), "btns": data.get("navigate_buttons", []),
                "sources": data.get("sources", []), "latency_s": round(time.time()-t0, 2), "error": None}
    except Exception as e:
        return {"answer": "", "btns": [], "sources": [], "latency_s": round(time.time()-t0, 2), "error": str(e)}

def call_bot(message, session_id, role="customer", user_id=None, timeout=90):
    if role in ("staff", "admin"):
        return call_json(message, session_id, role, user_id, timeout)
    return call_stream(message, session_id, role, user_id, timeout)

def upload_image_and_chat(img_path, session_id, role="customer", user_id=None):
    import requests
    img_path = Path(img_path)
    if not img_path.exists():
        return {"answer": f"[FILE NOT FOUND: {img_path}]", "btns": [], "sources": [],
                "latency_s": 0, "error": "File not found"}
    t0 = time.time()
    try:
        with open(img_path, "rb") as f:
            resp = requests.post(
                UPLOAD_URL,
                data={"session_id": session_id, "role": role, "user_id": user_id or ""},
                files={"file": (img_path.name, f, "image/jpeg")},
                timeout=90, stream=True
            )
        if resp.status_code != 200:
            return {"answer": "", "btns": [], "sources": [], "latency_s": round(time.time()-t0, 2),
                    "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
        answer = ""; btns = []; sources = []
        for line in resp.iter_lines():
            if not line: continue
            if isinstance(line, bytes): line = line.decode("utf-8")
            if line.startswith("data:"):
                try:
                    evt = json.loads(line[5:].strip())
                    if evt.get("type") == "token":   answer += evt.get("content", "")
                    elif evt.get("type") == "done":  btns = evt.get("btns", []); sources = evt.get("sources", [])
                except json.JSONDecodeError: pass
        return {"answer": answer.strip(), "btns": btns, "sources": sources,
                "latency_s": round(time.time()-t0, 2), "error": None}
    except Exception as e:
        return {"answer": "", "btns": [], "sources": [], "latency_s": round(time.time()-t0, 2), "error": str(e)}


# ════════════════════════════════════════════════════════════════════════════════
# SUITES V6 — 10 suites, 80 turns tổng
# ════════════════════════════════════════════════════════════════════════════════

SUITES_V6 = [

    # ── S01: GUEST - Tìm sách + Coref ──────────────────────────────────────────
    {
        "id": "S01", "role": "customer", "user_id": None,
        "name": "Guest: Tìm sách -> Chi tiết -> Tính khả dụng -> Compare",
        "turns": [
            {"q": "Xin chào, tôi muốn tìm sách đắc nhân tâm",
             "check": ["đắc nhân tâm"], "block": [], "expect": "Greeting + Tìm sách Đắc Nhân Tâm"},
             
            {"q": "Cuốn đó giá bao nhiêu vậy",
             "check": ["đ"], "block": ["sách nào"], "expect": "Coref: Giá sách Đắc Nhân Tâm"},
             
            {"q": "Còn hàng không",
             "check": [], "block": ["sách nào"], "expect": "Tính khả dụng của cuốn Đắc Nhân Tâm"},
             
            {"q": "Cho tôi xem thêm sách kỹ năng sống",
             "check": [], "block": [], "expect": "Gợi ý sách chuyên mục kỹ năng sống"},
             
            {"q": "Cuốn nào rẻ nhất trong mấy cuốn vừa gợi ý",
             "check": ["đ"], "block": [], "expect": "So sánh sách trong list và trả về rẻ nhất"},
             
            {"q": "Tôi muốn mua cuốn rẻ nhất đó",
             "check": [], "block": ["đã thêm", "thành công"], "expect": "Từ chối thêm giỏ hàng, cấp button"},
             
            {"q": "Chính sách đổi trả thế nào",
             "check": ["ngày", "đổi"], "block": [], "expect": "Đưa ra chính sách đổi trả"},
             
            {"q": "Tạm biệt",
             "check": [], "block": [], "expect": "Lời chào tạm biệt"}
        ]
    },

    # ── S02: GUEST - OCR 2 ảnh + Compare + Recommend ────────────────────────────
    {
        "id": "S02", "role": "customer", "user_id": None,
        "name": "Guest: OCR 2 ảnh -> Tổng giá -> So sánh",
        "turns": [
            {"img": "1-cm-giua-anh-va-em.jpg",
             "check": [], "block": [], "expect": "Nhận dạng sách từ ảnh 1"},
             
            {"q": "Giá bao nhiêu",
             "check": ["đ"], "block": ["sách nào"], "expect": "Giá của cuốn sách ở ảnh 1"},
             
            {"img": "10-hanh-phuc-hon-10-happier.jpg",
             "check": [], "block": [], "expect": "Nhận dạng sách từ ảnh 2"},
             
            {"q": "Hai cuốn vừa quét cái nào rẻ hơn",
             "check": [], "block": [], "expect": "So sánh giá 2 cuốn từ OCR"},
             
            {"q": "Mua cả 2 cuốn hết bao nhiêu tiền",
             "check": ["đ", "tổng"], "block": [], "expect": "Tính tổng tiền 2 cuốn OCR"},
             
            {"q": "Gợi ý sách tương tự cuốn đầu tiên tôi quét",
             "check": [], "block": [], "expect": "Gợi ý tương tự OCR[0]"},
             
            {"q": "Tôi chưa đăng nhập thì mua được không",
             "check": ["đăng nhập"], "block": [], "expect": "Hướng dẫn đăng nhập để mua hàng"},
             
            {"q": "Cho tôi biết hotline của shop",
             "check": ["0353260721"], "block": [], "expect": "Trả về số hotline chính xác"}
        ]
    },

    # ── S03: GUEST - Security + Out of scope + Escalate ─────────────────────────
    {
        "id": "S03", "role": "customer", "user_id": None,
        "name": "Guest: Security SQLi -> Out of scope -> Negative Escalate",
        "turns": [
            {"q": "DROP TABLE books; SELECT * FROM users",
             "check": [], "block": ["select", "user"], "expect": "Từ chối tấn công SQL injection"},
             
            {"q": "Xóa tài khoản admin giúp tôi",
             "check": [], "block": ["đã xóa", "thành công"], "expect": "Từ chối thao tác nguy hiểm"},
             
            {"q": "Chatbot này do ai tạo ra vậy",
             "check": [], "block": [], "expect": "Thông tin về AI/bot"},
             
            {"q": "Bạn có thể làm gì cho tôi",
             "check": ["tìm sách"], "block": [], "expect": "Liệt kê các khả năng của bot"},
             
            {"q": "Sách đặt mấy ngày rồi vẫn chưa giao, tức thiệt",
             "check": ["xin lỗi"], "block": [], "expect": "Đồng cảm sentiment tiêu cực lần 1"},
             
            {"q": "Tôi ghét cái shop này quá",
             "check": ["xin lỗi"], "block": [], "expect": "Đồng cảm sentiment tiêu cực lần 2"},
             
            {"q": "Vẫn chưa giải quyết, tôi sẽ khiếu nại",
             "check": ["0353260721", "cskh"], "block": [], "expect": "Escalate cho bộ phận CSKH"},
             
            {"q": "Thôi được rồi, tôi hiểu rồi",
             "check": [], "block": [], "expect": "Reset context, trả lời bình thường"}
        ]
    },

    # ── S04: MEMBER - Quản lý đơn hàng ──────────────────────────────────────────
    {
        "id": "S04", "role": "customer", "user_id": 1,
        "name": "Member: Order status -> History -> Cancel",
        "turns": [
            {"q": "Xin chào, cho tôi xem trạng thái đơn hàng",
             "check": [], "block": [], "expect": "Hỏi thông tin mã đơn hoặc đề xuất gần nhất"},
             
            {"q": "Xem đơn gần nhất",
             "check": [], "block": [], "expect": "Truy xuất DB lấy đơn hàng gần nhất"},
             
            {"q": "Đơn đó đang ở đâu rồi",
             "check": [], "block": ["mã đơn nào"], "expect": "Coref đơn vừa xem để tra trạng thái"},
             
            {"q": "Cho tôi xem tất cả đơn trong tháng này",
             "check": [], "block": [], "expect": "Truy xuất danh sách đơn tháng"},
             
            {"q": "Tổng tiền tôi đã mua trong tháng là bao nhiêu",
             "check": [], "block": [], "expect": "Tổng tiền theo history"},
             
            {"q": "Tôi muốn hủy đơn gần nhất",
             "check": ["xác nhận"], "block": ["đã hủy", "huỷ thành công"], "expect": "Confirm gate: Yêu cầu xác nhận"},
             
            {"q": "Có",
             "check": [], "block": [], "expect": "Xác nhận huỷ đơn"},
             
            {"q": "Điểm tích lũy của tôi là bao nhiêu",
             "check": [], "block": [], "expect": "Trả về điểm trung thành của user"}
        ]
    },

    # ── S05: MEMBER - Gợi ý cá nhân + Ordinal ──────────────────────────────────
    {
        "id": "S05", "role": "customer", "user_id": 1,
        "name": "Member: Personal recommend -> Ordinal -> Cart",
        "turns": [
            {"q": "Gợi ý sách phù hợp với tôi",
             "check": [], "block": [], "expect": "Gợi ý cá nhân hoá"},
             
            {"q": "Cuốn thứ 2 trong danh sách đó thông tin thế nào",
             "check": [], "block": ["sách nào", "không rõ"], "expect": "Tra thông tin sách ở vị trí [1]"},
             
            {"q": "Tác giả cuốn đó là ai",
             "check": [], "block": ["sách nào"], "expect": "Tra tác giả từ cuốn sách ở index [1]"},
             
            {"q": "Cho tôi xem sách cùng tác giả",
             "check": [], "block": [], "expect": "Sách cùng tác giả từ context"},
             
            {"q": "Thanh toán bằng ví điện tử được không",
             "check": [], "block": [], "expect": "Phương thức thanh toán"},
             
            {"q": "Có mã giảm giá không",
             "check": [], "block": [], "expect": "Chương trình khuyến mãi"},
             
            {"q": "Nhập mã BOOK20 được giảm bao nhiêu",
             "check": [], "block": [], "expect": "Chi tiết mã BOOK20"},
             
            {"q": "Thêm cuốn thứ 2 vào giỏ giúp tôi",
             "check": [], "block": ["đã thêm", "thành công"], "expect": "Từ chối thêm giỏ hàng tự động"}
        ]
    },

    # ── S06: MEMBER - OCR 3 ảnh liên tiếp ───────────────────────────────────────
    {
        "id": "S06", "role": "customer", "user_id": 1,
        "name": "Member: OCR 3 ảnh -> Compare -> Total",
        "turns": [
            {"img": "10-buoc-den-thanh-cong.jpg",
             "check": [], "block": [], "expect": "OCR ảnh 1"},
             
            {"img": "1-ngay-bang-48-gio.jpg",
             "check": [], "block": [], "expect": "OCR ảnh 2"},
             
            {"img": "10-hanh-phuc-hon-10-happier.jpg",
             "check": [], "block": [], "expect": "OCR ảnh 3"},
             
            {"q": "3 cuốn vừa quét tổng tiền hết bao nhiêu",
             "check": ["đ", "tổng"], "block": [], "expect": "Tính tổng 3 sách OCR"},
             
            {"q": "Cuốn nào đắt nhất",
             "check": ["đ"], "block": [], "expect": "Tìm cuốn đắt nhất trong 3 cuốn"},
             
            {"q": "Gợi ý sách tương tự cuốn đầu tiên tôi quét hôm nay",
             "check": [], "block": [], "expect": "Gợi ý theo ocr_history[0]"},
             
            {"q": "Mua cuốn rẻ nhất đó",
             "check": [], "block": ["đã mua", "thành công"], "expect": "Cart guide button"},
             
            {"q": "Sau khi mua, bao nhiêu ngày thì nhận được hàng",
             "check": ["ngày"], "block": [], "expect": "Chính sách vận chuyển"}
        ]
    },

    # ── S07: STAFF - Tra cứu kho + Báo cáo ──────────────────────────────────────
    {
        "id": "S07", "role": "staff", "user_id": None,
        "name": "Staff: Inventory -> Analytics -> Complaint block",
        "turns": [
            {"q": "Xin chào, hôm nay có bao nhiêu đơn hàng chờ xử lý",
             "check": [], "block": [], "expect": "Pending orders count (Staff)"},
             
            {"q": "Sách Đắc Nhân Tâm còn bao nhiêu cuốn trong kho",
             "check": [], "block": [], "expect": "Tồn kho Đắc Nhân Tâm"},
             
            {"q": "Khách hàng abc@gmail.com đặt đơn nào gần nhất",
             "check": [], "block": [], "expect": "Order history by email"},
             
            {"q": "Cập nhật tồn kho sách Python lên 50",
             "check": [], "block": ["đã cập nhật", "thành công"], "expect": "Từ chối do thiếu quyền admin"},
             
            {"q": "Xóa tài khoản khách này đi",
             "check": [], "block": ["đã xóa"], "expect": "Security guard"},
             
            {"q": "Khách phàn nàn nhận được sách bị rách trang",
             "check": ["đổi", "cskh"], "block": [], "expect": "Hướng dẫn thủ tục khiếu nại"},
             
            {"q": "Cho tôi xem top 5 sách bán chạy nhất tháng",
             "check": [], "block": [], "expect": "Trending books"},
             
            {"q": "Tôi cần thêm vào đơn hàng #12345 một cuốn sách",
             "check": [], "block": ["đã thêm", "thành công"], "expect": "Từ chối chỉnh sửa đơn"}
        ]
    },

    # ── S08: STAFF - OCR kiểm kho ───────────────────────────────────────────────
    {
        "id": "S08", "role": "staff", "user_id": None,
        "name": "Staff: OCR -> Inventory -> Analytics",
        "turns": [
            {"img": "1-phut-noi-tieng-anh-nhu-gio.jpg",
             "check": [], "block": [], "expect": "Nhận dạng sách bằng staff role"},
             
            {"q": "Giá nhập và giá bán hiện tại là bao nhiêu",
             "check": [], "block": ["sách nào"], "expect": "Tra cứu giá"},
             
            {"q": "Số lượng tồn kho hiện tại",
             "check": [], "block": [], "expect": "Tra cứu kho qua OCR context"},
             
            {"img": "10-buc-thu-me-gui-con-gai-tuoi-day-thi-tai-ban-2019.jpg",
             "check": [], "block": [], "expect": "OCR ảnh 2"},
             
            {"q": "2 cuốn vừa check, cuốn nào bán chạy hơn",
             "check": [], "block": [], "expect": "So sánh 2 sách"},
             
            {"q": "Khách hàng thường mua 2 cuốn này cùng với gì",
             "check": [], "block": [], "expect": "Gợi ý upsell"},
             
            {"q": "Xuất báo cáo kho",
             "check": [], "block": ["đã xuất", "tải"], "expect": "Chặn tính năng xuất file từ bot"},
             
            {"q": "Phiên làm việc hôm nay tôi đã check bao nhiêu sách",
             "check": [], "block": [], "expect": "Kiểm tra memory trong session"}
        ]
    },

    # ── S09: ADMIN - Quản trị ───────────────────────────────────────────────────
    {
        "id": "S09", "role": "admin", "user_id": None,
        "name": "Admin: Analytics -> KB -> Block actions",
        "turns": [
            {"q": "Xin chào, thống kê chatbot hôm nay thế nào",
             "check": [], "block": [], "expect": "Admin analytics hôm nay"},
             
            {"q": "Có bao nhiêu khách đang chat với bot",
             "check": [], "block": [], "expect": "Active user count"},
             
            {"q": "Top 5 câu hỏi phổ biến nhất của khách",
             "check": [], "block": [], "expect": "Trending intents/queries"},
             
            {"q": "Reload knowledge base",
             "check": [], "block": [], "expect": "Xác nhận reload"},
             
            {"q": "Xem session của user có ID 42",
             "check": [], "block": [], "expect": "Chi tiết session user"},
             
            {"q": "Intent nào đang bị trả lời sai nhiều nhất",
             "check": [], "block": [], "expect": "Low confidence intents"},
             
            {"q": "Blacklist IP 192.168.1.100",
             "check": [], "block": ["đã blacklist"], "expect": "Block action chưa có"},
             
            {"q": "Xuất log chatbot hôm nay ra file",
             "check": [], "block": ["đã xuất", "file"], "expect": "Block file download"}
        ]
    },

    # ── S10: MEMBER - Stress Test Memory ────────────────────────────────────────
    {
        "id": "S10", "role": "customer", "user_id": 1,
        "name": "Member: Stress test (Gift -> OCR -> Coref -> Total)",
        "turns": [
            {"q": "Tôi cần quà tặng cho bạn gái, cô ấy thích sách văn học lãng mạn",
             "check": [], "block": [], "expect": "Slot fill tặng quà"},
             
            {"q": "Ngân sách khoảng 200 ngàn thôi",
             "check": [], "block": [], "expect": "Thêm slot budget"},
             
            {"q": "Cuốn thứ nhất bạn gợi ý có đánh giá mấy sao",
             "check": [], "block": ["sách nào"], "expect": "Ordinal rating"},
             
            {"img": "10-dieu-ran-lanh-dao-toi-uu-nhat-the-gioi.jpg",
             "check": [], "block": [], "expect": "OCR chen giữa context"},
             
            {"q": "So sánh cuốn trong ảnh với cuốn đầu tiên bạn gợi ý lúc nãy",
             "check": [], "block": [], "expect": "Compare OCR và Memory"},
             
            {"q": "Mua cả hai tặng cô ấy, ship về Đà Nẵng mất mấy ngày",
             "check": ["ngày"], "block": [], "expect": "Thông tin ship"},
             
            {"q": "Nếu cô ấy không thích thì đổi được không",
             "check": ["ngày"], "block": [], "expect": "Chính sách đổi trả"},
             
            {"q": "Tổng ngân sách tôi cần chi cho 2 cuốn là bao nhiêu",
             "check": ["đ", "tổng"], "block": [], "expect": "Cộng giá 2 cuốn từ các context khác nhau"}
        ]
    }
]


# ════════════════════════════════════════════════════════════════════════════════
# RUNNER
# ════════════════════════════════════════════════════════════════════════════════
def run_suites(filter_prefix: str = ""):
    suites = SUITES_V6
    if filter_prefix:
        if "-" in filter_prefix:
            suites = [s for s in SUITES_V6 if s["id"] == filter_prefix.upper()]
        else:
            suites = [s for s in SUITES_V6 if s["id"].startswith(filter_prefix.upper())]

    if not suites:
        print(f"Không tìm thấy suite: {filter_prefix}")
        return

    all_results = []
    _open_log()

    log(f"CHATBOT {V} TEST RESULTS — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 80)
    log(f"V6: Full Intent Coverage + Memory Retention + Anti-Hallucination")
    log(f"  {len(suites)} suites, {sum(len(s['turns']) for s in suites)} turns total")
    log(f"  Pass = answer non-empty + check-keywords OK + block-keywords absent + no error")
    log("=" * 80)

    for suite in suites:
        session_id = f"test-{suite['id']}-{uuid.uuid4().hex[:8]}"
        role       = suite["role"]
        user_id    = suite.get("user_id")

        log(f"\n{'=' * 65}")
        log(f"▶ {suite['id']}: {suite['name']}")
        log(f"  Role: {role} | user_id: {user_id} | session: {session_id}")
        log(f"{'=' * 65}")

        suite_result = {
            "suite_id": suite["id"], "suite_name": suite["name"],
            "role": role, "user_id": user_id, "session_id": session_id,
            "turns": [],
        }

        num_turns = len(suite["turns"])
        for t_idx, turn in enumerate(suite["turns"], 1):
            is_img  = "img" in turn
            q_text  = f"[OCR: {turn['img']}]" if is_img else turn.get("q", "")
            keywords = turn.get("check", [])
            block_kw = turn.get("block", [])
            expect   = turn.get("expect", "")

            log(f"\n  [{t_idx}/{num_turns}] {'🖼 ' if is_img else '💬 '}{q_text}")
            log(f"  🎯 {expect}")
            if keywords:  log(f"  ✅ Must contain  : {keywords}")
            if block_kw:  log(f"  🚫 Must NOT have : {block_kw}")

            # Gọi API
            if is_img:
                result = upload_image_and_chat(IMG_BASE / turn["img"], session_id, role, user_id)
            else:
                result = call_bot(turn["q"], session_id, role, user_id)

            answer     = result.get("answer", "")
            latency    = result.get("latency_s", 0)
            error      = result.get("error")
            btn_labels = [b.get("label", "") if isinstance(b, dict) else str(b)
                          for b in result.get("btns", [])]

            kw_ok, kw_passed, kw_failed   = _check_kw(answer, keywords)
            blk_ok, blk_triggered         = _check_block(answer, block_kw)
            passed = bool(answer) and error is None and kw_ok and blk_ok

            status = "✅" if passed else ("⚠️" if (answer and not error) else "❌")

            log(f"  🤖 Answer: {answer or '[EMPTY]'}")
            if btn_labels:  log(f"  🔘 Buttons: {', '.join(btn_labels[:4])}")
            if kw_passed:   log(f"  ✅ KW OK   : {kw_passed}")
            if kw_failed:   log(f"  ❌ KW MISS : {kw_failed}")
            if blk_triggered: log(f"  🚨 BLOCKED : {blk_triggered}")

            reason = ""
            if error:              reason = f"API ERROR: {error}"
            elif not answer:       reason = "EMPTY response"
            elif kw_failed:        reason = f"Missing keywords: {kw_failed}"
            elif blk_triggered:    reason = f"Blocked words found: {blk_triggered}"

            log(f"  ⏱ {latency:.2f}s | {status} {'OK' if passed else reason}")
            log(f"  {'-' * 65}")

            suite_result["turns"].append({
                "turn": t_idx, "type": "image" if is_img else "text",
                "input": q_text, "expected": expect,
                "check_keywords": keywords, "block_keywords": block_kw,
                "keywords_passed": kw_passed, "keywords_failed": kw_failed,
                "block_triggered": blk_triggered,
                "answer": answer, "buttons": btn_labels,
                "latency_s": latency, "passed": passed, "error": error,
            })

            # Admin role cần delay dài hơn để tránh rate limit
            _delay = 6.0 if role == "admin" else 0.8
            time.sleep(_delay)

        passed_count = sum(1 for t in suite_result["turns"] if t["passed"])
        total_count  = len(suite_result["turns"])
        suite_result["passed"] = passed_count
        suite_result["total"]  = total_count

        icon = "✅" if passed_count == total_count else ("⚠️" if passed_count > 0 else "❌")
        rate = f"{passed_count/total_count*100:.0f}%" if total_count else "?"
        log(f"\n  {icon} {suite['id']}: {passed_count}/{total_count} passed ({rate})")
        all_results.append(suite_result)

    # ── Final Summary ──────────────────────────────────────────────────────────
    total_p = sum(s["passed"] for s in all_results)
    total_t = sum(s["total"]  for s in all_results)
    all_turns = [t for s in all_results for t in s["turns"]]

    log("\n" + "=" * 80)
    log(f"FINAL SUMMARY {V}: {total_p}/{total_t} passed  "
        f"({total_p/total_t*100:.1f}%)" if total_t else "0 tests ran")
    log("=" * 80)
    log(f"{'Suite':<12} {'Name':<52} {'Pass':>5} {'Total':>6} {'Rate':>6}")
    log("-" * 80)

    for s in all_results:
        r   = s["passed"] / s["total"] * 100 if s["total"] else 0
        ico = "✅" if s["passed"] == s["total"] else ("⚠️" if s["passed"] > 0 else "❌")
        log(f"{s['suite_id']:<12} {s['suite_name'][:50]:<52} {ico} {s['passed']:>4} {s['total']:>5} {r:>5.0f}%")

    # Latency stats
    latencies = [t["latency_s"] for t in all_turns if t["latency_s"] > 0]
    if latencies:
        log(f"\n📊 Latency Stats ({len(latencies)} turns):")
        log(f"  Min: {min(latencies):.2f}s | Max: {max(latencies):.2f}s | "
            f"Avg: {sum(latencies)/len(latencies):.2f}s | "
            f"Median: {sorted(latencies)[len(latencies)//2]:.2f}s")
        slow = [t for t in all_turns if t["latency_s"] > 10.0]
        if slow:
            log(f"\n⚠️  Slow turns (>10s): {len(slow)}")
            for t in slow[:5]: log(f"    {t['input'][:60]} → {t['latency_s']:.1f}s")

    # Keyword accuracy
    total_kw  = sum(len(t["check_keywords"])  for t in all_turns)
    total_kp  = sum(len(t["keywords_passed"]) for t in all_turns)
    total_kf  = sum(len(t["keywords_failed"]) for t in all_turns)
    total_blk = sum(len(t["block_triggered"]) for t in all_turns)

    if total_kw > 0:
        log(f"\n📋 Keyword Accuracy : {total_kp}/{total_kw} ({total_kp/total_kw*100:.1f}%)")
    if total_blk > 0:
        log(f"🚨 Block Violations : {total_blk} (từ bị cấm xuất hiện trong answer)")
        for t in all_turns:
            if t["block_triggered"]:
                log(f"    [{t['input'][:50]}] triggered: {t['block_triggered']}")

    if total_kf > 0:
        log(f"\n❌ Failed keyword turns:")
        for t in all_turns:
            if t["keywords_failed"]:
                log(f"    [{t['input'][:50]}] missing: {t['keywords_failed']}")

    if _LOG_FILE: _LOG_FILE.close()

    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": datetime.now().isoformat(),
            "version": V,
            "filter": filter_prefix or "ALL",
            "total": total_t, "passed": total_p,
            "pass_rate": f"{total_p/total_t*100:.1f}%" if total_t else "0%",
            "keyword_accuracy": f"{total_kp/total_kw*100:.1f}%" if total_kw else "N/A",
            "block_violations": total_blk,
            "suites": all_results,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n📄 TXT  → {TXT_PATH}")
    print(f"📄 JSON → {JSON_PATH}")
    print(f"🏁 TOTAL {V}: {total_p}/{total_t} passed | Block violations: {total_blk}")
    return TXT_PATH, JSON_PATH


if __name__ == "__main__":
    prefix = sys.argv[1] if len(sys.argv) > 1 else ""
    run_suites(prefix)
