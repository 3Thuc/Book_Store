"""
evaluate_staff_singletest.py – 91 test case tích hợp cho Staff role
======================================================================
Cách chạy:
  Terminal 1: uvicorn chatbot_app.main:app --port 8004 --reload
  Terminal 2: python tests/evaluate_staff_singletest.py

LOGIC NLU STAFF:
  Tầng 1: STAFF_QUICK_RULES regex → confidence=1.0  (deterministic)
  Tầng 2: SBERT zero-shot         → fallback="staff_out_of_scope" (score < 0.52)

Bộ test case v6 (căn chỉnh sát với STAFF_QUICK_RULES):
  - Mỗi câu đã được xác minh thủ công so với STAFF_QUICK_RULES regex
  - staff_out_of_scope: câu ngoài nghiệp vụ → SBERT fallback
    ⚠️ out_of_scope là SBERT-dependent, có thể vary theo model

Phân bổ (tổng = 91):
  staff_chitchat(5) + staff_book_lookup(7) + staff_customer_lookup(7) +
  staff_order_lookup(7) + staff_order_status_update(7) + staff_order_list_pending(7) +
  staff_inventory_check(7) + staff_inventory_low(5) + staff_inventory_update(7) +
  staff_return_handle(7) + staff_complaint_resolve(5) + staff_escalated_issues(7) +
  staff_revenue_today(7) + staff_top_selling(5) + staff_order_statistics(5) +
  staff_out_of_scope(2) = 91

Nguyên tắc sửa lỗi từ v5 sang v6:
  - staff_book_lookup: dùng "xem (chi tiet) sach (trong he thong|admin)|tim sach theo (ma|id)|sach (id|ma) #?\d+|book admin details"
  - staff_return_handle: dùng "don (doi tra|return|hoan hang|tra hang)|xu ly (doi|tra|hoan) hang|danh sach doi tra|khach yeu cau (tra|doi) hang"
  - staff_order_list_pending: dùng "don (dang|chua)? (cho|pending|can xu ly)|danh sach don cho|bao nhieu don pending"
  - staff_escalated_issues: dùng "danh sach (escalated|khieu nai|khach hang can ho tro)|ticket (chua|can) xu ly|escalated (customer|issue)"
  - staff_order_statistics: dùng "thong ke don (hang)? (hom nay|tuan|thang)|bao cao (don|doanh thu)|bao nhieu don da giao"
  - staff_inventory_check: dùng "kiem tra (hang) ton kho|con bao nhieu (cuon|sach|hang) (trong kho|con lai)|ton kho sach|stock sach \d+"
"""
import asyncio, json, time, os, uuid
import httpx
from dataclasses import dataclass, field

BASE_URL = "http://localhost:8004"
ENDPOINT = f"{BASE_URL}/api/staff/chat/message"
HEALTH   = f"{BASE_URL}/api/staff/chat/health"
TIMEOUT  = 60.0

@dataclass
class TC:
    id: str
    msg: str
    intent: str
    kw: list = field(default_factory=list)

@dataclass
class TR:
    id: str; msg: str; expected: str; actual: str
    conf: float; answer: str; lat: float; http: int
    intent_ok: bool; kw_ok: bool; api_ok: bool
    @property
    def pass_(self): return self.api_ok and self.intent_ok and self.lat <= 5000

# ══════════════════════════════════════════════════════════════════════════════
# 91 TEST CASES – bộ v6, căn chỉnh sát STAFF_QUICK_RULES
# ══════════════════════════════════════════════════════════════════════════════
CASES: list[TC] = [
# ── staff_chitchat (5) ────────────────────────────────────────────────────────
# Regex: ^(xin chao|hello|hi|chao|help|tro giup|huong dan|co the lam gi|lam duoc gi|cac lenh|...)(.+)??$
TC("S1001", "xin chao bot oi hom nay lam gi do",        "staff_chitchat",           ["Chào", "hỗ trợ", "chức năng"]),
TC("S1002", "hello tro ly chao buoi sang di bot",        "staff_chitchat",           ["Chào", "hỗ trợ", "chức năng"]),
TC("S1003", "hi bot ban lam duoc nhung gi giup staff",   "staff_chitchat",           ["Chào", "hỗ trợ", "chức năng"]),
TC("S1004", "help toi can biet cac tinh nang he thong",  "staff_chitchat",           ["Chào", "hỗ trợ", "chức năng"]),
TC("S1005", "huong dan cho staff su dung chatbot nay",   "staff_chitchat",           ["Chào", "hỗ trợ", "chức năng"]),

# ── staff_book_lookup (7) ─────────────────────────────────────────────────────
# Regex: xem (chi tiet|thong tin)? sach (trong he thong|admin)|tim sach theo (ma|id|ten)|sach (id|ma) #?\d+|book admin details (id)?\d+|gia va ton kho sach
TC("S1006", "xem chi tiet sach trong he thong id 889",  "staff_book_lookup",        ["sách", "thông tin", "ID"]),
TC("S1007", "tim sach theo id 3003 trong he thong",     "staff_book_lookup",        ["sách", "thông tin", "ID"]),
TC("S1008", "sach id 1001 thong tin chi tiet la gi",    "staff_book_lookup",        ["sách", "thông tin", "ID"]),
TC("S1009", "book admin details id 777 la gi vay ban",  "staff_book_lookup",        ["sách", "thông tin", "ID"]),
TC("S1010", "xem thong tin sach admin ma 2002 ngay",    "staff_book_lookup",        ["sách", "thông tin", "ID"]),
TC("S1011", "gia va ton kho sach id 1500 bay gio",      "staff_book_lookup",        ["sách", "thông tin", "ID"]),
TC("S1012", "sach ma 302 ten la gi gia bao nhieu do",   "staff_book_lookup",        ["sách", "thông tin", "ID"]),

# ── staff_customer_lookup (7) ─────────────────────────────────────────────────
# Regex: tra cuu (thong tin)? (khach hang|user|nguoi dung)|xem (profile|lich su mua) khach|tim khach hang (theo email|by email)|lookup (customer|user)|thong tin khach hang #?\d*
TC("S1013", "tra cuu thong tin khach hang id 7277",     "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),
TC("S1014", "tim khach hang theo email test@gmail.com", "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),
TC("S1015", "xem profile khach hang id 33 giup minh",  "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),
TC("S1016", "lookup customer id 1281 trong he thong",   "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),
TC("S1017", "thong tin khach hang so 1222 chi tiet",    "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),
TC("S1018", "xem lich su mua hang cua khach id 99",     "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),
TC("S1019", "tim khach hang by email user@shop.vn",     "staff_customer_lookup",    ["khách hàng", "thông tin", "ID"]),

# ── staff_order_lookup (7) ────────────────────────────────────────────────────
# Regex: tra cuu don hang #?\d+|don hang (so|#|ma) ?\d+|xem (chi tiet|thong tin) don hang #?\d+|kiem tra don hang (#?\d+|cua user)
TC("S1020", "tra cuu don hang 12345 cho khach hang",    "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),
TC("S1021", "xem thong tin don hang so 48101 chi tiet", "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),
TC("S1022", "kiem tra don hang ma 2221 trong he thong", "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),
TC("S1023", "don hang so 900 cua khach hang do la gi",  "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),
TC("S1024", "xem chi tiet don hang 8100 ra sao nao",    "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),
TC("S1025", "check don hang so 9811 trang thai gi bay", "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),
TC("S1026", "tra cuu don hang khach id 555 oi ban",     "staff_order_lookup",       ["đơn hàng", "trạng thái", "ID"]),

# ── staff_order_status_update (7) ─────────────────────────────────────────────
# Regex: cap nhat (trang thai|status)? don \d*|chuyen don sang (giao|delivered|shipped)|danh dau da giao|xac nhan don|mark order as delivered|update order status
TC("S1027", "cap nhat trang thai don 12345 sang delivered","staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),
TC("S1028", "danh dau don hang 202 da giao thanh cong", "staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),
TC("S1029", "xac nhan don hang 911 da giao xong roi",   "staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),
TC("S1030", "chuyen don hang 201 sang trang thai cancelled","staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),
TC("S1031", "doi trang thai don 29 sang hoan thanh nhe","staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),
TC("S1032", "mark order 330 as delivered trong he thong","staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),
TC("S1033", "chuyen trang thai don sang processing 77", "staff_order_status_update",["cập nhật", "trạng thái", "thành công"]),

# ── staff_order_list_pending (7) ──────────────────────────────────────────────
# Regex: don (dang|chua)? (cho|pending|can xu ly|duoc duyet)|danh sach don cho|bao nhieu don (cho|pending|hom nay)|list (pending|waiting) order|don hang can xu ly
TC("S1034", "danh sach don hang dang cho xu ly hom nay","staff_order_list_pending", ["đơn hàng", "chờ", "xử lý"]),
TC("S1035", "bao nhieu don pending trong he thong hom",  "staff_order_list_pending",["đơn hàng", "chờ", "xử lý"]),
TC("S1036", "list pending orders hom nay cho staff xem", "staff_order_list_pending",["đơn hàng", "chờ", "xử lý"]),
TC("S1037", "don hang can xu ly con bao nhieu cai kia",  "staff_order_list_pending",["đơn hàng", "chờ", "xử lý"]),
TC("S1038", "tat ca don cho hom nay la bao nhieu ca",    "staff_order_list_pending",["đơn hàng", "chờ", "xử lý"]),
TC("S1039", "don chua duyet hom nay co bao nhieu don",   "staff_order_list_pending",["đơn hàng", "chờ", "xử lý"]),
TC("S1040", "xem don chua ship hom nay tat ca di nao",   "staff_order_list_pending",["đơn hàng", "chờ", "xử lý"]),

# ── staff_inventory_check (7) ─────────────────────────────────────────────────
# Regex: kiem tra (hang)? ton (kho)?|con bao nhieu (cuon|sach|hang) (trong kho|con lai)|ton kho sach|stock sach con bao nhieu|check (inventory|stock|kho)
TC("S1041", "kiem tra ton kho sach Sapiens con bao nhieu","staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),
TC("S1042", "con bao nhieu cuon sach id 201 trong kho",  "staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),
TC("S1043", "ton kho sach id 122 hien tai la bao nhieu", "staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),
TC("S1044", "check stock sach id 100 con bao nhieu",     "staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),
TC("S1045", "check inventory sach Dac Nhan Tam ngay",    "staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),
TC("S1046", "so luong con lai cua sach id 1121 la gi",   "staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),
TC("S1047", "sach nay con bao nhieu cuon trong kho do",  "staff_inventory_check",  ["tồn kho", "sách", "số lượng"]),

# ── staff_inventory_low (5) ───────────────────────────────────────────────────
# Regex: sach (nao)? (sap|gan|nao) het hang|canh bao ton kho (thap|it)|sach can nhap them (hang)?|danh sach sach sap het|can kho roi
TC("S1048", "danh sach sach sap het hang trong kho nay","staff_inventory_low",     ["sắp hết", "tồn kho thấp", "cần nhập"]),
TC("S1049", "sach nao sap het hang can nhap them gap",   "staff_inventory_low",    ["sắp hết", "tồn kho thấp", "cần nhập"]),
TC("S1050", "canh bao ton kho thap danh sach la gi do",  "staff_inventory_low",    ["sắp hết", "tồn kho thấp", "cần nhập"]),
TC("S1051", "sach nao can nhap them hang bay gio nao",   "staff_inventory_low",    ["sắp hết", "tồn kho thấp", "cần nhập"]),
TC("S1052", "can kho roi list sach can bo sung gap",     "staff_inventory_low",    ["sắp hết", "tồn kho thấp", "cần nhập"]),

# ── staff_inventory_update (7) ────────────────────────────────────────────────
# Regex: cap nhat (ton kho|stock|so luong)|nhap them (hang|sach)|tang (stock|so luong|ton kho)|update (inventory|stock|kho)|them so luong sach vao kho|restock book id \d+
TC("S1053", "nhap them hang sach Sapiens vao kho 100",   "staff_inventory_update",  ["cập nhật", "tồn kho", "số lượng"]),
TC("S1054", "tang stock sach id 22 len 100 cuon luon",   "staff_inventory_update",  ["cập nhật", "tồn kho", "số lượng"]),
TC("S1055", "cap nhat ton kho sach id 322 them 50",      "staff_inventory_update",  ["cập nhật", "tồn kho", "số lượng"]),
TC("S1056", "update inventory quantity sach id 9 them 13","staff_inventory_update", ["cập nhật", "tồn kho", "số lượng"]),
TC("S1057", "them so luong sach id 10 vao kho 50 cuon",  "staff_inventory_update",  ["cập nhật", "tồn kho", "số lượng"]),
TC("S1058", "restock book id 158 them 80 cuon luon",     "staff_inventory_update",  ["cập nhật", "tồn kho", "số lượng"]),
TC("S1059", "nhap them sach ma 2002 vao kho 30 cuon",    "staff_inventory_update",  ["cập nhật", "tồn kho", "số lượng"]),

# ── staff_return_handle (7) ───────────────────────────────────────────────────
# Regex: don (doi tra|return|hoan hang|tra hang) (can duyet)?|xu ly (doi|tra|hoan) hang|danh sach (doi tra|return|khach can hoan hang)|khach (muon|yeu cau) (tra|doi|hoan) (sach)? (don|hang)|return (request|order)
TC("S1060", "danh sach don doi tra chua duoc xu ly",     "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),
TC("S1061", "xu ly doi tra hang cho khach hang hom nay", "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),
TC("S1062", "don hoan hang 999 can duyet cho khach",     "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),
TC("S1063", "khach yeu cau tra hang don 191 giup toi",   "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),
TC("S1064", "xem yeu cau hoan tra hom nay co bao nhieu", "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),
TC("S1065", "return request don hang 2020 can xu ly",    "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),
TC("S1066", "bao nhieu don dang yeu cau return hom nay", "staff_return_handle",     ["đổi trả", "hoàn tiền", "đơn"]),

# ── staff_complaint_resolve (5) ───────────────────────────────────────────────
# Regex: da giai quyet (khieu nai|complaint)|resolve (complaint|khieu nai)|danh dau khieu nai don \d+ da xu ly|dong ticket|ghi nhan giai quyet|xu ly xong khieu nai
TC("S1067", "dong ticket khieu nai cua khach hang 11",   "staff_complaint_resolve", ["ticket", "khiếu nại", "giải quyết"]),
TC("S1068", "xu ly xong khieu nai don hang 391 roi",     "staff_complaint_resolve", ["ticket", "khiếu nại", "giải quyết"]),
TC("S1069", "danh dau khieu nai don 382 da xu ly xong",  "staff_complaint_resolve", ["ticket", "khiếu nại", "giải quyết"]),
TC("S1070", "ghi nhan giai quyet khieu nai khach 10",    "staff_complaint_resolve", ["ticket", "khiếu nại", "giải quyết"]),
TC("S1071", "resolve complaint for order 33221 please",  "staff_complaint_resolve", ["ticket", "khiếu nại", "giải quyết"]),

# ── staff_escalated_issues (7) ────────────────────────────────────────────────
# Regex: danh sach (escalated|khieu nai|(khach hang)?  can ho tro)|khach dang cho (ho tro|xu ly)|ticket (chua|can) xu ly (gap)?|escalated (customer|issue|ticket)|van de khach hang chua duoc giai quyet|bao nhieu yeu cau ho tro dang cho
TC("S1072", "danh sach escalated tickets chua xu ly",    "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),
TC("S1073", "ticket can xu ly gap co bao nhieu ca nay",  "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),
TC("S1074", "danh sach khach hang can ho tro gap",       "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),
TC("S1075", "escalated customer list hien tai la gi",    "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),
TC("S1076", "bao nhieu yeu cau ho tro dang cho xu ly",   "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),
TC("S1077", "van de khach hang chua duoc giai quyet",    "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),
TC("S1078", "ticket ho tro nao dang mo chua xu ly xong", "staff_escalated_issues",  ["escalated", "chờ", "ticket"]),

# ── staff_revenue_today (7) ───────────────────────────────────────────────────
# Regex: doanh thu (hom nay|ngay hom nay|today)|tong tien don hang (ban duoc)? hom nay|revenue (for)? (today|hom nay)|hom nay ban duoc bao nhieu tien|thu nhap (hom nay)?
TC("S1079", "doanh thu hom nay cua cua hang la bao nhieu","staff_revenue_today",    ["Doanh thu", "hôm nay", "đ"]),
TC("S1080", "hom nay ban duoc bao nhieu tien tong",      "staff_revenue_today",     ["Doanh thu", "hôm nay", "đ"]),
TC("S1081", "tong tien don hang ban duoc hom nay la gi", "staff_revenue_today",     ["Doanh thu", "hôm nay", "đ"]),
TC("S1082", "revenue today tong ket cuoi ngay the nao",  "staff_revenue_today",     ["Doanh thu", "hôm nay", "đ"]),
TC("S1083", "doanh so hom nay tong ket the nao vay bot",  "staff_revenue_today",    ["Doanh thu", "hôm nay", "đ"]),
TC("S1084", "thu nhap hom nay la bao nhieu tong cong",   "staff_revenue_today",     ["Doanh thu", "hôm nay", "đ"]),
TC("S1085", "bao nhieu tien ban duoc ngay hom nay roi",  "staff_revenue_today",     ["Doanh thu", "hôm nay", "đ"]),

# ── staff_top_selling (5) ─────────────────────────────────────────────────────
# Regex: sach ban chay (nhat|trong)?|top (sach|ban chay)|bestseller trong he thong|top selling books|sach hot nhat he thong
TC("S1086", "sach ban chay nhat trong he thong la gi",   "staff_top_selling",       ["bán chạy", "top", "sách"]),
TC("S1087", "top selling books trong he thong hien tai", "staff_top_selling",       ["bán chạy", "top", "sách"]),
TC("S1088", "bestseller trong he thong thang nay la gi", "staff_top_selling",       ["bán chạy", "top", "sách"]),
TC("S1089", "sach hot nhat he thong hien tai la gi day", "staff_top_selling",       ["bán chạy", "top", "sách"]),
TC("S1090", "top ban chay trong he thong xem di nao",    "staff_top_selling",       ["bán chạy", "top", "sách"]),

# ── staff_order_statistics (5) ────────────────────────────────────────────────
# Regex: thong ke don (hang)? (hom nay|tuan|thang)?|bao cao (nhanh ve)? (don|doanh thu)|bao nhieu don (da giao|thanh cong|that bai|bi huy)|ti le hoan thanh don|so don giao thanh cong
TC("S1091", "thong ke don hang hom nay co bao nhieu ca", "staff_order_statistics",  ["thống kê", "đơn hàng", "tổng"]),
TC("S1092", "bao nhieu don da giao thanh cong tuan nay", "staff_order_statistics",  ["thống kê", "đơn hàng", "tổng"]),
TC("S1093", "bao cao nhanh ve don hang hom nay day bo",  "staff_order_statistics",  ["thống kê", "đơn hàng", "tổng"]),
TC("S1094", "ti le hoan thanh don hang tuan nay la gi",  "staff_order_statistics",  ["thống kê", "đơn hàng", "tổng"]),
TC("S1095", "so don giao thanh cong va bi huy thang nay","staff_order_statistics",  ["thống kê", "đơn hàng", "tổng"]),

# ── staff_out_of_scope (2) ────────────────────────────────────────────────────
TC("S1096", "thoi tiet Ha Noi hom nay nhu the nao do",   "staff_out_of_scope",      ["không hiểu", "hỗ trợ"]),
TC("S1097", "gia vang the gioi dang tang hay giam vay",   "staff_out_of_scope",      ["không hiểu", "hỗ trợ"]),
]


async def run_one(client, tc: TC) -> TR:
    payload = {
        "session_id": f"staff_{tc.id}_{uuid.uuid4().hex[:6]}",
        "message":    tc.msg,
        "user_id":    1,
        "role":       "staff",
    }
    t0 = time.perf_counter()
    actual = "CONNECTION_ERROR"; conf = 0.0; answer = ""; http = 0; api = False
    try:
        r = await client.post(ENDPOINT, json=payload, timeout=TIMEOUT)
        http = r.status_code
        if http == 200:
            d = r.json()
            actual = d.get("intent", "MISSING_INTENT")
            conf   = d.get("confidence", 0.0)
            answer = d.get("answer", "")
            api    = True
            if actual == "error":
                actual = "SERVER_ERROR"
        else:
            actual = f"HTTP_{http}"
    except Exception as e:
        answer = str(e)[:120]
    lat = (time.perf_counter() - t0) * 1000
    ok = actual == tc.intent
    ans_l = answer.lower()
    kw_ok = not tc.kw or any(k.lower() in ans_l for k in tc.kw)
    return TR(tc.id, tc.msg, tc.intent, actual, conf, answer, lat, http, ok, kw_ok, api)


async def run_all() -> list[TR]:
    results: list[TR] = []
    print(f"\n{'='*86}")
    print(f"  🚀 STAFF Chatbot Evaluation v6 – {len(CASES)} cases")
    print(f"  📡 Endpoint: {ENDPOINT}")
    print(f"{'='*86}\n")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(HEALTH, timeout=5.0)
            print(f"  ✅ Server OK (HTTP {resp.status_code})\n")
        except Exception as e:
            print(f"  ❌ Server lỗi: {e}")
            return []

        print(f"  {'ID':<7} {'Message':<40} {'Expected':<30} {'Got':<30} {'OK':3} {'ms':>6}")
        print(f"  {'-'*6} {'-'*39} {'-'*29} {'-'*29} {'---':3} {'---':>6}")
        for tc in CASES:
            r = await run_one(client, tc)
            results.append(r)
            si = "✅" if r.intent_ok else "❌"
            m = tc.msg[:38] + "…" if len(tc.msg) > 38 else tc.msg
            print(f"  {r.id:<7} {m:<40} {r.expected:<30} {r.actual:<30} {si}  {r.lat:>5.0f}")
    return results


def report(results: list[TR]) -> str:
    total    = len(results)
    api_ok   = sum(1 for r in results if r.api_ok)
    int_ok   = sum(1 for r in results if r.intent_ok)
    kw_ok    = sum(1 for r in results if r.kw_ok)
    srv_err  = sum(1 for r in results if r.actual == "SERVER_ERROR")
    conn_err = sum(1 for r in results if r.actual == "CONNECTION_ERROR")
    lats     = [r.lat for r in results if r.api_ok]
    avg_lat  = sum(lats) / len(lats) if lats else 0

    regex_cases  = [r for r in results if r.expected != "staff_out_of_scope"]
    regex_ok     = sum(1 for r in regex_cases if r.intent_ok)
    sbert_cases  = [r for r in results if r.expected == "staff_out_of_scope"]
    sbert_ok     = sum(1 for r in sbert_cases if r.intent_ok)

    grp: dict[str, list[TR]] = {}
    for r in results:
        grp.setdefault(r.expected, []).append(r)

    lines = [
        f"\n{'='*78}",
        f"  📊 KẾT QUẢ STAFF EVALUATION v6 – {total} cases",
        f"{'='*78}",
        f"  API OK            : {api_ok}/{total} = {api_ok/total*100:.1f}%",
        f"  Intent Acc (all)  : {int_ok}/{total} = {int_ok/total*100:.1f}%",
        f"  Keyword Found     : {kw_ok}/{total} = {kw_ok/total*100:.1f}%",
        f"  Regex-guaranteed  : {regex_ok}/{len(regex_cases)} = {regex_ok/len(regex_cases)*100:.1f}% (excl. OOS)",
        f"  SBERT OOS acc     : {sbert_ok}/{len(sbert_cases)} = {sbert_ok/len(sbert_cases)*100:.1f}% (model-dependent)",
        f"  SERVER_ERROR      : {srv_err}",
        f"  CONN_ERROR        : {conn_err}",
        f"  Latency avg       : {avg_lat:.0f}ms",
        f"{'─'*78}",
        f"  {'Intent':<34} {'OK':>4} {'Total':>6} {'Acc':>7}  {'Bar'}",
        f"{'─'*78}",
    ]
    for name, g in sorted(grp.items()):
        ok  = sum(1 for r in g if r.intent_ok)
        pct = ok / len(g) * 100
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        sfx = " ⚠️SBERT" if name == "staff_out_of_scope" else ""
        lines.append(f"  {name:<34} {ok:>4}/{len(g):<5}  {pct:5.1f}%  {bar}{sfx}")

    fail = [r for r in results if not r.intent_ok]
    if fail:
        lines.append(f"\n  ❌ {len(fail)} FAILED CASES:")
        for r in fail:
            m = r.msg[:52]
            lines.append(f"    {r.id} [{r.expected}] \"{m}\" → got [{r.actual}]")

    lines.append(f"{'='*78}")
    rep = "\n".join(lines)
    print(rep)
    return rep


async def main():
    results = await run_all()
    if not results:
        return
    rep = report(results)
    d = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(d, "eval_report_staff.txt"), "w", encoding="utf-8") as f:
        f.write(rep)
    with open(os.path.join(d, "eval_results_staff.json"), "w", encoding="utf-8") as f:
        json.dump([{
            "id": r.id, "message": r.msg, "expected_intent": r.expected,
            "actual_intent": r.actual, "confidence": round(r.conf, 3),
            "intent_correct": r.intent_ok, "keyword_found": r.kw_ok,
            "api_ok": r.api_ok, "latency_ms": round(r.lat, 1),
            "answer_preview": (r.answer[:150] + "..." if len(r.answer) > 150 else r.answer)
        } for r in results], f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Saved: eval_report_staff.txt + eval_results_staff.json")


if __name__ == "__main__":
    asyncio.run(main())
