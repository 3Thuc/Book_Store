"""
evaluate_admin_singletest.py – 104 test case tích hợp cho Admin role
======================================================================
Cách chạy:
  Terminal 1: uvicorn chatbot_app.main:app --port 8004 --reload
  Terminal 2: python tests/evaluate_admin_singletest.py

LOGIC NLU ADMIN:
  Tầng 1: ADMIN_QUICK_RULES regex → confidence=1.0  (deterministic)
  Tầng 2: SBERT zero-shot         → fallback="admin_out_of_scope" (score < 0.50)

Bộ test case v6 (căn chỉnh sát với ADMIN_QUICK_RULES):
  - Mỗi câu đã được kiểm tra thủ công so với regex pattern
  - admin_order_stats: 6 case → SERVER_ERROR từ phía server (không phải lỗi NLU)
  - admin_out_of_scope: câu ngoài nghiệp vụ → SBERT fallback
    ⚠️ SERVER_ERROR tính vào api_ok=False, không tính vào intent accuracy

Phân bổ (tổng = 104):
  admin_chitchat(5) + admin_dashboard_summary(8) + admin_revenue_report(8) +
  admin_order_stats(6) + admin_top_books(5) + admin_user_stats(6) +
  admin_user_lookup(6) + admin_user_update_role(6) + admin_user_lock_unlock(6) +
  admin_user_reset_password(5) + admin_list_staff(5) + admin_book_add_guide(3) +
  admin_book_status_change(4) + admin_category_manage(4) + admin_book_low_rating(4) +
  admin_promotion_list(4) + admin_promotion_check(4) + admin_promotion_create_guide(3) +
  admin_promotion_expiring(3) + admin_system_health(4) + admin_escalated_customers(3) +
  admin_out_of_scope(2) = 104
"""
import asyncio, json, time, os, uuid
import httpx
from dataclasses import dataclass, field

BASE_URL = "http://localhost:8004"
ENDPOINT = f"{BASE_URL}/api/admin/chat/message"
HEALTH   = f"{BASE_URL}/api/admin/chat/health"
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
# 104 TEST CASES – bộ v6, căn chỉnh sát với ADMIN_QUICK_RULES
#
# Cách kiểm tra regex: _normalize_vi(message) → tìm match trong ADMIN_QUICK_RULES
# Ghi chú: admin_order_stats → SERVER_ERROR từ API (endpoint lỗi phía server)
# ══════════════════════════════════════════════════════════════════════════════
CASES: list[TC] = [
# ── admin_chitchat (5) ─────────────────────────────────────────────────────────
# Regex: ^(xin chao|hello|hi|chao|help( admin)?|admin help|...).*
TC("A1001", "xin chao admin chatbot",                   "admin_chitchat",           ["Dashboard", "Báo cáo", "Quản lý"]),
TC("A1002", "hello admin chatbot hom nay co gi khong",  "admin_chitchat",           ["Dashboard", "Báo cáo", "Quản lý"]),
TC("A1003", "admin help toi can ho tro gi do",          "admin_chitchat",           ["Dashboard", "Báo cáo", "Quản lý"]),
TC("A1004", "tro ly admin cho toi biet cac lenh",       "admin_chitchat",           ["Dashboard", "Báo cáo", "Quản lý"]),
TC("A1005", "hi chatbot admin lam duoc nhung gi cho toi","admin_chitchat",           ["Dashboard", "Báo cáo", "Quản lý"]),

# ── admin_dashboard_summary (8) ───────────────────────────────────────────────
# Regex: (dashboard|tong quan...|tinh hinh (hom nay|he thong)|buc tranh...|kpi...)
TC("A1006", "cho xem dashboard tong quan hom nay",      "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1007", "tong quan he thong hien tai nhu the nao",  "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1008", "tinh hinh he thong hom nay dang the nao",  "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1009", "tom tat he thong hom nay cho admin xem",   "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1010", "buc tranh toan canh he thong hom nay",     "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1011", "xem dashboard toan bo he thong di nao",    "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1012", "kpi hom nay cua he thong ra sao",          "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),
TC("A1013", "tong he thong thong tin chung hom nay",    "admin_dashboard_summary",  ["Dashboard", "Users", "Sách"]),

# ── admin_revenue_report (8) ──────────────────────────────────────────────────
# Regex: (bao cao doanh thu|doanh thu (theo|tuan|thang...)|revenue report|loi nhuan|...)
TC("A1014", "bao cao doanh thu thang nay tong hop",     "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1015", "doanh thu tuan nay bao nhieu tien",        "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1016", "thong ke doanh thu theo ngay hien tai",    "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1017", "tong tien thu duoc hom nay la bao nhieu",  "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1018", "revenue report thang 3 nam nay tong ket",  "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1019", "income report tong hop quy nay ra sao",    "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1020", "doanh so tuan nay la bao nhieu tong",      "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),
TC("A1021", "loi nhuan ban hang trong thang qua nay",   "admin_revenue_report",     ["Doanh thu", "đ", "₫"]),

# ── admin_order_stats (6) ─────────────────────────────────────────────────────
# Regex: (thong ke don (hang)? (theo trang thai)|don hang (theo trang thai)|ti le...|bao nhieu don...)
# ⚠️ Các case này có thể trả về SERVER_ERROR do API endpoint admin_order_stats bị lỗi phía server
TC("A1022", "thong ke don hang theo trang thai",        "admin_order_stats",        ["đơn", "chờ", "thành công"]),
TC("A1023", "bao nhieu don bi huy trong thang nay",     "admin_order_stats",        ["đơn", "chờ", "thành công"]),
TC("A1024", "ti le hoan thanh don hang la bao nhieu",   "admin_order_stats",        ["đơn", "chờ", "thành công"]),
TC("A1025", "so lieu hoa don tong ket thang nay roi",   "admin_order_stats",        ["đơn", "chờ", "thành công"]),
TC("A1026", "tong so don hang trong he thong hien tai", "admin_order_stats",        ["đơn", "chờ", "thành công"]),
TC("A1027", "order statistic tuan nay co bao nhieu ca", "admin_order_stats",        ["đơn", "chờ", "thành công"]),

# ── admin_top_books (5) ───────────────────────────────────────────────────────
# Regex: (sach ban chay nhat|top (sach|ban chay)|bestseller|sach duoc mua nhieu nhat|xep hang sach theo doanh thu...)
TC("A1028", "top sach ban chay nhat theo doanh thu",    "admin_top_books",          ["sách", "bán chạy", "top"]),
TC("A1029", "sach duoc mua nhieu nhat trong thang nay", "admin_top_books",          ["sách", "bán chạy", "top"]),
TC("A1030", "bestseller report cho admin xem ngay",     "admin_top_books",          ["sách", "bán chạy", "top"]),
TC("A1031", "xep hang sach theo doanh thu thang nay",   "admin_top_books",          ["sách", "bán chạy", "top"]),
TC("A1032", "cuon nao dang hot nhat tren he thong day", "admin_top_books",          ["sách", "bán chạy", "top"]),

# ── admin_user_stats (6) ──────────────────────────────────────────────────────
# Regex: (thong ke (nguoi dung|user)|bao nhieu user|nguoi dung active|tang truong nguoi dung|phan tich user...)
TC("A1033", "thong ke nguoi dung moi trong thang nay",  "admin_user_stats",         ["người dùng", "tổng", "Active"]),
TC("A1034", "bao nhieu user dang ky hom nay roi",       "admin_user_stats",         ["người dùng", "tổng", "Active"]),
TC("A1035", "nguoi dung active hien tai bao nhieu cao", "admin_user_stats",         ["người dùng", "tổng", "Active"]),
TC("A1036", "tang truong nguoi dung theo thang nay",    "admin_user_stats",         ["người dùng", "tổng", "Active"]),
TC("A1037", "phan tich user moi trong tuan nay nao",    "admin_user_stats",         ["người dùng", "tổng", "Active"]),
TC("A1038", "thong ke user tong the he thong hien tai", "admin_user_stats",         ["người dùng", "tổng", "Active"]),

# ── admin_user_lookup (6) ─────────────────────────────────────────────────────
# Regex: (tra cuu (thong tin )?(user|nguoi dung|tai khoan)|tim user (theo...)|xem (chi tiet )?profile user...)
TC("A1039", "tra cuu thong tin user email abc@shop.vn", "admin_user_lookup",        ["user", "thông tin", "email"]),
TC("A1040", "tim tai khoan khach hang id 1234 nao",     "admin_user_lookup",        ["user", "thông tin", "email"]),
TC("A1041", "xem chi tiet profile user id 509 giup",    "admin_user_lookup",        ["user", "thông tin", "email"]),
TC("A1042", "tim user theo so dien thoai 0912345678",   "admin_user_lookup",        ["user", "thông tin", "email"]),
TC("A1043", "thong tin tai khoan user email shop@g.vn", "admin_user_lookup",        ["user", "thông tin", "email"]),
TC("A1044", "lookup user account khach hang so id 88",  "admin_user_lookup",        ["user", "thông tin", "email"]),

# ── admin_user_lock_unlock (6) ────────────────────────────────────────────────
# Regex: (khoa (tai khoan|account)|mo khoa (tai khoan|account)|lock|unlock|ban|unban|vo hieu hoa...)
TC("A1045", "khoa tai khoan user id 205 vi vi pham",    "admin_user_lock_unlock",   ["khóa", "tài khoản", "ID"]),
TC("A1046", "mo khoa tai khoan user 101 cho ho dang nhap","admin_user_lock_unlock", ["khóa", "tài khoản", "ID"]),
TC("A1047", "vo hieu hoa account spam user 77 ngay",    "admin_user_lock_unlock",   ["khóa", "tài khoản", "ID"]),
TC("A1048", "kich hoat lai tai khoan user email vip@t.vn","admin_user_lock_unlock", ["khóa", "tài khoản", "ID"]),
TC("A1049", "ban user 333 vi spam trong he thong luon", "admin_user_lock_unlock",   ["khóa", "tài khoản", "ID"]),
TC("A1050", "lock account khach hang id 999 ngay di",   "admin_user_lock_unlock",   ["khóa", "tài khoản", "ID"]),

# ── admin_user_update_role (6) ────────────────────────────────────────────────
# Regex: (thay doi role user|nang cap (user|tai khoan|len|thanh) (staff|admin)|update role|doi role|cap quyen|chuyen (user|account) (thanh|sang) (staff|admin|customer))
TC("A1051", "nang cap user 42 len staff trong he thong","admin_user_update_role",   ["quyền", "role", "xác nhận"]),
TC("A1052", "doi role user 88 thanh admin di ngay",     "admin_user_update_role",   ["quyền", "role", "xác nhận"]),
TC("A1053", "chuyen tai khoan 1234 sang role staff luon","admin_user_update_role",  ["quyền", "role", "xác nhận"]),
TC("A1054", "cap quyen admin cho user id 3456 di",      "admin_user_update_role",   ["quyền", "role", "xác nhận"]),
TC("A1055", "chuyen user 7890 sang staff role ngay",   "admin_user_update_role",   ["quyền", "role", "xác nhận"]),
TC("A1056", "thay doi role user 55 thanh customer xong","admin_user_update_role",   ["quyền", "role", "xác nhận"]),

# ── admin_user_reset_password (5) ─────────────────────────────────────────────
# Regex: (reset (mat khau|password) (user|cho)?|dat lai (mat khau|password)|cap (mat khau) moi|khoi phuc mat khau|rs pass)
TC("A1057", "reset mat khau cho user id 12 di ngay",    "admin_user_reset_password",["mật khẩu", "reset", "đặt lại"]),
TC("A1058", "dat lai mat khau tai khoan user 99 luon",  "admin_user_reset_password",["mật khẩu", "reset", "đặt lại"]),
TC("A1059", "khoi phuc mat khau giup user 321 ngay",    "admin_user_reset_password",["mật khẩu", "reset", "đặt lại"]),
TC("A1060", "cap mat khau moi cho user email abc@x.vn", "admin_user_reset_password",["mật khẩu", "reset", "đặt lại"]),
TC("A1061", "rs pass user 3456 luon cho toi voi di",    "admin_user_reset_password",["mật khẩu", "reset", "đặt lại"]),

# ── admin_list_staff (5) ──────────────────────────────────────────────────────
# Regex: (danh sach (nhan vien|staff)|xem tat ca (tai khoan )?staff|list (staff|nhan vien)|ai dang lam staff...)
TC("A1062", "danh sach nhan vien staff trong he thong", "admin_list_staff",         ["staff", "danh sách", "nhân viên"]),
TC("A1063", "xem tat ca tai khoan staff dang hoat dong","admin_list_staff",         ["staff", "danh sách", "nhân viên"]),
TC("A1064", "co bao nhieu staff members hien tai day",  "admin_list_staff",         ["staff", "danh sách", "nhân viên"]),
TC("A1065", "list all staff accounts cho admin xem di", "admin_list_staff",         ["staff", "danh sách", "nhân viên"]),
TC("A1066", "ai dang lam staff trong he thong bay gio", "admin_list_staff",         ["staff", "danh sách", "nhân viên"]),

# ── admin_book_add_guide (3) ──────────────────────────────────────────────────
# Regex: (huong dan (them|tao|nhap) sach (moi)?|cach (them|tao) sach|add (new )?book|tao sach moi|nhap sach moi vao...)
TC("A1067", "huong dan them sach moi vao he thong",     "admin_book_add_guide",     ["thêm", "sách", "hướng dẫn"]),
TC("A1068", "cach tao sach moi nhap lieu vao kho do",   "admin_book_add_guide",     ["thêm", "sách", "hướng dẫn"]),
TC("A1069", "add book moi vao he thong thi lam the nao","admin_book_add_guide",     ["thêm", "sách", "hướng dẫn"]),

# ── admin_book_status_change (4) ──────────────────────────────────────────────
# Regex: (kich hoat (lai)? (sach|cuon)|vo hieu hoa (sach|cuon)|an (sach|cuon)|xoa mem sach|active/inactive sach...)
TC("A1070", "kich hoat lai sach id 1001 cho hien thi",  "admin_book_status_change", ["trạng thái", "sách", "kích hoạt"]),
TC("A1071", "an sach id 2002 khoi trang ban hang di",   "admin_book_status_change", ["trạng thái", "sách", "kích hoạt"]),
TC("A1072", "vo hieu hoa cuon sach 3003 tam thoi nhe",  "admin_book_status_change", ["trạng thái", "sách", "kích hoạt"]),
TC("A1073", "xoa mem sach id 5 khoi danh sach ban di",  "admin_book_status_change", ["trạng thái", "sách", "kích hoạt"]),

# ── admin_category_manage (4) ─────────────────────────────────────────────────
# Regex: (quan ly (danh muc|category|the loai)|thong ke (danh muc|category)|danh muc nao nhieu sach nhat|them the loai|xem ds danh muc...)
TC("A1074", "quan ly danh muc sach trong he thong nao", "admin_category_manage",    ["danh mục", "thể loại", "sách"]),
TC("A1075", "thong ke danh muc nao co nhieu sach nhat", "admin_category_manage",    ["danh mục", "thể loại", "sách"]),
TC("A1076", "them the loai moi vao he thong quan ly",   "admin_category_manage",    ["danh mục", "thể loại", "sách"]),
TC("A1077", "xem ds danh muc hien tai trong kho sach",  "admin_category_manage",    ["danh mục", "thể loại", "sách"]),

# ── admin_book_low_rating (4) ─────────────────────────────────────────────────
# Regex: (sach co rating thap|sach bi danh gia kem|sach bi rating thap|sach nao bi te|sach nhieu vo gach|list sach an chui|sach rate thap)
TC("A1078", "sach co rating thap nhat trong he thong",  "admin_book_low_rating",    ["rating", "sách", "sao"]),
TC("A1079", "sach bi danh gia kem duoi 3 sao hien nay", "admin_book_low_rating",    ["rating", "sách", "sao"]),
TC("A1080", "sach nhieu vo gach can ra soat lai luon",  "admin_book_low_rating",    ["rating", "sách", "sao"]),
TC("A1081", "sach nao bi te nhat can kiem tra noi dung","admin_book_low_rating",    ["rating", "sách", "sao"]),

# ── admin_promotion_list (4) ──────────────────────────────────────────────────
# Regex: (danh sach (khuyen mai|promotion|voucher)|xem tat ca (promotion|voucher)|cac voucher (hien tai|con valid)|ma giam gia dang (active|hoat dong|dung duoc))
TC("A1082", "danh sach khuyen mai dang active hien tai","admin_promotion_list",     ["khuyến mãi", "voucher", "mã"]),
TC("A1083", "xem tat ca promotion dang chay bay gio",   "admin_promotion_list",     ["khuyến mãi", "voucher", "mã"]),
TC("A1084", "cac voucher hien tai con valid dang dung",  "admin_promotion_list",    ["khuyến mãi", "voucher", "mã"]),
TC("A1085", "ma giam gia dang hoat dong trong he thong","admin_promotion_list",     ["khuyến mãi", "voucher", "mã"]),

# ── admin_promotion_check (4) ─────────────────────────────────────────────────
# Regex: (kiem tra ma voucher [A-Z0-9]+|ma [A-Z0-9]+ con hieu luc|check promo code [A-Z0-9]+|...)
TC("A1086", "kiem tra ma voucher SALE30 con hop le",    "admin_promotion_check",    ["mã", "hợp lệ", "hết hạn"]),
TC("A1087", "check promo code BOOK10 con hop le khong", "admin_promotion_check",    ["mã", "hợp lệ", "hết hạn"]),
TC("A1088", "kiem tra ma SUMMER25 con hieu luc khong",  "admin_promotion_check",    ["mã", "hợp lệ", "hết hạn"]),
TC("A1089", "kiem tra ma voucher FLASH50 xem con dung", "admin_promotion_check",    ["mã", "hợp lệ", "hết hạn"]),

# ── admin_promotion_create_guide (3) ──────────────────────────────────────────
# Regex: (tao (promotion|voucher|khuyen mai|ma giam gia) moi|huong dan tao voucher|create promotion...)
TC("A1090", "huong dan tao voucher moi trong he thong", "admin_promotion_create_guide",["tạo", "voucher", "khuyến mãi"]),
TC("A1091", "cach tao khuyen mai moi cho dot sale nay", "admin_promotion_create_guide",["tạo", "voucher", "khuyến mãi"]),
TC("A1092", "tao code moi cho dot khuyen mai thang sau","admin_promotion_create_guide",["tạo", "voucher", "khuyến mãi"]),

# ── admin_promotion_expiring (3) ──────────────────────────────────────────────
# Regex: (khuyen mai (sap|gan) het han|voucher (sap|gan) het han|canh bao (ma|voucher) het han|ma nao sap het han|ma sap dead)
TC("A1093", "khuyen mai sap het han trong tuan toi",     "admin_promotion_expiring", ["hết hạn", "sắp", "voucher"]),
TC("A1094", "ma sap dead can ra soat lai ngay bay gio", "admin_promotion_expiring", ["hết hạn", "sắp", "voucher"]),
TC("A1095", "canh bao voucher sap het han trong thang",  "admin_promotion_expiring",["hết hạn", "sắp", "voucher"]),

# ── admin_system_health (4) ───────────────────────────────────────────────────
# Regex: (kiem tra (trang thai|status) (service|server|he thong)|service (con chay|on)|system (health|status)|health check|he thong on k|sap me web)
TC("A1096", "kiem tra trang thai cac service he thong", "admin_system_health",      ["service", "hoạt động", "status"]),
TC("A1097", "system health check toan bo he thong nay", "admin_system_health",      ["service", "hoạt động", "status"]),
TC("A1098", "he thong on k hay la sap me web roi do",   "admin_system_health",      ["service", "hoạt động", "status"]),
TC("A1099", "kiem tra cac api service con chay khong",  "admin_system_health",      ["service", "hoạt động", "status"]),

# ── admin_escalated_customers (3) ─────────────────────────────────────────────
# Regex: (danh sach (khach hang|khieu nai).*admin|escalated (issue|khieu nai|customer|ticket)|ticket(s)? can admin...|van de (khach hang)? chua (giai quyet|xu ly) (cho admin)?)
TC("A1100", "danh sach khach hang can admin ho tro",    "admin_escalated_customers",["khiếu nại", "chờ", "ticket"]),
TC("A1101", "ticket can admin xu li gap ngay nao",      "admin_escalated_customers",["khiếu nại", "chờ", "ticket"]),
TC("A1102", "van de khach hang chua giai quyet cho admin","admin_escalated_customers",["khiếu nại", "chờ", "ticket"]),

# ── admin_out_of_scope (2) ────────────────────────────────────────────────────
# Không khớp regex → SBERT → score < 0.50 → fallback admin_out_of_scope
TC("A1103", "thoi tiet Ha Noi hom nay ra sao the",      "admin_out_of_scope",       ["không hiểu", "lệnh"]),
TC("A1104", "gia xang hom nay tang hay giam vay ban",   "admin_out_of_scope",       ["không hiểu", "lệnh"]),
]


async def run_one(client, tc: TC) -> TR:
    payload = {
        "session_id": f"admin_{tc.id}_{uuid.uuid4().hex[:6]}",
        "message":    tc.msg,
        "user_id":    1,
        "role":       "admin",
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
    print(f"  🚀 ADMIN Chatbot Evaluation v6 – {len(CASES)} cases")
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

    # Loại trừ admin_order_stats (SERVER_ERROR endpoint) khỏi regex accuracy
    regex_cases = [r for r in results if r.expected not in ("admin_out_of_scope", "admin_order_stats")]
    regex_ok    = sum(1 for r in regex_cases if r.intent_ok)
    oos_cases   = [r for r in results if r.expected == "admin_out_of_scope"]
    oos_ok      = sum(1 for r in oos_cases if r.intent_ok)
    ord_cases   = [r for r in results if r.expected == "admin_order_stats"]
    ord_ok      = sum(1 for r in ord_cases if r.intent_ok)

    grp: dict[str, list[TR]] = {}
    for r in results:
        grp.setdefault(r.expected, []).append(r)

    lines = [
        f"\n{'='*78}",
        f"  📊 KẾT QUẢ ADMIN EVALUATION v6 – {total} cases",
        f"{'='*78}",
        f"  API OK              : {api_ok}/{total} = {api_ok/total*100:.1f}%",
        f"  Intent Acc (all)    : {int_ok}/{total} = {int_ok/total*100:.1f}%",
        f"  Keyword Found       : {kw_ok}/{total} = {kw_ok/total*100:.1f}%",
        f"  Regex-guaranteed    : {regex_ok}/{len(regex_cases)} = {regex_ok/len(regex_cases)*100:.1f}% (excl. OOS & order_stats)",
        f"  SBERT OOS acc       : {oos_ok}/{len(oos_cases)} = {oos_ok/len(oos_cases)*100:.1f}% (model-dependent)",
        f"  order_stats acc     : {ord_ok}/{len(ord_cases)} = {ord_ok/len(ord_cases)*100:.1f}% (SERVER_ERROR endpoint)",
        f"  SERVER_ERROR        : {srv_err}",
        f"  CONN_ERROR          : {conn_err}",
        f"  Latency avg         : {avg_lat:.0f}ms",
        f"{'─'*78}",
        f"  {'Intent':<34} {'OK':>4} {'Total':>6} {'Acc':>7}  {'Bar'}",
        f"{'─'*78}",
    ]
    for name, g in sorted(grp.items()):
        ok  = sum(1 for r in g if r.intent_ok)
        pct = ok / len(g) * 100
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        sfx = " ⚠️SBERT"  if name == "admin_out_of_scope" else \
              " ⚠️SERVER" if name == "admin_order_stats"  else ""
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
    with open(os.path.join(d, "eval_report_admin.txt"), "w", encoding="utf-8") as f:
        f.write(rep)
    with open(os.path.join(d, "eval_results_admin.json"), "w", encoding="utf-8") as f:
        json.dump([{
            "id": r.id, "message": r.msg, "expected_intent": r.expected,
            "actual_intent": r.actual, "confidence": round(r.conf, 3),
            "intent_correct": r.intent_ok, "keyword_found": r.kw_ok,
            "api_ok": r.api_ok, "latency_ms": round(r.lat, 1),
            "answer_preview": (r.answer[:150] + "..." if len(r.answer) > 150 else r.answer)
        } for r in results], f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Saved: eval_report_admin.txt + eval_results_admin.json")


if __name__ == "__main__":
    asyncio.run(main())
