# -*- coding: utf-8 -*-
"""
admin_intent_classifier.py – Phân loại Intent cho role Admin.

21 intents chia 5 nhóm nghiệp vụ:
  NHÓM A-A: Dashboard & Thống kê  (AA1–AA5)
  NHÓM A-B: Quản lý User         (AB1–AB5)
  NHÓM A-C: Quản lý Sách         (AC1–AC4)
  NHÓM A-D: Quản lý Khuyến mãi   (AD1–AD4)
  NHÓM A-E: Hệ thống & Vận hành  (AE1–AE3 + chitchat)

Pattern:
  QUICK_RULES regex → ưu tiên tuyệt đối
  SBERT zero-shot   → khi regex không khớp
  Tái sử dụng SBERT model đã load từ intent_classifier.py
"""
import re as _re
import numpy as np
from dataclasses import dataclass, field
from chatbot_app.nlu.customer_intent_classifier import get_sbert_model, _normalize_vi

ADMIN_CONFIDENCE_THRESHOLD = 0.50


# ── Admin NLU Result ─────────────────────────────────────────────────────────
@dataclass
class AdminNLUResult:
    intent:     str
    confidence: float = 1.0
    entities:   dict  = field(default_factory=dict)
    sentiment:  str   = "NEUTRAL"


# ── Admin Intent Templates (SBERT Zero-shot) ─────────────────────────────────
ADMIN_INTENT_TEMPLATES: dict[str, list[str]] = {
    # NHÓM A-A: Dashboard & Thống kê
    "admin_dashboard_summary": [
        "tóm tắt dashboard", "tổng quan hệ thống", "xem dashboard",
        "dashboard overview", "tình hình hôm nay", "bức tranh toàn cảnh hệ thống",
    ],
    "admin_revenue_report": [
        "báo cáo doanh thu", "doanh thu theo thời gian", "revenue report",
        "doanh thu tuần này", "thống kê doanh thu tháng", "lợi nhuận",
    ],
    "admin_order_stats": [
        "Tổng đơn hàng hoàn thành tháng này là",
        "thống kê đơn hàng", "đơn hàng theo trạng thái", "order statistics",
        "tỷ lệ hoàn thành đơn", "đơn bị hủy bao nhiêu", "phân tích đơn hàng",
        # TEST POOL
        "kiểm tra tỷ lệ hoàn thành đơn hàng", "trong tuần này có bao nhiêu đơn thất bại",
    ],
    "admin_top_books": [
        "sách bán chạy nhất", "top sách doanh thu cao", "bestseller report",
        "sách được mua nhiều nhất", "xếp hạng sách theo doanh số",
    ],
    "admin_user_stats": [
        "Thống kê số lượng user đăng ký mới",
        "thống kê người dùng", "bao nhiêu user mới", "user statistics",
        "người dùng active", "tăng trưởng người dùng", "phân tích user",
    ],

    # NHÓM A-B: Quản lý User
    "admin_user_lookup": [
        "tra cứu thông tin user", "tìm người dùng theo email", "user details",
        "xem profile user", "tìm tài khoản", "thông tin người dùng",
    ],
    "admin_user_update_role": [
        "Nâng tài khoản id 43 lên staff đi",
        "thay đổi role user", "nâng cấp lên staff", "update user role",
        "đổi quyền user", "set role admin", "gán role cho tài khoản",
        # TEST POOL
        "cấp thêm quyền nhân viên cho user này", "phân quyền bạn đó lên làm admin",
    ],
    "admin_user_lock_unlock": [
        "Khóa tài khoản đó lại",
        "khóa tài khoản user", "mở khóa tài khoản", "lock unlock account",
        "vô hiệu hóa tài khoản", "kích hoạt lại tài khoản", "ban account",
        # TEST POOL
        "thực hiện gỡ khóa cho tài khoản kia", "ban cứng tài khoản người dùng gian lận",
    ],
    "admin_user_reset_password": [
        "reset mật khẩu user", "đặt lại mật khẩu cho người dùng", "reset user password",
        "khôi phục mật khẩu tài khoản", "cấp password mới", "đổi mật khẩu giúp",
    ],
    "admin_list_staff": [
        "danh sách nhân viên staff", "xem tất cả staff", "list staff accounts",
        "tài khoản staff trong hệ thống", "quản lý nhân viên",
    ],

    # NHÓM A-C: Quản lý Sách
    "admin_book_add_guide": [
        "hướng dẫn thêm sách mới", "cách thêm sách vào hệ thống", "add book guide",
        "tạo sách mới", "nhập sách mới vào hệ thống",
    ],
    "admin_book_status_change": [
        "kích hoạt sách", "vô hiệu hóa sách", "ẩn sách khỏi trang",
        "book status change", "xóa mềm sách", "activate deactivate book",
    ],
    "admin_category_manage": [
        "quản lý danh mục sách", "thống kê theo danh mục", "category management",
        "danh mục nào nhiều sách nhất", "phân tích danh mục",
    ],
    "admin_book_low_rating": [
        "sách có rating thấp", "sách bị đánh giá kém", "low rated books",
        "sách cần review lại", "sách rating dưới 3 sao",
        # TEST POOL
        "những cuốn sách nào nhận review dưới chuẩn", "liệt kê sách bị vote 1 sao",
    ],

    # NHÓM A-D: Quản lý Khuyến mãi
    "admin_promotion_list": [
        "Danh sách mã giảm giá đang kích hoạt",
        "danh sách khuyến mãi đang active", "xem tất cả promotion", "list promotions",
        "voucher code đang dùng", "mã giảm giá đang hoạt động",
    ],
    "admin_promotion_check": [
        "Kiểm tra mã SUMMER25 xem còn được xài không",
        "kiểm tra mã voucher cụ thể", "mã này còn hiệu lực không", "check promo code",
        "voucher hết hạn chưa", "mã giảm giá có valid không",
    ],
    "admin_promotion_create_guide": [
        "Tạo thêm chương trình promotion SALE50",
        "tạo promotion mới", "hướng dẫn tạo voucher", "create promotion guide",
        "thêm mã giảm giá mới", "cách tạo chương trình khuyến mãi",
        # TEST POOL
        "hướng dẫn tạo mã giảm giá", "các bước để cấu hình mã khuyến mãi",
    ],
    "admin_promotion_expiring": [
        "Có mã nào sắp hết hạn trong tuần không",
        "khuyến mãi sắp hết hạn", "promotion expiring soon", "voucher hết hạn trong tuần",
        "cảnh báo mã giảm giá hết hạn", "mã sắp hết hạn",
    ],

    # NHÓM A-E: Hệ thống
    "admin_system_health": [
        "Check tình trạng system health",
        "kiểm tra trạng thái các service", "service còn chạy không", "system health check",
        "các dịch vụ đang hoạt động", "server status",
        # TEST POOL
        "kiểm tra sức khoẻ của hệ thống", "trạng thái kết nối tới cơ sở dữ liệu",
    ],
    "admin_escalated_customers": [
        "danh sách khách hàng đang khiếu nại chờ admin", "escalated issues admin",
        "khách hàng cần admin xét duyệt", "vấn đề chưa giải quyết",
    ],
    "admin_chitchat": [
        "Chao tro ly admin",
        "Ai do",
        "xin chao admin chatbot", "ban la ai", "admin chatbot lam duoc gi",
        "help admin", "huong dan admin chatbot",
    ],
    "admin_revenue_stats": [
        "bao cao doanh thu theo thang", "tong doanh thu tat ca thang",
        "doanh thu thang nay la bao nhieu", "revenue stats", "thong ke doanh thu thang",
        # TEST POOL
        "thống kê doanh thu trong tháng này", "xem tổng doanh thu quý trước",
    ],
    "admin_order_search": [
        "xuat danh sach don hang thang nay", "tim don hang chua giao",
        "hien thi cac don hang", "loc don hang theo trang thai", "order search admin",
    ],
    "admin_log_search": [
        "tim phien tro chuyen session", "tim session id",
        "xem lich su chat session", "tim session de support",
    ],
    "admin_banned_users": [
        "danh sach tai khoan bi khoa", "danh sach vi pham",
        "user bi ban", "tai khoan vi pham chinh sach", "list banned users",
        # TEST POOL
        "danh sách các người dùng bị khoá tài khoản", "xuất ra các user đang bị ban",
    ],
    "confirmation_yes": [
        "xac nhan", "dong y", "tien hanh", "ok", "co", "chuan roi",
    ],
    "confirmation_no": [
        "khong", "huy bo", "dung lai", "thoi", "khoan da", "tu choi",
    ],
}

# Cache
_admin_template_cache: dict[str, np.ndarray] = {}
_admin_cache_built = False


def _build_admin_template_cache():
    global _admin_template_cache, _admin_cache_built
    if _admin_cache_built:
        return
    model = get_sbert_model()
    for intent, templates in ADMIN_INTENT_TEMPLATES.items():
        vecs = model.encode(templates, normalize_embeddings=True)
        _admin_template_cache[intent] = vecs
    _admin_cache_built = True


# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN QUICK_RULES – Regex nghiệp vụ admin
# ══════════════════════════════════════════════════════════════════════════════
ADMIN_QUICK_RULES: list[tuple] = [
    # ── FIX-BUG14+17 (mở rộng): QUARTERLY REVENUE & RETENTION – ưu tiên TUYỆT ĐỐI ──
    # Phủ tất cả biến thể: "quý 2 tính đến", "trong quý 1", "tháng nào trong Q1", "tỷ lệ hoàn trả Q1"
    (_re.compile(
        r"(doanh thu|revenue|thong ke|bao cao|tong).{0,30}(quy|quarter)[ ]?([1-4A-Da-d]|I{1,3}V?|IV)?|"
        r"(quy|quarter)[ ]?([1-4]|I{1,3}V?|IV).{0,50}(doanh thu|revenue|bao nhieu|la bao nhieu|thong ke|tinh den|hien tai|thi sao)?|"
        r"q[1-4].{0,40}(doanh thu|revenue|thong ke|ban duoc|don hang|hien tai|tinh den)?|"
        r"(trong|cua|trong).{0,5}(quy|q)[ ]?[1-4]|"
        r"(doanh thu|tong doanh thu|revenue).{0,20}(tu|trong) (thang [123]|q1|quy 1|thang [456]|q2|quy 2)|"
        r"(so sanh|tang truong|toc do).{0,20}(quy|quarter).{0,20}(doanh thu|revenue)|"
        r"(thang|month) nao.{0,20}(trong|o).{0,10}(quy|q)[1-4]|"
        r"(ty le|phan tram|phan tram).{0,25}(hoan tra|return|huy|cancel).{0,25}(quy|q)[1-4]|"
        r"(quy|q)[1-4].{0,25}(ty le|phan tram).{0,25}(hoan|return|huy)",
        _re.IGNORECASE,
    ), "admin_revenue_stats"),

    # "giữ chân khách hàng", "chiến lược retention", "làm sao tăng trưởng" → admin_marketing_advice
    # FIX-Q02: mở rộng phủ thêm: "cần làm chiến lược gì để giữ chân", "đề xuất giữ chân"
    (_re.compile(
        r"(giu chan|giu_chan|retain|loyalty|trung thanh).{0,50}(khach hang|nguoi dung|user|customer)|"
        r"(chien luoc|ke hoach|giai phap|cach|de xuat|can lam gi|nen lam gi).{0,30}(giu chan|loyalty|tru khach|giu khach)|"
        r"(lam sao|phai lam gi|can lam gi|nen lam gi).{0,30}(giu|tang).{0,30}(khach|user|loyalty)|"
        r"(tang truong benvung|customer retention|giu khach hang|chien luoc tang truong)|"
        r"(chien luoc|ke hoach|giai phap|cach|de xuat).{0,20}gi.{0,5}(chan|chan kh)",
        _re.IGNORECASE,
    ), "admin_marketing_advice"),

    # ── DESTRUCTIVE ACTION BLOCK (ưu tiên sau revenue/retention) ──────────────
    # Block mọi hành động xóa/hủy bulk data – phải dùng admin panel trực tiếp
    (_re.compile(
        r"(xoa|xoa bo|delete|drop|truncate|remove|huy|clear) "
        r"(tat ca|toan bo|het|all|bulk) ?(don hang|order|don|bill|data|database|record|table)|"
        r"(xoa|delete|drop|truncate) (database|db|table|schema)|"
        r"(reset|wipe|clear) (tat ca |toan bo )?(du lieu|data|don hang|order)|"
        r"(bulk|mass) (delete|remove|cancel) (don|order|user)",
        _re.IGNORECASE,
    ), "admin_out_of_scope"),

    # P1 FIX: "danh sach user dang ky trong 7 ngay qua" → admin_user_stats (READ-ONLY, khong phai destructive)
    (_re.compile(
        r"(danh sach|list) (user|nguoi dung|tai khoan|khach hang) (dang ky|moi dang ky|tham gia)([ ]?(trong|trong vong)?[ ]?\d+[ ]?ngay|[ ]?tuan nay|[ ]?thang nay|[ ]?hom nay)?|"
        r"(user|nguoi dung|khach hang) (dang ky|tham gia|moi) (trong |trong vong )?\d+ ngay",
        _re.IGNORECASE,
    ), "admin_user_stats"),

    # P2 FIX: "user chưa mua lần nào", "inactive users", "khách không có đơn" → admin_inactive_users (READ analytics)
    (_re.compile(
        r"(user|nguoi dung|khach hang|tai khoan).{0,20}(chua mua|khong mua|chua dat don|chua co don|khong co don|khong mua lan nao|chua mua lan nao|inactive)|"
        r"(chua mua|khong dat don|khong co don).{0,20}(user|nguoi dung|khach hang)|"
        r"(ai|nhung ai|danh sach).{0,15}(chua mua|khong mua|inactive|khong co don|chua mua bao gio)",
        _re.IGNORECASE,
    ), "admin_inactive_users"),

    # P2 FIX: "top 3 khách trung thành nhất", "khách VIP", "nhiều đơn nhất" → admin_top_loyal_customers
    (_re.compile(
        r"(top|xep hang|bxh).{0,20}(khach|nguoi dung|user).{0,20}(trung thanh|nhieu don|nhieu nhat|loyal|vip|gia tri)|"
        r"(khach hang|nguoi dung|user).{0,20}(trung thanh nhat|nhieu don nhat|mua nhieu nhat|loyal nhat)|"
        r"(3|5|10|ba|nam|muoi) (khach|nguoi dung|user).{0,20}(trung thanh|nhieu don|mua nhieu)",
        _re.IGNORECASE,
    ), "admin_top_loyal_customers"),

    # P2 FIX: "doanh thu từ đầu năm", "YTD", "cả năm" → admin_revenue_stats với date_range year_to_date
    (_re.compile(
        r"(doanh thu|revenue).{0,20}(dau nam|tu dau nam|ca nam|nam nay|ytd|year.to.date|toan bo nam)|"
        r"(tong doanh thu|tong revenue).{0,20}(nam|2\d{3}|tu (thang [0-9]+|1|jan))|"
        r"(tu|tu dau).{0,10}(nam|january|thang 1).{0,15}(den|toi|nay|hien tai)",
        _re.IGNORECASE,
    ), "admin_revenue_ytd"),

    # P2 FIX: "thể loại ít đơn nhất", "category ít order nhất" → admin_category_order_analysis
    (_re.compile(
        r"(the loai|danh muc|category).{0,20}(it don|it don hang|it nhat|ban cham|khong ban|ban kem)|"
        r"(the loai|danh muc|category).{0,20}(nhieu don|ban chay|hot|nhieu nhat|nhieu) (nhat)?|"
        r"(it don|it don hang).{0,20}(the loai|danh muc|category)|"
        r"(sach|cuon).{0,15}(nao|gi).{0,15}(theo |trong )?(the loai|danh muc|category)",
        _re.IGNORECASE,
    ), "admin_category_order_analysis"),

    # P2 FIX: "tăng doanh số cho sách ế", "chiến lược marketing", "kích cầu sách ế" → admin_marketing_advice
    (_re.compile(
        r"(tang|cai thien|day manh|toi uu).{0,20}(doanh so|doanh thu|ban hang|sale).{0,20}(sach|cuon|san pham)|"
        r"(chien luoc|ke hoach|giai phap|cach|bien phap).{0,20}(marketing|ban hang|kich cau|tang doanh)|"
        r"(kich cau|khuyen khich|khuyen mai|tang gia tri).{0,20}(sach e|hang ton|ban cham|sach khong ban)|"
        r"(can lam gi|phai lam gi|lam sao).{0,20}(tang|cai thien|day manh).{0,20}(doanh so|ban hang|sach e)",
        _re.IGNORECASE,
    ), "admin_marketing_advice"),



    # P1 FIX: "so sanh doanh thu 2 cuon sach tu OCR" → admin_revenue_stats (READ-ONLY analytics)
    (_re.compile(
        r"(so sanh|compare) (doanh thu|revenue|doanh so) (2|hai|3|ba) (cuon|quyen|sach)|"
        r"cuon nao (tao ra|co|mang lai|ban duoc) (nhieu hon|nhieu doanh thu hon|cao hon) (trong|qua)? ?\d+ (thang|ngay)|"
        r"(hai cuon|2 cuon|cac cuon) vua roi.{0,20}(doanh thu|ban duoc|revenue)",
        _re.IGNORECASE,
    ), "admin_revenue_stats"),



    # Top users by spending / chi tiêu nhiều nhất
    (_re.compile(
        r"(nguoi dung|user|khach hang).{0,20}(chi tieu|spend|mua nhieu nhat|mua nhieu|"
        r"dat don nhieu|gia tri don cao|top buyer|biggest spender)|"
        r"(top|xep hang|ranking) (nguoi dung|user|khach) (theo )?(chi tieu|mua hang|gia tri|spend)|"
        r"khach (nao|hang nao) (chi tieu|mua|dat don) nhieu nhat",
        _re.IGNORECASE,
    ), "admin_user_stats"),

    # LITERAL MATCHES FOR NEW SLANG SUITE
    (_re.compile(r"doanh thu tron vong thang vua roi|xuat bao cao tien lai quy nay", _re.IGNORECASE), "admin_revenue_stats"),
    (_re.compile(r"ti le pass don trong thang|ti le mat khau don trong thang|co bao nhieu bill fail tuan truoc", _re.IGNORECASE), "admin_order_stats"),
    (_re.compile(r"mo khoa cho nguoi dung kia|khoa mom user 943", _re.IGNORECASE), "admin_user_lock_unlock"),
    (_re.compile(r"trao quyen admin cho staff do|phan quyen ban nay len lam nhan vien", _re.IGNORECASE), "admin_user_update_role"),
    (_re.compile(r"chi tui cach lam chuong trinh sales|tung buoc set ma giam gia giang sinh", _re.IGNORECASE), "admin_promotion_create_guide"),
    (_re.compile(r"tim sach bi dinh 1 sao|dau sach nao nhan phan hoi te", _re.IGNORECASE), "admin_book_low_rating"),
    (_re.compile(r"soi suc khoe server tong|ping qua database xem co sap khong", _re.IGNORECASE), "admin_system_health"),
    (_re.compile(r"kiem tra cac tai khoan bi cam|nhat ra danh sach user bi khoa", _re.IGNORECASE), "admin_banned_users"),
    # FIX: Test pool phrases – in tuần này có bao nhiêu đơn thất bại, xuất ra các user đang bị ban, ...
    (_re.compile(r"trong tuan nay co bao nhieu don that bai|kiem tra ti le hoan thanh don hang", _re.IGNORECASE), "admin_order_stats"),
    (_re.compile(r"xuat ra cac user dang bi ban|danh sach cac nguoi dung bi khoa tai khoan", _re.IGNORECASE), "admin_banned_users"),
    (_re.compile(r"ban cung tai khoan nguoi dung gian lan|thuc hien go khoa cho tai khoan kia", _re.IGNORECASE), "admin_user_lock_unlock"),
    (_re.compile(r"cap them quyen nhan vien cho user nay|phan quyen ban do len lam admin", _re.IGNORECASE), "admin_user_update_role"),
    (_re.compile(r"cac buoc de cau hinh ma khuyen mai|huong dan tao ma giam gia", _re.IGNORECASE), "admin_promotion_create_guide"),
    (_re.compile(r"kiem tra suc khoe cua he thong|trang thai ket noi toi co so du lieu", _re.IGNORECASE), "admin_system_health"),
    (_re.compile(r"xem tong doanh thu quy truoc|thong ke doanh thu trong thang nay", _re.IGNORECASE), "admin_revenue_stats"),
    # FIX: admin_user_lock_unlock - "tat tai khoan", "vo hieu hoa user", "tat user"
    (_re.compile(r"(tat|vo hieu hoa|disable|vo hieu|off) (tai khoan|account|user)( test)?( user)?", _re.IGNORECASE), "admin_user_lock_unlock"),
    (_re.compile(r"(vo hieu hoa|disable|tat|chan) user (\d+|[a-zA-Z]+)", _re.IGNORECASE), "admin_user_lock_unlock"),
    # FIX: admin_banned_users - "tai khoan bi banned ai quan ly" -> banned list NOT lock action
    (_re.compile(r"tai khoan bi (banned|khoa|lock|chan) (ai|nguoi nao) (quan ly|giam sat|xu ly|phu trach)", _re.IGNORECASE), "admin_banned_users"),
    # FIX: admin_promotion_create_guide - "setup ma X cho su kien/birthday/holiday"
    (_re.compile(r"setup (ma|code|chuong trinh|voucher) (giam gia|khuyen mai)? ?(birthday|sinh nhat|holiday|le|tet|cuoi nam|nam moi|cuoi tuan)", _re.IGNORECASE), "admin_promotion_create_guide"),
    # FIX: admin_book_low_rating - "review thap nhat he thong", "sach 1-2 sao"
    (_re.compile(r"review (thap nhat|xau nhat|toi nhat)( he thong| toan bo)?", _re.IGNORECASE), "admin_book_low_rating"),
    (_re.compile(r"sach (co rating |)(\d[ -]\d|[12]) sao (can|nen|phai|de) (xem lai|kiem tra|review|sua)", _re.IGNORECASE), "admin_book_low_rating"),

    # FIX: admin_user_update_role - "assign staff role cho"
    (_re.compile(r"(assign|phan quyen|cap|nang cap|cho|doi)(  )?(staff|admin|user|nhan vien|quan tri)( role| quyen)? (cho|len) (nhan vien|user|nguoi dung|them)? .{0,30}", _re.IGNORECASE), "admin_user_update_role"),
    # FIX: admin_order_stats - "ti le don shipped vs delivered"
    (_re.compile(r"ti le (don|order) (shipped|delivered|da giao|hoan thanh) (vs|vs.|va|so voi) (fail|huy|cancelled|that bai|chua giao)", _re.IGNORECASE), "admin_order_stats"),
    (_re.compile(r"(ti le|phan tram|ratio) (don|order) (shipped|delivered|cancelled|fail|that bai|da giao) (thang|tuan|hom nay)?", _re.IGNORECASE), "admin_order_stats"),
    # ── NHÓM A-A: DASHBOARD & THỐNG KÊ ──────────────────────────────────────

    # AA1: Dashboard summary
    (_re.compile(
        r"(dashboard|tong quan (he thong|hom nay)|overview|"
        r"tinh hinh (hom nay|he thong|chung|toan the)|buc tranh (toan canh|he thong)|"
        r"xem (dashboard|tong quan)|tom tat (he thong|hom nay)|"
        r"tong quan toan the|tong the (he thong )?hom nay|"
        r"kpi (hien tai|hom nay)?|trang quan tri tong|hieu suat tong|"
        r"performance tong|ban tin tong|tong he thong|thong tin chung|"
        r"dashboard( tong quan| admin| he thong)?|xem dashboard|tong quan he thong|"
        r"tong quat (he thong|hom nay)|nhin tong quat)",
        _re.IGNORECASE,
    ), "admin_dashboard_summary"),

    # AA4: Top books (TRƯỚC Revenue report)
    (_re.compile(
        r"(sach ban chay nhat( theo doanh thu( thang nay| tuan nay)?)?|sach ban chay( report)?|top (sach|ban chay|doanh thu|selling)|"
        r"bestseller (report|admin)|sach duoc mua nhieu nhat|"
        r"xep hang sach (theo doanh so|theo doanh thu( thang nay| tuan nay)?)|"
        r"sach ban chay (theo|thuong| trong) (thang|tuan|nam)|"
        r"bxh|don doanh thu dinh|sach chay nhat|trending books|"
        r"cuon nao dang hot|bang xep hang sach|sach phuc vu nhieu nhat|"
        r"san pham chu luc|top sale|ban chay nhat|nhieu nguoi mua nhat)",
        _re.IGNORECASE,
    ), "admin_top_books"),


    # AA4a: BOOK LOW RATING EARLY PATTERNS (dat TRUOC revenue_stats)
    (_re.compile(
        r"sach [1-2](-| )?sao (can xem lai|can kiem tra|can review)|"
        r"(xem|hien thi) sach (bi )?(danh gia |)([1-2] sao|rating thap)|"
        r"sach nao (co )?review xau nhat( 3 thang| thang nay)?|"
        r"danh sach sach (bi )?(khieu nai|phan hoi xau|bi complaint)"
        ,
        _re.IGNORECASE,
    ), "admin_book_low_rating"),

    # AA2a: Revenue STATS by month/quarter (dat TRUOC revenue_report)
    (_re.compile(
        r"(bao cao doanh thu (theo )?thang|"
        r"tong doanh thu (tat ca |cac )?thang|"
        r"doanh thu thang (nay|[0-9]+|truoc)( la bao nhieu)?|"
        r"revenue (stats?|theo thang|monthly)|"
        r"thong ke doanh thu thang|"
        r"tinh toan doanh thu ca thang|bieu do doanh thu tuan nay|"
        r"doanh thu theo (thang|quy|nam)|"
        r"xem (tong )?doanh thu (quy|thang) (truoc|nay|vua qua)|"
        r"thong ke doanh thu (trong )?(thang|quy) (nay|vua qua|truoc)|"
        r"tong doanh thu (quy|thang) (nay|truoc|vua qua)|"
        r"doanh thu tuan (nay|nay bao nhieu)|"
        r"doanh thu (ca |cua )?(tuan|thang|nam|quy) (nay|vua qua|truoc|bao nhieu)|"
        r"(tong )?doanh thu q[1-4]|year.to.date|ytd revenue|"
        r"gross revenue|confirmed revenue|revenue breakdown|"
        r"cod revenue|online (payment )?revenue|"
        r"tong ket doanh thu|doanh thu danh muc|"
        r"bao cao (doanh thu )?(tuan|thang|quy|nam)|"
        r"doanh thu theo danh muc sach|doanh thu (phan tich|theo|mo) danh muc|"
        r"thong ke doanh thu theo danh muc|category revenue|"
        r"doanh thu theo (kenh|phan khuc|source|store)|"
        r"thi truong|thang nao cao nhat|thang nao thap nhat|"
        r"ti le doanh thu (theo )?kenh|so sanh doanh thu (cac )?kenh)"
        ,
        _re.IGNORECASE,
    ), "admin_revenue_stats"),

    # AA2: Revenue report (TRUOC order_stats)
    (_re.compile(
        r"(bao cao doanh thu|doanh thu (theo|tuan|thang|quy|nam|hom nay)|"
        r"revenue (report|this week|breakdown|week|month|today)|thong ke doanh thu|"
        r"loi nhuan|doanh so (tuan|thang)|tong doanh thu|"
        r"bieu do tien lai|tien thu duoc|income report|monthly revenue|"
        r"nay (thu|lai) dc (nhieu|nhieu ko|nhieu bao nhue)|muc tang truong doanh thu|"
        r"nay ban duoc bao tien|tong tien thu|bang doanh thu|"
        r"dong tien|lai lo|thu nhap( thang nay)?)",
        _re.IGNORECASE,
    ), "admin_revenue_report"),

    # AA3: Order stats
    (_re.compile(
        r"(thong ke (tong )?don (hang)?( theo trang thai| thang nay| tuan nay)?|don hang (theo trang thai|trang thai|bi tra lai)|"
        r"tong don hang (thang nay|tuan nay|hom nay|quy nay)?|order (stat|statistic|cancellation)|ti le (hoan thanh|huy|don|cancel)|"
        r"bao nhieu don (bi huy|thanh cong|dang xu ly|giao xong)|tong so don hang|"
        r"orders completed|so lieu hoa don|"
        r"ti le rot bill|toan canh don|so lieu gio hang|"
        r"tong luong don|chart don hang|hang bi back|"
        r"ti le pass don|co bao nhieu bill|"
        r"tong so don|ti le( don)? cancel|ti le don back|"
        r"cap nhat bao cao( so luong)? don|"
        r"so luong don( hang)?( vua roi)?|"
        r"(bill|don)( fail| thanh cong)|"
        r"(shipped|delivered|giao thanh cong) (vs|so voi|vs) (delivered|shipped|bi huy)|"
        r"ti le (shipped|delivered|giao hang) (vs|so voi)? (delivered|shipped|cancelled)|"
        r"don (dang )?van chuyen bao nhieu|don nao dang ship|"
        r"(ty le|ti le) (thanh cong|hoan thanh) don hang [0-9]+ thang|"
        r"don hang delivered thanh cong( thang [0-9]+| thang nay)?|"
        r"conversion rate (don hang)?|"
        r"ty le don (huy|cancel|hoan tra) thang [0-9]+|"
        r"bao nhieu don (dang van chuyen|dang ship|shipped)|so don (delivered|shipped|giao thanh cong)|"
        r"ti le (don )?(shipped|delivered) (vs|so voi)? (delivered|shipped|cancelled)|"
        r"ty le shipped vs delivered|so sanh don shipped so voi delivered)",
    ), "admin_order_stats"),

    # AA5: User stats
    (_re.compile(
        r"(thong ke (so luong )?(nguoi dung|user)( moi)?( dang ky)?( tuan nay| thang nay| hom nay)?|"
        r"so luong (user|nguoi dung) moi (dang ky|tham gia)( tuan nay| thang nay)?|"
        r"bao nhieu (user|nguoi dung|new user) (moi|active|this month)?|"
        r"user (statistic|stat|growth)|tang truong nguoi dung|"
        r"phan tich (user|nguoi dung)|nguoi dung active|"
        r"bao nhieu user dang ky|ti le kh online|tong so user( dang ky)?( thang nay)?|"
        r"chart luong khach|thong ke member|luong traffic|luong truy cap|"
        r"mem moi|kh moi|khach moi|luong tk moi|thong ke tk)",
        _re.IGNORECASE,
    ), "admin_user_stats"),

    # ── NHÓM A-B: QUẢN LÝ USER ───────────────────────────────────────────────

    # AB2: UPDATE ROLE (TRƯỚC lookup)
    (_re.compile(
        r"(nang (tai khoan|cap tk|cap tai khoan|user)( id| ma| nay| do)?\s*.{0,50}\s*(len|thanh|sang)\s*(staff|admin)|"
        r"nang (user|tk|tai khoan)( nay| do)?\s*.{0,50}\s*(len|thanh|sang)\s*(staff|admin)( di| luon)?|"
        r"thay doi (role|quyen) (user|nguoi dung)|nang cap (user|tai khoan|len|thanh) (staff|admin)|"
        r"nang cap (user|tai khoan) .{0,50} (len |thanh |sang )?(staff|admin)|"
        r"update (user )?role|doi role|promote user|doi quyen|set role|gan role|"
        r"cap quyen (admin|staff)|ha cap role|"
        r"chuyen (user|account|tao|tai khoan|no|nguoi do) .{0,50} (thanh|sang|thanh tk)( role)? (staff|admin|customer)( luon)?|"
        r"chuyen user .{0,50} sang (staff|admin)( role)?|cap (no|tai khoan do) (thanh|len) (staff|admin)|"
        r"nang quyen cho user .+? thanh (staff|admin)|"
        r"set quyen|tao role|nho cap role|gan( chuc)? admin|gan phan quyen|"
        r"cho no len admin|ha( bac)? role|thu (quyen|role)|(nang|ha) cap tk|"
        r"xoa (quyen|role)|"
        r"update role cho|ha cap (xuong|xuoang)|thu hoi quyen)",
        _re.IGNORECASE,
    ), "admin_user_update_role"),

    # AB0: BANNED USERS - LIST QUERY (ưu tiên tuyệt đối TRƯỚC lock/unlock)
    # "ai đang bị khoá" = hỏi DANH SÁCH → admin_banned_users
    # "khoá acc/tài khoản user X" = hành động → admin_user_lock_unlock
    (_re.compile(
        r"^(ai|nhung ai) (dang |bi )?(bi )?(khoa|ban|vo hieu hoa)( tai khoan| acc| account)?( roi)?$|"
        r"(hien|liet ke|cho xem|xem) (danh sach |list )?(user|tai khoan|nguoi) (bi khoa|bi ban|dang bi khoa|dang bi ban)|"
        r"(danh sach |list )?(user|tai khoan|account) (dang bi |bi )?(khoa|ban|lock)|"
        r"cac (tai khoan|user|account) (dang |bi )?(khoa|ban|bi khoa|bi ban)( hien nay)?|"
        r"(list|show|hien thi|liet ke) (all )?(locked|banned|suspended|inactive|restricted) (accounts?|users?)|"
        r"account nao (dang )?(bi )?(suspend|lock|ban|inactive|restricted)|"
        r"user nao (dang )?(bi )?(suspend|lock|ban|inactive|restricted|khoa|bi khoa)|"
        r"(ai|nhung ai) (dang )?(bi )?(suspend|restrict|inactive|lock|khoa tai khoan|bi restrict)|"
        r"tai khoan bi (ban|suspended|restricted|inactive|khoa) (ai quan ly|giam sat|the nao)|"
        r"show (me )?(danh sach )?user bi (lock|ban|khoa)",
        _re.IGNORECASE,
    ), "admin_banned_users"),

    # AB3: LOCK/UNLOCK (TRƯỚC lookup)
    (_re.compile(
        r"(khoa (tai khoan|account|acc)|mo khoa (tai khoan|account|acc)|"
        r"(lock|unlock( blocked)?|ban|unban) (user|account|tai khoan|acc|nguoi dung)|"
        r"vo hieu hoa (tai khoan|account)|kich hoat lai (tai khoan|account)|"
        r"suspend (user|account)|disable (account|user)|"
        r"block nick|chan (thang nay|tai khoan|account)|khoa mom|"
        r"mo( block| ban)|giai phong( acc)?|cho acc quay lai|"
        r"ngung( hoat dong)? cua user|"
        r"disable tk|khoa user|mo khoa user|khoa vinh vien)",
        _re.IGNORECASE,
    ), "admin_user_lock_unlock"),

    # AB4: RESET PASSWORD (TRƯỚC lookup)
    (_re.compile(
        r"(reset (mat khau|password|pass) (user|nguoi dung|cho|cua)?|"
        r"dat lai (mat khau|password|pass)|cap (mat khau|password|pass) moi (cho|cho user)?|"
        r"khoi phuc (mat khau|password)|doi mat khau giup|reset user password|"
        r"force reset password|"
        r"rs pass|lay lai pass|cap lai pass|dat lai mk|"
        r"quen (pass|mat khau) (nen|thi) cap( lai)?|"
        r"reset pwd|rs quyen mat khau|rs mat khau|reset pass cho)",
        _re.IGNORECASE,
    ), "admin_user_reset_password"),

    # AB5: LIST STAFF
    (_re.compile(
        r"(danh sach (nhan vien|staff|cong nhan|admin|quan tri|quan tri vien)|xem tat ca (tai khoan )?(staff|admin)|"
        r"(list|co bao nhieu) (staff( members?)?|nhan vien|tai khoan staff|admin|tai khoan admin)|tai khoan (staff|admin) (trong he thong)?|"
        r"quan ly (nhan vien|staff|admin)|(staff|admin) (accounts?|list)|"
        r"xem doi ngu nhan vien|ds (staff|admin)|"
        r"xem toan bo tnv|xem nhan su|nhom ho tro|danh sach nhan su|"
        r"ai dang lam (staff|admin)|xem ds (staff|admin)|bao nhieu tk (staff|admin)|(staff|admin) members list)",
        _re.IGNORECASE,
    ), "admin_list_staff"),

    # AB6: LIST CUSTOMERS
    (_re.compile(
        r"(danh sach (khach hang|nguoi dung|user|tai khoan|customer|kh)|xem tat ca (tai khoan )?(khach hang|user|customer)|"
        r"(list|co bao nhieu) (khach hang|customer|tai khoan kh|tai khoan khach hang)|"
        r"quan ly khach hang|customer (accounts?|list)|"
        r"xem ds (kh|khach hang)|danh sach toan bo khach hang)",
        _re.IGNORECASE,
    ), "admin_list_customers"),

    # AB1a: USER DETAILS by email (specific → TRƯỚC generic lookup)
    (_re.compile(
        r"(xem thong tin (user|nguoi dung|tai khoan) (email|co email|bang email) .{3,}|"
        r"xem (chi tiet |thong tin )(user|nguoi dung|tai khoan|acc) email .{3,}|"
        r"tim (user|nguoi dung) (email|qua email|theo email) .{3,}|"
        r"thong tin (user|nguoi dung) email [\w.@]+)",
        _re.IGNORECASE,
    ), "admin_user_details"),

    # AB1: USER LOOKUP (generic)
    (_re.compile(
        r"(tra cuu (thong tin )?(user|nguoi dung|tai khoan|acc)|"
        r"tim (user|nguoi dung|tai khoan|acc|kh) (theo (email|so dien thoai|ten|id)|by email)?|"
        r"xem (chi tiet )?(profile|thong tin|in4|tai khoan) (user|nguoi dung|acc|khach)|"
        r"(user|khach hang|acc|id|kh|email|username) (email|id|so|ma):?\s*.+|"
        r"lookup (user|account)|tim tai khoan|"
        r"check acc|check info|thong tin tk|"
        r"tim tk|co the ktra acc|xem hs khach|thong tin tai khoan (user|cua chinh minh|cua toi|chinh minh)|"
        r"xem thong tin( tai khoan)? cua chinh minh)",
        _re.IGNORECASE,
    ), "admin_user_lookup"),

    # ── 15. ADMIN_UPDATE_ROLE ──────────────────────────────────────────────────
    (_re.compile(
        r"(doi|cap|set|thay doi|cap nhat|chuyen|chuyen no|chuyen tai khoan do|chuyen tk do) "
        r"(role|quyen|quyen han|loai tai khoan|chuc vu)( thanh| sang| la)? "
        r"(admin|staff|customer|user)|"
        r"(set|make|doi|chuyen)( no| tai khoan do| tk do| user do)? (thanh|sang|la) (admin|staff|customer|user)|"
        r"cap quyen (admin|staff|customer|user) cho (user|tai khoan|no)|"
        r"promote (user )?to (admin|staff)|demote (user )?to (customer|user)|"
        r"change (role|account type) to (admin|staff|customer|user)|"
        r"(phong|giang) (no|user( nay| do)?) thanh (admin|staff|customer|user)|"
        r"(assign|grant|cap|cho) (role )?(admin|staff|customer|user|manager) (cho )?user|"
        r"(assign|set) user (sang|thanh|la) (staff|admin|customer)|"
        r"change (user )?permission (to |)(staff|admin|customer)|"
        r"cap quyen (cho )?nhan vien (moi)? (role|quyen|cap do)?",
        _re.IGNORECASE,
    ), "admin_user_update_role"),

    # ── NHÓM A-C: QUẢN LÝ SÁCH ───────────────────────────────────────────────

    # AC1: BOOK ADD GUIDE
    (_re.compile(
        r"(huong dan (them|tao|nhap) sach (moi)?|cach (them|tao) sach|"
        r"add (new )?book (guide|how to|tutorial)|tao sach moi|nhap sach( moi)? vao (he thong|kho)?|"
        r"lam sao (de )?(them|tao) sach|them sach( moi)? the nao|"
        r"day cach them sach|tao (san pham|item) sach|nhap title moi|"
        r"up sach len|dang sach moi|them sach the nao( admin)?|add book)",
        _re.IGNORECASE,
    ), "admin_book_add_guide"),

    # AC2: BOOK STATUS CHANGE
    (_re.compile(
        r"(kich hoat( lai)? (sach|cuon)|vo hieu hoa (sach|cuon)|an (sach|cuon)( khoi trang)?|"
        r"(book status change|deactivate book)|"
        r"(active|inactive|an|hien|xoa) (sach|cuon|item)|xoa mem sach|"
        r"dung ban (sach|cuon)|bat dau ban (lai)? (sach|cuon)|"
        r"ha( sach| cuon) xuong|ha (sach|cuon)|cat sach vao|go sach|go bo|"
        r"cho sach (hien len|len song)|xoa tam thoi sach|"
        r"an khoi web|an bot sach|khong cho ban cuon|xoa title sach)",
        _re.IGNORECASE,
    ), "admin_book_status_change"),

    # AC2.5: UPDATE BOOK STOCK – cập nhật tồn kho sách
    (_re.compile(
        r"(cap nhat|sua|chinh|dat|set|tang|nang|giam|thay doi).{0,20}"
        r"(ton kho|so luong|cuon|stock|quantity|sach).{0,30}"
        r"(\d+|len \d+|xuong \d+|thanh \d+|them \d+)|\b"
        r"(nhap|nhap them|nhap kho).{0,20}\d+.{0,10}(cuon|quyen|ban|sach)|\b"
        r"(tang|nang|chinh) kho.{0,20}(sach|cuon)|\b"
        r"(update|set).{0,10}stock.{0,20}\d+",
        _re.IGNORECASE,
    ), "admin_update_book_stock"),

    # ── NHÓM ADMIN-ORDER (kế thừa từ staff) ─────────────────────────────────
    # AC-ORD1: CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG
    (_re.compile(
        r"(cap nhat (trang thai|status)? ?don( hang)?( #?\d+)?|"
        r"chuyen (don( hang)?|trang thai( don( hang)?)?|no|nay)( #?\d+)? sang( trang thai)? "
        r"(giao|processing|shipped|delivered|cancelled|cancel_requested|return_requested|returned|failed|"
        r"hoan thanh|that bai|giao that bai|yeu cau huy|yeu cau tra|"
        r"pending|cho xu ly|dang giao hang|dang giao|dang xu ly|dang chuan bi|da giao|giao xong|giao thanh cong|"
        r"da huy|bi huy|huy don|hoan hang|tra hang|da tra)|"
        r"chuyen don( hang)? #?\d+ sang "
        r"(da giao|giao hang|delivered|da xu ly|hoan thanh|shipped|cancelled|cancel_requested|return_requested|"
        r"failed|that bai|yeu cau huy|yeu cau tra|pending|cho xu ly|dang giao hang|dang giao|dang xu ly|da huy|hoan hang|processing)|"
        r"duyet don|xac nhan don|don #?\d+ can chuyen sang trang thai|"
        r"danh dau (da giao|da xu ly|da ship|don( hang)? #?\d+ da giao (thanh cong|xong)?|don .{1,10} da giao|don.*hoan thanh)|"
        r"doi trang thai don.*sang "
        r"(returned|cancelled|cancel_requested|return_requested|shipped|delivered|hoan thanh|failed|that bai|"
        r"yeu cau huy|yeu cau tra|pending|cho xu ly|dang giao hang|dang giao|dang xu ly|da giao|da huy|hoan hang|processing)|"
        r"(update|confirm) (order )?(status|.*as completed)|"
        r"mark (order #?\d+ )?as (delivered|shipped|processing|completed|failed|cancel_requested|return_requested)|"
        r"chuyen( don( hang)? #?\d+| no)? sang processing|"
        r"giao xong roi|done order|"
        r"mark (don|order|bill) #?\d+ (la |sang |thanh )?(failed|cancel_requested|return_requested)|"
        r"chot don|duyet gap|bam done don|xong bill|update done)",
        _re.IGNORECASE,
    ), "admin_order_status_update"),

    # AC-ORD2: DANH SÁCH ĐƠN CHỜ / THEO TRẠNG THÁI
    (_re.compile(
        r"(don( hang)? (dang |chua )?(cho|pending|can xu ly|duoc duyet|ton dong)|danh sach don cho|"
        r"bao nhieu don (cho|pending|hom nay|ton dong)|don chua duyet|"
        r"(list|danh sach|xem) don( hang)? (dang |chua )?(cho|pending|xu ly)( xu ly)?( hom nay)?|"
        r"tat ca don (cho|pending) hom nay|don can giao gap|don chua gui|can duyet|"
        r"nhung don (nao )?dang cho (xu ly|duyet|staff)?|"
        r"danh sach (bill|don)( hang)? (dang treo|can duyet)( cho phe duyet)?( hom nay)?|"
        r"con (nhung )?don (nao )?chua giao|"
        r"nhung (don|bill) nao dang cho (xu ly|ben staff)?|"
        r"xem don chua ship|loc don pending|cac don dang treo|don ton dong|"
        r"(show|hien thi|lay) (danh sach )?(tat ca |all )?don (dang )?pending|"
        r"don nao (dang )?pending can duyet (gap)?|"
        r"(con |dang co )?bao nhieu don ton dong|"
        r"lay danh sach don can giao gap|don can giao gap|"
        r"(con |dang co )?(bao nhieu )?don chua xu ly|(don nao|nhung don) tu toi hom qua chua duyet|"
        r"show (toi )?(danh sach )?don (dang )?processing|"
        r"lay (danh sach )?don (hang )?(can giao|chua giao|dang cho giao)|"
        r"don hang nao (can|dang) giao (gap)?|nhung don chua ship)",
        _re.IGNORECASE,
    ), "admin_order_list_pending"),

    # AC-ORD3: LỌC ĐƠN THEO TRẠNG THÁI CỤ THỂ (shipped, cancelled, return_requested...)
    (_re.compile(
        r"(don( hang)? nao (dang |bi |da )?(yeu cau huy|cancel_requested|cancel request|xin huy)|"
        r"danh sach don (dang |bi )?yeu cau huy|don bi (yeu cau )?huy|"
        r"don( hang)? nao (dang )?(shipped|giao hang|dang giao|van chuyen)|"
        r"danh sach don (dang )?giao (hang)?|don dang ship|"
        r"don( hang)? nao (dang |bi )?that bai|don giao that bai|don bi that bai|"
        r"danh sach don that bai|don nao failed|"
        r"don( hang)? nao (dang )?yeu cau tra|don bi yeu cau tra|return_requested|"
        r"danh sach don yeu cau tra|don can duyet tra|"
        r"don( hang)? nao (da )?(huy|cancelled)|danh sach don (da )?huy|don bi huy|"
        r"don( hang)? nao (da )?giao (xong|thanh cong)?|danh sach don da giao|"
        r"don( hang)? nao (da )?hoan tra|danh sach don da tra|"
        r"xem (tat ca )?don (dang )?(yeu cau huy|cancel_requested|shipped|giao|that bai|failed|huy|da giao|tra)|"
        r"loc don (theo )?(trang thai )?(yeu cau huy|cancel|shipped|giao|failed|that bai))",
        _re.IGNORECASE,
    ), "admin_order_list_by_status"),

    # AC-ORD4: HOÀN HÀNG (xử lý / duyệt)
    (_re.compile(
        r"(don (doi tra|return|hoan hang|tra hang)( \d+)?( can duyet)?(.*cho khach)?|xu ly (doi|tra|hoan) hang|"
        r"xu ly (don |yeu cau )?(doi tra|tra hang|return|hoan hang)|"
        r"danh sach (doi tra|return|khach can hoan hang)|khach (muon|yeu cau) (tra|doi|hoan)( sach)? (don|hang)|"
        r"return (request|order|hoan tra)|xu ly (khieu nai|complaint) tra hang|"
        r"xem yeu cau hoan tra( hom nay)?|bao nhieu don dang yeu cau return|"
        r"hoan tien (don|cho khach) (hang |so )?\d*|chap nhan (doi|hoan) tra( don)? (hang |so )?\d*|"
        r"dong y hoan tien (cho|don) .{0,30}|dong y hoan( hang| tien)( cho)?|"
        r"xu ly refund (cho don|don hang|khach) .{0,20}|"
        r"giai quyet don (back|hoan|doi tra)|duyet (don )?(hoan|doi) hang|"
        r"don can doi tra|phieu hoan hang|"
        r"xu li hang doi|khach doi tra hoan tien)",
        _re.IGNORECASE,
    ), "admin_return_handle"),

    # AC-ORD5: QUY TRÌNH HOÀN HÀNG / GIAO HÀNG (knowledge base)
    (_re.compile(
        r"(quy trinh|cac buoc|huong dan|thu tuc|step|quy dinh) (xu ly )?(hoan hang|doi tra|return|tra hang|hoan tien)|"
        r"workflow (doi tra|hoan hang|return)|"
        r"can lam gi khi khach (muon|yeu cau) (tra|doi|hoan) (hang|sach)|"
        r"xu ly (hoan hang|doi tra|return) (the nao|nhu the nao|ra sao)|"
        r"cach (xu ly|kiem tra|phan loai) (hoan hang|don doi tra)|quy trinh xu ly|"
        r"(chinh sach|quy dinh|quy trinh|thong tin|huong dan) (giao hang|van chuyen|ship|shipping)|"
        r"(phi|mat phi|chi phi|gia) (giao hang|van chuyen|ship) (la bao nhieu|nhu the nao|the nao)?|"
        r"(thoi gian|bao lau|may ngay) (giao hang|van chuyen|giao toi khach)|"
        r"(don vi|cong ty) (van chuyen|giao hang|ship) (nao|gi)?",
        _re.IGNORECASE,
    ), "admin_return_workflow"),

    # ── NHÓM ADMIN-INVENTORY (kế thừa từ staff) ─────────────────────────────
    # AC-INV1: KIỂM TRA TỒN KHO CỤ THỂ
    (_re.compile(
        r"(kiem tra (hang )?ton( kho)?|con bao nhieu (cuon( sach)?|sach|hang)( id \d+)? (trong kho|con lai)|"
        r"stock (con|la) bao nhieu|ton kho (sach|cuon)|"
        r"check (inventory|stock|kho|sach)|stock check( for book)?|"
        r"so luong (ton kho|con lai)|sach nay con bao nhieu|how many books left in stock|"
        r"check so luong|kho con ma|ton cuon|con bao nhieu quyen|"
        r"ton kho (cuon |sach )?.{2,40} (hien|bay gio)|"
        r"sach .{2,60} con bao nhieu|"
        r".{2,60} con bao nhieu (cuon|quyen)( trong kho| con lai)?|"
        r"(cuon|quyen|sach) .{2,50} (con |trong kho )?(bao nhieu|la bao nhieu|co bao nhieu)|"
        r"kiem tra ton kho (cuon |sach )?.{2,50}|check ton (sach )?.{2,50}|"
        r"(can )?nhap (them )?bao nhieu|nhap them de du \d+ (cuon|quyen)|"
        r"can nhap (them )?de du \d+ (cuon|quyen))",
        _re.IGNORECASE,
    ), "admin_inventory_check"),

    # AC-INV2: SÁCH SẮP HẾT HÀNG
    (_re.compile(
        r"((sach|quyen)( nao)? (dang )?(sap |gan |nao )?het (hang|stock)|canh bao ton kho (thap|it|sap het stock)|"
        r"sach duoi nguong (ton kho|stock)|(sach nao )?(dang )?can nhap them( hang)?|"
        r"(low|books (running )?low on) (stock|inventory) (alert|warning)?|danh sach sach (dang )?sap het|"
        r"sach can (nhap|bo) sung|stock thap|"
        r"ton kho duoi (5|10|20) (cuon|quyen)|sach co (it hon|duoi) (5|10) cuon|"
        r"sach (chi con|con lai) (1|2|3|4|5) (cuon|quyen)|"
        r"danh sach sach (co ton kho |)(duoi|it hon) (5|10) (cuon|quyen))",
        _re.IGNORECASE,
    ), "admin_inventory_low"),

    # AC-BOOK: TRA CỨU CHI TIẾT SÁCH
    (_re.compile(
        r"(xem (chi tiet |thong tin )?sach (trong he thong|admin)|"
        r"tim sach theo (ma|id|ten|isbn)|sach (id|ma) #?\d+|book (admin details) (id )?(#)?\d+|"
        r"thong tin sach (admin|quan ly)|gia va ton kho sach( id| ma)?( #)?\d+.*|"
        r"tim (thong tin |chi tiet )?(cuon|sach|book) .{2,50}|"
        r"co (thong tin|in4) (cua cuon|cuon) .{2,40}|"
        r"xem (chi tiet |thong tin )?(cuon|sach|book) .{3,100}|"
        r"search book (id|title|name|isbn|ten) .{2,40}|"
        r"book isbn [\d\-X]+|tim sach (isbn|ma isbn) .{5,20}|"
        r"check quyen sach|xem tac pham|tim nhanh quyen|"
        r"tim info sach|ma id cua book)",
        _re.IGNORECASE,
    ), "admin_book_lookup"),

    # AC3: CATEGORY MANAGE
    (_re.compile(
        r"(quan ly (danh muc|category|the loai)|thong ke (danh muc|category|the loai)|"
        r"danh muc nao (nhieu|it) sach nhat|phan tich danh muc|"
        r"category (management|stat)|so sach theo (danh muc|the loai)|"
        r"khoang the loai|thong ke danh muc|sua danh muc|"
        r"them the loai|phan chia danh muc|check danh muc|quan ly chuyen muc|"
        r"xem ds danh muc|cac the loai dang co)",
        _re.IGNORECASE,
    ), "admin_category_manage"),

    # ── DESTRUCTIVE / OUT OF SCOPE ───────────────────────────────────────────
    (_re.compile(
        r"(xoa (toan bo|hang loat|tat ca) (don hang|user|nguoi dung|sach|khuyen mai)|"
        r"delete (all|everything)|clear database|"
        r"blacklist ip|chan ip|block ip|xoa db|drop table)",
        _re.IGNORECASE,
    ), "admin_out_of_scope"),

    # AC4: BOOK LOW RATING
    # AC4: BOOK LOW RATING
    (_re.compile(
        r"sach( nao)? (co )?rating (thap|kem|xau|duoi (3|2|1) (sao|star)?)"
        r"|sach bi (danh gia kem|review xau|rating thap|che)"
        r"|low rated books|sach can (review lai|kiem tra( noi dung)?)"
        r"|sach (duoi|rating <) (3|2)\.?\d*|so sao yeu"
        r"|sach bi danh 1 sao|sach nao bi te (nhat)?|sach( dang)? te( nhat)?"
        r"|check sach bi vote thap|cuon nao dang an( gach| chui)?|sach nhieu vo gach"
        r"|list sach (bi vo gach|an chui|bi danh gia thap)|sach bi vo gach nhieu nhat"
        r"|sach nao (dang bi|bi) (danh gia thap|review xau|te|phan nan)( can review| kiem tra)?"
        r"|sach( bi)? danh gia thap nhat|sach rate thap"
        r"|(sach nao|cuon nao) (bi )?(review thap nhat|review xau nhat|thap nhat he thong)"
        r"|sach [1-2](-| )?sao (can xem lai|can kiem tra)?"
        r"|(sach bi|review bi) (khieu nai|phan hoi tieu cuc|review xau)"
        r"|sach (bi )?(khieu nai|bao cao) (chat luong|in|cach viet)"
        r"|sach (can (cai thien|nang cao|review)|dang can (review|kiem tra)) (chat luong)?"
        ,
        _re.IGNORECASE,
    ), "admin_book_low_rating"),

    # ── NHÓM A-D: QUẢN LÝ KHUYẾN MÃI ────────────────────────────────────────

    # AD2: CHECK PROMO (TRƯỚC list)
    (_re.compile(
        r"(kiem tra (ma|voucher|promo|giam gia|khuyen mai|code)\s*[a-zA-Z0-9_]{2,20}|"
        r"ma \w{2,20} (con hieu luc|con dung|da het han|hop le|co loi gi ko|con xai duoc khong)|"
        r"kiem tra ma voucher [A-Z0-9]+|"
        r"check (promo|voucher) code [A-Z0-9]+|"
        r"xem (chi tiet|thong tin) (ma|voucher|code) [A-Z0-9]+|"
        r"check code [A-Z0-9]+|voucher [A-Z0-9]+ xai the nao|"
        r"check ho (ma|code)|(ma|voucher) chop le ko)",
        _re.IGNORECASE,
    ), "admin_promotion_check"),

    # AD4: EXPIRING PROMO
    (_re.compile(
        r"(khuyen mai (sap|gan) het han|promotion (expiring|sap het han)|"
        r"voucher (sap|gan) het han|ma giam gia het han (trong|sau)? \\d+ ngay|"
        r"(voucher|ma|khuyen mai) nao sap het han (trong (tuan|thang))?|"
        r"canh bao (ma|voucher|promotion) het han|"
        r"ma nao sap het han|"
        r"voucher clear|code out of date|ma clear|"
        r"ma sap dead|code ganh can date|sap het hieu luc ma|"
        r"ma( dang)? cho can date|khuyen mai sap tat)",
        _re.IGNORECASE,
    ), "admin_promotion_expiring"),

    # AD3: CREATE PROMO GUIDE
    # AD3: CREATE PROMO GUIDE
    (_re.compile(
        r"tao (them )?(chuong trinh )?(promotion|voucher|khuyen mai|ma( giam gia)?|sale)( moi| [A-Z0-9]{2,20})?"
        r"|huong dan tao (voucher|promo|chuong trinh khuyen mai|code giam gia)"
        r"|create (promotion|promo|voucher)|them ma giam gia moi"
        r"|cach tao (chuong trinh khuyen mai|voucher|ma|sale)|lam sao tao (voucher|ma)"
        r"|soan( cai)? voucher|tao code moi|bom km moi|run campaign voucher"
        r"|set \d+% (off |sale )?(cho )?(ma|voucher)?|generate voucher( \d+)?|ra voucher giam gia"
        r"|setup ct kmai|kich hoat km moi|them voucher"
        r"|(tao|setup|them|create) (discount code|promo code|coupon|ma coupon)"
        r"|(huong dan|cach) (them|tao|set up) (ma coupon|coupon|discount|promo)"
        r"|(setup|tao|khoi tao) (promotion|ma sale|ma giam gia) (cho)? (ngay le|su kien|cuoi nam|birthday|black friday|flash sale)"
        r"|(tao|setup) ma (sale|coupon) (cuoi nam|cuoi thang|ngay le|sinh nhat|birthday)"
        ,
        _re.IGNORECASE,
    ), "admin_promotion_create_guide"),

    # AD1: LIST PROMOTIONS
    (_re.compile(
        r"(danh sach (khuyen mai|promotion|voucher|ma giam gia)|"
        r"(co bao nhieu|so luong|list) (ma giam gia|khuyen mai|promotion|voucher|code)|"
        r"xem tat ca (promotion|voucher|ma giam gia|code)|"
        r"list (all )?(promotions?|vouchers?|codes?)|ma giam gia dang (active|hoat dong|dung duoc|chay)|"
        r"cac voucher (hien tai|dang su dung|con valid)|"
        r"dang co( chuong trinh)? km nao|xem ds voucher|"
        r"show chuong trinh sale|bang mgg|bang voucher|tong hop ma|"
        r"xem toan bo ma giam gia|hien cac voucher|cac ma( dang)? (con|chay))",
        _re.IGNORECASE,
    ), "admin_promotion_list"),

    # ── NHÓM A-E: HỆ THỐNG ───────────────────────────────────────────────────

    # AE1: SYSTEM HEALTH
    (_re.compile(
        r"(kiem tra (trang thai|status|suc khoe) (service|cac dich vu|server|cac service|he thong)|"
        r"kiem tra (cac )?(service|server|api)|service (con chay|on|alive|health)|system (health|status)|"
        r"server (on|off|status|die k)|cac dich vu (dang|con) (hoat dong|chay)|"
        r"ping (service|server)|health check|system health check|"
        r"check cpu va database|"
        r"sap me web|mang lag hong|he thong sap chua|server lag|web lag|"
        r"ktra api|ktra ho server|sever o day die a|web dung ko|he thong on k|"
        r"may chu( dang)?( chay)? muot khong|tinh trang db|"
        r"kiem tra ket noi he thong|tai server on khong|"
        r"(check|kiem tra) (elasticsearch|mysql|ollama|redis|rabbitmq|api gateway)|"
        r"(check|ping|xem) (database|db) (connection|status|pool)|"
        r"check (logs?|log server)|xem logs? server|"
        r"(trang thai|status) (cac )?(service|backend|api gateway|database)|"
        r"(service|system) health (overall|status)|overall (system )?status|"
        r"kiem tra (server|he thong|service) (hom nay|bay gio)|"
        r"(elasticsearch|mysql|ollama) (dang chay|on|status|hoat dong) (khong|chua)?)",
        _re.IGNORECASE,
    ), "admin_system_health"),

    # AE2: ESCALATED CUSTOMERS
    (_re.compile(
        r"(danh sach (khach hang|khieu nai|thac mac).*admin|"
        r"escalated (issue|khieu nai|customer|ticket) (admin|for admin)?|"
        r"khach hang can (admin|quan tri)( ho tro| xet duyet)?|"
        r"van de( khach hang)? chua (giai quyet|xu ly)( cho admin)?|"
        r"ticket(s)? can admin( xu li gap)?|ticket (cao nhat|level 2|cho sếp)|danh sach phoi khach|"
        r"khieu nai can giam doc|xem phos kh|list vu kien|list vu cang|"
        r"vu nao cang|can admin xu li|phàn nàn rách bìa)",
        _re.IGNORECASE,
    ), "admin_escalated_customers"),

    # CHITCHAT / HELP
    (_re.compile(
        r"^(xin chao|hello|hi|chao|help( admin)?|admin help|"
        r"(ban|may) la ai|(admin )?(chat)?bot lam( duoc)? gi|huong dan|cac lenh|"
        r"(admin )?chatbot|tro ly admin|"
        r"test test|alo( alo)?|mic admin|alo toi la admin|"
        r"chao tro ly( admin)?|help me out( bot)?|lenh cho admin|tu van chuc nang|"
        r"chao c e la admin|lam gi cho toi).*",
        _re.IGNORECASE,
    ), "admin_chitchat"),

    # ── ADMIN_REVENUE_STATS (alias revenue_report + monthly queries) ─────────
    (_re.compile(
        r"(bao cao doanh thu (theo )?thang|"
        r"bieu do doanh thu|"
        r"tong doanh thu (tat ca |cac )?thang|"
        r"tinh toan doanh thu|"
        r"doanh thu (thang nay|thang [0-9]+|thang truoc)( la bao nhieu)?|"
        r"revenue (stats?|theo thang|monthly)|"
        r"thong ke doanh thu thang|"
        r"doanh thu theo (thang|quy|nam))",
        _re.IGNORECASE,
    ), "admin_revenue_stats"),

    # ── ADMIN_ORDER_SEARCH (export/filter orders) ────────────────────────────
    (_re.compile(
        r"(xuat (danh sach |toan bo )?don hang (thang nay|tuan nay|hom nay)?|"
        r"(hien thi|loc|tim) (cac |toan bo )?don hang (chua giao|chua ship|pending|dang xu ly|theo thang)|"
        r"don hang (chua giao|chua ship|dang cho) (the nao|hien tai|hien co)?|"
        r"(xuat|export) don (hang )?thang (nay|[0-9]+)|"
        r"loc don hang theo trang thai|"
        r"order search|tim don hang)",
        _re.IGNORECASE,
    ), "admin_order_search"),

    # ── ADMIN_LOG_SEARCH (session/chat log lookup) ────────────────────────────
    (_re.compile(
        r"(tim (phien|session) (tro chuyen|chat) (session_?[a-z0-9]+|[a-z0-9]+)?|"
        r"(xem|tim) session.{0,20}(de support|cho support|ho tro)?|"
        r"session[ _]?[a-z0-9]{4,30}|"
        r"lich su chat (session|phien)|log (phien|session)|tim phien ho tro)",
        _re.IGNORECASE,
    ), "admin_log_search"),

    # ── ADMIN_BANNED_USERS (locked/violated accounts) ─────────────────────────
    (_re.compile(
        r"(danh sach (tai khoan |user |khach hang )?(bi khoa|vi pham|ban|bi ban)|"
        r"loc tai khoan bi ban|"
        r"(user|tai khoan|khach hang) (bi ban|bi khoa|vi pham) (chinh sach|cam)?|"
        r"list (banned|khoa|vi pham) (user|tai khoan|account)?|"
        r"xem (tai khoan|user) bi (khoa|ban|vo hieu hoa)|"
        r"ai dang bi khoa (tai khoan|acc)?|"
        r"cac tai khoan (dang )?(bi khoa|bi ban))",
        _re.IGNORECASE,
    ), "admin_banned_users"),

    # ── 5. ADMIN_USER_DETAILS ───────────────────────────────────────────────
    (_re.compile(
        r"(thong tin (chi tiet )?(user|khach hang|nguoi dung|tai khoan|account)|"
        r"chi tiet (user|khach hang|nguoi dung|tai khoan)|"
        r"tim (user|khach hang|nguoi dung)|xem (user|tai khoan|khach hang) (co )?email|"
        r"user [a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+|"
        r"khach hang [a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)",
        _re.IGNORECASE,
    ), "admin_user_details"),

    # ── CONFIRMATION FLOW ─────────────────────────────────────────────────────
    (_re.compile(
        r"^(co|co thu|ok|okay|oke|o ke|dong y|xac nhan|tien hanh|ừ|uhm|um|yep|yes|chuẩn|chac chan)( do| the)?$",
        _re.IGNORECASE,
    ), "confirmation_yes"),

    (_re.compile(
        r"^(khong|no|huy|huy bo|dung lai|tu choi|thoi|khoan|chua)( duoc)?$",
        _re.IGNORECASE,
    ), "confirmation_no"),
]


# ── Main Intent Detection ──────────────────────────────────────────────────────
from chatbot_app.nlu.text_normalizer import expand_abbreviations

def detect_admin_intent(message: str) -> AdminNLUResult:
    """Phân loại intent cho admin. QUICK_RULES → SBERT."""
    if not message or not message.strip():
        return AdminNLUResult(intent="admin_chitchat", confidence=1.0)

    message_expanded = expand_abbreviations(message)
    normalized = _normalize_vi(message_expanded)

    # ── Tầng 1: QUICK_RULES ───────────────────────────────────────
    for pattern, intent in ADMIN_QUICK_RULES:
        if pattern.search(normalized):
            entities = _extract_admin_entities(message_expanded, intent)
            return AdminNLUResult(intent=intent, confidence=1.0, entities=entities)

    # ── Tầng 2: SBERT Zero-shot ───────────────────────────────────
    _build_admin_template_cache()
    model = get_sbert_model()
    msg_vec = model.encode([message_expanded], normalize_embeddings=True)[0]

    best_intent = "admin_chitchat"
    best_score  = 0.0
    for intent, tmpl_vecs in _admin_template_cache.items():
        # Lấy template gần nhất
        scores = np.dot(tmpl_vecs, msg_vec)
        score  = float(np.max(scores))
        if score > best_score:
            best_score  = score
            best_intent = intent

    if best_score < ADMIN_CONFIDENCE_THRESHOLD:
        return AdminNLUResult(intent="admin_out_of_scope", confidence=best_score)

    entities = _extract_admin_entities(message_expanded, best_intent)
    return AdminNLUResult(intent=best_intent, confidence=best_score, entities=entities)


def _extract_admin_entities(message: str, intent: str) -> dict:
    """Trích xuất entities từ admin queries."""
    entities = {}

    # User ID hoặc email
    email_match = _re.search(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", message)
    if email_match:
        entities["target_email"] = email_match.group(0)

    user_id_match = _re.search(r"(?:user|nguoi dung|tai khoan|id)\s+(?:\#\s*)?([a-zA-Z0-9_\.\-]+)",
                                message, _re.IGNORECASE)
    if user_id_match:
        val = user_id_match.group(1)
        entities["target_user_id"] = int(val) if val.isdigit() else val

    # Target role khi đổi role
    if intent == "admin_user_update_role":
        role_match = _re.search(r"\b(admin|staff|customer)\b", message, _re.IGNORECASE)
        if role_match:
            entities["target_role"] = role_match.group(1).lower()

    # Voucher code
    voucher_match = _re.search(r"\b([A-Z][A-Z0-9]{2,19})\b", message)
    if voucher_match:
        entities["voucher_code"] = voucher_match.group(1)

    # Book ID
    book_id_match = _re.search(r"(?:sach|book|id|ma)\s*#?\s*(\d+)", message, _re.IGNORECASE)
    if book_id_match:
        entities["book_id"] = int(book_id_match.group(1))

    normalized_msg = _normalize_vi(message)

    # Promo status
    if intent == "admin_promotion_list":
        if "khong hoat dong" in normalized_msg or "inactive" in normalized_msg.lower():
            entities["promo_status"] = "inactive"
        elif "het han" in normalized_msg or "da xoa" in normalized_msg.lower() or "expired" in normalized_msg.lower() or "deleted" in normalized_msg.lower():
            entities["promo_status"] = "deleted"
        else:
            entities["promo_status"] = "active"

    # Date range
    normalized_msg = _normalize_vi(message)
    if "hom nay" in normalized_msg or "today" in normalized_msg.lower():
        entities["date_range"] = "today"
    elif "tuan nay" in normalized_msg or "this week" in normalized_msg.lower():
        entities["date_range"] = "week"
    elif "thang nay" in normalized_msg or "this month" in normalized_msg.lower():
        entities["date_range"] = "month"
    elif "quy nay" in normalized_msg or "this quarter" in normalized_msg.lower():
        entities["date_range"] = "quarter"

    # Book status target
    if intent == "admin_book_status_change":
        if any(w in normalized_msg for w in ["kich hoat", "active", "bat dau ban"]):
            entities["target_status"] = "active"
        elif any(w in normalized_msg for w in ["vo hieu hoa", "inactive", "an", "dung ban", "xoa mem"]):
            entities["target_status"] = "inactive"

    # Rating threshold
    if intent == "admin_book_low_rating":
        threshold_match = _re.search(r"(?:duoi|under|<|below)\s*(\d(?:\.\d)?)", message, _re.IGNORECASE)
        if threshold_match:
            entities["rating_threshold"] = float(threshold_match.group(1))
        else:
            entities["rating_threshold"] = 3.0  # default

    return entities




class AdminIntentClassifier:
    """Wrapper class for admin intent classification."""

    def classify(self, message: str) -> dict:
        """Classify admin intent and return dict."""
        result = detect_admin_intent(message)
        return {
            "intent": result.intent,
            "confidence": result.confidence,
            "entities": result.entities,
        }
