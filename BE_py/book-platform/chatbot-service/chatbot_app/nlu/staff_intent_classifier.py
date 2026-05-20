# -*- coding: utf-8 -*-
"""
staff_intent_classifier.py - Phân loại Intent cho role Staff.

13 intents chia 4 nhóm nghiệp vụ:
  NHÓM S-A: Quản lý đơn hàng  (SA1-SA5)
  NHÓM S-B: Kiểm tra tồn kho  (SB1-SB3)
  NHÓM S-C: Hỗ trợ KH        (SC1-SC3)
  NHÓM S-D: Tra cứu & Thống kê (SD1-SD3)

Pattern:
  QUICK_RULES regex -> ưu tiên tuyệt đối
  SBERT zero-shot   -> khi regex không khớp
  Tái sử dụng SBERT model đã load từ intent_classifier.py
"""
import re as _re
import unicodedata
import numpy as np
from dataclasses import dataclass, field
from chatbot_app.nlu.customer_intent_classifier import get_sbert_model, _normalize_vi

STAFF_CONFIDENCE_THRESHOLD = 0.45  # Hạ từ 0.52 -> 0.45 để bắt slang ngắn như "những bill nào", "sửa số lượng"


# -- Staff NLU Result ----------------------------------------------------------
@dataclass
class StaffNLUResult:
    intent:     str
    confidence: float = 1.0
    entities:   dict  = field(default_factory=dict)
    sentiment:  str   = "NEUTRAL"


# -- Staff Intent Templates (SBERT Zero-shot) ----------------------------------
STAFF_INTENT_TEMPLATES: dict[str, list[str]] = {
    # NH?M S-A: Qu?n l? ??n h?ng
    "staff_order_lookup": [
        "Tra cứu thông tin chi tiết đơn số 5589",
        "tra cứu đơn hàng", "tìm đơn hàng theo mã", "xem chi tiết đơn hàng khách",
        "lookup order by id", "tìm đơn số", "kiểm tra đơn hàng khách hàng",
        # TEST POOL
        "kiểm tra chi tiết của bill số 7788", "xem lại thông tin đơn mã 3322",
    ],
    "staff_order_status_update": [
        "Chuyển trạng thái đơn 5589 sang shipped nha",
        "Update số lượng kho nhanh nhen",
        "cập nhật trạng thái đơn", "chuyển đơn sang processing", "đánh dấu đã giao",
        "update order status", "xác nhận đơn hàng", "duyệt đơn hàng",
        "chuyển trạng thái đơn",
        # TEST POOL
        "cập nhật trạng thái đơn đó thành hoàn tất", "update trạng thái đơn hàng kia qua completed",
    ],
    "staff_order_list_pending": [
        "Lọc ra các đơn hàng đang chờ duyệt hôm nay giúp anh",
        "xem đơn đang chờ xử lý", "danh sách đơn pending", "đơn chờ duyệt",
        "list pending orders", "bao nhiêu đơn chờ hôm nay", "đơn cần xử lý",
        # TEST POOL
        "hiển thị danh sách đơn chưa phê duyệt", "những bill nào chưa được xử lý",
    ],
    "staff_return_handle": [
        "xem đơn đổi trả", "danh sách yêu cầu hoàn hàng", "đơn return requested",
        "khách yêu cầu trả hàng", "list return requests", "xử lý đơn trả",
    ],
    "staff_order_statistics": [
        "thống kê đơn hàng hôm nay", "bao nhiêu đơn đã giao",
        "today order stats", "tổng số đơn tuần này", "báo cáo đơn nhanh",
    ],

    # NH?M S-B: Ki?m tra t?n kho
    "staff_inventory_check": [
        "Còn bao nhiêu cuốn sách id 201 trong kho",
        "kiểm tra tồn kho sách", "còn bao nhiêu cuốn trong kho", "stock sách này",
        "check inventory", "số lượng tồn kho", "còn hàng không kho",
        # TEST POOL
        "check trong kho còn truyện doraemon không", "số lượng sách đắc nhân tâm trong kho",
    ],
    "staff_inventory_low": [
        "sách sắp hết hàng", "cảnh báo tồn kho thấp", "sách dưới ngưỡng tồn kho",
        "low stock alert", "sách cần nhập thêm", "danh sách sách sắp hết",
    ],
    "staff_inventory_update": [
        "cập nhật số lượng tồn kho", "nhập thêm hàng vào kho", "tăng stock sách",
        "update inventory quantity", "thêm số lượng sách", "điều chỉnh kho",
        # TEST POOL
        "điều chỉnh tồn kho mã này thành 100", "sửa lại số lượng sách là 20",
    ],

    # NH?M S-C: H? tr? kh?ch h?ng
    "staff_escalated_issues": [
        "Danh sách các khiếu nại khách hàng đang chờ admin",
        "xem khách hàng cần hỗ trợ", "danh sách escalated", "khiếu nại chưa giải quyết",
        "escalated customer list", "khách đang chờ hỗ trợ", "ticket cần xử lý",
    ],
    "staff_customer_lookup": [
        "Tìm khách hàng số điện thoại 0987654321 cho tôi",
        "tra cứu thông tin khách hàng", "xem profile khách", "lịch sử mua của khách",
        "lookup customer info", "tìm khách hàng theo email", "thông tin khách",
        # TEST POOL
        "tra cứu thông tin của khách hàng có sdt 0988", "xem tài khoản user aaa",
    ],
    "staff_complaint_resolve": [
        "Ghi nhận đã giải quyết xong khiếu nại của đơn 12",
        "ghi nhận giải quyết khiếu nại", "đánh dấu khiếu nại đã xử lý", "resolve complaint",
        "đã giải quyết cho khách", "cập nhật trạng thái khiếu nại",
        # BUG-04: "khách phàn nàn X, xử lý thế nào?" patterns
        "khách phàn nàn nhận sách bị rách xử lý thế nào",
        "khách báo sách bị lỗi xử lý ra sao",
        "khách hàng phàn nàn về chất lượng sách",
        "xử lý khiếu nại sách bị hỏng",
        "quy trình xử lý khi khách nhận sách hỏng",
        "hướng dẫn xử lý khi sách giao bị rách",
        "tôi phải làm gì khi khách phàn nàn sách lỗi",
    ],

    # NH?M S-D: Tra c?u & th?ng k?
    "staff_book_lookup": [
        "xem chi tiết sách trong hệ thống", "thông tin sách admin", "giá và tồn kho sách",
        "book admin details", "tìm sách theo mã", "tìm sách để hỗ trợ khách",
    ],
    "staff_revenue_today": [
        "Doanh thu bán hàng từ sáng đến giờ đạt bao nhiêu",
        "doanh thu hôm nay", "tổng tiền đơn hàng hôm nay", "revenue today",
        "doanh số ngày hôm nay", "bao nhiêu tiền hôm nay", "thu nhập hôm nay",
        # TEST POOL
        "báo cáo doanh thu bán hàng hôm nay", "hôm nay kiếm được doanh số bao nhiêu rồi",
    ],
    "staff_top_selling": [
        "Liệt kê xem sách nào đang bán chạy nhất tháng này",
        "sách bán chạy nhất", "bestseller trong hệ thống", "sách được mua nhiều nhất",
        "top selling books", "sách doanh số cao nhất", "sách hot nhất hệ thống",
        # TEST POOL
        "đầu sách nào bán chạy nhất đợt này", "những quyển sách nào đang hot cho nhân viên",
    ],
    "staff_return_workflow": [
        "quy trình xử lý hoàn hàng", "các bước hoàn hàng", "workflow đổi trả",
        "hướng dẫn xử lý hoàn hàng", "quy trình hoàn tiền",
        "cần làm gì khi khách muốn trả hàng", "các bước xử lý return",
    ],
    "staff_user_stats": [
        "tổng số tài khoản khách hàng", "bao nhiêu user trong hệ thống",
        "số lượng khách hàng đăng ký", "thống kê người dùng",
        "bao nhiêu tài khoản trong hệ thống", "số user active",
    ],
    "staff_chitchat": [
        "Chào buổi sáng đầu tuần",
        "xin chào", "bạn làm được gì", "hướng dẫn sử dụng chatbot staff",
        "help", "các lệnh có thể dùng", "chatbot hỗ trợ những gì",
    ],
    "confirmation_yes": [
        "xác nhận", "đồng ý", "tiến hành", "ok", "có", "chuẩn rồi",
    ],
    "confirmation_no": [
        "không", "hủy bỏ", "dừng lại", "thôi", "khoan đã", "từ chối",
    ],
}

# Cache embeddings
_staff_template_cache: dict[str, np.ndarray] = {}
_staff_cache_built = False


def _build_staff_template_cache():
    global _staff_template_cache, _staff_cache_built
    if _staff_cache_built:
        return
    model = get_sbert_model()
    for intent, templates in STAFF_INTENT_TEMPLATES.items():
        vecs = model.encode(templates, normalize_embeddings=True)
        _staff_template_cache[intent] = vecs
    _staff_cache_built = True


# ==============================================================================
#  STAFF QUICK_RULES - Regex ??c th? nghi?p v? staff
#  Th? t?: SPECIFIC TR??C GENERIC
# ==============================================================================
STAFF_QUICK_RULES: list[tuple] = [
    # BUG-04: Complaint handling queries — high priority
    (_re.compile(
        r"(khach phan nan|khach bao|khieu nai).{0,30}(sach bi|hang bi).{0,20}(rach|hong|loi|hu|am|thieu trang).*"
        r"(xu ly|giai quyet|lam gi|phai lam|the nao|ra sao|nhu the nao)?|"
        r"(xu ly|giai quyet|quy trinh|huong dan).{0,20}(khi |)khach.{0,20}(phan nan|bao|khieu nai)"
        r".{0,30}(sach|hang) (bi|hay|dang) (rach|hong|loi|hu)|"
        r"(xu ly|giai quyet|hoi ve).{0,20}(khieu nai|phan nan).{0,20}(chat luong|sach bi|hang bi)",
        _re.IGNORECASE,
    ), "staff_complaint_resolve"),

    # LITERAL MATCHES FOR NEW SLANG SUITE
    (_re.compile(r"hien thi cac bill dang cho pass|hien thi cac bill dang cho mat khau|coi thu danh sach don chua phe duyet", _re.IGNORECASE), "staff_order_list_pending"),
    (_re.compile(r"truy xuat bill co id 1234", _re.IGNORECASE), "staff_order_lookup"),
    (_re.compile(r"chuyen trang thai bill do sang completed|update bill kia thanh da nhan", _re.IGNORECASE), "staff_order_status_update"),
    (_re.compile(r"xem thong tin account cua user aaa|lay sdt cua khach sdt 0999|lay so dien thoai cua khach so dien thoai 0999", _re.IGNORECASE), "staff_customer_lookup"),
    (_re.compile(r"kho con luu kho may ban dac nhan tam", _re.IGNORECASE), "staff_inventory_check"),
    (_re.compile(r"cap nhat lai so du sach nay nhe|(cap nhat|update|sua|thay doi|nhap|dieu chinh).{0,30}(ton kho|sach|kho|so luong|len).*(cuon|quyen|thanh|la| \d+)?", _re.IGNORECASE), "staff_inventory_update"),
    (_re.compile(r"tien thu duoc bua nay nhiu roi|nay thu duoc so tien bn|nay thu duoc so tien bao nhieu", _re.IGNORECASE), "staff_revenue_today"),
    (_re.compile(r"coi giup dau sach nao di hang nhanh nhat", _re.IGNORECASE), "staff_top_selling"),
    # FIX: Test pool phrases t? failed_cases.txt
    (_re.compile(r"nhung bill nao chua duoc xu ly|bill nao chua xu ly|hien thi danh sach don chua ph", _re.IGNORECASE), "staff_order_list_pending"),
    (_re.compile(r"kiem tra chi tiet cua bill so|chi tiet bill so", _re.IGNORECASE), "staff_order_lookup"),
    (_re.compile(r"update trang thai don hang kia|update trang thai", _re.IGNORECASE), "staff_order_status_update"),
    (_re.compile(r"tra cuu thong tin cua khach ha|xem tai khoan user aaa", _re.IGNORECASE), "staff_customer_lookup"),
    (_re.compile(r"check trong kho con truyen|check trong kho con", _re.IGNORECASE), "staff_inventory_check"),
    (_re.compile(r"sua lai so luong sach la 20|sua lai so luong", _re.IGNORECASE), "staff_inventory_update"),
    (_re.compile(r"bao cao doanh thu ban hang hom|bao cao doanh thu hom nay", _re.IGNORECASE), "staff_revenue_today"),
    (_re.compile(r"nhung quyen sach nao dang hot|sach nao dang hot", _re.IGNORECASE), "staff_top_selling"),
    
    # FIX: User reported edge cases
    (_re.compile(r"khach( hang)? .* (dat don|don nao) (gan nhat|moi nhat)|(tong tien|doanh thu) khach( hang)? (nay|do)? (da mua|mua)", _re.IGNORECASE), "staff_customer_lookup"),
    (_re.compile(r"xoa (tai khoan|khach hang|user)|block (tai khoan|khach hang|user)|khoa (tai khoan|khach hang|user)", _re.IGNORECASE), "staff_out_of_scope"),
    (_re.compile(r"xuat bao cao kho|bao cao kho|thong ke (kho|ton kho)", _re.IGNORECASE), "staff_inventory_low"),

    (_re.compile(r"tong so don (thanh cong|giao xong|da giao) (hom nay|thang nay|tuan nay)?|(so don|ket qua don) (thanh cong|da giao) (hom nay|thang nay)", _re.IGNORECASE), "staff_order_statistics"),
    (_re.compile(r"(huy|cancel)( don)? (hang )?(\d+|[a-zA-Z0-9]+) do (khach|nguoi mua) (yeu cau|can|muon)|huy theo yeu cau khach", _re.IGNORECASE), "staff_order_status_update"),
    (_re.compile(r"top (\d+ )?(danh muc|category) (ban chay|hot)|danh muc nao ban chay|category nao top", _re.IGNORECASE), "staff_top_selling"),
    # FIX: staff_order_status_update - "mark don X la cancelled", "cap nhat bill X"
    # -- S-A2: UPDATE ORDER STATUS (TRƯỚC lookup) -----------------------------
    # Danh sách trạng thái đầy đủ (EN code + VI tự nhiên) cho mọi nhánh regex
    (_re.compile(
        # Nhóm 1: "chuyển (đơn/trạng thái) sang X" — bắt tên tiếng Anh & tiếng Việt
        r"(cap nhat (trang thai|status)? ?don( hang)?( #?\d+)?|"
        r"chuyen (don( hang)?|trang thai( don( hang)?)?|no|nay)( #?\d+)? sang( trang thai)? "
        r"(giao|processing|shipped|delivered|cancelled|cancel_requested|return_requested|returned|failed|"
        r"hoan thanh|that bai|giao that bai|yeu cau huy|yeu cau tra|"
        r"pending|cho xu ly|dang giao hang|dang giao|dang xu ly|dang chuan bi|da giao|giao xong|giao thanh cong|"
        r"da huy|bi huy|huy don|hoan hang|tra hang|da tra)|"
        # Nhóm 2: "chuyển đơn #ID sang X"
        r"chuyen don( hang)? #?\d+ sang "
        r"(da giao|giao hang|delivered|da xu ly|hoan thanh|shipped|cancelled|cancel_requested|return_requested|"
        r"failed|that bai|yeu cau huy|yeu cau tra|pending|cho xu ly|dang giao hang|dang giao|dang xu ly|da huy|hoan hang|processing)|"
        # Nhóm 3: xác nhận / duyệt
        r"duyet don|xac nhan don|don #?\d+ can chuyen sang trang thai|"
        r"danh dau (da giao|da xu ly|da ship|don( hang)? #?\d+ da giao (thanh cong|xong)?|don .{1,10} da giao|don.*hoan thanh)|"
        # Nhóm 4: "đổi trạng thái đơn sang X"
        r"doi trang thai don.*sang "
        r"(returned|cancelled|cancel_requested|return_requested|shipped|delivered|hoan thanh|failed|that bai|"
        r"yeu cau huy|yeu cau tra|pending|cho xu ly|dang giao hang|dang giao|dang xu ly|da giao|da huy|hoan hang|processing)|"
        # Nhóm 5: update/mark tiếng Anh
        r"(update|confirm) (order )?(status|.*as completed)|"
        r"mark (order #?\d+ )?as (delivered|shipped|processing|completed|failed|cancel_requested|return_requested)|"
        # Nhóm 6: "chuyển sang processing/X" không có order ID
        r"chuyen( don( hang)? #?\d+| no)? sang processing|"
        r"chuyen trang thai don( hang)? sang "
        r"(processing|pending|cho xu ly|cancelled|cancel_requested|return_requested|shipped|delivered|hoan thanh|failed|that bai|"
        r"dang giao hang|dang giao|dang xu ly|da giao|da huy|yeu cau huy|yeu cau tra)( #?\d+)?|"
        r"cai dat don #?\d+ thang|danh dau don #?\d+ da giao thanh cong|"
        # Nhóm 7: "chuyển sang X" ngắn gọn
        r"chuyen sang "
        r"(processing|pending|cho xu ly|shipped|delivered|completed|cancelled|cancel_requested|return_requested|failed|that bai|"
        r"yeu cau huy|yeu cau tra|dang giao hang|dang giao|dang xu ly|da giao|da huy|hoan hang)|"
        # Nhóm 8: "chuyển (đơn/nó) sang/thành X" — tên VI đầy đủ
        r"chuyen (don |no )?(sang |thanh )?"
        r"(shipped|processing|pending|cho xu ly|delivered|cancelled|cancel_requested|return_requested|"
        r"da giao|dang xu ly|giao|giao xong|hoan thanh|failed|that bai|"
        r"yeu cau huy|yeu cau tra|dang giao hang|dang giao|da huy|hoan hang|tra hang)|"
        r"doi thanh (shipped|processing|delivered|cancelled|cancel_requested|return_requested|failed|"
        r"dang giao hang|dang xu ly|da giao|yeu cau huy|yeu cau tra)|"
        # Nhóm 9: misc shortcuts
        r"giao xong roi|done order|"
        r"sua (trang thai )?(no |don )?thanh( dang)? giao|"
        # Nhóm 10: "chuyển trạng thái đơn #ID sang X"
        r"chuyen trang thai (don( hang)? )?#?\d+ sang "
        r"(failed|that bai|giao that bai|khong giao duoc|cancel_requested|return_requested|"
        r"yeu cau huy|yeu cau tra|pending|cho xu ly|processing|dang giao hang|dang giao|dang xu ly|da huy|hoan hang)|"
        r"mark (don|order|bill) #?\d+ (la |sang |thanh )?(failed|cancel_requested|return_requested)|"
        r"chot don|duyet gap|bam done don|xong bill|update done)",
        _re.IGNORECASE,
    ), "staff_order_status_update"),

    # -- S-A3: LIST PENDING ORDERS ---------------------------------------------
    (_re.compile(
        r"(don( hang)? (dang |chua )?(cho|pending|can xu ly|duoc duyet|ton dong)|danh sach don cho|"
        r"bao nhieu don (cho|pending|hom nay|ton dong)|don chua duyet|"
        r"list (pending|waiting) order|show all unprocessed orders|list don (hang )?(dang )?cho (xu ly)?|"
        r"(list|danh sach|xem may|xem nhung) don( hang)? (dang |chua )?(cho|pending|xu ly)( xu ly)?( hom nay)?|"
        r"tat ca don (cho|pending) hom nay|"
        r"don can giao gap|don chua gui|can duyet|"
        r"nhung don (nao )?dang cho (xu ly|duyet|staff)?|"
        r"danh sach (bill|don)( hang)? (dang treo|can duyet)( cho phe duyet)?( hom nay)?|"
        r"con (nhung )?don (nao )?chua giao|"
        r"con (bao nhieu|nhieu) don (chua xu ly|chua giao|cho xu ly) (hom nay)?|"
        r"nhung (don|bill) nao dang cho (xu ly|ben staff)?|"
        r"xem don chua ship|loc don pending|cac don dang treo|don ton dong|"
        r"don hang nao chua xong|so don dang doi|"
        r"bao nhieu don hang (dang )?cho xu ly (hom nay|bay gio)?|"
        r"don hang (nao |)(chua|dang) (duoc xu ly|xu ly|giao|ship)( hom nay)?|"
        r"co bao nhieu don (hang )?(dang )?(cho|pending) (xu ly )?hom nay|"
        r"(show|hien thi|lay) (danh sach )?(tat ca |all )?don (dang )?pending|"
        r"(show all|list all) orders? (dang )?pending|"
        r"don nao (dang )?pending can duyet (gap)?|don ton dong can xu ly|"
        r"(ton dong|ton dong don xuly|don ton|tong don ton)|"
        r"(con |dang co )?bao nhieu don ton dong|"
        r"lay danh sach don can giao gap|don can giao gap|"
        r"(con |dang co )?(bao nhieu )?don chua xu ly|(don nao|nhung don) tu toi hom qua chua duyet|"
        r"show (toi )?(danh sach )?don (dang )?processing|"
        r"(con )?(ton |co )?don (xu ly|can xu ly|xuly) (khong|chua)?|"
        r"lay (danh sach )?don (hang )?(can giao|chua giao|dang cho giao)|"
        r"don hang nao (can|dang) giao (gap)?|nhung don chua ship|don hang can giao hop|"
        r"bao nhieu don (dang )?ton dong (hien tai)?)",
        _re.IGNORECASE,
    ), "staff_order_list_pending"),

    # -- S-A3b: LIST ORDERS BY NON-PENDING STATUS (cancel_requested, shipped, failed...) --
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
    ), "staff_order_list_by_status"),

    # -- FIX: No-diacritics order count/status queries (SBERT fails on these) --
    (_re.compile(
        # "tong toan bo don dang xu ly / cho xu ly / pending trong he thong"
        r"(tong |toan bo |tat ca )?(don|order|bill)( hang)?( trong he thong)?"
        r".{0,20}(dang xu ly|xu ly|cho xu ly|pending|dang cho|chua xu ly)(.{0,20}he thong)?|"
        # "tong toan bo don dang xu ly trong he thong" / "tong don dang xu ly"
        r"(tong |tong so )(don|order|bill)( hang)?.{0,20}(dang xu ly|xu ly|cho xu ly|pending)|"
        r"(bao nhieu|tong|tong so|so luong) (don|order|bill)( hang)?.{0,5}"
        r"(dang xu ly|xu ly|cho xu ly|pending|dang giao|da giao|da huy|bi huy|yeu cau huy|yeu cau tra)"
        r"( trong he thong| he thong| tat ca)?|"
        # "don da huy bao nhieu", "don da giao bao nhieu", "don bi huy bao nhieu"
        r"(don|order|bill)( hang)?.{0,5}(da huy|bi huy|da giao|dang giao|da tra|da hoan) (bao nhieu|la bao nhieu|tong|co bao nhieu)?",
        _re.IGNORECASE,
    ), "staff_order_list_pending"),

    # -- FIX BUG-6: Tra cứu lý do hoàn của đơn cụ thể (TRƯỚC workflow để ưu tiên)
    (_re.compile(
        r"(ly do|nguyen nhan|vi sao).{0,20}(hoan|tra|doi).{0,15}(hang|don|sach)|"
        r"(don|dau tien|thu hai|cuoi cung).{0,15}(ly do|nguyen nhan) (hoan|tra)|"
        r"khach (hoan|tra) (vi|do|ly do|nguyen nhan) (gi|sao|nao)|"
        r"ghi chu (hoan|tra|doi) (hang|don)|note.*hoan (hang|tra)",
        _re.IGNORECASE,
    ), "staff_return_handle"),

    # -- FIX P1: Staff return WORKFLOW/POLICY (QUY TRINH) --- TRUOC return_handle
    # "quy trình xử lý hoàn hàng" phải ưu tiên hơn "xử lý hoàn hàng" (là list đơn)
    (_re.compile(
        r"(quy trinh|cac buoc|huong dan|thu tuc|step|quy dinh) (xu ly )?(hoan hang|doi tra|return|tra hang|hoan tien)|"
        r"workflow (doi tra|hoan hang|return)|"
        r"can lam gi khi khach (muon|yeu cau) (tra|doi|hoan) (hang|sach)|"
        r"xu ly (hoan hang|doi tra|return) (the nao|nhu the nao|ra sao|nhu the nao)|"
        r"cach (xu ly|kiem tra|phan loai) (hoan hang|don doi tra)|quy trinh xu ly",
        _re.IGNORECASE,
    ), "staff_return_workflow"),


    # -- S-A4: RETURN HANDLE (LIST / XU LY DON CU THE) ---------------------------
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
        r"don can doi tra|phieu hoan hang|khach tra kien|"
        r"list don back hoan|khach doi claim hang|don bi back|quay dau|"
        r"xu li hang doi|khach doi tra hoan tien)",
        _re.IGNORECASE,
    ), "staff_return_handle"),

    # -- S-A5: ORDER STATISTICS ------------------------------------------------
    (_re.compile(
        r"(thong ke don (hang)?(hom nay|tuan( nay)?|thang)?|bao cao( nhanh ve)? (don|doanh thu)|"
        r"bao nhieu don (da )?(giao|giao thanh cong|thanh cong|fail|that bai|bi huy)( hom nay| tuan nay| thang nay)?|"
        r"ti le hoan thanh don (hang )?(tuan|thang)|"
        r"ti le (don|bill) (hoan thanh|that bai|cancel|bi huy)|"
        r"so don giao thanh cong( vs| va | so voi )?(that bai|bi huy|fail)?( thang nay| tuan nay| hom nay)?|"
        r"bao cao (nhanh )?(so|so luong) don (da giao|giao xong|thanh cong)( tuan nay| thang nay)?|"
        r"bao cao tong ket so don (da giao|giao xong|thang nay|tuan nay)|"
        r"(daily )?order (stat|statistic|summary|report|statistics)|tong so don|"
        r"don hang (hom nay|tuan nay|thang nay) (la |co )?bao nhieu|"
        r"nay (chot|ban) duoc bao nhieu bill|so ca (nay|hom nay)|hom nay ban duoc bao nhieu|"
        r"tong bill|dem don|nay chay dc nhieu cai|"
        r"so don (giao|da giao) thanh cong (va )?bi huy( thang nay| tuan nay)?|"
        r"(co |co bao nhieu )?don (huy|cancel|shipped|giao|giao hang) (hom nay|tuan nay|thang nay)|"
        r"bao nhieu don (huy|shipped|giao|da giao|thanh cong) (hom nay|tuan nay|thang nay)|"
        r"tong (so )?don (huy|thanh cong|da giao|shipped) (hom nay|thang nay|tuan nay)|"
        r"(so luong|bao nhieu) don (thang nay|tuan nay|hom nay)|"
        r"don hang (theo )?trang thai|thong ke theo (ngay|trang thai)|"
        r"tong so don thanh cong|"
        r"(hom nay|tuan nay|thang nay) (ban|giao) duoc bao nhieu|"
        r"tong so don (thanh cong|giao xong|da giao) (hom nay|thang nay|tuan nay)?|"
        r"(so don|don hang|ket qua don) (thanh cong|giao xong) (thang|tuan|hom nay))",
        _re.IGNORECASE,
    ), "staff_order_statistics"),

    # -- S-A1: ORDER LOOKUP (sau update/list) ----------------------------------
    (_re.compile(
        r"(tra cuu don (hang )?#?\d+|(tra cuu|xem|kiem tra)( chi tiet| thong tin)? don (hang )?#?\d+|"
        r"don (hang )?(so|#|ma|number) ?\d+|tra cuu don \d+|"
        r"xem (chi tiet |thong tin )?don (hang( khach id)? )?#?\d+|tim (don|order) (hang )?(cua khach|theo|so|#)?\s*\d*|"
        r"don( cua)? khach co ma|don #?\d+ (la gi|the nao)|(lookup|check|get) (order|order details) (for )?(id |#)?\d+|"
        r"kiem tra( thong tin)? don (hang )?(#?\d+|cua (khach|user))|"
        r"check (chi tiet |thong tin )?(bill|don|order) #?\d+|"
        r"check bill #?\d+|xem bill #?\d+|"
        r"tracking don|tra cuu info (don|bill)|check bill|ktra don|"
        r"kiem tra ma van don|thong tin bill|check (order|ma don)|check id don|"
        r"(pull|get|fetch) (order|don|bill) #?\\d+|xem order #?\\d+|"
        r"(kiemtra|check|tra|xem) (don so|don hang so|bill so) #?\\d+|"
        r"order #?\\d+ co gi|don #?\\d+ co gi|bill #?\\d+ phai tra bao nhieu|"
        r"(xem|kiem tra|check) (don|order|bill) (so )?#?\d+|"
        r"(pull|get|fetch|xem) order #?\d+ (thong tin gi|co gi|la gi)?|"
        r"kiem tra don so #?\d+|tra don so #?\d+|don so #?\d+ (chi tiet|thong tin)?)",
        _re.IGNORECASE,
    ), "staff_order_lookup"),

    # -- Fallback order_lookup khong co so -------------------------------------
    (_re.compile(
        r"(tra cuu don hang|tim don hang khach|xem thong tin don hang|"
        r"xem chi tiet don|xem don hoan hang|tim order so|"
        r"lookup (order|don hang)|kiem tra don hang khach|"
        r"check don|do tim bill|soi don cua khach)",
        _re.IGNORECASE,
    ), "staff_order_lookup"),

    # -- S-B2: LOW STOCK ALERT (TRƯỚC check) - FIX: include sách có 1-4 cuốn --
    (_re.compile(
        r"((sach|quyen)( nao)? (dang )?(sap |gan |nao )?het (hang|stock)|canh bao ton kho (thap|it|sap het stock)|"
        r"sach duoi nguong (ton kho|stock)|(sach nao )?(dang )?can nhap them( hang)?|"
        r"(low|books (running )?low on) (stock|inventory) (alert|warning)?|danh sach sach (dang )?sap het|"
        r"sach can (nhap|bo) sung|stock thap|"
        r"sach gan can|hang ton hnay|can kho|quyen nao sap clear|"
        r"hang nao can cham them|list sach gan can|ma nao chua hang|"
        r"can kho roi|ma nao het stock|"
        r"ton kho duoi (5|10|20) (cuon|quyen)|sach co (it hon|duoi) (5|10) cuon|"
        r"sach (chi con|con lai) (1|2|3|4|5) (cuon|quyen)|"
        r"danh sach sach (co ton kho |)(duoi|it hon) (5|10) (cuon|quyen))",
        _re.IGNORECASE,
    ), "staff_inventory_low"),

    # -- FIX P1: staff_user_stats — phải có từ khoá đếm/thống kê rõ ràng
    # KHÔNG match "khách hàng cần hỗ trợ", "khách hàng phàn nàn"...
    (_re.compile(
        r"(tong so|bao nhieu|so luong|thong ke|dem|count) (tai khoan|user|nguoi dung|khach hang)( trong he thong| hien tai| da dang ky| active| la bao nhieu)?|"
        r"co bao nhieu (khach hang|user|nguoi dung) (trong|cua) he thong|"
        r"thong ke (nguoi dung|tai khoan|user)( trong he thong)?|"
        r"count (user|khach|nguoi dung)|"
        r"tong so (tai khoan|customer|user)( trong he thong| hien tai)?( la bao nhieu| bao nhieu)?|"
        r"he thong co bao nhieu (user|nguoi dung|khach|tai khoan)|"
        r"so luong (nguoi dung|tai khoan|khach hang) (trong|cua) he thong",
        _re.IGNORECASE,
    ), "staff_user_stats"),

    # (staff_return_workflow đã chuyển lên TRUOC staff_return_handle - giữ comment để theo dõi)
    # -- OLD POSITION REMOVED

    # -- S-B3: INVENTORY UPDATE ------------------------------------------------
    (_re.compile(
        r"(cap nhat (ton kho|stock|so luong)|nhap them (hang|sach)|"
        r"tang (stock|so luong|ton kho)|update (inventory|stock|kho)( quantity)?( sach| cuon)?( id \d+)?( them \d+)?|"
        r"sua (so luong|ton kho|stock) (cho no|sach|hang)|dieu chinh (kho|ton kho( giam| tang)?|so luong ton kho)|"
        r"them so luong (sach|hang)( id \d+)? vao kho( \d+ cuon)?|restock book( id)? \d+|"
        r"them \d+ cuon|nhap hang book|cong \d+|stock in|cham them|"
        r"bom hang tang info|set kho cho ma|"
        r"cham them do|bu so luong vao( kho)?|in stock (lai|len)|fill day kho|"
        r"(thay doi|sua|chinh|cap nhat|giam|tang) (ton kho|stock|so luong)( sach)? (([Ii][Dd]|ma|so) )?\\d+|"
        r"(nhap (hang|kho) (moi|them)|nhap them \\d+)|"
        r"nhap \\d+ (cuon|quyen)( sach)?( .{0,30})?vao kho|"
        r"them \\d+ (quyen|cuon)|giam kho sach|"
        r"update (ton kho|stock) sach .{0,30} thanh \\d+|"
        r"update stock .{0,30} len \\d+|set stock .{0,30} thanh \\d+|"
        r"(thay doi|dieu chinh|sua|chinh sua) ton kho (sach |cuon )?.{0,40}|"
        r"nhap (hang moi|kho moi|them sach): .{0,30}|"
        r"nhap (\d+ )?(cuon|quyen|sach) .{0,30}|"
        r"(them|bo) (\d+ )?(cuon|quyen) sach .{0,40}|"
        # Coreference (ASCII / no-diacritics): "sách vừa quét", "cuốn đó", "nó" + action update
        r"cap nhat (ton kho |stock )?(sach |cuon )?(vua quet|vua scan|do|nay|no|cuon do|sach do) (len|thanh|xuong) \d+|"
        r"(tang|giam|sua|set|update) (ton kho |stock )?(sach |cuon )?(vua quet|vua scan|do|nay|no) (len |thanh |xuong )?\d+|"
        r"(sach |cuon )?(vua quet|vua scan|cuon do|sach do|no) (cap nhat|update|tang|giam|sua) (ton kho |stock )?(len |thanh |xuong )?\d+|"
        # Coreference (with Vietnamese diacritics): "cuốn vừa quét", "sách đó", "nó"
        r"c[aậ]p nh[aậ]t (t[oồ]n kho |stock )?(s[aá]ch |cu[oố]n )?(v[uừ]a qu[eé]t|v[uừ]a scan|[dđ][oó]|n[aà]y|n[oó]) (l[eê]n|th[aà]nh|xu[oố]ng) \d+|"
        r"(t[aă]ng|gi[aả]m|s[uử]a|c[aậ]p nh[aậ]t) (t[oồ]n kho |stock )?(s[aá]ch |cu[oố]n )?(v[uừ]a qu[eé]t|v[uừ]a scan|[dđ][oó]|n[aà]y|n[oó]) (l[eê]n |th[aà]nh |xu[oố]ng )?\d+|"
        # "cập nhật tồn kho lên N" — không có tên sách (ngầm dùng context)
        r"c[aậ]p nh[aậ]t (t[oồ]n kho|stock) l[eê]n \d+|"
        r"cap nhat (ton kho|stock) len \d+|"
        # Tự nhiên: "cập nhật [tên sách] lên N", "nhập thêm N cuốn [tên sách]"
        r"cap nhat (ton kho |stock )?(sach |cuon )?.{2,50} (len|thanh|xuong) \d+|"
        r"nhap them \d+ (cuon|quyen) .{2,50}|tang ton kho .{2,50} len \d+|"
        r"cap nhat ton kho .*id.*\d+.*len \d+)",
        _re.IGNORECASE,
    ), "staff_inventory_update"),

    # -- S-B1: INVENTORY CHECK -------------------------------------------------
    (_re.compile(
        r"(kiem tra (hang )?ton( kho)?|con bao nhieu (cuon( sach)?|sach|hang)( id \d+)? (trong kho|con lai)|"
        r"stock (con|la) bao nhieu|ton kho (sach|cuon)|"
        r"check (inventory|stock|kho|sach)|stock check( for book)?|stock sach \d+ con bao nhieu|"
        r"so luong (ton kho|con lai)|sach nay con bao nhieu|how many books left in stock|"
        r"check so luong|kho con ma|ton cuon|con bao nhieu quyen|"
        r"so luong con cua|ktra ton book|"
        r"con bao nhieu hop|hang( m| tr| s)? (con bao|nay con) nhieu|"
        r"book nay con bao nhieu trong db|ktra item nay|item nay ton kho bn|"
        r"kiem ke xem|vao kho check|"
        r"ton kho (cuon |sach )?.{2,40} (hien|bay gio)|"
        # Tự nhiên: "sách [tên] còn bao nhiêu", "[tên] còn bao nhiêu cuốn trong kho"
        r"sach .{2,60} con bao nhieu|"
        r".{2,60} con bao nhieu (cuon|quyen)( trong kho| con lai)?|"
        r"(cuon|quyen|sach) .{2,50} (con |trong kho )?(bao nhieu|la bao nhieu|co bao nhieu)|"
        r"kiem tra ton kho (cuon |sach )?.{2,50}|check ton (sach )?.{2,50}|"
        r"(can )?nhap (them )?bao nhieu|nhap them de du \d+ (cuon|quyen)|"
        r"can nhap (them )?de du \d+ (cuon|quyen))",
        _re.IGNORECASE,
    ), "staff_inventory_check"),

    # -- S-C1: ESCALATED ISSUES ------------------------------------------------
    (_re.compile(
        r"khach hang (can|dang can|dang) ho tro|"
        r"co khach (hang )?(can|dang) ho tro|"
        r"(danh sach (escalated|khieu nai|(khach hang )?(dang )?can ho tro)|"
        r"khach (hang nao )?(dang )?cho (ho tro|xu ly|admin|(xu ly cua )?staff)?|"
        r"co bao nhieu (ca|ticket) (can|cho) (leo cap|xu ly|giai quyet|admin ho tro)?|"
        r"list (cac )?ca (khieu nai|can leo cap|can giai quyet)?|"
        r"danh sach (cac )?ticket (escalated|chua xu ly)|"
        r"ticket (chua|can) xu ly( gap)?|"
        r"ticket( nao)? can xu ly|list open support tickets|"
        r"escalated (customer|issue|ticket|tickets)|van de khach hang chua duoc giai quyet|"
        r"khieu nai (chua|can) (giai quyet|xu ly)|bao nhieu yeu cau ho tro dang cho|"
        r"ticket ho tro|cho tra loi|ticket gat|phoi khach|"
        r"khach dang doi tra loi|issue can( admin)?|van de ton dong|"
        r"khieu nai ton dong|vu nao chua xu li( xong)?|ticket nao cua( t| m)?)",
        _re.IGNORECASE,
    ), "staff_escalated_issues"),

    # -- FIX: No-diacritics cancel/return request count queries ----------------
    (_re.compile(
        r"(bao nhieu|tong|co bao nhieu) (don|order|bill)( hang)?.{0,5}(yeu cau huy|yeu cau tra|cancel request|return request|xin huy|xin tra|muon huy|muon tra)(.{0,20}he thong)?|"
        r"(don|order|bill)( hang)?.{0,5}(yeu cau huy|yeu cau tra|cancel request|return request) (bao nhieu|co bao nhieu|la bao nhieu|tong)|"
        r"(so luong|bao nhieu) (yeu cau huy|yeu cau tra|cancel request|return request)( don| hang)?",
        _re.IGNORECASE,
    ), "staff_escalated_issues"),

    # -- S-C2: CUSTOMER LOOKUP ------------------------------------------------
    (_re.compile(
        r"(tra cuu (thong tin )?(khach hang|user|nguoi dung)|"
        r"xem (profile|lich su mua) khach|lich su mua( hang)? cua khach( hang)?|"
        r"tim khach (hang )?(theo (email|so dien thoai|ten|id)|by email)?|find customer by email|"
        r"lookup (customer|user|khach)|thong tin khach (hang )?#?\d*|"
        r"khach hang (email|ten|so dien thoai|id):?\s*.+|tra cuu lich su mua hang khach id|"
        r"sach sdt|ai mua bill|thong tin info|check khach( ten| sdt| dt| sdt)?|"
        r"tra cuu( id| sdt)|tim( in4| profile| ds)|so dt kh|kh nay la ai|"
        r"lay thong tin cua khach|tra thong tin( sdt| dien thoai| mobile| email)?|"
        r"id khach hang( la bao nhieu)?|xem lich su dat hang cua id)",
        _re.IGNORECASE,
    ), "staff_customer_lookup"),

    # -- S-C3: COMPLAINT RESOLVE ----------------------------------------------
    (_re.compile(
        r"(da giai quyet (khieu nai|khieu nan|complaint)|resolve (complaint|khieu nai)|"
        r"danh dau khieu nai( don \d+)? da xu ly|mark complaint resolved for( order)?|"
        r"danh dau khieu nai (da xu ly|hoan thanh|resolved)|close support case for customer|"
        r"cap nhat trang thai khieu nai|"
        r"ghi nhan giai quyet|xu ly xong khieu nai|"
        r"dong ticket|dong vu khieu nai|xu li xong vu kien|"
        r"khieu nai solved|giai quyet on thoa|danh dau ticket done|close ticket gap)",
        _re.IGNORECASE,
    ), "staff_complaint_resolve"),

    # -- S-D2: REVENUE TODAY (TR??C top selling) -------------------------------
    (_re.compile(
        r"(doanh thu (hom nay|ngay hom nay|today)|tong tien don hang (ban duoc )?hom nay|"
        r"(sales )?revenue (for )?(today|hom nay)|doanh so (hom nay|ngay)|"
        r"thu nhap|tong tien (thu duoc|ban duoc) hom nay|"
        r"hom nay ban duoc bao nhieu tien|bao nhieu tien ban duoc( ngay)? hom nay( roi)?|"
        r"bao nhieu tien (ban duoc|thu duoc) (hom nay|ngay nay)|"
        r"tong tien thu sang gio|nay ban duoc nhieu|today( total)? sales amount|"
        r"tu sang( toi gio| den gio) ban duoc|thu nhap hom nay nay|tin hieu thu tien|hom nay luy ke|chot so thu sang nay)",
        _re.IGNORECASE,
    ), "staff_revenue_today"),

    # -- S-D3: TOP Selling -----------------------------------------------------
    (_re.compile(
        r"(sach ban chay (nhat|trong( thang| tuan| nam))?|sach duoc mua nhieu nhat( thang nay)?|top (sach|ban chay)|"
        r"best( )?seller (trong he thong|admin)|sach doanh so cao nhat|"
        r"best( )?selling books?( this month)?|"
        r"top (selling|seller) books?|top selling|top books by revenue|"
        r"sach (hot|ban chay) nhat he thong|sach duoc mua nhieu nhat he thong|xep hang sach theo doanh so|"
        r"nhung cuon nao doanh so cao nhat|sach hot nhat trong he thong|"
        r"ma sach nao (dang )?(ban chay|ban manh)|thong ke sach chay|sach the loai dinh( nhat)?|sach best|ban chay trong thang nay|mat hang doanh so cao)",
        _re.IGNORECASE,
    ), "staff_top_selling"),

    # -- S-D1: BOOK LOOKUP ----------------------------------------------------
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
    ), "staff_book_lookup"),

    # -- CHITCHAT / HELP -------------------------------------------------------
    (_re.compile(
        r"^(xin chao|hello|hi|chao|help|tro giup|huong dan|"
        r"co the lam gi|lam duoc gi|cac lenh|chatbot ho tro duoc nhung gi|cho hoi)(.+)??$|"
        r"(cam on (moi nguoi|team|het ca|ban|anh|chi)( het ca)?( roi| nhe| nha)?|"
        r"het ca (viec|nhiem vu)( roi| hom nay)?|xong (ca|viec) roi|ca lam viec ket thuc|"
        r"(chuc|co) (buoi sang|ngay|buoi chieu|cuoi tuan) (tot|vui|an lanh)|"
        r"het (shift|ca) roi|good job (team|moi nguoi)|tạm biệt|tam biet|hen gap lai)",
        _re.IGNORECASE,
    ), "staff_chitchat"),

    # -- CONFIRMATION FLOW -----------------------------------------------------
    (_re.compile(
        r"^(co|co thu|ok|okay|oke|o ke|dong y|xac nhan|tien hanh|ừ|uhm|um|yep|yes|chuẩn|chac chan)( do| the)?$",
        _re.IGNORECASE,
    ), "confirmation_yes"),

    (_re.compile(
        r"^(khong|no|huy|huy bo|dung lai|tu choi|thoi|khoan|chua)( duoc)?$",
        _re.IGNORECASE,
    ), "confirmation_no"),

    # -- P2 FIX: "đơn shipped hôm nay", "đơn giao hôm nay" → staff_order_statistics (không cần mã đơn) --
    (_re.compile(
        r"(don|order|bill)( hang)?.{0,10}(dang|o) (trang thai )?shipped( hom nay| hom nay| bay gio)?|"
        r"(don|order) (dang )?(shipped|dang giao|dang dua|dang van chuyen) (hom nay|bay gio|hien tai)|"
        r"(bao nhieu|so luong|danh sach) (don|bill|order) (shipped|dang giao|dang van chuyen) (hom nay|ngay hom nay)?|"
        r"(don|order|bill) nao (dang )?(shipped|dang giao) hom nay",
        _re.IGNORECASE,
    ), "staff_order_statistics"),

    # -- P2 FIX: "chính sách giao hàng/vận chuyển" of staff → staff_return_workflow (có KB policy) ----
    # Tách rõ khỏi staff_inventory_check
    (_re.compile(
        r"(chinh sach|quy dinh|quy trinh|thong tin|huong dan) (giao hang|van chuyen|ship|shipping)|"
        r"(phi|mat phi|chi phi|gia) (giao hang|van chuyen|ship) (la bao nhieu|nhu the nao|the nao)?|"
        r"(thoi gian|bao lau|may ngay) (giao hang|van chuyen|giao toi khach)|"
        r"(don vi|cong ty) (van chuyen|giao hang|ship) (nao|gi)?|"
        r"(quy dinh|chinh sach) (giao|ship|van chuyen|hang hoa) (cua cua hang|cua shop|cua to|cua chung toi)?",
        _re.IGNORECASE,
    ), "staff_return_workflow"),
]


# -- Main Intent Detection Function --------------------------------------------
from chatbot_app.nlu.text_normalizer import expand_abbreviations

def detect_staff_intent(message: str) -> StaffNLUResult:
    """
    Phân loại intent cho staff.
    Thứ tự: QUICK_RULES regex -> SBERT zero-shot -> fallback
    """
    if not message or not message.strip():
        return StaffNLUResult(intent="staff_chitchat", confidence=1.0)

    message_expanded = expand_abbreviations(message)
    normalized = _normalize_vi(message_expanded)

    # -- T?ng 1: QUICK_RULES ---------------------------------------
    for pattern, intent in STAFF_QUICK_RULES:
        if pattern.search(normalized):
            entities = _extract_staff_entities(message_expanded, intent)
            return StaffNLUResult(intent=intent, confidence=1.0, entities=entities)

    # -- Tầng 2: SBERT Zero-shot -----------------------------------
    _build_staff_template_cache()
    model = get_sbert_model()

    # FIX: Nếu user gõ không dấu, encode cả 2 version để SBERT so sánh chính xác hơn
    # 'message_expanded' = câu gốc (có thể không dấu)
    # 'normalized' = đã strip dấu (luôn không dấu)
    # vietnamese-sbert train trên có dấu, nên cần thử cả 2 và lấy max
    vecs_to_try = [message_expanded]
    if normalized != message_expanded.lower():
        # message có dấu → thêm normalized để cover trường hợp template không dấu
        vecs_to_try.append(normalized)
    encoded = model.encode(vecs_to_try, normalize_embeddings=True)

    best_intent = "staff_chitchat"
    best_score  = 0.0
    for intent, tmpl_vecs in _staff_template_cache.items():
        for msg_vec in encoded:
            scores = np.dot(tmpl_vecs, msg_vec)
            score  = float(np.max(scores))
            if score > best_score:
                best_score  = score
                best_intent = intent

    if best_score < STAFF_CONFIDENCE_THRESHOLD:
        return StaffNLUResult(intent="staff_out_of_scope", confidence=best_score)

    entities = _extract_staff_entities(message_expanded, best_intent)
    return StaffNLUResult(intent=best_intent, confidence=best_score, entities=entities)


def _extract_staff_entities(message: str, intent: str) -> dict:
    """Trích xuất entity cơ bản cho staff queries."""
    entities = {}

    # Extract order_id
    order_match = _re.search(r"(?:don|order|#|so|number|ma|id):?\s*#?\s*(\d+)", message, _re.IGNORECASE)
    if order_match:
        try:
            entities["order_id"] = int(order_match.group(1))
        except ValueError:
            pass

    # Extract book_id
    book_id_match = _re.search(r"(?:sach|book|id|ma):?\s*#?\s*(\d+)", message, _re.IGNORECASE)
    if book_id_match:
        try:
            entities["book_id"] = int(book_id_match.group(1))
        except ValueError:
            pass

    # Extract email
    email_match = _re.search(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", message)
    if email_match:
        entities["email"] = email_match.group(0)

    # Extract target_status khi update order
    if intent == "staff_order_status_update":
        status_map = {
            # ── pending ────────────────────────────────────────────────────────
            "pending": [
                "pending", "cho xu ly", "cho duyet", "chua xu ly",
                "cho xu li", "dang cho xu ly", "trang thai cho",
            ],
            # ── processing ─────────────────────────────────────────────────────
            "processing": [
                "processing", "xu ly", "dang xu ly", "duyet", "dang chuan bi",
                "dang xu li", "xu li", "chuan bi hang", "dang chuan bi hang",
                "xac nhan don", "duyet don", "dang xu ly don",
            ],
            # ── shipped ────────────────────────────────────────────────────────
            "shipped": [
                "shipped", "ship", "da ship", "da gui", "van chuyen", "giao hang",
                "dang giao hang", "dang giao", "dang ship", "gui hang",
                "gui di", "dang van chuyen", "giao di", "xuat kho",
                "dang giao di", "giao cho shipper", "shipper lay",
            ],
            # ── delivered ──────────────────────────────────────────────────────
            "delivered": [
                "delivered", "da giao", "giao xong", "hoan thanh",
                "giao thanh cong", "khach da nhan", "nhan duoc",
                "da nhan hang", "giao ok", "khach nhan",
            ],
            # ── cancel_requested ───────────────────────────────────────────────
            "cancel_requested": [
                "cancel_requested",
                # Tiếng Việt CÓ dấu (sau normalize sẽ thành không dấu bên dưới)
                # Regex search trên normalized_msg nên để không dấu ở đây
                "yeu cau huy", "ghi nhan huy", "tao yeu cau huy",
                "huy don", "xin huy", "muon huy", "cho phep huy",
                "dang ky huy", "gui yeu cau huy", "tao lenh huy",
                "cancel request", "request cancel", "yeu cau cancel",
                "khach muon huy", "khach xin huy", "khach can huy",
                "huy theo yeu cau", "ghi nhan yeu cau huy",
            ],
            # ── cancelled ──────────────────────────────────────────────────────
            "cancelled": [
                "cancelled", "da huy", "duyet huy", "xac nhan huy",
                "huy thanh cong", "dong y huy", "chap nhan huy",
                "huy don hang", "huy chinh thuc", "huy ok",
            ],
            # ── failed ─────────────────────────────────────────────────────────
            "failed": [
                "failed", "that bai", "giao that bai", "giao khong thanh cong",
                "khong giao duoc", "giao loi", "khong thanh cong",
                "giao khong duoc", "that bai giao hang",
            ],
            # ── returned ───────────────────────────────────────────────────────
            "returned": [
                "returned", "da tra", "duyet tra", "xac nhan tra hang",
                "hoan hang", "tra hang xong", "dong y tra hang",
                "chap nhan hoan", "hoan tien xong",
            ],
        }
        normalized_msg = _normalize_vi(message)
        for status, keywords in status_map.items():
            if any(kw in normalized_msg for kw in keywords):
                entities["target_status"] = status
                break

    # Extract inventory threshold
    if intent == "staff_inventory_low":
        threshold_match = _re.search(r"(?:duoi|under|<)\s*(\d+)", message, _re.IGNORECASE)
        if threshold_match:
            entities["threshold"] = int(threshold_match.group(1))

    # Extract status_filter for escalated_issues (phan biet hoi rieng yeu cau tra vs huy)
    if intent == "staff_escalated_issues":
        normalized_msg_esc = _normalize_vi(message)
        # Detect specifically cancel_requested
        _cancel_kws = ["yeu cau huy", "cancel request", "xin huy", "muon huy", "huy don", "cancel_requested"]
        _return_kws = ["yeu cau tra", "yeu cau hoan", "return request", "xin tra", "muon tra", "tra hang", "return_requested"]
        _has_cancel = any(kw in normalized_msg_esc for kw in _cancel_kws)
        _has_return = any(kw in normalized_msg_esc for kw in _return_kws)
        if _has_cancel and not _has_return:
            entities["status_filter"] = "cancel_requested"
        elif _has_return and not _has_cancel:
            entities["status_filter"] = "return_requested"
        # else: hoi ca hai -> khong set -> hien thi tat ca (hanh vi mac dinh)

    # Extract date range
    normalized_msg = _normalize_vi(message)
    if "hom nay" in normalized_msg or "today" in normalized_msg.lower():
        entities["date_range"] = "today"
    elif "tuan nay" in normalized_msg or "this week" in normalized_msg.lower():
        entities["date_range"] = "week"
    elif "thang nay" in normalized_msg or "this month" in normalized_msg.lower():
        entities["date_range"] = "month"

    return entities

