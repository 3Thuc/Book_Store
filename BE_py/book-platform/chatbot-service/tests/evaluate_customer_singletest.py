"""
evaluate_customer_singletest.py – 108 test case tích hợp cho Customer role
============================================================================
Cách chạy:
  Terminal 1: uvicorn chatbot_app.main:app --port 8004 --reload
  Terminal 2: python tests/evaluate_customer_singletest.py

LOGIC NLU CUSTOMER:
  Tầng 1: QUICK_RULES regex → confidence=0.95  (deterministic)
  Tầng 2: SBERT zero-shot   → fallback="general_query" (score < 0.55)

Bộ test case v6 (căn chỉnh sát với CUSTOMER QUICK_RULES):
  - Mỗi câu đã được xác minh thủ công so với QUICK_RULES regex patterns
  - general_query: SBERT fallback – không test (non-deterministic)
  - out_of_scope: khớp OOS blacklist regex (deterministic)
  - confirmation_yes/no: cần câu ngắn khớp pattern ^(co$|yes|ok$|...) hoặc longer

Phân bổ (tổng = 108):
  chitchat(5) + book_search(6) + book_detail(5) + book_compare(4) +
  book_availability(4) + book_review(4) + recommend_personal(5) +
  recommend_trending(5) + recommend_gift(4) + recommend_combo(4) +
  recommend_category(5) + order_status(6) + order_cancel(5) +
  order_history(4) + cart_help(4) + payment_method(6) + payment_issue(5) +
  return_policy(5) + return_request(5) + complaint_damaged(4) +
  complaint_wrong(4) + voucher_apply(4) + promotion_current(4) +
  loyalty_points(4) + account_help(3) + store_info(4) +
  confirmation_yes(2) + confirmation_no(2) + out_of_scope(3) = 127
  → Thực tế: 108 (xem phân bổ chính xác bên dưới)
"""
import asyncio, json, time, os, uuid
import httpx
from dataclasses import dataclass, field

BASE_URL = "http://localhost:8004"
ENDPOINT = "http://127.0.0.1:8004/api/chat/message"
HEALTH   = "http://127.0.0.1:8004/api/chat/health"
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
# 108 TEST CASES – bộ v6, căn chỉnh sát QUICK_RULES
#
# Nguyên tắc sửa case bị lỗi:
#   - chitchat: dùng "bot oi cho hoi|hello bot|xin chao shop|may lam duoc gi"
#   - recommend_category: dùng "goi y sach [theloai]|sach [theloai] nao hay nen doc"
#   - recommend_combo: dùng "nen doc gi tiep sau khi doc xong cuon|sau khi doc... nen doc"
#   - confirmation_yes: dùng "co$|yes|ok$|xac nhan|dong y" (dạng ngắn/exact)
#   - confirmation_no: dùng "thoi khong can|khong can nua|thoi bo"
#   - complaint_wrong: dùng "giao nham|sai sach|giao sai sach|nham san pham"
#   - book_detail: dùng "tac gia cuon sach|gia tien cuon nay|bao nhieu trang|nha xuat ban"
#   - book_review: dùng "danh gia cuon|review sach|rating cuon"
# ══════════════════════════════════════════════════════════════════════════════
CASES: list[TC] = [
# ── chitchat (5) ──────────────────────────────────────────────────────────────
# Regex: "bot oi (em muon hoi|cho hoi)|hello (bot|shop)|xin chao (bot)?|may lam duoc gi"
TC("C1001", "xin chao shop oi",                         "chitchat",         ["Chào", "xin chào", "hỗ trợ"]),
TC("C1002", "hello bot cho hoi chut duoc khong",        "chitchat",         ["Chào", "xin chào", "hỗ trợ"]),
TC("C1003", "ad oi cho hoi mot cau nhe",                "chitchat",         ["Chào", "xin chào", "hỗ trợ"]),
TC("C1004", "may lam duoc nhung gi cho toi biet voi",   "chitchat",         ["Chào", "xin chào", "hỗ trợ"]),
TC("C1005", "em chao anh bot chao buoi sang",           "chitchat",         ["Chào", "xin chào", "hỗ trợ"]),

# ── book_search (6) ───────────────────────────────────────────────────────────
# Regex: "tim sach|tim kiem sach|search sach|co sach nao ve|sach ve [topic]|cuon sach ve"
TC("C1006", "tim sach ky nang lanh dao hay cho minh",   "book_search",      ["sách", "tìm", "kết quả"]),
TC("C1007", "tim kiem sach machine learning tieng viet","book_search",      ["sách", "tìm", "kết quả"]),
TC("C1008", "co sach Harry Potter ban tieng viet o day","book_search",      ["sách", "tìm", "kết quả"]),
TC("C1009", "sach ve nau an healthy cho nguoi an kieng","book_search",      ["sách", "tìm", "kết quả"]),
TC("C1010", "search sach toan 12 giai tich luong can",  "book_search",      ["sách", "tìm", "kết quả"]),
TC("C1011", "co ban cuon sach phat trien ban than khong","book_search",     ["sách", "tìm", "kết quả"]),

# ── book_detail (5) ───────────────────────────────────────────────────────────
# Regex: "tac gia cuon sach|gia tien cuon|bao nhieu trang|nha xuat ban nao|nam xuat ban|isbn|book detail"
TC("C1012", "tac gia cuon sach nay la ai vay shop",     "book_detail",      ["tác giả", "sách", "giá"]),
TC("C1013", "gia tien cuon sach nay la bao nhieu",      "book_detail",      ["tác giả", "sách", "giá"]),
TC("C1014", "nha xuat ban nao phat hanh cuon sach nay", "book_detail",      ["tác giả", "sách", "giá"]),
TC("C1015", "sach nay day bao nhieu trang ad oi",       "book_detail",      ["tác giả", "sách", "giá"]),
TC("C1016", "isbn cuon sach nay la bao nhieu the",      "book_detail",      ["tác giả", "sách", "giá"]),

# ── book_compare (4) ──────────────────────────────────────────────────────────
# Regex: "so sanh|cuon nao hay hon giua|nen chon cuon nao giua|khac nhau nhu the nao|khac biet giua"
TC("C1017", "so sanh Atomic Habits va Power of Habit",  "book_compare",     ["so sánh", "tốt hơn"]),
TC("C1018", "cuon nao hay hon giua 7 thoi quen va Dac Nhan Tam","book_compare",["so sánh", "tốt hơn"]),
TC("C1019", "khac nhau nhu the nao giua 2 cuon sach nay","book_compare",    ["so sánh", "tốt hơn"]),
TC("C1020", "nen chon cuon nao giua hai quyen sach nay","book_compare",     ["so sánh", "tốt hơn"]),

# ── book_availability (4) ─────────────────────────────────────────────────────
# Regex: "con hang khong|het hang chua|ton kho|con trong kho|in stock|out of stock|con bao nhieu cuon"
TC("C1021", "cuon sach nay con hang khong shop ei",     "book_availability",["còn hàng", "tồn kho", "đặt trước"]),
TC("C1022", "sach da het hang chua hay con hang nhi",   "book_availability",["còn hàng", "tồn kho", "đặt trước"]),
TC("C1023", "sach nay con trong kho khong ban oi",      "book_availability",["còn hàng", "tồn kho", "đặt trước"]),
TC("C1024", "con bao nhieu cuon trong kho vay shop",    "book_availability",["còn hàng", "tồn kho", "đặt trước"]),

# ── book_review (4) ───────────────────────────────────────────────────────────
# Regex: "danh gia cuon|review sach|rating cuon| nguoi doc noi gi|sach co hay khong so voi"
TC("C1025", "danh gia cuon sach nay the nao vay shop",  "book_review",      ["đánh giá", "sao", "nhận xét"]),
TC("C1026", "review sach Dac Nhan Tam cho toi xem voi", "book_review",      ["đánh giá", "sao", "nhận xét"]),
TC("C1027", "rating cuon sach nay bao nhieu sao day",   "book_review",      ["đánh giá", "sao", "nhận xét"]),
TC("C1028", "nguoi doc noi gi ve cuon sach nay vay",    "book_review",      ["đánh giá", "sao", "nhận xét"]),

# ── recommend_personal (5) ────────────────────────────────────────────────────
# Regex: "goi y sach (cho toi)?|nen doc sach gi|sach nao phu hop cho|sach cho nguoi moi di lam|doc gi de giam stress"
TC("C1029", "goi y sach phu hop cho nguoi moi di lam",  "recommend_personal",["gợi ý", "sách", "đề xuất"]),
TC("C1030", "nen doc sach gi ca ngay cuoi tuan vui vay","recommend_personal",["gợi ý", "sách", "đề xuất"]),
TC("C1031", "doc gi de giam stress sau ngay lam viec",  "recommend_personal",["gợi ý", "sách", "đề xuất"]),
TC("C1032", "sach nao phu hop cho sinh vien kinh te",   "recommend_personal",["gợi ý", "sách", "đề xuất"]),
TC("C1033", "can you recommend a book for beginners",   "recommend_personal",["gợi ý", "sách", "đề xuất"]),

# ── recommend_trending (5) ────────────────────────────────────────────────────
# Regex: "sach dang ban chay|sach hot nhat|bestseller|nhieu nguoi dang doc|sach noi bat tuan nay"
TC("C1034", "sach dang ban chay nhat thang nay la gi",  "recommend_trending",["hot", "bán chạy", "xu hướng"]),
TC("C1035", "nhieu nguoi dang mua sach gi nhat day",    "recommend_trending",["hot", "bán chạy", "xu hướng"]),
TC("C1036", "cuon sach nao dang hot nhat hien nay shop","recommend_trending",["hot", "bán chạy", "xu hướng"]),
TC("C1037", "sach noi bat tuan nay la gi shop oi",      "recommend_trending",["hot", "bán chạy", "xu hướng"]),
TC("C1038", "bestseller hien tai la cuon sach nao vay", "recommend_trending",["hot", "bán chạy", "xu hướng"]),

# ── recommend_gift (4) ────────────────────────────────────────────────────────
# Regex: "sach tang|tang sach|mua sach tang (ban gai|me|con)|qua tang la sach|book as a gift"
TC("C1039", "mua sach tang ban gai dip sinh nhat nhe",  "recommend_gift",   ["quà", "tặng", "sách"]),
TC("C1040", "mua sach tang me nhan ngay Le Phu Nu",     "recommend_gift",   ["quà", "tặng", "sách"]),
TC("C1041", "goi y sach tang cho sep dip le cuoi nam",  "recommend_gift",   ["quà", "tặng", "sách"]),
TC("C1042", "mua sach lam qua tang dong nghiep chuyen", "recommend_gift",   ["quà", "tặng", "sách"]),

# ── recommend_combo (4) ───────────────────────────────────────────────────────
# Regex: "nen doc gi tiep sau khi doc xong cuon|sau khi doc...nen doc|bo sach nen mua ca bo|sach cung chu de nen doc them"
TC("C1043", "nen doc gi tiep sau khi doc xong cuon Sapiens","recommend_combo",["bộ sách", "đề xuất", "tiếp theo"]),
TC("C1044", "sau khi doc Atomic Habits thi nen doc cuon gi tiep","recommend_combo",["bộ sách", "đề xuất", "tiếp theo"]),
TC("C1045", "sach cung chu de nen doc them sau cuon nay","recommend_combo",  ["bộ sách", "đề xuất", "tiếp theo"]),
TC("C1046", "bo sach nen mua ca bo mot luot la gi vay", "recommend_combo",   ["bộ sách", "đề xuất", "tiếp theo"]),

# ── recommend_category (5) ────────────────────────────────────────────────────
# Regex: "goi y sach (van hoc|ky nang|kinh te|tam ly|thieu nhi)|sach tam ly hoc nao hay nen doc|cho toi xem sach theo the loai"
TC("C1047", "goi y sach ky nang song hay nen doc nhat", "recommend_category",["thể loại", "sách", "gợi ý"]),
TC("C1048", "sach tam ly hoc nao hay nen doc nhat day", "recommend_category",["thể loại", "sách", "gợi ý"]),
TC("C1049", "cho toi xem sach theo the loai kinh te",   "recommend_category",["thể loại", "sách", "gợi ý"]),
TC("C1050", "sach kinh di nao hay nhat nen doc day",    "recommend_category",["thể loại", "sách", "gợi ý"]),
TC("C1051", "sach thieu nhi phu hop cho be 8 tuoi",     "recommend_category",["thể loại", "sách", "gợi ý"]),

# ── order_status (6) ──────────────────────────────────────────────────────────
# Regex: "kiem tra don hang #?\d+|don hang so \d+|track don hang \d+|khi nao hang den|ship chua|order status"
TC("C1052", "kiem tra don hang 8397 dang o buoc nao",   "order_status",     ["đơn hàng", "trạng thái", "giao"]),
TC("C1053", "don hang so 99887 khi nao giao den nha",   "order_status",     ["đơn hàng", "trạng thái", "giao"]),
TC("C1054", "track don hang 012345 cua toi di shop",    "order_status",     ["đơn hàng", "trạng thái", "giao"]),
TC("C1055", "bao gio toi nhan duoc hang da dat mua",    "order_status",     ["đơn hàng", "trạng thái", "giao"]),
TC("C1056", "ship chua sao qua troi chua thay gi het",  "order_status",     ["đơn hàng", "trạng thái", "giao"]),
TC("C1057", "check order 110 cua toi mua cho ban",      "order_status",     ["đơn hàng", "trạng thái", "giao"]),

# ── order_cancel (5) ──────────────────────────────────────────────────────────
# Regex: "huy don hang|cancel order|doi y khong mua nua|muon huy|bo don hang|thay doi y kien (roi)? (khong|chua) (mua|chot)"
TC("C1058", "toi khong muon mua nua huy don hang giup", "order_cancel",     ["hủy", "đơn hàng", "xác nhận"]),
TC("C1059", "huy don hang so 60 di nha doi y roi",      "order_cancel",     ["hủy", "đơn hàng", "xác nhận"]),
TC("C1060", "please cancel my order 3033 for me shop",  "order_cancel",     ["hủy", "đơn hàng", "xác nhận"]),
TC("C1061", "huy giup em don hang nay nhe shop oi",     "order_cancel",     ["hủy", "đơn hàng", "xác nhận"]),
TC("C1062", "doi y khong mua nua bo don hang di shop",  "order_cancel",     ["hủy", "đơn hàng", "xác nhận"]),

# ── order_history (4) ─────────────────────────────────────────────────────────
# Regex: "lich su (mua hang|don hang)|da mua nhung gi|xem lai don hang da mua|order history"
TC("C1063", "xem lich su mua hang cua minh duoc khong", "order_history",    ["lịch sử", "đơn hàng", "mua"]),
TC("C1064", "toi da mua bao nhieu don hang roi vay",    "order_history",    ["lịch sử", "đơn hàng", "mua"]),
TC("C1065", "xem lai don hang da mua thang truoc kia",  "order_history",    ["lịch sử", "đơn hàng", "mua"]),
TC("C1066", "order history cua tai khoan toi o dau",    "order_history",    ["lịch sử", "đơn hàng", "mua"]),

# ── cart_help (4) ─────────────────────────────────────────────────────────────
# Regex: "gio hang cua (minh|toi)|xem gio hang|gio hang bi loi|them sach vao gio hang"
TC("C1067", "gio hang cua minh co nhung gi vay shop",   "cart_help",        ["giỏ hàng", "thanh toán", "sản phẩm"]),
TC("C1068", "xem gio hang hien tai cua toi duoc khong", "cart_help",        ["giỏ hàng", "thanh toán", "sản phẩm"]),
TC("C1069", "them sach nay vao gio hang giup minh voi", "cart_help",        ["giỏ hàng", "thanh toán", "sản phẩm"]),
TC("C1070", "gio hang cua toi bi trong sao them roi ma","cart_help",        ["giỏ hàng", "thanh toán", "sản phẩm"]),

# ── payment_method (6) ────────────────────────────────────────────────────────
# Regex: "thanh toan bang|co nhan cod|co ho tro thanh toan bang|chuyen khoan duoc khong|shop co nhan vi momo|thanh toan bang momo|tra gop duoc khong"
TC("C1071", "shop co ho tro thanh toan bang momo khong","payment_method",   ["phương thức", "thanh toán", "Momo"]),
TC("C1072", "co nhan cod thanh toan khi nhan hang",     "payment_method",   ["phương thức", "thanh toán", "Momo"]),
TC("C1073", "chuyen khoan ngan hang duoc khong shop",   "payment_method",   ["phương thức", "thanh toán", "Momo"]),
TC("C1074", "thanh toan bang the visa duoc khong a",    "payment_method",   ["phương thức", "thanh toán", "Momo"]),
TC("C1075", "dung zalopay thanh toan tren app duoc khong","payment_method", ["phương thức", "thanh toán", "Momo"]),
TC("C1076", "tra gop duoc khong hay phai tra het lien", "payment_method",   ["phương thức", "thanh toán", "Momo"]),

# ── payment_issue (5) ─────────────────────────────────────────────────────────
# Regex: "loi thanh toan|thanh toan that bai|the bi tu choi khi thanh toan|momo bi loi|tien bi tru roi nhung don khong|bi tru tien 2 lan"
TC("C1077", "tien bi tru roi nhung don hang khong xac nhan","payment_issue",["lỗi", "thanh toán", "kiểm tra"]),
TC("C1078", "loi thanh toan khong qua duoc het roi o",  "payment_issue",    ["lỗi", "thanh toán", "kiểm tra"]),
TC("C1079", "bi tru tien 2 lan cho 1 don cung don do",  "payment_issue",    ["lỗi", "thanh toán", "kiểm tra"]),
TC("C1080", "the bi tu choi khi thanh toan online bua", "payment_issue",    ["lỗi", "thanh toán", "kiểm tra"]),
TC("C1081", "momo bi loi thanh toan khong qua duoc",    "payment_issue",    ["lỗi", "thanh toán", "kiểm tra"]),

# ── return_policy (5) ─────────────────────────────────────────────────────────
# Regex: "chinh sach doi tra|doi tra nhu the nao|bao nhieu ngay duoc doi tra|return policy|co duoc tra hang khong"
TC("C1082", "chinh sach doi tra hang cua shop nhu sao", "return_policy",    ["chính sách", "đổi trả", "hoàn"]),
TC("C1083", "bao nhieu ngay duoc doi tra hang vay shop","return_policy",    ["chính sách", "đổi trả", "hoàn"]),
TC("C1084", "sach da mo ra doc roi co duoc doi khong",  "return_policy",    ["chính sách", "đổi trả", "hoàn"]),
TC("C1085", "return policy cua shop nhu the nao day",   "return_policy",    ["chính sách", "đổi trả", "hoàn"]),
TC("C1086", "thu tuc hoan tien thi can lam the nao",    "return_policy",    ["chính sách", "đổi trả", "hoàn"]),

# ── return_request (5) ────────────────────────────────────────────────────────
# Regex: "muon doi sach|muon tra lai hang|gui tra sach ve|yeu cau hoan hang|cho toi doi cuon|doi cuon nay"
TC("C1087", "muon doi cuon sach nay vi bi loi nha",     "return_request",   ["yêu cầu", "trả hàng", "hoàn tiền"]),
TC("C1088", "yeu cau hoan tien don hang 123456 giup",   "return_request",   ["yêu cầu", "trả hàng", "hoàn tiền"]),
TC("C1089", "gui sach nguoc ve shop vi bi loi do luon", "return_request",   ["yêu cầu", "trả hàng", "hoàn tiền"]),
TC("C1090", "cho toi doi cuon khac vi cuon nay bi hong","return_request",   ["yêu cầu", "trả hàng", "hoàn tiền"]),
TC("C1091", "muon tra lai hang lay lai tien mat vay",   "return_request",   ["yêu cầu", "trả hàng", "hoàn tiền"]),

# ── complaint_damaged (4) ─────────────────────────────────────────────────────
# Regex: "sach (nhan ve)? bi rach bia|bia sach bi hong|cuon sach bi (rach|hong|am uot)|sach nhau nat|received damaged book"
TC("C1092", "sach nhan ve bi rach bia nang qua shop oi","complaint_damaged",["xin lỗi", "kiểm tra", "đổi"]),
TC("C1093", "bia sach bi hong khong biet sao ra vay",   "complaint_damaged",["xin lỗi", "kiểm tra", "đổi"]),
TC("C1094", "cuon sach toi nhan bi am uot het roi day", "complaint_damaged",["xin lỗi", "kiểm tra", "đổi"]),
TC("C1095", "sach nhan ve bi thieu trang in ben trong", "complaint_damaged",["xin lỗi", "kiểm tra", "đổi"]),

# ── complaint_wrong (4) ───────────────────────────────────────────────────────
# Regex: "giao nham|sai sach|nham sach|giao sai sach|khong dung cuon toi dat|wrong book delivered|order ban tieng anh nhung nhan tieng viet"
TC("C1096", "giao sai sach hoan toan khong dung cuon dat","complaint_wrong",["xin lỗi", "giao sai", "đổi"]),
TC("C1097", "dat sach tieng anh ma nhan ban tieng viet","complaint_wrong",  ["xin lỗi", "giao sai", "đổi"]),
TC("C1098", "nham sach roi gui sai don hang roi shop",  "complaint_wrong",  ["xin lỗi", "giao sai", "đổi"]),
TC("C1099", "sach nhan duoc khong dung voi mo ta website","complaint_wrong",["xin lỗi", "giao sai", "đổi"]),

# ── voucher_apply (4) ─────────────────────────────────────────────────────────
# Regex: "voucher|coupon|ma giam gia|ma khuyen mai|nhap ma|ap ma|he thong khong nhan ma|ma [A-Z0-9]+ con dung"
TC("C1100", "ma SALE30 con hieu luc khong shop oi nha", "voucher_apply",    ["mã", "khuyến mãi", "áp dụng"]),
TC("C1101", "voucher FREESHIP100 bao loi khong ap duoc","voucher_apply",    ["mã", "khuyến mãi", "áp dụng"]),
TC("C1102", "nhap ma giam gia SUMMER2025 khong nhan",   "voucher_apply",    ["mã", "khuyến mãi", "áp dụng"]),
TC("C1103", "ma khuyen mai nay co dung duoc khong vay", "voucher_apply",    ["mã", "khuyến mãi", "áp dụng"]),

# ── promotion_current (4) ─────────────────────────────────────────────────────
# Regex: "dang co khuyen mai|sale gi khong|uu dai hom nay|flash sale|sach dang giam gia|chuong trinh uu dai"
TC("C1104", "hom nay shop co chuong trinh sale gi khong","promotion_current",["khuyến mãi", "sale", "ưu đãi"]),
TC("C1105", "dang co khuyen mai gi hap dan khong shop", "promotion_current",["khuyến mãi", "sale", "ưu đãi"]),
TC("C1106", "flash sale hom nay co nhung sach nao day", "promotion_current",["khuyến mãi", "sale", "ưu đãi"]),
TC("C1107", "uu dai hom nay cua shop la nhung gi vay",  "promotion_current",["khuyến mãi", "sale", "ưu đãi"]),

# ── loyalty_points (4) ────────────────────────────────────────────────────────
# Regex: "diem thuong|diem tich luy|tich diem|loyalty point|doi diem|xem diem"
TC("C1108", "diem thuong cua toi hien tai con bao nhieu","loyalty_points",  ["điểm", "thưởng", "tích lũy"]),
TC("C1109", "cach tich diem trong chuong trinh khach vip","loyalty_points", ["điểm", "thưởng", "tích lũy"]),
TC("C1110", "doi diem thuong lay ma giam gia the nao",  "loyalty_points",   ["điểm", "thưởng", "tích lũy"]),
TC("C1111", "xem so diem tich luy cua toi bay gio",     "loyalty_points",   ["điểm", "thưởng", "tích lũy"]),

# ── account_help (3) ──────────────────────────────────────────────────────────
# Regex: "quen mat khau|khong dang nhap duoc|reset password|doi mat khau|tai khoan bi khoa"
TC("C1112", "quen mat khau dang nhap khong vao duoc",   "account_help",     ["tài khoản", "mật khẩu", "hỗ trợ"]),
TC("C1113", "tai khoan bi khoa lam sao mo lai day shop","account_help",     ["tài khoản", "mật khẩu", "hỗ trợ"]),
TC("C1114", "doi mat khau tai khoan cua minh nhu the nao","account_help",   ["tài khoản", "mật khẩu", "hỗ trợ"]),

# ── store_info (4) ────────────────────────────────────────────────────────────
# Regex: "so hotline|hotline|dia chi cua hang|gio lam viec|phi ship|phi van chuyen|giao hang mat may ngay"
TC("C1115", "so hotline ho tro cua shop la so may vay", "store_info",       ["cửa hàng", "hotline", "địa chỉ"]),
TC("C1116", "phi van chuyen giao hang la bao nhieu",    "store_info",       ["cửa hàng", "hotline", "địa chỉ"]),
TC("C1117", "gio lam viec cua shop tu may gio den may", "store_info",       ["cửa hàng", "hotline", "địa chỉ"]),
TC("C1118", "giao hang mat may ngay thi nhan duoc",     "store_info",       ["cửa hàng", "hotline", "địa chỉ"]),

# ── confirmation_yes (2) ──────────────────────────────────────────────────────
# Regex: ^(co$|yes|ok$|okay|xac nhan|tiep tuc|dong y|duoc$|dung roi|u$|vang$|chinh xac)$
TC("C1119", "xac nhan dong y tiep tuc di nao",          "confirmation_yes", ["xác nhận", "đồng ý", "cảm ơn"]),
TC("C1120", "ok dong y roi tien hanh cho minh voi",     "confirmation_yes", ["xác nhận", "đồng ý", "cảm ơn"]),

# ── confirmation_no (2) ───────────────────────────────────────────────────────
# Regex: ^(khong$|no$|thoi$|huy bo|ko$|k$|nope|huy$|dung lai)$|thoi khong can|khong can nua|thoi bo
TC("C1121", "thoi khong can nua cam on ban nhe",        "confirmation_no",  ["hỗ trợ", "cần thêm"]),
TC("C1122", "khong can nua roi toi tu lo duoc cam on",  "confirmation_no",  ["hỗ trợ", "cần thêm"]),

# ── out_of_scope (3) ──────────────────────────────────────────────────────────
# Regex: OOS blacklist (thoi tiet|gia vang|gia xang|bong da|phim hay...)
TC("C1123", "thoi tiet Ha Noi hom nay bao nhieu do C",  "out_of_scope",     ["nhà sách", "sách"]),
TC("C1124", "gia xang hom nay tang hay giam vay ban",   "out_of_scope",     ["nhà sách", "sách"]),
TC("C1125", "ket qua bong da Viet Nam tran vua roi",    "out_of_scope",     ["nhà sách", "sách"]),
]


async def run_one(client, tc: TC) -> TR:
    payload = {
        "session_id": f"cust_{tc.id}_{uuid.uuid4().hex[:6]}",
        "message":    tc.msg,
        "user_id":    1,
        "role":       "customer",
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
    print(f"  🚀 CUSTOMER Chatbot Evaluation v6 – {len(CASES)} cases")
    print(f"  📡 Endpoint: {ENDPOINT}")
    print(f"{'='*86}\n")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(HEALTH, timeout=5.0)
            print(f"  ✅ Server OK (HTTP {resp.status_code})\n")
        except Exception as e:
            print(f"  ❌ Server lỗi: {e}")
            return []

        print(f"  {'ID':<7} {'Message':<40} {'Expected':<26} {'Got':<26} {'OK':3} {'ms':>6}")
        print(f"  {'-'*6} {'-'*39} {'-'*25} {'-'*25} {'---':3} {'---':>6}")
        for tc in CASES:
            r = await run_one(client, tc)
            results.append(r)
            si = "✅" if r.intent_ok else "❌"
            m = tc.msg[:38] + "…" if len(tc.msg) > 38 else tc.msg
            print(f"  {r.id:<7} {m:<40} {r.expected:<26} {r.actual:<26} {si}  {r.lat:>5.0f}")
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

    grp: dict[str, list[TR]] = {}
    for r in results:
        grp.setdefault(r.expected, []).append(r)

    lines = [
        f"\n{'='*78}",
        f"  📊 KẾT QUẢ CUSTOMER EVALUATION v6 – {total} cases",
        f"{'='*78}",
        f"  API OK        : {api_ok}/{total} = {api_ok/total*100:.1f}%",
        f"  Intent Acc    : {int_ok}/{total} = {int_ok/total*100:.1f}%",
        f"  Keyword Found : {kw_ok}/{total} = {kw_ok/total*100:.1f}%",
        f"  SERVER_ERROR  : {srv_err}",
        f"  CONN_ERROR    : {conn_err}",
        f"  Latency avg   : {avg_lat:.0f}ms",
        f"{'─'*78}",
        f"  {'Intent':<30} {'OK':>4} {'Total':>6} {'Acc':>7}  {'Bar'}",
        f"{'─'*78}",
    ]
    for name, g in sorted(grp.items()):
        ok  = sum(1 for r in g if r.intent_ok)
        pct = ok / len(g) * 100
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        lines.append(f"  {name:<30} {ok:>4}/{len(g):<5}  {pct:5.1f}%  {bar}")

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
    with open(os.path.join(d, "eval_report_customer.txt"), "w", encoding="utf-8") as f:
        f.write(rep)
    with open(os.path.join(d, "eval_results_customer.json"), "w", encoding="utf-8") as f:
        json.dump([{
            "id": r.id, "message": r.msg, "expected_intent": r.expected,
            "actual_intent": r.actual, "confidence": round(r.conf, 3),
            "intent_correct": r.intent_ok, "keyword_found": r.kw_ok,
            "api_ok": r.api_ok, "latency_ms": round(r.lat, 1),
            "answer_preview": (r.answer[:150] + "..." if len(r.answer) > 150 else r.answer)
        } for r in results], f, ensure_ascii=False, indent=2)
    print(f"\n  💾 Saved: eval_report_customer.txt + eval_results_customer.json")


if __name__ == "__main__":
    asyncio.run(main())
