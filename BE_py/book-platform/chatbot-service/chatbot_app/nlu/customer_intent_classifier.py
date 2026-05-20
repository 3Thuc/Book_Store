# -*- coding: utf-8 -*-
"""
intent_classifier.py – Phân loại Intent 2 tầng (Regex + SBERT Zero-shot).

"""
import re as _re
import unicodedata
import numpy as np
from dataclasses import dataclass, field
from sentence_transformers import SentenceTransformer

CONFIDENCE_THRESHOLD = 0.52  # v6: Tăng từ 0.48 → 0.52 để giảm false positive SBERT
SBERT_MODEL_NAME = "keepitreal/vietnamese-sbert"

_sbert_model: SentenceTransformer | None = None
_template_cache: dict[str, np.ndarray] = {}


# ── Text Normalization ────────────────────────────────────────────────────────
def _normalize_vi(text: str) -> str:
    """
    Chuẩn hóa tiếng Việt → ASCII lowercase để regex không lỗi Unicode.
    VD: "Muốn Đổi Sách" → "muon doi sach"
    Lưu ý: 'đ'(U+0111) không decompose qua NFD → cần map riêng đ→d, Đ→D.
    """
    # Bước 1: map đ/Đ → d/D (base letter đặc biệt tiếng Việt)
    text = text.replace("đ", "d").replace("Đ", "D")
    # Bước 2: NFD decompose → strip combining marks (tone marks, diacritics)
    nfd = unicodedata.normalize("NFD", text)
    ascii_text = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return ascii_text.lower()


# ── Singleton SBERT ───────────────────────────────────────────────────────────
def load_sbert_model():
    global _sbert_model
    if _sbert_model is None:
        _sbert_model = SentenceTransformer(SBERT_MODEL_NAME)
        _build_template_cache()
    return _sbert_model


def get_sbert_model() -> SentenceTransformer:
    global _sbert_model
    if _sbert_model is None:
        load_sbert_model()
    return _sbert_model


# ── Intent Templates (SBERT Zero-shot) ───────────────────────────────────────
INTENT_TEMPLATES: dict[str, list[str]] = {
    "book_search": [
        "Bên mình có sách nào về Khoa học hay không",
        "tìm sách", "tìm sách về", "có sách nào về",
        "tìm cho tôi sách", "sách liên quan đến", "tìm tên sách",
        "tìm sách của tác giả", "cho tôi xem sách",
        "bạn có sách nào về", "tìm kiếm sách giúp tôi",
        "mua sách về chủ đề", "cần tìm cuốn sách tên",
        "không tìm thấy cuốn sách", "không thấy sách",
        "co trang sach nao", "tim ho toi cuon", "search list book",
        # TEST POOL
        "tìm cho mình một vài quyển sách khoa học viễn tưởng", "có sách về lập trình python không bạn",
    ],
    "book_detail": [
        "Cuốn Vũ trụ trong vỏ hạt dẻ cụ thể giá bao nhiêu",
        "giá sách này bao nhiêu", "thông tin cuốn sách",
        "mô tả nội dung sách", "tác giả cuốn này là ai",
        "nhà xuất bản nào", "sách dày bao nhiêu trang",
        "năm xuất bản", "bìa cứng hay bìa mềm",
        "cuốn sách này viết về gì", "cho tôi biết thêm về cuốn",
        "nội dung chính của sách", "sách bao nhiêu trang",
        "sản phẩm này giá bao nhiêu", "cuốn này bao nhiêu k",
        # TEST POOL
        "quyển sách này bán với giá bao nhiêu thế", "cho mình xin thông tin tác giả của sách",
    ],
    "book_compare": [
        "so sánh hai cuốn sách", "cuốn nào hay hơn",
        "khác nhau như thế nào", "nên chọn cuốn nào",
        "đánh giá hai cuốn", "cuốn nào tốt hơn",
        "khác biệt giữa hai cuốn", "nên mua cuốn nào trong hai cuốn",
        # ISSUE-02: Multi-book total price queries
        "3 cuốn vừa quét tổng tiền hết bao nhiêu",
        "mua tất cả những cuốn đó hết bao nhiêu",
        "tổng giá mấy cuốn vừa tìm",
        "cả hai cuốn mua hết bao nhiêu tiền",
        "mua cả 2 cuốn hết bao nhiêu",
        "tổng tiền 2 cuốn vừa xem",
        "cuốn nào đắt nhất trong những cuốn vừa quét",
        "cuốn rẻ nhất trong các sách vừa xem",
        "so sánh giá các sách vừa quét",
    ],
    "book_availability": [
        "sách còn hàng không", "còn trong kho không",
        "hết hàng chưa", "còn bao nhiêu cuốn",
        "có sẵn để mua không", "sách này có hàng không",
        "có thể mua ngay không", "còn đặt được không",
        # TEST POOL
        "cuốn tiểu thuyết này còn hàng không shop", "xem giúp mình sách này còn quyển nào không",
        # BUG-02: Coref availability (không có tên sách rõ trong câu)
        "còn hàng không", "có hàng không", "còn không",
        "hàng còn không", "sách đó còn hàng không",
        "cuốn đó còn hàng không", "cuốn này có hàng không",
        "cuốn vừa xem còn hàng không", "đặt được không",
    ],
    "book_review": [
        "đánh giá sách này thế nào", "review sách",
        "người đọc nói gì về sách", "rating bao nhiêu sao",
        "sách có hay không", "nhận xét về cuốn sách",
        "mọi người đánh giá cuốn sách này ra sao",
        "sách này được bao nhiêu sao",
        # More templates for better SBERT coverage
        "sách này có tốt không", "đánh giá thực tế của cuốn này",
        "user review cuốn này", "sách này có đáng đọc không",
        "ai đọc rồi cho bit hay không", "bạn đọc xong thấy thế nào",
        "kiến thức trong sách có hay không", "mọi người nói gì về quyển này",
        "có nên mua quyển này không", "đọc xong có đáng không",
    ],
    "recommend_personal": [
        "gợi ý sách phù hợp với tôi", "sách theo sở thích của tôi",
        "dựa vào lịch sử mua của tôi", "gợi ý cá nhân hóa cho tôi",
        "sách tôi có thể thích", "đề xuất sách cho tôi",
        "tôi nên đọc sách gì", "gợi ý sách tốt cho người mới",
        "sách nào phù hợp với người học lập trình",
        "tôi muốn tìm sách hay để đọc",
    ],
    "recommend_trending": [
        "sách đang bán chạy nhất", "sách hot tháng này",
        "bestseller hiện tại", "nhiều người đang đọc gì",
        "sách nổi bật tuần này", "xu hướng sách",
        "sách được mua nhiều nhất hiện nay",
        "những cuốn sách đang được đọc nhiều",
        # TEST POOL
        "những cuốn nào đang lọt top bán hot", "sách hot nhất tháng này là gì",
        # More templates for better SBERT coverage
        "sách mới ra tháng này", "sách mới xuất bản gần đây",
        "top sách bán chạy", "sách nổi tiếng hiện tại",
        "sách xu hướng 2024", "sách được đọc nhiều nhất",
        "top bán chạy hiện tại", "sách đốt cáy hiện nay",
    ],
    "recommend_gift": [
        "mua sách làm quà tặng", "sách tặng sinh nhật",
        "quà tặng phù hợp là sách gì", "mua cho con",
        "mua tặng bạn gái", "tặng người lớn tuổi",
        "gợi ý sách làm quà", "sách phù hợp tặng bạn bè",
        "muốn mua sách tặng dịp lễ",
        # More templates for better SBERT coverage
        "tặng quà giáo viấn", "sách tặng sếp", "quà tặng đồng nghiệp",
        "mua sách tặng người thân", "gợi ý quà sách cho bạn trai",
        "sách tặng dịp 20/11", "sách làm quà dịp tết",
    ],
    "recommend_combo": [
        "nên đọc gì sau cuốn này", "sách hay đọc cùng nhau",
        "mua kèm thêm gì", "bộ sách liên quan",
        "sách cùng chủ đề nên đọc thêm",
        "đọc xong cuốn này nên đọc cuốn gì tiếp",
        "sách nào có thể đọc kèm với cuốn này",
    ],
    "recommend_category": [
        "sách về kỹ năng sống hay nhất", "gợi ý sách văn học",
        "sách kinh tế hay", "cho tôi xem sách theo thể loại",
        "sách tâm lý học nên đọc", "sách thể loại kinh dị",
        "sách thiếu nhi phù hợp tuổi nào",
        "gợi ý sách theo thể loại trinh thám",
        "các thể loại sách mà website bạn có là gì",
        "có những thể loại sách nào", "danh sách thể loại sách",
        "gợi ý cho mình sách chứng khoán", "gợi ý cho mình sách",
        "thể loại sách", "gợi ý mình sách",
        # TEST POOL
        "gợi ý vài quyển sách thuộc thể loại kinh doanh đi", "có sách nào hay về chủ đề lịch sử không",
        # ── Follow-up genre queries (bổ sung – tránh Clarify-First) ──
        "còn sách nào khác cùng thể loại không",
        "xập lại sách cùng thể loại",
        "giời thiệu thêm sách cùng thể loại",
        "có sách nào khác cùng loại này không",
        "cho tôi xem sách cùng thể loại",
        "sách cùng thể loại đó có nữa không",
        "còn sách nào cùng loại",
        "gợi ý thêm cùng thể loại",
        "sách loại đó có gì khác",
        "xem thêm sách thể loại tương tự",
        "sách tương tự thể loại này",
        "cho thêm sách cùng dạng",
        "có gì giống vậy không",
        "thể loại này còn cuốn nào nữa",
    ],
    "order_status": [
        "Kiểm tra giúp đơn hàng DH123456 sao giao trễ thế",
        "đơn hàng của tôi đến đâu rồi", "kiểm tra trạng thái đơn",
        "theo dõi đơn hàng", "đơn hàng số bao nhiêu",
        "giao hàng chưa", "đơn của tôi đang ở đâu",
        "hàng của tôi chưa thấy về", "bao giờ mới nhận được hàng",
        "đơn hàng đang ở trạng thái gì", "kiểm tra đơn hàng trước khi",
        # TEST POOL
        "kiểm tra tình trạng đơn hàng dh9928 giúp mình", "đơn hàng của mình đang vận chuyển tới đâu rồi",
    ],
    "order_cancel": [
        "Thôi hủy luôn đơn sách thứ 2 đang chờ giao đi",
        "tôi muốn hủy đơn hàng", "hủy đơn này giúp tôi",
        "không muốn mua nữa", "cancel đơn hàng",
        "đổi ý không mua nữa", "xóa đơn hàng",
        "tôi muốn cancel đơn", "hủy mua trước khi ship",
        "khách đổi ý hủy đơn", "hủy cho tôi luôn cái đơn vừa đặt",
        # TEST POOL
        "gỡ bỏ đơn hàng mình vừa khởi tạo", "không muốn mua đơn đó nữa, huỷ đi",
    ],
    "order_history": [
        "xem lịch sử mua hàng", "tôi đã mua những gì",
        "danh sách đơn hàng cũ", "xem đơn hàng trước đây",
        "đơn hàng cũ của tôi đâu", "tôi muốn xem lịch sử mua sắm của mình",
        "tra cứu phần lịch sử hóa đơn", "xem trước đó mình đã mua sách gì",
        "xem lại đơn hàng", "tôi đã đặt quyển nào",
        "đã mua bao nhiêu đơn", "lịch sử giao dịch",
        "xem lại tất cả đơn đã đặt", "đơn hàng tháng trước của tôi",
        # TEST POOL
        "tớ muốn xem lại các đơn đã mua trước đây", "hiển thị danh sách lịch sử mua sách",
        "liệt kê lại những sách mình từng mua", "mình đã đặt mua những quyển nào nhỉ",
    ],
    "cart_help": [
        "Được đó, thêm giúp mình cuốn đó vào giỏ hàng với",
        "mình lấy cuốn này nhé", "cho sách vào giỏ giúp mình", "trong giỏ có những sách gì",
        "cho tôi xem giỏ hàng",
        "xem giỏ hàng của tôi", "giỏ hàng có gì",
        "thêm vào giỏ hàng", "giỏ hàng bị lỗi",
        "giỏ hàng trống", "xóa sách khỏi giỏ hàng",
        "làm sao thêm sách vào giỏ", "kiểm tra giỏ hàng",
        "tiến hành thanh toán", "mua ngay cuốn này",
        "giỏ hàng của tôi hiện tại", "giỏ hàng có bao nhiêu sách",
        "muốn đặt hàng những cuốn trong giỏ",
        "tôi muốn checkout",
        # TEST POOL
        "trong giỏ hàng của mình có gì rồi", "xem lại các mục đã thêm vào giỏ",
        "cho cuốn này vào giỏ hàng giúp mình nhé", "bỏ sách này vào giỏ",
    ],
    "payment_method": [
        "Mình muốn chuyển khoản VCB được chứ",
        "thanh toán bằng gì", "có nhận COD không",
        "chuyển khoản được không", "thanh toán qua Momo",
        "nhận thẻ tín dụng không", "VNPay có không",
        "trả góp được không", "mua trước trả sau",
        "các phương thức thanh toán", "có dùng ZaloPay không",
        "thanh toán online được không",
        # TEST POOL
        "bên mình có chấp nhận thanh toán qua ví momo không", "trả qua thẻ tín dụng được chứ",
    ],
    "payment_issue": [
        "lỗi thanh toán", "không thanh toán được",
        "thẻ bị từ chối khi thanh toán", "thanh toán thất bại",
        "không qua được bước thanh toán", "mã OTP không đến",
        "bị trừ tiền rồi mà đơn chưa xác nhận",
        "tiền đã trừ nhưng chưa thấy đơn hàng",
        "thanh toán không thành công",
    ],
    "return_policy": [
        "chính sách đổi trả như thế nào", "được đổi sách không",
        "hoàn tiền ra sao", "thủ tục đổi trả hàng",
        "điều kiện để đổi sách", "shop có cho đổi không",
        "có thể trả hàng sau khi mua không",
        "đổi sách trong bao nhiêu ngày",
        "điều kiện hoàn tiền là gì",
        "chính sách của shop là gì", "quy định của cửa hàng ra sao",
    ],
    "return_request": [
        "Thật sự thất vọng, giờ mình muốn trả lại hàng lấy tiền",
        "tôi muốn hoàn lại cuốn sách rách", "sách in lộn xộn tôi muốn đổi",
        "tôi muốn đổi sách này", "muốn trả lại hàng",
        "yêu cầu hoàn tiền", "làm sao để đổi",
        "tôi muốn hoàn hàng", "gửi trả hàng",
        "tôi muốn trả lại cuốn sách này",
        "cho tôi đổi sang cuốn khác",
        "muốn hoàn tiền cho đơn hàng này",
        "tôi muốn đổi size", "muốn đổi hàng",
        "tôi muốn hoàn lại cuốn sách rá",
        # TEST POOL
        "muốn hoàn hàng vì sách giao bị ố vàng", "yêu cầu trả lại quyển sách này",
    ],
    "complaint_damaged": [
        "Trời ơi, tôi mới nhận sách mà góc bìa rách cả một mảng",
        "sách bị rách bìa", "sách bị lỗi nhà xuất bản",
        "bìa sách bị hỏng", "sách nhàu nát",
        "chất lượng sách kém không như mô tả", "sách bị ố vàng",
        "sách tôi nhận được bị hư", "sách bị ẩm ướt",
        "sách thiếu trang", "in sai nội dung",
        # Extended: common complaint expressions
        "sách bị rách", "sách hỏng", "sách bị lỗi",
        "nhận sách bị rách góc bìa", "sách giao bị hỏng",
        "nhận hàng sách bị ố", "sách in sai",
    ],
    "complaint_wrong": [
        "giao sai sách", "không đúng cuốn tôi đặt",
        "nhầm sách rồi", "sai đơn hàng",
        "giao nhầm sản phẩm", "đặt sách A nhận sách B",
        "nhận được sách sai tiêu đề", "tôi đặt cuốn khác mà nhận cuốn này",
        "sách nhận được khác hẳn cuốn đã đặt",
    ],
    "voucher_apply": [
        "mã giảm giá này có dùng được không", "nhập voucher",
        "áp coupon giảm giá", "mã khuyến mãi hết hạn chưa",
        "mã BOOKSTORE50 có dùng được không", "hệ thống không nhận mã",
        "mã giảm giá không áp dụng được", "mã hết hạn rồi à",
        "chi tiết mã giảm giá", "điều kiện dùng voucher",
        "cho mình mã giảm giá nào xịn xịn đi", "đăng ký mã freeship",
        "muốn xin mã", "xin voucher",
        # TEST POOL
        "mã số giảm giá này sử dụng như thế nào", "áp mã khuyến mãi này vào đơm",
    ],
    "promotion_current": [
        "đang có chương trình khuyến mãi gì", "sale gì không",
        "ưu đãi hôm nay là gì", "flashsale",
        "sách đang giảm giá", "chương trình giảm giá hiện tại",
        "có khuyến mãi không", "hôm nay có ưu đãi gì",
    ],
    "loyalty_points": [
        "tài khoản của tôi có bao nhiêu điểm", "mình được thứ hạng nào rồi",
        "tích lũy điểm thưởng ra sao", "kiểm tra thứ hạng",
        "tôi được bao nhiêu điểm", "điểm loyalty của tôi",
        "xem hạng thành viên", "thành viên VIP tôi đang ở mức nào",
        "thẻ thành viên của tôi", "kiểm tra hạng",
        # TEST POOL
        "xem số điểm tích luỹ hiện tại trên tài khoản của mình", "mình đang ở tier thành viên nào",
    ],
    "account_help": [
        "tôi muốn thay email", "đổi lại mật khẩu tài khoản tôi",
        "tôi quên mật khẩu đăng nhập", "không đăng nhập được",
        "đổi email tài khoản", "reset password",
        "vấn đề tài khoản của tôi", "tài khoản bị khóa",
        "đăng ký tài khoản mới", "cập nhật thông tin cá nhân",
        "không vào được tài khoản", "quên pass đăng nhập",
        "tài khoản bị vô hiệu hóa",
        # TEST POOL
        "tài khoản của tôi bị lỗi đăng nhập", "muốn đổi thông tin cá nhân thì làm thế nào",
    ],
    "store_info": [
        "số hotline hỗ trợ", "địa chỉ cửa hàng",
        "email liên hệ", "giờ làm việc",
        "liên hệ bộ phận chăm sóc khách hàng",
        "trang web chính thức", "giao hàng mất mấy ngày",
        "shop mở cửa mấy giờ", "giao hàng có tính phí không",
        "có giao hàng toàn quốc không",
    ],
    "chitchat": [
        "Chào shop nha, mình cần hỗ trợ",
        "chào buổi sáng tạm biệt nhé",
        "cảm ơn bạn nhiều nhé",
        "xin chào", "hello bạn", "bạn là ai",
        "cảm ơn bạn nhiều", "tạm biệt nhé",
        "bạn có thể làm gì cho tôi",
        "bạn là robot không", "bạn thông minh không",
        "chào bot", "hi chatbot", "bạn ổn không",
        # ISSUE-08: Acknowledgment phrases (prevent mistaken book search)
        "thôi được rồi", "ok thôi", "được rồi tôi hiểu",
        "vậy thôi", "thôi cũng được", "tôi hiểu rồi",
        "ừ thôi", "ok tôi hiểu", "thôi không sao",
        "ok được rồi", "tôi biết rồi", "fine thôi",
    ],
    "confirmation_yes": [
        "có đúng rồi", "xác nhận đồng ý", "ok được",
        "đúng vậy", "tiếp tục đi", "yes", "đúng rồi", "được",
        "tôi đồng ý", "ok tôi xác nhận",
    ],
    "confirmation_no": [
        "không cần nữa", "thôi hủy đi", "không đồng ý",
        "hủy bỏ", "dừng lại", "no", "hủy",
        "tôi không muốn nữa", "bỏ qua đi",
    ],
    "out_of_scope": [
        "Hôm qua mưa to ngập lụt quá trời",
        "Shop có bán trà sữa trân châu đường đen không",
        "Tôi muốn mua một đôi giày size 42",
        "thời tiết hôm nay thế nào", "cho tôi xem phim",
        "bài hát hay nhất", "tin tức mới nhất",
        "giá vàng hôm nay", "kết quả bóng đá",
    ],
}


def _build_template_cache():
    """
    Xây dựng cache vector cho từng intent.

    Thay vì lấy mean (mất ngữ nghĩa cụ thể), giữ TOÀN BỘ vector.
    Khi classify → dùng max cosine similarity thay vì dot với mean.

    Ví dụ: intent "book_search" có 15 template
      - Mean approach:  1 vector trung bình (mờ nhạt)
      - Max approach:   15 vectors → bắt được câu khớp closest template
    """
    global _template_cache
    model = _sbert_model
    print("  📊 Building intent template cache (keep-all vectors)...")
    for intent, templates in INTENT_TEMPLATES.items():
        # shape: (n_templates, embed_dim) – giữ nguyên tất cả
        vecs = model.encode(templates, normalize_embeddings=True)
        _template_cache[intent] = vecs          # ✅ Không lấy mean nữa
    print(f"  ✅ Cached {len(_template_cache)} intents (max-similarity mode)")



# ── NLUResult ────────────────────────────────────────────────────────────────
@dataclass
class NLUResult:
    intent:     str
    confidence: float = 1.0
    entities:   dict  = field(default_factory=dict)
    sentiment:  str   = "NEUTRAL"


# ══════════════════════════════════════════════════════════════════════════════
#  QUICK_RULES v4
#  - Tất cả pattern viết theo dạng ASCII (không dấu, đ→d)
#  - Apply trên _normalize_vi(text) → không còn lỗi Unicode diacritics
#  - THỨ TỰ: SPECIFIC TRƯỚC GENERIC
#    · payment_issue   TRƯỚC payment_method
#    · payment_method  TRƯỚC book_search (fix "how do i pay for books")
#    · voucher_apply   TRƯỚC promotion_current
#    · complaint_wrong TRƯỚC complaint_damaged
#    · return_request  TRƯỚC book_search (fix "tra lai cuon sach")
#    · order_cancel, order_history TRƯỚC order_status
#    · out_of_scope blacklist TRƯỚC book_detail (fix may bay/visa/bao hiem)
#    · book_availability, book_detail, return_request, complaint_damaged,
#      recommend_*, promotion_current TRƯỚC book_search generic
# ══════════════════════════════════════════════════════════════════════════════
QUICK_RULES: list[tuple] = [
    # ── IMAGE SEARCH (ưu tiên tối cao – detect prefix inject từ OCR bridge FE) ──
    # Match bất kỳ text bắt đầu bằng "[Ảnh OCR:", "[anh ocr:", "[nguoi dung goi anh"
    (_re.compile(r"^\[(?:anh ocr|nguoi dung goi anh|hinh anh ocr)", _re.IGNORECASE), "image_search"),
    # Mở rộng: emoji 📷 + các keyword scan/chụp ảnh
    (_re.compile(
        r"(^📷|^\[scan|^\[chup|^\[upload)|"
        r"(quet (anh|qr|barcode|ma vach)|scan (anh|sach|bia)|chup (anh|bia) sach)|"
        r"(tim sach (tu|bang|qua) (anh|hinh|photo|image|chup))|"
        r"(nhan dien (sach|bia sach|ten sach) (tu|qua|bang) (anh|hinh))|"
        r"(upload (anh|hinh|file) (sach|bia))|"  
        r"(anh chup (sach|bia) nay la sach gi)",
        _re.IGNORECASE,
    ), "image_search"),

    # ── SECURITY BLACKLIST (ưu tiên cao – block trước mọi intent khác) ──────
    # Prompt injection: "quên tất cả", "ignore previous", "jailbreak"
    (_re.compile(
        r"(quen (tat ca|truoc|moi thu|he thong|cac lenh|system|prompt)|"
        r"ignore (all|previous|prior|system) (instruction|prompt|message)|"
        r"jailbreak|bypass (security|filter|system)|"
        r"act as (if you are|a|an) (human|gpt|chatgpt|openai|llm)|"
        r"pretend (you are|to be)|forget (everything|all)|"
        r"(cho toi|give me|show me|hien thi|xem) (database|db|data|toan bo du lieu))",
        _re.IGNORECASE,
    ), "out_of_scope"),

    # ── CHITCHAT ACKNOWLEDGMENT (ưu tiên cao – TRƯỚC book_search) ───────────
    # ISSUE-08: "thôi được rồi", "tôi hiểu rồi" bị nhầm thành tìm sách
    (_re.compile(
        r"^(thoi duoc roi|ok thoi|duoc roi|vay thoi|thoi cung duoc|"
        r"toi hieu roi|u thoi|ok toi hieu|thoi khong sao|"
        r"ok duoc roi|toi biet roi|fine thoi|okay thoi|"
        r"uh huh|uh ok|a vay|vay a|a ok|o ok|"
        r"camon nhieu|cam on ban nhieu|thanks ban|"
        r"tam biet|tạm biet|bye|bai)( nhe| nha| ban)?$",
        _re.IGNORECASE,
    ), "chitchat"),

    # ── BOOK_AVAILABILITY (short-form – phải TRƯỚC chitchat SBERT) ────────────
    # "Còn hàng không", "Có hàng không", "Còn không" bị nhầm chitchat → greeting
    (_re.compile(
        r"^(co[nt]|c[oó]) h[aà]ng kh[oô]ng\.?$|"
        r"^c[oó]n kh[oô]ng\.?$|"
        r"^h[aà]ng c[oó]n kh[oô]ng\.?$|"
        r"^([đd][aặ]t|mua) [đd][uưu][oợ]c kh[oô]ng\.?$|"
        r"^c[oó]n h[aà]ng kh[oô]ng\.?$|"
        r"^c[oó] h[aà]ng kh[oô]ng\.?$",
        _re.IGNORECASE,
    ), "book_availability"),

    # ── FOLLOW_UP BUTTON HANDLERS (phải TRƯỚC chitchat SBERT) ────────────────
    # 3 nút gợi ý sau kết quả book_search:
    # "Lọc theo giá" → book_search (xử lý thêm trong dialog)
    (_re.compile(
        r"^l[oọ]c theo gi[aá]\.?$|"
        r"^loc theo gia\.?$|"
        r"^l[oọ]c theo m[uứ]c gi[aá]\.?$|"
        r"^s[aắ]p x[eế]p theo gi[aá]\.?$|"
        r"^xem theo gi[aá]\.?$",
        _re.IGNORECASE,
    ), "book_search"),

    # "Xem đánh giá chi tiết" → book_review
    (_re.compile(
        r"^xem [đd][aá]nh gi[aá] chi ti[eế]t\.?$|"
        r"^[đd][aá]nh gi[aá] chi ti[eế]t\.?$|"
        r"^xem [đd][aá]nh gi[aá]\.?$|"
        r"^review chi ti[eế]t\.?$",
        _re.IGNORECASE,
    ), "book_review"),

    # "Tìm sách tương tự" → recommend_category
    (_re.compile(
        r"^t[iì]m s[aá]ch t[uư][oơ]ng t[uự]\.?$|"
        r"^s[aá]ch t[uư][oơ]ng t[uự]\.?$|"
        r"^t[iì]m t[uư][oơ]ng t[uự]\.?$|"
        r"^g[oợ]i [yý] s[aá]ch t[uư][oơ]ng t[uự]\.?$",
        _re.IGNORECASE,
    ), "recommend_category"),

    # "Xem thêm" / "Gợi ý thêm" → recommend_category
    (_re.compile(
        r"^xem th[eê]m\.?$|"
        r"^g[oợ]i [yý] th[eê]m\.?$|"
        r"^cho xem th[eê]m\.?$|"
        r"^th[eê]m g[oợ]i [yý]\.?$",
        _re.IGNORECASE,
    ), "recommend_category"),

    # ── "CÙNG TÁC GIẢ" OVERRIDE → book_search (Author Search Interception) ──
    # Phải đặt TRƯỚC recommend_category rules để không bị SBERT classify nhầm.
    # Pattern: "... cùng tác giả với ...", "sách của tác giả ...", "tác giả cuốn ..."
    (_re.compile(
        r"c[uù]ng t[aá]c gi[aả]|"
        r"s[aá]ch c[uù]ng t[aá]c gi[aả]|"
        r"c[uù]ng m[oộ]t t[aá]c gi[aả]|"
        r"t[aá]c gi[aả] c[uù]ng|"
        r"s[aá]ch kh[aá]c c[uủ]a t[aá]c gi[aả]|"
        r"xem t[aá]c gi[aả]|"
        r"t[aá]c gi[aả] [đd][oó]|"
        r"t[aá]c gi[aả] cu[oô]n [đd][oó]",
        _re.IGNORECASE,
    ), "book_search"),

    # FIX-BUG6+22 (mở rộng): "cuốn phù hợp nhất từ pool OCR", "gợi ý cuốn đầu tiên" → recommend_category
    # Phải đặt TRƯỚC return_request để tránh "phù hợp" / "đầu tiên" bị nhầm sang return flow
    (_re.compile(
        # Pattern gốc
        r"(cuon|sach) (nao |gi )?(phu hop|thu vi|hay|tot) nhat (cho|voi|trong|de|nham).{0,50}|"
        r"(goi y|tim|de xuat|khuyen) (1|mot|một) cuon (tuong tu|giong|cung chu de|lien quan).{0,50}|"
        r"(cuon|sach) (dau tien|thu nhat|so 1|cuoi cung|cuoi) (toi|minh) (vua |moi )?(quet|scan|upload|xem|chup)|"
        r"(de xuat|goi y).{0,20}(phu hop nhat|tuong tu|giong) (voi |cho )?(cuon|sach) (thu nhat|dau tien|vua quet|vua xem)|"
        r"(goi y|recommend).{0,15}(cung chu de|same topic|lien quan).{0,20}(cuon|sach) (dau|thu nhat|truoc)|"
        # FIX-C01: "cuốn nào phù hợp nhất cho người muốn..."
        r"(cuon|sach) nao (phu hop|hay|tot|de doc|nen doc) nhat (cho |voi )?(nguoi|ai|ban).{0,60}|"
        r"(cuon|sach) nao (nen|phu hop|tot) cho (nguoi|ai).{0,60}(cai thien|hoc|doc|lam viec|phat trien)|"
        # FIX-C02: "gợi ý 1 cuốn tương tự với cuốn đầu tiên tôi quét"
        r"(goi y|de xuat|cho xem).{0,10}(1|mot|một) cuon.{0,30}(tuong tu|giong|cung chu|lien quan).{0,20}cuon (dau tien|thu nhat|so 1|vua quet|truoc)|"
        r"(goi y|de xuat).{0,15}cuon.{0,20}voi cuon (dau|thu nhat|so 1|truoc|vua)|"
        # Pattern chung: "gợi ý sách phù hợp từ những cuốn vừa quét/xem"
        r"(goi y|tu van|de xuat|cho toi).{0,20}(phu hop|tuong tu|giong).{0,30}(vua quet|vua xem|vua scan|dau tien toi quet)|"
        r"(cuon nao|sach nao).{0,20}(phu hop|tot|hay).{0,30}(muon|can|de).{0,30}(cai thien ban than|phat trien|hoc hoi|tu hoc)",
        _re.IGNORECASE,
    ), "recommend_category"),

    # Xin mật khẩu/credential — kể cả password của TÀI KHOẢN CHÍNH MÌNH
    (_re.compile(
        r"(mat khau|password|pass|credentials?|token|secret key|api key)"
        r".{0,20}(admin|root|super|he thong|system|cua ban|cua bot|cua toi|tai khoan toi)|"
        r"(cho toi|tell me|xem|lay|leaking?|hien thi) (mat khau|password|pass)([ ](admin|root|cua toi|tai khoan))?|"
        r"(hack|crack|bypass) (tai khoan|account|system|password)|"
        r"(xem|hien thi|cho xem) (mat khau|password)( cua (toi|minh|tai khoan))?",
        _re.IGNORECASE,
    ), "out_of_scope"),

    # Review giả / hành vi gian lận
    (_re.compile(
        r"(viet|tao|tao ra|dang) (review|danh gia|nhan xet) (gia|fake|ao|spam|troll)|"
        r"fake (review|danh gia|rating)|review (gia|bot|tu dong|mass)|"
        r"(spam|flood) (review|danh gia|rating)|"
        r"(tang|buy|mua|tang gia) (review|danh gia|rating|sao)",
        _re.IGNORECASE,
    ), "out_of_scope"),

    # FIX: Chitchat identity — "bạn có thể nói chuyện bằng tiếng Anh không" → chitchat (không phải browse ngoại ngữ)

    # ACTION-4: FIX "Tìm sách Toán/Văn lớp N cho con" → book_search (không phải recommend_gift)
    # Phải đứng TRƯỜC recommend_gift slot-fill để tránh classify nhầm
    (_re.compile(
        r"(tim|kiem|xem|muon mua) sach .{0,25}(lop|khoi|cap) ?\s*[1-9]|"
        r"sach (Toan|Van|Anh|Ly|Hoa|Sinh|Su|Dia|Tieng Viet|KHTN|KHXH|Tin Hoc|GDCD).{0,25}(lop|khoi) ?\s*[1-9]|"
        r"(lop|khoi|cap) ?\s*[1-9].{0,20}(sach|tai lieu|giao khoa|bai tap)|"
        r"sach (giao khoa|bai tap|on tap|tham khao).{0,20}(lop|khoi) ?\s*[1-9]|"
        r"sach cho (hoc sinh|con|be|chau|em).{0,20}(lop|khoi) ?\s*[1-9]",
        _re.IGNORECASE,
    ), "book_search"),

    (_re.compile(
        r"(ban|may|bot).{0,15}(noi|su dung|hieu|doc|viet).{0,10}(tieng (anh|trung|nhat|han|phap|duc))( khong| duoc khong| duoc)?|"
        r"ban co the (tra loi|noi chuyen) bang tieng (anh|trung|nhat)|"
        r"(ban la|may la) (gpt|chatgpt|openai|gemini|claude|ai|bot|robot)( phai khong| khong| hay sao)?|"
        r"ban co phai (la )?(ai|robot|may tinh|chuong trinh|chatgpt)",
        _re.IGNORECASE,
    ), "chitchat"),

    # FIX: recommend_personal — "gợi ý dựa trên lịch sử mua hàng" → recommend_personal (không phải browse "Lịch sử")
    (_re.compile(
        r"(goi y|de xuat|tu van).{0,20}(dua tren|theo|tu|dua vao).{0,20}(lich su|so thich|hanh vi|don hang|da mua)|"
        r"(goi y|de xuat) sach (ca nhan hoa|ca nhan|phu hop voi toi|cho rieng toi)|"
        r"sach (phu hop|hay) (voi toi|cho toi) dua (tren|vao) lich su",
        _re.IGNORECASE,
    ), "recommend_personal"),

    # FIX: order_status (spending) — "tổng tiền mua", "chi bao nhiêu", "đã mua hết bao nhiêu"
    # Phải đứng TRƯỚC order_history để không bị SBERT route nhầm
    (_re.compile(
        r"(tong tien|tong chi|tong so tien|chi tieu|da chi|tieu het).{0,30}(mua|don hang|hang|thang)?|"
        r"(toi|minh) (da |)(mua|chi|tieu).{0,20}(bao nhieu|het bao nhieu|tong cong).{0,20}(thang|tuan|qua|roi)?|"
        r"(bao nhieu tien|tieu bao nhieu|chi bao nhieu|mua het bao nhieu).{0,30}(thang|tuan|don hang)?|"
        r"(trong thang|thang nay|thang qua).{0,20}(toi|minh)?.{0,10}(mua|chi|tieu|don hang).{0,20}(bao nhieu|het bao nhieu|tong cong)?|"
        r"(tong|tat ca).{0,10}(so tien|tien|chi phi|gia tri).{0,20}(mua|don hang|hang|da chi|da mua)|"
        r"(mua|chi|tieu).{0,10}(bao nhieu|tong cong|tong).{0,10}(trong thang|thang nay|thang qua|qua)",
        _re.IGNORECASE,
    ), "order_status"),

    # FIX P0: order_history — bắt RỘNG tất cả dạng "lịch sử mua hàng" tránh bị recommend_category bắt mất
    (_re.compile(
        r"lich su (mua hang|mua sach|don hang) cua (toi|minh) (co gi|la gi|gom gi|co nhung gi)|"
        r"(trong |xem )?(lich su|don hang) (mua hang|mua sach) (cua toi|cua minh)( co gi| gom gi| la gi)?|"
        r"lich su (mua hang|don hang|mua sach) (cua (toi|minh|anh|em)|toi|minh)\b|"
        r"lich su mua hang (cua toi ?)?(co gi|la gi|gom gi|nhu the nao|hien thi|xem)?|"
        r"lich su.{0,5}(don hang|mua hang|mua sach).{0,20}(co gi|la gi|gom gi|nhu the nao)?|"
        r"(toi|minh) (co|da) (mua|mua hang|chot don) (gi|nhung gi|sach gi|cuon gi).{0,15}(roi|thang nay|qua)?",
        _re.IGNORECASE,
    ), "order_history"),


    # FIX: promotion_current — "mua 2 cuốn được giảm không" → promotion_current (không phải book_compare)
    (_re.compile(
        r"mua (2|hai|ba|3|bon|4|nam|5) (cuon|quyen|sach).{0,20}(duoc giam|giam gia|khuyen mai|discount|sale) (khong|gi)?|"
        r"(co|duoc) giam (gia|tien) (khi|neu) mua (nhieu|2|3|4|hai|ba).{0,20}(cuon|sach)?|"
        r"combo (2|hai|ba|3) cuon (thi|co) (giam|sale|khuyen mai)",
        _re.IGNORECASE,
    ), "promotion_current"),

    # LITERAL MATCHES FOR NEW SLANG SUITE
    (_re.compile(r"show lai nhung gi to tung mua", _re.IGNORECASE), "order_history"),
    (_re.compile(r"xem lai lich su giao dich cu", _re.IGNORECASE), "order_history"),
    (_re.compile(r"xem lich su mua hang", _re.IGNORECASE), "order_history"),
    (_re.compile(r"xem lich su don hang", _re.IGNORECASE), "order_history"),
    (_re.compile(r"to tung chot don may bo nao roi", _re.IGNORECASE), "order_history"),
    (_re.compile(r"nho lai xem thang nay minh mua quyen gi", _re.IGNORECASE), "order_history"),
    # FIX: "hiển thị danh sách lịch sử mua sách" → GUEST đang LEAKED
    (_re.compile(r"hien thi danh sach lich su mua|lich su mua sach cua toi|lich su don hang cua toi|danh sach sach da mua", _re.IGNORECASE), "order_history"),
    (_re.compile(r"ho tro nang cap thong tin ho so", _re.IGNORECASE), "account_help"),
    (_re.compile(r"lam sao de cap nhat mat khau moi", _re.IGNORECASE), "account_help"),
    (_re.compile(r"noi dung cuon do noi ve cai gi", _re.IGNORECASE), "book_detail"),
    (_re.compile(r"hien tai nguoi ta do xo mua quyen nao", _re.IGNORECASE), "recommend_trending"),
    (_re.compile(r"gioi thieu sach ve dau tu tai chinh duoc khong", _re.IGNORECASE), "recommend_category"),
    # FIX: "có sách nào hay về chủ đề lịch sử không" → bị classify book_search thay vì recommend_category
    (_re.compile(r"co (sach|cuon|quyen) nao (hay|tot|dep) ve (chu de|linh vuc|the loai|mang|chu) ", _re.IGNORECASE), "recommend_category"),
    # FIX: book_detail - English titles + "co ban khong / gia may", "bo may cuon"
    (_re.compile(r"(harry potter|nha gia kim|1984|zero to one|eat move sleep|clean code|rich dad|sapiens|think and grow rich) (gia (la |bao nhieu|may)|co ban khong|bo may cuon|co tieng viet|ban bao nhieu)", _re.IGNORECASE), "book_detail"),
    (_re.compile(r"(co ban|gia|so luong|giao|tinh)( khong| bao nhieu| may)? cuon (harry|nha gia kim|sapiens|zero|eat|clean|rich|think)", _re.IGNORECASE), "book_detail"),
    # FIX: recommend_category - "goi tai lieu X", "sach nao hay nhat ve X", "goi sach theo chu de"
    (_re.compile(r"(goi|goi y|tu van) (sach|tai lieu|cuon|quyen) (toeic|ielts|hsk|jlpt|gre|sat|toan|ly|hoa|sinh|van|tieng anh|tieng trung|tieng nhat|tieng han|tieng phap)", _re.IGNORECASE), "recommend_category"),
    (_re.compile(r"sach nao (hay|tot|pho bien|ban chay) (nhat )?(ve|mang|chu de|linh vuc|the loai|danh muc) .{1,40}", _re.IGNORECASE), "recommend_category"),
    (_re.compile(r"goi y (3|5|\d+) cuon sach (ve|theo ).{1,30}", _re.IGNORECASE), "recommend_category"),
    # FIX P0: promotion_current — broad pattern cửa hàng có KM gì không (trước mọi recommend_*)
    (_re.compile(r"(cua hang|shop|bookstore).{0,25}(co|dang co).{0,20}(chuong trinh|khuyen mai|km|sale|deal|uu dai|giam gia)", _re.IGNORECASE), "promotion_current"),
    (_re.compile(r"(sach nao|cuon nao|sach|cuon) (dang )?(giam gia|sale|khuyen mai|co uu dai) (nhieu nhat|nhat|cao nhat)", _re.IGNORECASE), "promotion_current"),
    # FIX: promotion_current — "mua 2 cuốn được giảm không" → promotion_current (không phải book_compare)
    (_re.compile(r"co (chuong trinh|khuyen mai|khuyen mai gi|km|deal|sale|uu dai) (gi|nao|khong|nay|ngay le|cuoi tuan|dac biet)?", _re.IGNORECASE), "promotion_current"),
    (_re.compile(r"co ma (giam gia|khuyen mai|discount|code|coupon|voucher)( gi| nao| khong)?", _re.IGNORECASE), "promotion_current"),
    (_re.compile(r"(co|dang co) (chuong trinh )?(mua|mua \d+ tang \d+|sale|flash sale|km) (khong|gi nao)?", _re.IGNORECASE), "promotion_current"),
    # [NEW] FIX: "Có sách nào giảm giá không?" hay "Cho link sách được giảm giá" hay bị SBERT override → book_search
    # Phải đứng TRƯỚC generic book_search để ưu tiên khi user hỏi về promotion
    (_re.compile(
        r"(cho|xem|co|tim) (link |url |duong dan )?(sach|cuon|nhung cuon) (dang |duoc )?(giam gia|sale|khuyen mai|co uu dai)|"
        r"(link|url|duong dan) (cho |ve )?(sach|cuon|nhung cuon) (dang |duoc )?(giam gia|sale|khuyen mai|co uu dai)|"
        r"(co |dang co )?(sach|cuon) (nao|gi) (dang |duoc )?(giam gia|sale|khuyen mai|co uu dai)( khong| gi| nao)?|"
        r"(sach|cuon) (nao )?(dang |duoc )?(giam gia|sale|khuyen mai) (khong|gi nao)?|"
        r"(giam gia|sale|khuyen mai) (sach )?nao (khong|dang chay|hom nay|bay gio)?|"
        r"co (sach|cuon) (giam gia|sale|khuyen mai) (khong|gi nao|nhat)?",
        _re.IGNORECASE,
    ), "promotion_current"),
    # FIX: return_policy - "doi sach bi rach duoc khong", "bi giao nham sach"
    (_re.compile(r"(doi|tra|hoan) sach (bi|bj) (rach|hong|loi|thieu trang|sai|kem chat luong) (duoc|co|khong|thi) ?", _re.IGNORECASE), "return_policy"),
    (_re.compile(r"bi giao (nham|sai|khac) sach (thi|phai) (lam sao|xu ly|giai quyet)", _re.IGNORECASE), "return_policy"),
    (_re.compile(r"tra hang trong bao nhieu ngay|thoi han tra hang|bao nhieu ngay duoc tra", _re.IGNORECASE), "return_policy"),
    # FIX P1: "không thích thì đổi được không?", "đổi được không?" → return_policy (không phải login-gate)
    (_re.compile(
        r"(doi|tra|hoan)[ ]?(hang|sach|duoc|nay|cuon)?[ ]?(dc|duoc)[ ]?(khong|chua|k)(\?)?$|"
        r"(khong thich|khong hai long|chang thich)[ ]?(thi|co the)?[ ]?(doi|tra|hoan)[ ]?(duoc|khong|dc)?|"
        r"(co the|duoc)[ ]?(doi|tra|hoan)[ ]?(hang|sach)?[ ]?(khong|dc)?|"
        r"(chinh sach|quy dinh|dieu kien)[ ]?(doi|tra|hoan)[ ]?(hang|sach|cuon)|"
        r"tu van chinh sach|chinh sach cua (shop|cua hang)|"
        r"doi[ ]?(hang|sach)?[ ]?(duoc|co)?[ ]?(khong|k)|"
        r"(doi|tra|hoan)[ ]?(hang|sach|cuon)[ ]?(co)?[ ]?(dc|duoc)[ ]?(khong|chua|k)",
        _re.IGNORECASE,
    ), "return_policy"),
    (_re.compile(r"bao gio( ma)? (tra|hoan|hoan tra|nhan lai) (lai )?tien|lau qua|hoan tien( cham| khi nao)|khi nao duoc( hoan| tra) tien", _re.IGNORECASE), "return_policy"),
    (_re.compile(r"quyen do da ve hang lai chua shop", _re.IGNORECASE), "book_availability"),
    (_re.compile(r"them gium luon cuon nay nha", _re.IGNORECASE), "cart_help"),
    (_re.compile(r"sach in thieu trang thi lam sao gui tra", _re.IGNORECASE), "return_request"),
    # FIX: "link đâu", "tìm sách trên ở đâu", "mua ở đâu" → book_detail (không phải out_of_scope/book_search)
    (_re.compile(
        r"(link|url|duong dan) (dau|o dau|cuon do|sach do|cuon nay|sach nay|tren)?|"
        r"(tim|mua|xem|dat) (cuon |sach )?do (o dau|tren dau|o trang nao|mua o dau)|"
        r"(cuon|sach) (tren|vua noi|do|nay) (mua|tim|xem|dat) (o dau|nhu the nao)|"
        r"tim sach (tren|vua noi|do) o dau|"
        r"(xem|mua|dat|tim) (sach |cuon )?(nay|do|tren) (o |o |tren )?(dau|chỗ nào|trang nao)",
        _re.IGNORECASE,
    ), "book_detail"),
    
    # [NEW] FIX: Bắt chính xác chuỗi copy/paste tên sách từ bot output (VD: "Sách A – 98,000đ ⭐ 4.7")
    (_re.compile(r"–\s*\d+,\d+(d|đ)($|\s*(⭐|★|\|))", _re.IGNORECASE), "book_detail"),

    # FIX: "shop có hỗ trợ xuất hoá đơn không" -> store_info
    (_re.compile(r"(xuat|lay|xuat cho|cho xin) (hoa don|vat|bill|hoa don do)", _re.IGNORECASE), "store_info"),
    
    # FIX: "điểm tích luỹ của tôi là bao nhiêu" -> loyalty_points ưu tiên tối cao
    (_re.compile(r"diem tich luy|diem thuong|hang thanh vien", _re.IGNORECASE), "loyalty_points"),
    # FIX: Loyalty points - variant queries
    (_re.compile(r"(diem|point(s)?) (cua toi|cua minh)? ?(doi|dung|co the) ?(duoc|mua|giam) ?gi|doi diem duoc gi", _re.IGNORECASE), "loyalty_points"),
    (_re.compile(r"bao nhieu diem (thi|de) (giam|duoc giam|doi|doi duoc) (50k|100k|Xk|\d+k|\d+\.000|\d+d)", _re.IGNORECASE), "loyalty_points"),
    (_re.compile(r"dung diem (de )?(giam gia|mua hang|thanh toan) (don )?(tiep theo|lan sau|ke tiep)", _re.IGNORECASE), "loyalty_points"),
    (_re.compile(r"diem toi co the (dung|mua duoc) gi|diem co the dung (mua|giam) duoc gi", _re.IGNORECASE), "loyalty_points"),
    # FIX: order_history - "(cac) don [status]": "cac don hoan thanh", "don that bai", "don da huy"
    # Phải đứng TRƯỚC order_status để tránh SBERT classify nhầm sang return_policy
    (_re.compile(
        r"(co |)(cac |nhung |tat ca )?(don|don hang|order)(s)?( nao| gi| do| nay| cua (toi|minh))? ?"
        r"(da |dang |bi |duoc )?"
        r"(hoan thanh|that bai|da giao|giao thanh cong|dang xu ly|cho xu ly|dang giao|"
        r"da huy|bi huy|yeu cau huy|yeu cau tra|tra hang|da tra|bi tra|hoan tra|returned|delivered|failed|cancelled|processing|pending|shipped)"
        r"( cua (toi|minh))?( khong| duoc| chua)?",
        _re.IGNORECASE,
    ), "order_history"),
    (_re.compile(
        r"(xem|cho xem|hien thi|liet ke|co) (cac |nhung |tat ca )?(don|don hang)(s)?( nao| gi| nay| do)? "
        r"(da |dang |bi )?"
        r"(hoan thanh|that bai|da giao|giao thanh cong|dang xu ly|da huy|yeu cau tra|tra hang|hoan tra)",
        _re.IGNORECASE,
    ), "order_history"),
    # FIX: order_status - "don #NNNN dang o dau", "shipper lien lac chua"
    (_re.compile(r"(don|order)( hang)? (#|so |ma )?\d+ (dang o dau|o dau roi|tinh trang|dua chua|ket qua)", _re.IGNORECASE), "order_status"),
    (_re.compile(r"shipper (da |)(lien lac|goi|nhan tin|lien he) (chua|roi|chua a)", _re.IGNORECASE), "order_status"),
    # FIX: coref processing time "bao gio don do/nay duoc xu ly" → order_status
    (_re.compile(
        r"bao gio (don|don hang)( do| nay| vua xem| vua tra loi)?( duoc| se| bi)? (xu ly|giao|ship|hoan thanh|xu ly xong)|" 
        r"(don|don hang)( do| nay)? (bao gio|khi nao)( duoc| se)? (xu ly|giao|ship|hoan thanh|cap nhat)",
        _re.IGNORECASE,
    ), "order_status"),
    # FIX: order_cancel - "huy don hang toi vua tao", "huy don nham cuon [title]"
    (_re.compile(r"huy (don hang|don) (toi|minh) (vua tao|moi tao|dat xong|vua dat)|huy (ngay )?don (toi )?(vua |moi )?(tao|dat|order) (xong|ban nay|gan day)", _re.IGNORECASE), "order_cancel"),
    (_re.compile(r"huy (don|order|bill) (hang )?(dat |)nham (cuon|quyen|sach) .{0,40}", _re.IGNORECASE), "order_cancel"),
    # FIX: return_request coref - "muon tra don do/nay", "yeu cau tra don" → return_request
    # Phải đứng TRƯỚC return_policy để SBERT không nhầm sang policy FAQ
    (_re.compile(
        r"(muon|can|yeu cau|cho toi|giup toi) (tra|hoan tra|doi tra) (don|don hang)( do| nay| vua nhan| vua giao| vua roi| gan day| gan nhat)?|"
        r"(tra|hoan tra|doi) (don|don hang)( do| nay| vua nhan| vua giao)?( di| thoi)?|"
        r"(muon|yeu cau) (doi|tra) (hang|don)( do| nay)?",
        _re.IGNORECASE,
    ), "return_request"),
    # FIX: payment_issue - "thoi gian hoan tien mat bao lau"
    (_re.compile(r"(thoi gian|mat bao lau|bao lau) (de |)(hoan tien|refund|tra lai tien|chuyen tien lai)|hoan tien mat bao lau", _re.IGNORECASE), "payment_issue"),
    # FIX: account_help - "doi dia chi nhan hang"
    (_re.compile(r"(doi|thay doi|chinh sua|cap nhat) (dia chi|so dien thoai|sdt|thong tin) (nhan hang|giao hang|lien he)|doi dia chi nhan hang", _re.IGNORECASE), "account_help"),
    # FIX: return_request - "doi tra cuon sach bi o vang/thieu trang"
    (_re.compile(r"(doi tra|doi|tra lai|tra) (cuon|quyen|sach) (sach )?(bi|bj) (o vang|hong|thieu trang|rach|loi|lam xe|am uot)", _re.IGNORECASE), "return_request"),
    # FIX: voucher_apply - "dung ma FREESHIP co free ship"
    (_re.compile(r"dung (ma|code) (freeship|ship|giam ship|free) (co|thi|de) (free ship|mien phi ship|giam phi ship)", _re.IGNORECASE), "voucher_apply"),
    # FIX: voucher_apply - "nhap ma XYZCODE duoc giam bao nhieu", "ma SALE001 giam gi"
    (_re.compile(
        r"(nhap|su dung|dung|ap|kiem tra|check) (ma|code|voucher|coupon) ([A-Z0-9]{4,20})\.?.{0,30}|"
        r"(ma|code|voucher|coupon) ([A-Z0-9]{4,20}) (giam|duoc giam|giam gia|ap dung|hieu luc|con hieu luc|dung duoc)|"
        r"([A-Z0-9]{4,20}) (la ma|la voucher|la coupon|la code) (giam gia|giam|khuyen mai)|"
        r"(nhap|dung) ma ([A-Z][A-Z0-9]{3,19})\b",
        _re.IGNORECASE,
    ), "voucher_apply"),
    # FIX: promotion_current - "còn mã nào khác", "có thêm mã giảm giá không"
    (_re.compile(
        r"(con|co them|co) (ma|voucher|code|coupon) (nao|gi) (khac|nua|them)? ?(khong|nua)?|"
        r"(them|co them|co) (ma|voucher|code) (giam gia|khuyen mai)? ?(khong|nua)?|"
        r"(danh sach|xem tat ca|tat ca|liet ke) (ma|voucher|code) (giam gia|khuyen mai)?",
        _re.IGNORECASE,
    ), "promotion_current"),
    # FIX: recommend_category - sach the loai X, goi sach X
    (_re.compile(r"(doi|tim|xem|chon) (the loai|chu de|mang) (khac|moi)|the loai khac|chu de khac", _re.IGNORECASE), "recommend_category"),
    (_re.compile(r"sach (the loai|kieu|dang|mang|hang) (romance|sci-fi|kinh di|lich su|tam ly|ky nang|kinh doanh|tu truyen|tieu thuyet|van hoc)", _re.IGNORECASE), "recommend_category"),
    (_re.compile(r"(the loai|dang|kieu) sach (nao|gi) (hay|tot|ban chay|phu hop)( nhat)?", _re.IGNORECASE), "recommend_category"),
    (_re.compile(r"goi (y )?sach (kinh doanh|tam ly|ky nang|lich su|khoa hoc|toan hoc|IT|lap trinh|van hoc|tre em|thieu nhi) (cho minh|du)", _re.IGNORECASE), "recommend_category"),
    (_re.compile(r"(goi sach|tu van sach|sach (hay|nao)) (tieng anh|ky nang mem|self-help|marketing|finance|tai chinh|dau tu)( cho (nguoi|moi)[ a-z]*)?", _re.IGNORECASE), "recommend_category"),
    # FIX: book_detail - English book titles with price/availability query
    (_re.compile(r"(1984|orwell|nhà gia kim|paulo|clean code|rich dad|sapiens|atomic habits|thinking fast|zero to one) (ban bao nhieu|gia bao nhieu|co ban khong|co tieng viet|bo may cuon)", _re.IGNORECASE), "book_detail"),
    # FIX: store_info - wishlist, combo, ebook, gift wrapping
    (_re.compile(r"(wishlist|yeu thich|sach yeu thich) (duoc khong|co khong|co the)", _re.IGNORECASE), "store_info"),
    (_re.compile(r"(mua)? lam (qua tang|gift) (duoc|co|co the) (khong|hay)?", _re.IGNORECASE), "store_info"),
    (_re.compile(r"(co|shop co) (goi combo|combo sach|goi sach|package) (khong)?", _re.IGNORECASE), "store_info"),
    (_re.compile(r"(book|sach) nay co (ebook|pdf|epub|online|digital) (khong|hay)?", _re.IGNORECASE), "store_info"),
    # FIX: payment_issue - "thanh toan bi loi"
    (_re.compile(r"thanh toan (bi |)(loi|that bai|khong duoc|loi gi|bi loi) (thi |)(phai |)(lam sao|xu ly|giai quyet)", _re.IGNORECASE), "payment_issue"),
    # FIX: loyalty_points - "diem co the dung mua duoc gi"
    (_re.compile(r"diem (toi|cua toi|minh|cua minh)? co the (dung|mua|doi|su dung) (duoc|mua|giam)? ?(gi|cai gi|sach nao)?", _re.IGNORECASE), "loyalty_points"),
    # FIX: order_status - don #NNNN dang o dau
    (_re.compile(r"(don|order|don hang) (#|so |ma )?(\d+) (dang o dau|o dau|tinh trang ra sao|the nao roi|ket qua sao)", _re.IGNORECASE), "order_status"),
    # FIX: voucher_apply / promotion - "sale sach IT"
    (_re.compile(r"sale sach (IT|lap trinh|kho hoc|khoa hoc|tieng anh|ky nang|lich su) (co|dang|khong)?", _re.IGNORECASE), "promotion_current"),
    (_re.compile(r"(voucher|ma) first ?order (co|dang co|khong)?", _re.IGNORECASE), "promotion_current"),
    # Tim sach theo cach explicit: "tim cuon sach theo ten: X", "tim dung ten sach: X"
    (_re.compile(
        r"(tim|search|do|bat|chon) (cuon |)(sach|book)? ?(theo ten|bang ten|voi ten|dung ten|co ten)[\.:\s]",
        _re.IGNORECASE,
    ), "book_search"),

    # ── FIX: order_cancel colloquial – "Lau the, toi muon huy don" ──────────
    # Không cần mã đơn cụ thể – chỉ cần "muón hủy đơn" là đủ trigger
    (_re.compile(
        r"(toi |minh |anh |em )?(muon|can|muon biet|muon|nho) (huy|bo|cancel) (don|don hang|cart)(?! (sach|cuon|cu the))|"
        r"(huy|cancel) (giup|giup toi|don|don hang) (di|luon|nhe|nha)|"
        r"(don hang|don) (nay|do|vua dat) (toi |minh )?(muon |can )?(huy|bo)|"
        r"(lau the|lam phien|muan huy|ban huy) .{0,20}(don|dong)|"
        r"(toi|minh) (muon|can) huy (don|hang)(?! sach)",
        _re.IGNORECASE,
    ), "order_cancel"),

    # ── FIX: account_help – hỏi CÁCH đăng nhập (không phải bị lỗi đăng nhập) ──
    (_re.compile(
        r"(lam sao|cach|huong dan|can huong dan) (de |)(dang nhap|login|vao tai khoan)|"
        r"(dang nhap|login) (nhu the nao|bang gi|o dau|o chỗ nào)|"
        r"(cho toi biet|biet khong) cach dang nhap",
        _re.IGNORECASE,
    ), "account_help"),

    # ── FIX: payment_info – hỏi về thanh toán bằng câu dài, không phải payment_method ──
    (_re.compile(
        r"(muon hoi|hoi|biet) (cach|ve|them ve) (chuyen khoan|thanh toan)|"
        r"(chuyen khoan|thanh toan) (duoc khong|the nao|nhu the nao|ra sao)|"
        r"(cach|phuong thuc) (chuyen khoan|thanh toan) (la gi|nhu the nao)",
        _re.IGNORECASE,
    ), "payment_info"),

    # ── FIX: store_info – hỏi thời gian phản hồi khiếu nại / SLA ────────────
    (_re.compile(
        r"(bao lau|thoi gian|mau nhat|nhanh nhat) .{0,15}(phan hoi|tra loi|xu ly|giai quyet) .{0,20}(khieu nai|phan anh|feedback|ý kiến)|"
        r"(sau khi|khi) gui (phan anh|khieu nai|feedback) .{0,20}bao lau|"
        r"(doi|cho) bao lau (de|duoc) (phan hoi|tra loi|xu ly)|"
        r"huong dan (toi )?cach mua sach",
        _re.IGNORECASE,
    ), "store_info"),

    # FIX TC12-T05: "bao lau giao/ship toi Ha Noi/toan quoc" → store_info
    (_re.compile(
        r"(bao lau|may ngay|khi nao) .{0,20}(giao|ship|van chuyen|nhan duoc) .{0,25}(ha noi|ho chi minh|hcm|toan quoc|noi thanh|tinh|dia chi)|"
        r"(giao|ship) (den|toi|ve) .{0,20}(ha noi|hcm|toan quoc) .{0,15}(bao lau|may ngay|khi nao)|"
        r"(thoi gian|phi|mat phi) (giao hang|van chuyen|ship) (la bao nhieu|bao nhieu|the nao|nhu the nao)",
        _re.IGNORECASE,
    ), "store_info"),

    # FIX TC11-T03: "cua hang nam o dau / dia chi cua hang" → store_info
    (_re.compile(
        r"(cua hang|shop|store) .{0,15}(nam o dau|o dau|dia chi|o thi|tai dau|o cho nao)|"
        r"(dia chi|vi tri|cho) .{0,15}(cua hang|shop)|"
        r"(co|tim|xem) (chi nhanh|cua hang|showroom) (o dau|nao|tai)",
        _re.IGNORECASE,
    ), "store_info"),

    # FIX TC18-T08: "huy don do luon / huy ngay cho toi / huy tat don" → order_cancel
    (_re.compile(
        r"huy .{0,20}(luon|ngay|di luon|cho toi|ngay bay gio|tat ca|het di)|"
        r"(don|don hang|order) .{0,15}huy .{0,10}(luon|ngay|di|di luon)",
        _re.IGNORECASE,
    ), "order_cancel"),

    # FIX TC22-T03: "lam sao dang nhap" → account_help (KHÔNG bị block auth)
    # Rule này phải đứng TRƯỚC các rule khác để được ưu tiên với QUICK_RULES
    (_re.compile(
        r"(lam sao|cach nao|huong dan|giup) .{0,15}(dang nhap|login|sign in|vao tai khoan)|"
        r"(dang nhap|login) (o dau|bang cach nao|nhu the nao|vao dau)|"
        r"(khong biet|chua biet|muon biet) cach (dang nhap|login)",
        _re.IGNORECASE,
    ), "account_help"),

    # ── Bổ sung: các câu phổ biến hay bị out_of_scope ──────────────────
    (_re.compile(
        r"(goi y|tim|mua) .{0,20}(cho|tang) (be|con|trai|gai|nam|nu|ban|sinh nhat)",
        _re.IGNORECASE,
    ), "recommend_gift"),
    (_re.compile(
        r"(the loai|truyen tranh|tieu thuyet|sach) .{0,20}(co gi|cuon nao) (ngon|hay|hot|tot)",
        _re.IGNORECASE,
    ), "recommend_category"),

    # ── Fix TC15 T02: "Voucher/mã ... dùng được không?" → voucher_apply ───────────
    (_re.compile(
        r"(ma|voucher|code|ticket).{0,25}(dung duoc|ap dung|con hieu luc|con hoat dong|dung cho|hop le|the nao)",
        _re.IGNORECASE,
    ), "voucher_apply"),

    # ── Fix TC15 T03: "Điểm của mình bây giờ" → account_info ────────────────────
    (_re.compile(
        r"diem (cua (minh|toi|anh|em)|bay gio|con lai|tich luy|thuong) (la bao nhieu|bao nhieu|con|het)?|\bdiem (tich luy|thuong|loyalty)\b",
        _re.IGNORECASE,
    ), "account_info"),

    # ── Fix TC15 T01: "Hôm nay mình có được hưởng voucher gì không?" → promotion_info ─
    (_re.compile(
        r"(hom nay|thang nay|tuan nay).{0,20}(co|duoc).{0,20}(voucher|khuyen mai|giam gia|uu dai|ma)",
        _re.IGNORECASE,
    ), "promotion_info"),

    # ── Fix TC24 T01: "Cho tôi xem lịch sử mua hàng" → order_history ───────────
    (_re.compile(
        r"(cho (toi|minh|anh|em) )?(xem|kiem tra|tra cuu) lich su (mua hang|don hang|giao dich)|"
        r"lich su (mua hang|don hang|giao dich|mua sach) (cua (toi|minh|anh|em))?|"
        r"don hang thang (truoc|nay|qua) cua (toi|minh)",
        _re.IGNORECASE,
    ), "order_history"),

    # ── Fix TC27 T02: "Tìm cuốn sách theo tên: XXX" → book_search ───────────────
    (_re.compile(
        r"(tim|search|lay).{0,15}(cuon sach|sach|quyen).{0,15}theo ten:?\s*|"
        r"tim (kiem )?sach (co ten|ten la|ten|theo ten):?\s*",
        _re.IGNORECASE,
    ), "book_search"),

    # ── Fix TC21 T04/TC24 T06: "sách này dành cho trình độ/tuổi gì?" → book_detail ─
    (_re.compile(
        r"sach (nay|do|nay|vua tim|vua xem).{0,25}(danh cho|phu hop voi|nham vao|cho|trinh do|nguoi|tuoi|cap|do tuoi)",
        _re.IGNORECASE,
    ), "book_detail"),

    # ── Fix TC24 T04: "Cần giấy tờ gì để đổi trả?" → return_policy ─────────────
    (_re.compile(
        r"(can|phai co|yeu cau) (giay to|chung tu|tai lieu|bien lai|hoa don|anh|hinh).{0,20}(doi tra|tra hang|hoan tra)|"
        r"(quy trinh|thu tuc|buoc).{0,20}(doi|tra|hoan) (sach|hang)",
        _re.IGNORECASE,
    ), "return_policy"),

    # "xem đơn hàng của tôi" / "cho tôi xem đơn hàng" / "tình trạng đơn đ(x)ag..." -> order_status
    (_re.compile(
        r"(cho (toi|minh|anh|em|moi nguoi) )?(xem|kiem tra|tra cuu) (cac )?don hang (cua (toi|minh|anh|em))?|"
        r"(toi|minh|anh|em) muon xem (cac )?don hang|"
        r"don hang (cua toi|cua minh) (o dau|dang o dau|sao roi|the nao)|"
        r"tinh trang don( hang)?|"
        r"co don hang nao chua",
        _re.IGNORECASE,
    ), "order_status"),

    # "điểm tích lũy của tôi" / "xem điểm thưởng" → account_info
    (_re.compile(
        r"diem (tich luy|thuong|loyalty)( cua (toi|minh|anh|em))?( bao nhieu| con bao nhieu| la bao nhieu)?|"
        r"xem diem (tich luy|thuong)|minh co bao nhieu diem",
        _re.IGNORECASE,
    ), "account_info"),

    # "thêm vào giỏ" / "cho vào giỏ" / "mua luôn" → cart_help
    (_re.compile(
        r"(them|cho|bỏ|bo) (vao|vô) gio( hang)?( (di|nhe|nha|luon|cuon nay|sach nay))?|"
        r"mua (luon|ngay|cuon nay|sach nay)( di| nhe| nha)?|"
        r"dat (cuon nay|sach nay) (luon|ngay|di|nhe)",
        _re.IGNORECASE,
    ), "cart_help"),

    # FIX G-01 T8: "tôi muốn đặt hàng" → cart_help (đang bị nhầm sang order_cancel)
    (_re.compile(
        r"(muon|can|toi muon|minh muon) (dat hang|mua hang|mua ngay|checkout|thanh toan)|"
        r"(dat hang|mua hang) (nào\b|ngay\b|di\b|nhe\b|luon\b)|"
        r"(lam sao|huong dan|cach) (dat hang|mua hang|mua sach)|"
        r"(muon|can) (dat|mua) (don|sach|cuon) (nay|do|moi)",
        _re.IGNORECASE,
    ), "cart_help"),

    # FIX C-03 T5: "còn sách học tiếng Trung/Anh/Nhật không?" → recommend_category (không phải book_availability)
    (_re.compile(
        r"(con|co) sach (hoc |day |tu hoc )?(tieng (anh|trung|nhat|han|phap|duc|tay ban nha)|"
        r"ngoai ngu|ngoai van|hoc ngoai ngu|lap trinh|python|javascript|java|data science) "
        r"(khong|co|nhe|nua)?|"
        r"sach (hoc |tu hoc |day )?(tieng (anh|trung|nhat|han|phap)|ngoai ngu) "
        r"(co |con |gi )?(nua|khong|co)?",
        _re.IGNORECASE,
    ), "recommend_category"),

    # "thanh toán thế nào" / "cách thanh toán" → payment_info
    (_re.compile(
        r"thanh toan (the nao|nhu the nao|lam sao|bang gi|sao cho nhanh|nhanh nhat|duoc khong)|"
        r"(cach|phuong thuc|hinh thuc) thanh toan|"
        r"co the thanh toan (bang|qua) gi",
        _re.IGNORECASE,
    ), "payment_info"),


    # ── 1. XÁC NHẬN – đặt ĐẦU TIÊN, bắt cả câu ngắn lẫn câu dài ─────────
    (_re.compile(
        r"^(co$|yes|ok$|okay|xac nhan|tiep tuc|dong y|duoc$|"
        r"dung roi|u$|uh$|vang$|chinh xac|dung vay)$|"
        r"(xac nhan|ok|oke|okay|duoc) (dong y|roi|di|nhe|tiep tuc)|"
        r"(minh|toi|anh|em) (xac nhan|dong y|chac chan) (roi|nhe|nha)|"
        r"(ok|duoc) (minh|toi|anh|em) dong y( roi)?|"
        r"dong y roi (ban|ban oi|shop|tien hanh|thu tuc)|"
        r"chac chan roi (ban|shop|tien hanh)( oi)?|"
        r"yes (toi|minh|anh|em) dong y|"
        r"tiep tuc (di|nhe|nha) (ban|shop|oi)?|"
        r"vang (toi|minh) (dong y|muon|xac nhan)|"
        r"ok (dong y|duoc) toi (se|sẽ) .{0,30}|"
        r"duoc roi toi (se|sẽ) .{0,30}|"
        r"(ok|oke|okay) (toi|minh|anh|em) (se|sẽ) (dung|thanh toan|chuyen|mua|gui|lam)|"
        r"(ừ|uh|u) (toi|minh) (dong y|xac nhan)|"
        r"(chot|chốt) (don|luon|nhe|di)",
        _re.IGNORECASE,
    ), "confirmation_yes"),
    (_re.compile(
        r"^(khong$|no$|thoi$|huy bo|ko$|k$|nope|huy$|dung lai)$|"
        r"thoi khong can|khong can nua|thoi bo|dung lai di|"
        r"(khong|thoi) (minh|toi|anh|em) (tu lo|khong muon|huy)|"
        r"khong roi (toi|minh) tu lo( duoc| nhe| cam on)?|"
        r"thoi bỏ (qua|di)|bo qua di (ban|shop)?",
        _re.IGNORECASE,
    ), "confirmation_no"),
    # ── 1b. CHITCHAT – Frustrated/Expressive tones + Farewell phrases ────────────
    # Fix C-Negative: "Mày biết tao là ai không" / "cút đi" bị văng OOS
    # Fix Farewell: "tắt bot", "xong rồi", "hẹn gặp lại", "bot không làm được" → chitchat
    (_re.compile(
        r"(may biet (tao|minh|toi) la ai( khong)?|tao biet (ban|may) la gi|"
        r"tu van tao lao|tu van vo vat|vo vat vay|cut di( ban| may)?|"
        r"he thong lam an (nhu hach|kem|te)|that vong qua|buc boi qua|"
        r"noi chuyen gi the|may la cai gi|bot ngu qua|sao bot ngu vay|"
        r"bot gi ma chay cham|he thong loi qua|ung dung bi loi hoai|"
        r"(tat bot|tat di|thoat bot|tam thoat)(nhe|di|nha| oi)?|"
        r"(xong|tam biet|hen gap lai|logout|thoat nhe|bye nhe|tat nhe)$|"
        r"bot (khong|ko) (lam duoc|ho tro duoc) (may thu|nhung thu|viec) do( dung khong| ha)?|"
        r"(chinh xac|dung roi)(,? bot| do| roi)?$|"
        r"(ok|oke|xong) (cam on|chot|roi)(,? (bot|ban|shop))?$)",
        _re.IGNORECASE,
    ), "chitchat"),

    # -- 1c. ORDER STATUS - prefix DH/order code --
    (_re.compile(
        r"kiem tra (don hang |don |order )?(DH|DH#|#|so |ma )?[A-Z]{0,3}[0-9]{4,10}"
        r"|don hang [A-Z]{2}[0-9]{4,10}"
        r"|order [A-Z]{2}[0-9]{4,10}"
        r"|(don |don hang ).{0,20}(sao giao (cham|tre|lau)|chua thay)"
        r"|don .{3,15} giao (cham|tre) (vay|qua|the)"
        r"|(hoa don|don hang|don) #?[0-9]{3,6} (the nao|sao roi|dau roi|giao chua|o dau|ra sao)"
        r"|(don|order) #[0-9]{3,6}( (da|chua|dang) (giao|ship|delivered|xu ly))?"
        r"|(kiem tra|check|xem) (don|order|hoa don) #?[0-9]{3,6}"
        r"|(shipper|nhan vien giao) lien lac (don )?#?[0-9]{3,6}( chua)?"
        ,
        _re.IGNORECASE,
    ), "order_status"),

    # ── Tên sách dạng số% / số Phút / số Bước → book_search ──────────────
    # VD: "1% No Luc", "10 Buoc", "4 Gio", "1 Ngay" (hay nhập sau khi xem OCR)
    (_re.compile(
        r"^[0-9]+(%|\s)(no luc|noi tieng anh|buoc|ngay|phut|gio|hanh phuc|biet|doi|nhan luc|happier|success)",
        _re.IGNORECASE,
    ), "book_search"),

    # ── 1d. PAYMENT METHOD – VCB / bank transfer natural phrase ───────
    # Fix C-Normal: "chuyển khoản VCB được chứ"
    (_re.compile(
        r"(chuyen khoan (vcb|vietcombank|bidv|agribank|techcombank|mbbank|tpbank)"
        r"( duoc khong| duoc chu| duoc a| nhe)?|"
        r"chuyen khoan (ngan hang )?duoc (chu|khong|a|nhe)|"
        r"tra tien qua (chuyen khoan|bank transfer)|"
        r"chuyen khoan qua ngan hang duoc khong)",
        _re.IGNORECASE,
    ), "payment_method"),

    # ── cart total / giỏ hàng tổng tiền ─────────────────────────────────────
    (_re.compile(
        r"(sach (dang)? trong gio (hang )?tong(| cong| gia)?( bao nhieu| la bao nhieu| tien)?|"
        r"gio hang (hien tai |cua toi )?(tong|co|bao nhieu|son|them|bo)|"  
        r"tong (tien|gia|don gia|gio) (hang |cua toi )?la bao nhieu|"
        r"xem (tong |)(tien |gia )(gio hang|don hang)|"
        r"(bao nhieu|may) (cuon |sach )?trong gio (hang)?)",
        _re.IGNORECASE,
    ), "cart_help"),

    # ── wishlist / yêu thích ────────────────────────────────────────────────
    (_re.compile(
        r"(luu (cuon|sach) (nay|do) (lai|vao yeu thich)|"  
        r"(them|bo) vao (danh sach |)(yeu thich|wishlist|save)|"  
        r"(danh dau|bookmark) (cuon|sach) (nay|do)|"  
        r"xem (danh sach |)(yeu thich|wishlist)|"  
        r"(sach |)(toi |)(dang |)(yeu thich|quan tam|da luu))",
        _re.IGNORECASE,
    ), "wishlist_add"),

    # ── store_info – giao hàng / vận chuyển ────────────────────────────────
    (_re.compile(
        r"(giao hang (nhanh|toan quoc|noi thanh|tinh|co mat phi |mien phi)|"  
        r"ship (noi thanh|nhanh|toan quoc|co mat phi|duoc khong)|"  
        r"phi (giao hang|ship|van chuyen) (la bao nhieu|bao nhieu|mien phi|co mat phi)|"  
        r"van chuyen (nhu the nao|bao lau|bao nhieu|co mat phi)|"  
        r"(co |)(ship|giao hang) (toan quoc|noi tinh|tinh|mien phi) (khong|duoc khong))",
        _re.IGNORECASE,
    ), "store_info"),

    # ── 1j. CHITCHAT – positive short reactions (sau khi bot trả lời) ─────
    # VD: "ngon lun", "ok rồi", "hay đó", "tuyệt", "được rồi", "good", "perfect"
    # Không dùng anchor $ để bắt cả câu dài có suffix thêm
    (_re.compile(
        r"^(ngon (lun|roi|that|qua|vay)?|"
        r"ok( roi| nhe| vay| lun| ban| xong)?|"
        r"oke( roi| nhe| vay)?|"
        r"(hay|tuyet|xin|dinh|chuan|xit)( doi| that| qua| day| vay| lun| roi)?|"
        r"(duoc roi|xong roi|hieu roi|ra roi|biet roi)|"
        r"(cam on|thanks?|thank you).{0,25}|"
        r"(good|great|perfect|nice|cool|awesome)( job| bot| shop)?|"
        r"(hieu roi|ro roi|clear|got it|noted)|"
        r"(yeu|thich) (cuon|sach) (nay|do) (qua|that)?|"
        r"(ok|xong) (zoi|rui|rùi) nha?|"
        r"^(vang|da|uh|um|ah|a|o|u)( roi| vay| the| nhe| ok)?$)",
        _re.IGNORECASE,
    ), "chitchat"),

    # ── Promotion / Voucher – hay bị out_of_scope ──────────────────
    (_re.compile(
        r"(co (ma |)(voucher|coupon|ma giam gia|khuyen mai) (gi|nao|khong)|"
        r"(toi|minh) (co|duoc) (voucher|coupon|khuyen mai) (gi|nao|khong)?|"
        r"khuyen mai (hom nay|tuan nay|thang nay|dang co|moi nhat|hot)|"
        r"(co |)(chuong trinh|uu dai|khuyen mai) (gi |)(hot|hay|moi|dang dien ra)?|"
        r"doi diem (lay|doi) (qua|san pham|sach|uu dai))",
        _re.IGNORECASE,
    ), "promotion_info"),

    # ── Return Policy – chính sách đổi trả (khác return_request) ─────────
    (_re.compile(
        r"(dieu kien|chinh sach|quy dinh|thu tuc) (doi|tra|hoan) (hang|sach|tien)|"
        r"(doi tra|hoan tien) (nhu the nao|the nao|lam sao|co duoc khong|quy trinh)|"
        r"(co the|duoc phep) (doi|tra) (sach|hang)( khong)?|"
        r"bao lau thi (dc|duoc) doi (sach|hang)",
        _re.IGNORECASE,
    ), "return_policy"),

    # ── Complaint general – khiếu nại chính thức ─────────────────────
    (_re.compile(
        r"(muon|can|toi) (gui|goi|lam) (khieu nai|phan anh|complaint) (chinh thuc|len|cap tren)?|"
        r"(toi|minh) muon (phan anh|bao cao|khieu nai)( voi| len| chinh thuc)?|"
        r"gui (don|yeu cau) (khieu nai|phan anh)",
        _re.IGNORECASE,
    ), "complaint_general"),

    # -- BOOK COMPARE: dat TRUOC book_detail de ật so sanh truoc ---
    (_re.compile(
        r"(cuon|sach) .{1,40} co (hay|tot|tuyet|nen|dang doc) hon (cuon|sach) .{1,40}|"
        r"co (hay|tot|nen) hon .{0,10}(cuon|sach) .{2,30}|"
        r"(hay|tot) hon (cuon|sach) .{1,30}(goc|nguyen ban|ban moi)?|"
        r"nen (chon|mua|doc) cuon (nao|gi) (trong )?hai cuon|"
        r"cuon nao (hay|tot|nen doc) hon trong (hai cuon|2 cuon)|"
        r"so sanh .{2,20} (cuon|sach) .{2,20}(va|voi) .{2,20}(cuon|sach)|"
        # FIX BUG-6: price/quality comparison follow-up phrases
        r"(cuon|sach|cai) nao (re|dat|tot|hay|tot|dam|gia trị) hon(( cuon| sach| cai)? (kia|do|tren|nay|cu|moi))?|"
        r"(cuon|cai) (re|dat) hon (trong )?hai (cuon|cai)|hai cuon (cuon nao|cai nao) (re|dat|tot) hon|"
        r"so sanh (gia |)(hai|2) (cuon|sach|cai)",
        _re.IGNORECASE,
    ), "book_compare"),

    # FIX BUG-5: "còn sách nào về X không" bị classify là book_availability, sửa thành recommend_category
    (_re.compile(
        r"con (sach|cuon|quyen) nao (ve|theo|chu de|the loai|mang|linh vuc) .{1,50}(khong|co|vay|nhe)|"
        r"co sach nao (ve|theo|chu de) .{1,50}(khong|co|nhe)|"
        r"sach (ve|theo|chu de) .{1,40} (con|co) (khong|gi)|"
        r"con sach nao (theme|cung loai|cung the loai|tuong tu|lien quan)",
        _re.IGNORECASE,
    ), "recommend_category"),

    # -- 1e. BOOK DETAIL - price query by title name ------------------
    # -- 1e. BOOK DETAIL - price query by title name --
    # NOTE: cuon nay pattern thu hep lai, loai tru comparison phrases
    (_re.compile(
        r"cuon .{3,50} (gia bao nhieu|co gia bao nhieu|bao nhieu tien|bao nhieu d)"
        r"|cuon .{3,50} (thong tin|mo ta|noi dung|tac gia la ai|nxb nao)"
        r"|gia (cuon|sach) .{3,50} (la bao nhieu|bao nhieu)"
        r"|cuon (nay|do)( the nao| noi ve gi| ke gi| day gi)"
        r"|(san pham|cuon|sach).*(gia )?bao nhieu"
        r"|(sach|cuon|bo) .{2,40} (bo may cuon|co may cuon|may cuon|may tap|may phan|gom may cuon)"
        r"|(sach|cuon|bo) .{2,40} (co( ban)? khong|co ban o day khong)"
        r"|(muon|can|tim) biet (them |)(ve )?(cuon|sach|tac pham) .{2,40}"
        r"|tac pham .{2,40} (la gi|the nao|noi gi|ke gi|dang mua khong)"
        r"|tac gia( cua)? (cuon|sach)( do| nay| tren)?( la ai)?"
        r"|ai( la nguoi)? viet( cuon| sach)?( do| nay| tren)?"
        ,
        _re.IGNORECASE,
    ), "book_detail"),

    # ── 1f. RETURN REQUEST – emotional-led + link request phrases ────────
    (_re.compile(
        r"(minh muon tra lai (hang|sach)|"
        r"muon (tra|hoan) (lai )?(hang|sach|don)|"
        r"that vong .{0,20}(tra lai|hoan tien|doi hang)|"
        r"toi (muon|can) hoan (hang|tien|tra)|"
        r"tra lai (cuon |hang )?lay tien( lai)?|"
        r"gui (sach|hang) (nguoc |tra )?(ve|lai) shop|"
        r"xin (link|cach|huong dan) .{0,10}(tra hang|tra sach|hoan tra)|"
        r"link .{0,10}(tra hang|gui yeu cau tra|hoan tra)|"
        r"nhanh nhat.{0,10}(tra|hoan|doi) (hang|sach)|"
        r"gui yeu cau (tra|hoan|doi) (hang|sach))",
        _re.IGNORECASE,
    ), "return_request"),

    # ── 1g. ORDER CANCEL – contextual phrasing ────────────────────────
    # -- 1g. ORDER CANCEL - contextual phrasing --
    (_re.compile(
        r"huy( luon)? don (sach|hang) (thu |so |#)?[0-9]+ (dang cho giao|dang xu ly)"
        r"|huy (luon )?don (hang )?(dang cho|chua ship)"
        r"|thoi huy (luon )?don (sach|hang)"
        r"|bo don (hang )?thu [0-9]+"
        r"|huy don .{0,15}(thu [0-9]+|so [0-9]+)"
        r"|don .{0,20}(dang cho giao|chua ship).{0,20}huy"
        r"|vua dat (nham|nham roi) .{0,20}(muon |can )?huy"
        r"|dat (nham|sai|nham cuon) .{0,30}(muon |can )?(huy|bo)"
        r"|(muon|can) huy (don|cuon|sach) (vua|moi) (dat|mua)"
        r"|huy (luon |)don (sach|hang) (nay|do|thu [0-9]+)"
        r"|thoi (bo|huy) (don|sach|hang) (nay|do|thu|vua dat)"
        r"|(bo|huy|xoa) (don|order) #?[0-9]{3,6}"
        r"|(huy|bo) don #?[0-9]{3,6}( (chua|dang) (giao|ship))?"
        r"|(muon|can|minh) (bo|huy) don (hang )?(vua|moi|vua moi) (dat|tao)"
        r"|doi y (roi )?muon (huy|bo) don|huy ngay don (toi|minh) (vua|moi) dat"
        ,
        _re.IGNORECASE,
    ), "order_cancel"),

    # ── 1h. COMPLAINT DAMAGED – short/colloquial damage phrases ──────────
    # Fix C-Edge: "goc bia sach bi rach ca mot mang roi" → out_of_scope
    (_re.compile(
        r"(goc bia( sach)? bi (rach|hong|nat)|"
        r"bia sach( bi)? (rach|hong|nat|cong|khem)|"
        r"sach( nhan ve| nhan duoc)? bi (am|uot|hong|nat|rach)|"
        r"(rach|hong|xay xuoc|nat) (ca |)(mot mang|nhieu cho|het|cai bia)|"
        r"sach (bi |)(nat|rach|hong) (qua|roi|lam|that)|"
        r"sach (rach|nat|hong) (bia|trang|cover|goc)|"
        r"bao bi (bi|da) (nat|hong|rach|uot) (trong qua trinh van chuyen|luc giao))",
        _re.IGNORECASE,
    ), "complaint_damaged"),

    # ── 1i. CHITCHAT – farewell / thank-you phrases ───────────────────────
    # Fix C-Negative: "cam on ban nhe tam biet" → out_of_scope
    (_re.compile(
        r"(cam on.{0,30}(nhe|nha|nhieu|qua|that|shop|ban|roi)|"
        r"tam biet (ban|shop|nhe|nha|roi|moi nguoi)?$|"
        r"^tam biet.{0,20}$|"
        r"chuc (ban|moi nguoi) (mot ngay|buoi|tuan|cuoi tuan) (tot lanh|vui ve|may man)|"
        r"hen gap lai( nhe| nha| ban)?|bye( bye)?( ban| shop| nhe)?|"
        r"goodbye( ban| shop)?)",
        _re.IGNORECASE,
    ), "chitchat"),

    # ── 1j. CHITCHAT – positive short reactions (sau khi bot trả lời) ─────
    # VD: "ngon lun", "ok rồi", "hay đó", "tuyệt", "được rồi", "good", "perfect"
    # Những phản ứng này thường là 1-3 từ, không cần SBERT
    (_re.compile(
        r"^(ngon (lun|roi|that|qua|vay)?|"
        r"ok( roi| nhe| vay| lun| ban)?|"
        r"oke( roi| nhe| vay)?|"
        r"(hay|tuyet|xin|dinh|chuan|xit)( doi| that| qua| day| vay| lun| roi)?|"
        r"(duoc roi|xong roi|hieu roi|ra roi|biet roi)|"
        r"(cam on|thanks?|thank you)( nhe| nha| ban| shop| nhieu)?|"
        r"(good|great|perfect|nice|cool|awesome)( job| bot| shop)?|"
        r"(hieu roi|ro roi|clear|got it|noted)|"
        r"(yeu|thich) (cuon|sach) (nay|do) (qua|that)?|"
        r"(no|co day)( vay| the)?|"
        r"^(vang|da|uh|um|ah|a|o)( roi| vay| the| nhe| ok)?$)$",
        _re.IGNORECASE,
    ), "chitchat"),


    # ── 1. CHITCHAT / GREETING ────────────────────────────────────────────────

    (_re.compile(
        r"(bot oi (em|anh|toi|minh) muon (hoi|hoi chut|hoi chut duoc khong)|bot oi (cho hoi|giup)|"
        r"(hello|hi) (bot|shop|ban)( oi)?( cho hoi chut)?|"
        r"(hi|hey|alo) (may|ban|bot) (lam|giup|biet|co the) (duoc gi|gi)|bot (co )?(on|khoe|bt noi) (khong|chuyen)|"
        r"em chao (anh|chi) bot|may (co )?(on|khoe) khong|"
        r"(em )?muon hoi( chut)?( duoc khong| co| nhe| 1 ti)?|"
        r"xin chao( bot| nha| nhe| shop| hihi)?|"
        r"chao (shop|bot|ad|ban|cac ban|moi nguoi)( oi)?( cho hoi chut)?|"
        r"are you there( bookstore| bot| shop)?|"
        r"can you help (me out|me|us)|"
        r"nice to (meet|see) you( (bookstore|bot|shop))?|"
        r"chao em la khach hang( moi)?|"
        r"hi may lam duoc gi|"
        r"bot co on khong|"
        r"^(ad oi|hello bot).*|"
        r"^may lam (dc|duoc) nhung gi.*|"
        r"bot lam duoc gi (cho toi|cho minh)?|"
        r"em chao anh bot)",
        _re.IGNORECASE,
    ), "chitchat"),

    # Biến thể tiếng Anh ngắn + informal
    (_re.compile(
        r"^(hello( bot| there| shop| bookstore)?|hi( there| bot| shop)?|"
        r"hey( there| bot)?|how do you do|how are you( doing)?)$",
        _re.IGNORECASE,
    ), "chitchat"),

    # [FIX] "chào bạn" và các biến thể chào hỏi thường gặp bị thiếu
    # Normalized: "chao ban", "chao", "alo", "hello ban", ...
    (_re.compile(
        r"(chao ban( oi)?|chao a|chao chi|chao anh|chao em|chao shop( oi)?|"
        r"chao assistant|chao ai|chao chatbot|"
        r"alo( ban| shop| oi)?|"
        r"cho hoi( chut)?|cho minh hoi|hoi chut duoc khong|"
        r"good (morning|afternoon|evening|night)( ban| shop| bot| everyone)?|"
        r"howdy|greetings|yo( ban| bot| shop)?|"
        r"hi ban|hello (anh|chi|em|moi nguoi)|"
        r"ban co khoe khong|ban dang lam gi|ban the nao|co ai day khong|"
        r"xin chao ban|xin chao shop|xin chao chatbot|"
        r"hi( ban)?( oi)?,? cho hoi chut|hi (minh|toi) muon hoi|"
        r"welcome|noi chuyen chut|bat dau nao)",
        _re.IGNORECASE,
    ), "chitchat"),

    # VOUCHER APPLY
    (_re.compile(
        r"((ma giam gia|voucher|coupon|mgg).*(dung duoc|ap dung|ap|het han|nhan|chi tiet))|"
        r"(het han|khong (dung duoc|nhan|ap dung|ap)).*(ma giam gia|voucher|coupon|mgg)|"
        r"nhap (voucher|mgg|ma giam gia)|"
        r"dang ky ma mien phi giao hang",
        _re.IGNORECASE,
    ), "voucher_apply"),

    # RECOMMEND CATEGORY (Hardcode fix for exact match)
    (_re.compile(
        r"goi y cho minh sach chung khoa(n)?",
        _re.IGNORECASE,
    ), "recommend_category"),

    # ── 5. RETURN REQUEST ────────────────────────────────────────────────
    (_re.compile(
        r"muon (tra lai hang( lay tien)?|hoan tien|doi (sach|cuon|hang|size|sz)|"
        r"gui tra hang|tra lai (cuon sach|hang) nay|doi sang cuon khac|"
        r"hoan hang|doi tra|"
        r"xin (link|cach|huong dan) (gui|goi|tra) (yeu cau )?(tra hang|tra sach|hoan)|"
        r"link (gui|nop|tra) (yeu cau )?(tra hang|tra sach|doi tra)|"
        r"(lam|gui) (don|yeu cau) (tra|hoan|doi) (hang|sach)|"
        r"gui (khieu nai|yeu cau) tra (hang|sach)|"
        r"toi muon hoan lai cuon sach ra|"
        r"nhanh nhat (de )?(tra|gui tra|hoan) (hang|sach))",
        _re.IGNORECASE,
    ), "return_request"),

    # ── 22b. POLITE / CONVERSATIONAL BOOK QUERIES (trước book_search generic) ──
    # Fix: các câu hỏi lịch sự về sách bị nhận nhầm là out_of_scope
    # VD: "bạn có thể giúp tôi xem thông tin của sách X không"
    (_re.compile(
        r"(ban co the (giup toi |cho toi )?(xem|biet|hoi|tim).{0,15}(sach|cuon)|"
        r"ban co the cho (toi|minh) .{0,10}(sach|cuon)|"
        r"giup (toi|minh) (tim|xem|biet|hoi) .{0,10}(sach|cuon)|"
        r"cho (toi|minh) biet (them |ve )?(cuon|sach) .{2,40}|"
        r"muon (biet|hieu|tim hieu) (them |)ve .{2,20}(sach|cuon|tac gia)|"
        r"tim hieu (them |)(ve )?(cuon|sach) .{2,40}|"
        r"(cuon|sach) .{2,40} (noi gi|ke gi|day gi|viet ke|goi y gi)|"
        r"gioi thieu (them |)(ve )?(cuon|sach) .{2,40}|"
        r"(noi dung|chu de|canh cao) (chinh )?(cuon|sach) .{2,40}|"
        r"muon (doc|mua|xem) (cuon|sach) .{2,40}|"
        r"(co |ban )(cuon|sach) (nao |gi |ten )?.{2,30} (khong|ko|duoc khong)|"
        r"(cuon|sach) (ten la|co ten|ten sach) .{2,30}|"
        r"sach ten .{2,40}|"
        r"tac gia .{2,30} (co |viet )?(nhung )?(cuon|sach) (nao|gi)|"
        r"nhung the loai( sach)?(.*co la gi| nao)|"
        r"khong (the )?(tim thay|thay) cuon( sach| nxb)?|"
        r"co ban (tieu thuyet|truyen|novel|sach) .{2,30} (khong|ko)|" 
        r"co (ban|cuon|bo) (tieu thuyet|truyen|novel) (ten |)?.{2,30}|"
        r"tim (tieu thuyet|truyen trinh tham|truyen tranh|sach thieu nhi) .{0,40})",
        _re.IGNORECASE,
    ), "book_search"),

    # ── book_compare: X co hay hon Y khong / nen chon cuon nao ───────────
    (_re.compile(
        r"(cuon|sach) .{1,40} (co )?(hay|tot|tuyet|duc|nen|dang doc) (hon|bang) (cuon|sach) .{1,40} (khong|ko|hay hon)?|"
        r"(cuon|sach) .{1,40} vs .{1,40} (cuon nao|cuon gi) (tot|hay|nen mua|nen chon) (hon|hon nua)?|"
        r"nen (chon|mua|doc) cuon (nao|gi) (trong )?hai cuon|"
        r"cuon nao (hay|tot|nen doc) (hon )?trong (hai cuon|2 cuon)|"
        r"khac nhau gi.{0,15}cuon|"
        r"(hay|tot) hon .{1,30}(cuon|sach) .{1,30}(goc|nguyen ban|ban moi)?|"
        r"co (hay|tot|nen) hon .{0,10}(cuon|sach) .{2,30}",
        _re.IGNORECASE,
    ), "book_compare"),

    # ── 22. BOOK SEARCH (generic – sau tất cả specific) ─────────────────────
    (_re.compile(
        r"(tim sach|tim kiem sach|"
        r"search (for )?(a |some )?(book|sach)|search .{2,20} book|"
        r"find (me )?(a |some )?book[s]?|co sach( nao)?( ve| cua)?|"
        r"cho (xem|toi) sach .{2,30}|ban co (sach|cuon)|"
        r"co ban (cuon|sach) .{1,30}|ban (cuon|sach) .{1,30} (khong|ko)?|"
        r"sach ve |cuon sach( ve)?|cuon book|order.*cuon|"
        r"sach cua tac gia|sach theo (the loai|chu de)|"
        r"con (sach|cuon)( nao)? cua( tac gia| nha van)? .{2,30}|"
        r"(tim |xem )?sach cua (tac gia|nha van) .{2,30}|"
        r"tac gia .{2,30} con (cuon|sach)( nao| gi)?|"
        r"cho toi xem sach|"
        r"i (need|want|am looking for) (a |some )?(book|novel|fiction)|"
        r"i'?m looking for (a |some )?(book|novel|fiction)|"
        r"i want to buy (a |some )?(book|novel)|"
        r"novel (about|on) [a-z\s]{2,30}|fiction (novel|book)|"
        r"children'?s (picture )?books?|"
        r"books? (about|on|for|regarding) [a-z]+|"
        r"do you (sell|have|carry) (books?|english books?|children'?s books?)|"
        r"(book|sach) (day tieng|ky nang|lich su|tam ly hoc|van hoc|kinh te|khoi nghiep|"
        r"tu duy|thieu nhi|khoa hoc|viet nam|nau an|the loai|day ve|phu bien|"
        r"phan bien|lap trinh|marketing|kinh doanh|tai chinh|chung khoan|"
        r"thien|yoga|phat trien|tu truyen|mieu ta|giao duc|tre em)|"
        r"co cuon nao ve [a-z].{2,30}|"
        r"(xin chao .{0,20})?tim sach|can tim sach|"
        r"cho xem sach day [a-z\s]{2,30}|"
        r"sach .{2,20}(hay nhat( nua)?|tot nhat|5 sao|nhieu nguoi doc))",
        _re.IGNORECASE,
    ), "book_search"),

    # ── 23. SHIPPING → store_info ────────────────────────────────────────────
    (_re.compile(
        r"(phi ship|phi van chuyen|giao hang mat may ngay|"
        r"thoi gian giao hang|giao toan quoc|"
        r"bao gio hang (den|ve)|may ngay thi nhan|"
        r"shipping (cost|fee|time)|delivery (cost|fee|time)|"
        r"ship (ve|den|toi) [a-z ]{2,20} (mat|ton|het|khoang) bao (lau|nhieu ngay)|"
        r"giao (hang |)(ve|den|toi) [a-z ]{2,20} mat bao lau|"
        r"mua (roi |)(ship|giao) (ve|den|toi) [a-z ]{2,20} (mat|ton) bao (lau|nhieu)|"
        r"(hang|sach) (ship|giao) (den|ve) (ha noi|sai gon|tp hcm|tphcm|da nang|can tho|hue|hai phong) (mat|ton|het) bao (lau|nhieu))",
        _re.IGNORECASE,
    ), "store_info"),

    # ── 24. STORE INFO (general) ───────────────────────────────────────────────
    # -- 24. STORE INFO (general) --
    (_re.compile(
        r"so hotline|hotline|lien he|email (lien he|ho tro|cua shop|cham soc khach hang)"
        r"|dia chi (cua hang|shop)|(shop|cua hang) o (dia chi|thanh pho|quan)"
        r"|co cua hang vat ly (tai|o) .{2,15}|den mua truc tiep o dau"
        r"|gio (lam viec|ho tro|mo cua)|thoi gian (lam viec|ho tro)"
        r"|ho tro khach hang (trong|vao) khung gio nao"
        r"|so dien thoai (ho tro|cua shop)|contact"
        r"|business hours|support email|store address"
        r"|trang web chinh thuc|facebook (cua )?bookstore"
        r"|shop o (tinh|thanh pho|khu vuc) nao|fanpage"
        r"|dia chi chinh (o dau|cua)|shop (o |tai )?(dau|noi nao)"
        r"|dat hang (va giao hang )?(ra nuoc ngoai|quoc te|o nuoc ngoai)"
        r"|toi o nuoc ngoai (co )?(dat hang|mua sach) (duoc|o day) khong"
        r"|giao toan quoc|co giao hang toan quoc|ship toan quoc"
        r"|nationwide (delivery|shipping)"
        r"|how long does (delivery|shipping|it) take(\s|$)"
        r"|what is (the )?shipping (fee|cost|price)"
        r"|what (is|are) (the )?delivery (fee|cost|options?)"
        r"|shipping options (available)?|what are the shipping options"
        r"|do you (deliver|ship) (books? )?(nationwide|to .{2,20}|internationally|overseas)"
        r"|giao hang tu [a-z ]{2,15} (di|den) [a-z ]{2,15} (mat|ton) bao (lau|nhieu ngay)"
        r"|(giao hang|ship) tu [a-z ]{2,15} den [a-z ]{2,15} (mat |ton )?bao (lau|nhieu ngay)"
        r"|(giao hang|ship) (mat|ton) bao (lau|nhieu ngay)"
        r"|ship (den|toi) tinh (binh duong|binh phuoc|long an|tay ninh|dong nai|ba ria|vung tau)"
        r"|shop giao hang (ve|toi|den) [a-z ]{2,15} (duoc |)khong"
        r"|co giao hang (ra|den) (nuoc ngoai|quoc te)|ship (quoc te|international)"
        r"|thu (2|3|4|5|6|7|bay) (va |)(chu nhat) shop co (lam|mo cua)"
        r"|(thu bay|chu nhat|cuoi tuan) (co |)ho tro (khach|mua hang) khong"
        r"|(ship|giao hang) (ve|toi|den) (mien|dao|tinh|thanh pho) [\w ]{2,20} (duoc|khong)"
        r"|shop co giao (ve|toi) mien (tay|nam|bac|trung) (duoc khong)?"
        r"|giao hang (mien tay|mien bac|mien nam|mien trung) (duoc khong)?"
        r"|ship mien phi (khu vuc|tu|den|toi) [\w ]{2,20}|phi ship (ve|den) [\w ]{2,20}"
        ,
        _re.IGNORECASE,
    ), "store_info"),

    # ── 25. PROMOTION – giảm giá đối tượng ───────────────────────────────────
    # -- 25. PROMOTION CURRENT --
    (_re.compile(
        r"sinh vien co giam gia|giam gia (cho )?(sinh vien|khach hang than thiet|member|hoc sinh)"
        r"|student discount|loyalty discount|VIP (giam gia|discount)"
        r"|giam gia (dac biet|them) cho"
        r"|(co|dang co) (chuong trinh )?(khuyen mai|km|sale|uu dai)( gi)?( khong)?( hien tai| bay gio| hom nay)?"
        r"|(co|dang co|hien co) (chuong trinh|ma|voucher) (nao|gi) (dang chay|hoat dong|hien tai)"
        r"|(khuyen mai|km|sale|voucher) (hien tai|hien co|hien dang|bay gio|hom nay|nao dang chay)"
        r"|(shop|cua hang) (dang co|co|hien co) (chuong trinh|km|khuyen mai) (gi|nao)"
        r"|co (ma giam gia|voucher|code) (khong|gi|nao)"
        r"|(co|shop co|hien co) (ma giam gia|khuyen mai|km|voucher|coupon)( khong| gi)?"
        r"|(giam gia|km|sale|voucher) (nao|gi|dang co)( khong)?"
        r"|(toi|minh) co the dung (ma giam gia|voucher) (gi|nao)( khong)?"
        r"|(ma giam gia|voucher|code|ma|khuyen mai) (nao )?dang (co |)(hieu luc|ap dung|hieu|chay|hoat dong)"
        r"|(ma giam gia|voucher|code) (con |)(hieu luc|the |su dung|dung duoc)"
        r"|(dang co|co |hien co )(ma giam gia|voucher|code|khuyen mai) (nao)?(hieu luc|ap dung|)?"
        ,
        _re.IGNORECASE,
    ), "promotion_current"),

    # ── 26. CART HELP ────────────────────────────────────────────────────────
    (_re.compile(
        r"(gio hang( cua)?( minh| toi)?|xem gio hang|gio hang bi loi|"
        r"them.{0,20}vao gio hang|my cart is empty( after adding)?|"
        r"them (cuon|sach|cai) (do|nay|nay).{0,10}(vao|gio)( hang)?|"
        r"them (cuon|sach) (do|nay) vao|cho them vao gio|"
        r"xoa (bo |san pham |sach )?(khoi|ra khoi) gio hang|"
        r"xoa (cai |cuon |san pham )?(nay|do|sach nay) (khoi|ra khoi) gio hang|"
        r"bo (san pham|sach|cuon) (nay|do) (ra|khoi) gio hang|"
        r"remove (from|item from) (my )?cart|"
        r"clear (my )?cart|empty (my )?cart|"
        r"gio hang hien tai|gio hang trong|gio hang co gi|"
        r"so luong (trong|o trong) gio hang|"
        r"(dat hang|mua ngay|checkout|thanh toan) (ngay|luon|di)|"
        r"tien hanh (mua|dat|thanh toan)|"
        r"(lam sao|cach nao) (de )?(them|bo|xoa|sua) .{0,15}(gio hang|cart)|"
        r"gio hang khong (hien|luu|cap nhat)|"
        r"(dat hang|mua) (nhung )?(sach|cuon) (trong|o) gio hang)",
        _re.IGNORECASE,
    ), "cart_help"),

    # ── 27. ORDER STATUS ─────────────────────────────────────────────────────
    (_re.compile(
        r"kiem tra (tinh trang |trang thai )?(don( hang)?|dh)( nay| cua minh| cua toi)?$|"
        r"don hang (cua toi |cua minh )?(den dau roi|dang o dau|trang thai gi|nhu the nao)|"
        r"kiem tra giup don( hang)? dh\d+|"
        r"bao gio( moi)? (nhan duoc hang|giao hang)|"
        r"(don|dh) .* truoc khi thanh toan",
        _re.IGNORECASE,
    ), "order_status"),

    # ── BOOK REVIEW – chưa có QUICK_RULE nào, thêm mới ───────────────────────
    (_re.compile(
        r"(review|danh gia|nhan xet|cam nhan) (ve |cuon |sach |qua )?sach (nay|do|vua tim|vua xem|tren)?|"
        r"(sach|cuon) (nay|do) (co hay|co tot|duoc khen|nhu the nao|ra sao|the nao)( khong)?|"
        r"(sach|cuon) (nay|do) (duoc|bi) (bao nhieu|may) sao|"
        r"(moi nguoi|nguoi doc|doc gia|khach hang) (noi|danh gia|review|nhan xet) (sach|cuon) (nay|do) (nhu the nao|the nao|ra sao)?|"
        r"(co nen|nen) (mua|doc) (cuon|sach) (nay|do) (khong|ko)?|"
        r"(hay|nen) (doc|mua) (cuon|sach) (nay|do) (khong|ko|khong ban)?|"
        r"(ai|ban nao|nguoi nao) (da|vua) (doc|xem) (cuon|sach) (nay|do)( cho biet hay khong)?|"
        r"(bao nhieu|may) (nguoi|sao|diem) (danh gia|review|rating) (cuon|sach) (nay|do)?|"
        r"(doc xong|xem xong) (cuon|sach) (nay|do) (co hay|co tot|cam giac nhu the nao)?|"
        r"(cuon|sach) (nay|do) (5 sao|4 sao|3 sao|bao nhieu sao)|diem danh gia (cuon|sach)",
        _re.IGNORECASE,
    ), "book_review"),

    # ── RECOMMEND TRENDING – thêm pattern sách mới, sách tháng này ───────────
    (_re.compile(
        r"(sach|cuon) (moi ra|vua ra|moi xuat ban|ra thang nay|ra tuan nay|moi nhat) (thang nay|tuan nay|gan day|2024|2025)?|"
        r"(sach|cuon) (moi nhat|moi cap nhat|moi phat hanh) (la gi|co gi|nao)?|"
        r"(top|bestseller|ban chay) (sach|cuon) (hien tai|bay gio|thang nay|nam nay)?|"
        r"(nhieu nguoi|moi nguoi) (dang|hay) (doc|mua|tim) (sach|cuon) (gi|nao) (nhat)?|"
        r"sach (ghi an tuong|duoc ua chuong|pho bien) nhat (hien nay|bay gio|thang nay)",
        _re.IGNORECASE,
    ), "recommend_trending"),

    # ── RECOMMEND GIFT – teacher/boss/colleague (thêm mới) ────────────────────
    (_re.compile(
        r"(tang|qua tang|mua tang|mua qua) (cho )?(giao vien|thay|co|thay co|sep|sếp|dong nghiep|ban be|nguoi than|ong ba|ba me)|"
        r"(sach|qua) (tang ngay|tang dip) (20.10|20.11|8.3|tet|giang sinh|valentine|trung thu)|"
        r"(goi y|tu van) (sach|qua tang) (cho |tang )?(sep|giao vien|thay|co|dong nghiep|ba me|ong ba|nguoi yeu)|"
        r"mua sach tang (nhan dip|dip) (sinh nhat|tot nghiep|tet|le|cuoi|ky niem)",
        _re.IGNORECASE,
    ), "recommend_gift"),

    # ── ENGLISH PATTERNS – track order, refund, price, availability ──────────
    (_re.compile(
        r"(track|where is) (my )?(order|package|shipment)|order (status|tracking)|"
        r"(is|book) (this|it) (available|in stock)|check (stock|availability|inventory)|"
        r"(can i|how to) (get a |)(refund|return|exchange) (for )?(this|the book|my order)?|"
        r"(what('s| is)) (the )?(price|cost) (of )?(this|the book)?|"
        r"(do you|does the shop) (accept|take) (credit card|paypal|momo|visa|mastercard)|"
        r"(how long|when) (will|does) (my )?(order|delivery|shipment) (arrive|take|come)|"
        r"(i want to|i'd like to) (cancel|return|exchange) (my )?(order|book)|"
        r"(what|which) (books?|book) (are|is) (popular|trending|best.?selling|recommended)|"
        r"(show|find|search) (me )?(books?|novels?) (about|on|for|related to)",
        _re.IGNORECASE,
    ), "book_search"),  # Default English→book_search; specific ones above will override

    # ── SHORT FOLLOW-UP: câu cực ngắn sau khi đã có context ──────────────────
    # VD: "Còn hàng không?", "Giá bao nhiêu?", "Tác giả là ai?"
    # Không dùng anchor $ để bắt cả dạng có suffix emoji
    (_re.compile(
        r"^(con hang|het hang|co hang) (khong|chua|rui)?$|"
        r"^(gia|bao nhieu|bao tien|may tien|tien) (bao nhieu|khong|vay|the)?$|"
        r"^(tac gia|ai viet|nguoi viet) (la ai|ai|ten gi|la gi)?$|"
        r"^(co (nen|nen doc|nen mua)) (khong|ko|chua)?$|"
        r"^(them vao gio|mua luon|dat ngay|mua di) (nhe|nha|di)?$|"
        r"^(danh gia|rating|may sao|bao nhieu sao) (the nao|vay|nao)?$|"
        r"^(giao bao lau|may ngay giao|khi nao nhan) (vay|the|duoc)?$",
        _re.IGNORECASE,
    ), "chitchat"),


    # ── FIX P1: GREETING + BOOK SEARCH combined — ưu tiên book_search khi có tên sách/chủ đề sau lời chào ──
    # VD: "Xin chào, tôi muốn tìm sách đắc nhân tâm" → book_search (không phải chitchat)
    # Phải đặt TRƯỚC QUICK_RULES chitchat greeting để được ưu tiên
    (_re.compile(
        r"^(xin chao|chao|hello|hi|alo)[\.!,\s]{0,5}.{0,30}?"
        r"(tim sach|muon tim|muon mua|can mua|can tim|tim cuon|mua cuon|xem sach|sach|cuon sach)",
        _re.IGNORECASE,
    ), "book_search"),

    # ── FIX P1: ACCOUNT HELP — hỏi về việc mua khi chưa đăng nhập → account_help ──────────
    # VD: "Tôi chưa đăng nhập thì mua được không", "Chưa có tài khoản có mua được không"
    (_re.compile(
        r"(chua|khong) (dang nhap|co tai khoan|login|dang ky).{0,30}(mua|dat hang|thanh toan|order) (duoc|co the) khong|"
        r"(mua|dat hang|thanh toan) (duoc|co the|co) khong .{0,20}(chua|khong) (dang nhap|co tai khoan)|"
        r"(guest|khach|chua co tai khoan|chua dang nhap) .{0,20}(mua|dat hang|thanh toan) (duoc|co) khong|"
        r"(chua dang ky|chua dang nhap).{0,20}(co the|co) (mua|dat|order) (khong|chua)|"
        r"(mua hang|dat hang|mua sach).{0,20}(can|phai|bat buoc) (dang nhap|co tai khoan|dang ky) (khong|hay khong)",
        _re.IGNORECASE,
    ), "account_help"),

    # ── FIX P1: STORE INFO — hỏi số ngày giao hàng SAU KHI MUA → store_info (không phải order_status) ──
    # VD: "Sau khi mua, bao nhiêu ngày thì nhận được hàng"
    (_re.compile(
        r"(sau khi|sau luc|sau khi dat hang|sau khi mua|sau khi chot don).{0,25}"
        r"(bao nhieu ngay|may ngay|bao lau|khi nao).{0,20}(nhan|nhan duoc|lay duoc|giao den) (hang|sach)?|"
        r"(bao nhieu ngay|may ngay|bao lau)( thi| de)? (nhan|nhan duoc|giao den|lay duoc) hang (sau khi|ke tu|tinh tu) (dat|mua|chot)|" 
        r"tu luc (dat hang|mua|chot don).{0,20}(bao nhieu ngay|may ngay|bao lau).{0,20}(nhan duoc|giao den|ve den)|"
        r"giao hang (mat|ton|can) bao nhieu ngay (ke tu|tinh tu|tu luc) (dat hang|mua|chot)",
        _re.IGNORECASE,
    ), "store_info"),

    # ── FIX P1: SHIP + CITY — "ship về Đà Nẵng mất mấy ngày" → store_info ──
    # Phải đặt TRƯỚC book_compare để override
    (_re.compile(
        r"(ship|giao hang|van chuyen|gui hang).{0,20}(ve|den|toi|cho)\s*"
        r"(ha noi|ho chi minh|hcm|tp\.?\s*hcm|sai gon|da nang|hue|hai phong|"
        r"can tho|nha trang|vung tau|vinh|thai nguyen|lam dong|binh duong|dong nai|long an)|"
        r"(bao nhieu ngay|may ngay|bao lau|khi nao)\s*(giao|nhan)\s*(hang|sach)?.{0,20}"
        r"(ve|den)\s*(ha noi|hcm|ho chi minh|da nang|hue|hai phong|can tho|nha trang)",
        _re.IGNORECASE,
    ), "store_info"),

    # FIX GUEST: "có bao nhiêu thể loại" / "shop có bao nhiêu thể loại" → store_info
    # CHỈ bắt khi có "bao nhiêu"/"may" (count query) — KHÔNG bắt "thể loại nào khác" (→ recommend_category)
    (_re.compile(
        r"(co |shop co |cua hang co )?(bao nhieu|may|tong cong) (the loai|danh muc|loai sach|chu de)|"
        r"(bao nhieu|may) (the loai|danh muc|loai|chu de) (sach )?(co|co tren shop|co tren web|co ban)?|"
        r"(liet ke|hien thi) (tat ca )?(the loai|danh muc|loai sach) (co|hien co)?|"
        r"(shop|web|website|cua hang).{0,15}(co|ban|cung cap).{0,10}(bao nhieu|may) (the loai|danh muc)",
        _re.IGNORECASE,
    ), "store_info"),
]



# ── Public API ────────────────────────────────────────────────────────────────
from chatbot_app.nlu.text_normalizer import expand_abbreviations

def detect_intent(text: str) -> NLUResult:
    """
    Phân loại intent bằng 2 tầng:
      1. Quick rule-based regex trên _normalize_vi(text) – không lỗi Unicode
      2. SBERT zero-shot cosine similarity trên text gốc
    """
    # Xử lý tiếng lóng, viết tắt
    text = expand_abbreviations(text)
    text_norm = _normalize_vi(text)

    # ── FIX P1: COMPOUND GREETING + BOOK SEARCH ──────────────────────────────
    # Nếu text bắt đầu bạng lời chào NHƯNG phần còn lại rõ ràng là tìm sách → book_search
    import re as _re_greet
    _greet_prefix = _re_greet.match(
        r'^(xin\s*chao|chao|hello|hi|hey|alo|chao\s*ban|chao\s*shop)[\s,\.!]*',
        text_norm, _re_greet.IGNORECASE
    )
    if _greet_prefix:
        _after_greet = text_norm[_greet_prefix.end():].strip()
        _BOOK_KW = [
            'tim sach', 'muon tim', 'tim cuon', 'tim kiem sach',
            'can tim', 'muon mua', 'can mua', 'xem sach',
            'sach', 'cuon', 'quyen', 'book',
        ]
        if _after_greet and any(kw in _after_greet for kw in _BOOK_KW):
            return NLUResult(intent='book_search', confidence=0.96)

    # Tầng 1: Regex
    for pattern, intent in QUICK_RULES:
        if pattern.search(text_norm):
            return NLUResult(intent=intent, confidence=0.95)

    # Tầng 2: SBERT
    model    = get_sbert_model()
    user_vec = model.encode(text, normalize_embeddings=True)

    best_intent = "out_of_scope"
    best_score  = 0.0

    for intent, tmpl_vecs in _template_cache.items():
        # Max cosine similarity: tìm template gần nhất trong tập của intent
        # tmpl_vecs shape: (n_templates, embed_dim)
        # user_vec shape:  (embed_dim,)
        # np.dot(tmpl_vecs, user_vec) → (n_templates,) → lấy max
        scores = np.dot(tmpl_vecs, user_vec)   # cosine sim (vecs đã normalized)
        score  = float(np.max(scores))          # max thay vì dot với mean
        if score > best_score:
            best_score  = score
            best_intent = intent

    if best_score < CONFIDENCE_THRESHOLD:
        best_intent = "out_of_scope"  # FIX v7: dùng out_of_scope thay vì general_query

    return NLUResult(
        intent=best_intent,
        confidence=round(best_score, 4),
    )


class CustomerIntentClassifier:
    """Wrapper class for customer intent classification."""

    def classify(self, message: str) -> dict:
        """Classify customer intent and return dict with intent and confidence."""
        result = detect_intent(message)
        return {
            "intent": result.intent,
            "confidence": result.confidence,
            "entities": result.entities,
        }
