"""
text_normalizer.py – Xử lý chữ viết tắt, tiếng lóng (teencode) và sai chính tả phổ biến
trước khi đưa vào luồng nhận diện Intent và trích xuất Entity.
"""
import re

ABBREVIATIONS = {
    # ── Nhóm phủ định ──────────────────────────────────────────────────────
    "ko":   "không",
    "k":    "không",
    "kh":   "không",
    "khog": "không",
    "hông": "không",
    "hong": "không",
    "chả":  "không",
    "kp":   "không phải",
    "kbt":  "không biết",

    # ── Nhóm khẳng định / đồng ý ───────────────────────────────────────────
    "dc":   "được",
    "đc":   "được",
    "dk":   "được",
    "oke":  "đồng ý",
    "uk":   "đồng ý",
    "uh":   "đồng ý",

    # ── Nhóm đại từ / giao tiếp chung ─────────────────────────────────────
    "mk":   "mình",
    "mik":  "mình",
    "t":    "tôi",
    "mn":   "mọi người",
    "ae":   "anh em",
    "cx":   "cũng",
    "vẫy":  "vậy",
    "vs":   "với",
    "bt":   "bình thường",
    "bthg": "bình thường",

    # ── Nhóm hỏi đáp ──────────────────────────────────────────────────────
    "bn":   "bao nhiêu",
    "ntn":  "như thế nào",
    "nnao": "như thế nào",
    "tn":   "thế nào",
    "nhu":  "như",
    "thế nào vậy": "thế nào",

    # ── Nhóm thương mại / sản phẩm ──────────────────────────────────────
    "sp":   "sản phẩm",
    "s/p":  "sản phẩm",
    "đh":   "đơn hàng",
    "dh":   "đơn hàng",
    "nxb":  "nhà xuất bản",
    "xb":   "xuất bản",
    "sz":   "size",
    "km":   "khuyến mãi",
    "kmai": "khuyến mãi",
    "mgg":  "mã giảm giá",
    "ck":   "chuyển khoản",
    "cod":  "thanh toán khi nhận hàng",
    "stk":  "số tài khoản",
    "nv":   "nhân viên",
    "tk":   "tài khoản",
    "acc":  "tài khoản",
    "mk":   "mật khẩu",    # context thương mại: mk thường = mật khẩu
    "pw":   "mật khẩu",
    "pass": "mật khẩu",
    "sdt":  "số điện thoại",
    "phone":"số điện thoại",
    "info": "thông tin",
    "ship": "giao hàng",
    "free ship": "miễn phí giao hàng",
    "freeship": "miễn phí giao hàng",

    # ── Nhóm giao tiếp / mạng xã hội ──────────────────────────────────────
    "ib":   "nhắn tin tư vấn",
    "inb":  "nhắn tin tư vấn",
    "rep":  "trả lời",
    "nt":   "nhắn tin",
    "ad":   "quản trị viên",
    "mod":  "quản trị viên",
    "cmt":  "bình luận",
    "like": "thích",
    "share":"chia sẻ",

    # ── Nhóm hành động ──────────────────────────────────────────────────────
    "kt":   "kiểm tra",
    "ktra": "kiểm tra",
    "tt":   "thanh toán",
    "gd":   "giao dịch",
    "trc":  "trước",
    "sau":  "sau",
    "cl":   "còn lại",

    # ── Nhóm cảm ơn / lời chào ──────────────────────────────────────────────
    "tks":  "cảm ơn",
    "thx":  "cảm ơn",
    "ty":   "cảm ơn",
    "camon":"cảm ơn",
    "thank":"cảm ơn",
    "tygb": "cảm ơn bạn",

    # ── Nhóm từ viết tắt tiếng Anh thông dụng ──────────────────────────────
    "asap": "ngay lập tức",
    "fyi":  "để bạn biết",
    "idk":  "tôi không biết",
    "btw":  "nhân tiện",
    "imo":  "theo tôi nghĩ",
    "tbh":  "thật ra",
    "omg":  "ôi trời",
    "lol":  "haha",

    # ── Nhóm sách đặc thù ───────────────────────────────────────────────────
    "tg":   "tác giả",
    "tác giả": "tác giả",
    "bia":  "bìa sách",
    "nxb":  "nhà xuất bản",
    "hết hàng": "hết hàng",
    "còn hàng": "còn hàng",
}


# Tiền biên dịch regex cho hiệu suất cao. Mẫu \b (word boundary) giúp chỉ thay thế từ độc lập.
# Vì tiếng Việt có các ký tự UTF-8, \b đôi khi không bám sát hoàn toàn nếu không khéo, 
# nhưng đa số từ viết tắt dạng ASCII nên \b hoạt động tốt.
# Ta dùng (?:^|\s) và (?=\s|$) để kiểm soát ranh giới từ vựng an toàn hơn \b với utf-8.

_PATTERN = re.compile(
    r'(?<!\S)(' + '|'.join(re.escape(k) for k in ABBREVIATIONS.keys()) + r')(?!\S)',
    re.IGNORECASE
)

def expand_abbreviations(text: str) -> str:
    """
    Tìm và thay thế toàn bộ từ viết tắt trong câu bằng dạng đầy đủ.
    """
    def _replace(match):
        word = match.group(1).lower()
        full_word = ABBREVIATIONS.get(word, word)
        
        # Giữ nguyên case (viết hoa/thường) nếu input gốc viết hoa chữ cái đầu
        if match.group(1).istitle():
            return full_word.capitalize()
        elif match.group(1).isupper():
            return full_word.upper()
        return full_word

    return _PATTERN.sub(_replace, text)
