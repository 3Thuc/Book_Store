# -*- coding: utf-8 -*-
"""
dialog_utils.py – Utility functions dùng chung cho Dialog Managers.

Cung cấp:
  - resolve_genre_alias(): map alias thể loại sang chuẩn hóa
  - is_garbled_query(): phát hiện text lỗi encoding / garbled
  - is_ocr_message(): phát hiện tin nhắn từ OCR pipeline
"""
import re
import unicodedata


# ── Genre Alias Map ──────────────────────────────────────────────────────────
# Map từ keyword/alias → thể loại chuẩn trong DB
_GENRE_ALIAS: dict[str, str] = {
    # ── Kỹ năng sống / Self-help ──────────────────────────────────────────────
    # DB: 'Kỹ năng sống - Phát triển bản thân' (category_id=8)
    "ky nang song":        "Kỹ năng sống - Phát triển bản thân",
    "ky nang mem":         "Kỹ năng sống - Phát triển bản thân",
    "self help":           "Kỹ năng sống - Phát triển bản thân",
    "self-help":           "Kỹ năng sống - Phát triển bản thân",
    "phat trien ban than": "Kỹ năng sống - Phát triển bản thân",
    "nang cao ban than":   "Kỹ năng sống - Phát triển bản thân",
    # ── Tâm lý học ─────────────────────────────────────────────────────────────
    # DB: 'Tâm lý học' (category_id=17)
    "giam stress":         "Tâm lý học",
    "stress":              "Tâm lý học",
    "thien dinh":          "Tâm lý học",
    "mindfulness":         "Tâm lý học",
    "tam ly hoc":          "Tâm lý học",
    "tam ly":              "Tâm lý học",
    "tu duy":              "Tâm lý học",
    "cam xuc":             "Tâm lý học",
    # ── Kinh doanh / Kinh tế ──────────────────────────────────────────────────
    # DB: 'Kinh doanh - Kinh tế' (category_id=6) + 'Kinh tế - Quản lý' (42)
    "kinh doanh":          "Kinh doanh - Kinh tế",
    "kinh te":             "Kinh doanh - Kinh tế",
    "tai chinh":           "Kinh doanh - Kinh tế",
    "dau tu":              "Kinh doanh - Kinh tế",
    "khoi nghiep":         "Kinh doanh - Kinh tế",
    "startup":             "Kinh doanh - Kinh tế",
    "marketing":           "Kinh doanh - Kinh tế",
    "quan tri":            "Kinh doanh - Kinh tế",
    "lanh dao":            "Kinh doanh - Kinh tế",
    "quan ly":             "Kinh tế - Quản lý",
    "nhan su":             "Kinh tế - Quản lý",
    # ── Văn học / Fiction ──────────────────────────────────────────────────────
    # DB: 'Văn học - Tiểu thuyết'(21), 'Văn học - Thơ - Tản văn'(20), 'Văn Học'(39)
    "van hoc":             "Văn học - Tiểu thuyết",
    "lang man":            "Ngôn tình - Lãng mạn",
    "ngon tinh":           "Ngôn tình - Lãng mạn",
    "tieu thuyet":         "Tiểu Thuyết",
    "truyen ngan":         "Văn học - Tiểu thuyết",
    "truyen tranh":        "Truyện tranh",
    "manga":               "Truyện tranh",
    "comics":              "Truyện tranh",
    "kinh di":             "Kinh dị - Thriller",
    "thriller":            "Kinh dị - Thriller",
    "trinh tham":          "Trinh thám - Pháp y",
    "phap y":              "Trinh thám - Pháp y",
    "xuyen khong":         "Xuyên không - Trọng sinh",
    "trong sinh":          "Xuyên không - Trọng sinh",
    "tien hiep":           "Tiên hiệp - Huyền huyễn",
    "huyen huyen":         "Tiên hiệp - Huyền huyễn",
    "fantasy":             "Tiên hiệp - Huyền huyễn",
    "vong du":             "Võng du",
    "khoa hoc vien tuong": "Khoa học tự nhiên",
    "sci-fi":              "Khoa học tự nhiên",
    # ── Thiếu nhi / Kids ───────────────────────────────────────────────────────
    # DB: 'Thiếu nhi'(12), 'Sách thiếu nhi'(51)
    "thieu nhi":           "Thiếu nhi",
    "tre em":              "Thiếu nhi",
    "ehon":                "Thiếu nhi",
    "picture book":        "Thiếu nhi",
    "sach thieu nhi":      "Sách thiếu nhi",
    "nuoi day con":        "Nuôi dạy con",
    "day con":             "Nuôi dạy con",
    # ── Lịch sử ────────────────────────────────────────────────────────────────
    # DB: 'Lịch sử' (category_id=9)
    # Lịch sử – thêm plain alias "́lich su" để bắt button click
    "lich su":             "Lịch sử",
    "lich su sach":        "Lịch sử",
    "sach lich su":        "Lịch sử",
    "doc lich su":         "Lịch sử",
    "lich su viet nam":    "Lịch sử",
    "tu truyen":           "Hồi ký - Tự truyện",
    "hoi ky":              "Hồi ký - Tự truyện",
    "biography":           "Hồi ký - Tự truyện",
    "tieu su":             "Hồi ký - Tự truyện",
    # ── Công nghệ / Kỹ thuật ─────────────────────────────────────────────────
    # DB: 'Công nghệ - Kỹ thuật' (category_id=1)
    "cong nghe":           "Công nghệ - Kỹ thuật",
    "ky thuat":            "Công nghệ - Kỹ thuật",
    "lap trinh":           "Công nghệ - Kỹ thuật",
    "python":              "Công nghệ - Kỹ thuật",
    "javascript":          "Công nghệ - Kỹ thuật",
    "java":                "Công nghệ - Kỹ thuật",
    "c++":                 "Công nghệ - Kỹ thuật",
    "data science":        "Công nghệ - Kỹ thuật",
    "tri tue nhan tao":    "Công nghệ - Kỹ thuật",
    "machine learning":    "Công nghệ - Kỹ thuật",
    "deep learning":       "Công nghệ - Kỹ thuật",
    "neural network":      "Công nghệ - Kỹ thuật",
    "it":                  "Công nghệ - Kỹ thuật",
    # ── Khoa học tự nhiên ────────────────────────────────────────────────────
    # DB: 'Khoa học tự nhiên' (category_id=4)
    "khoa hoc tu nhien":   "Khoa học tự nhiên",
    "vat ly":              "Khoa học tự nhiên",
    "hoa hoc":             "Khoa học tự nhiên",
    "sinh hoc":            "Khoa học tự nhiên",
    "toan hoc":            "Khoa học tự nhiên",
    "vu tru":              "Khoa học tự nhiên",
    "thien van":           "Khoa học tự nhiên",
    "khoa hoc":            "Khoa học tự nhiên",
    # ── Khoa học xã hội ──────────────────────────────────────────────────────
    # DB: 'Khoa học xã hội' (category_id=5)
    "khoa hoc xa hoi":     "Khoa học xã hội",
    "xa hoi hoc":          "Khoa học xã hội",
    # ── Ngoại ngữ ──────────────────────────────────────────────────────────────
    # DB: 'Sách học ngoại ngữ'(46), 'Tiếng Anh'(47), 'Tiếng Hoa - Tiếng Trung'(48)
    "tieng anh":           "Tiếng Anh",
    "hoc tieng anh":       "Tiếng Anh",
    "english":             "Tiếng Anh",
    "toeic":               "Tiếng Anh",
    "ielts":               "Tiếng Anh",
    "vstep":               "Tiếng Anh",
    "tieng trung":         "Tiếng Hoa - Tiếng Trung",
    "tieng hoa":           "Tiếng Hoa - Tiếng Trung",
    "hoa ngu":             "Tiếng Hoa - Tiếng Trung",
    "tieng han":           "Sách học ngoại ngữ",
    "tieng nhat":          "Sách học ngoại ngữ",
    "ngoai ngu":           "Sách học ngoại ngữ",
    "hoc tieng":           "Sách học ngoại ngữ",
    # ── Giáo dục / Tham khảo ─────────────────────────────────────────────────
    # DB: 'Giáo dục - Giáo trình'(2), 'Giáo khoa - Tham khảo'(44)
    "giao duc":            "Giáo dục - Giáo trình",
    "giao trinh":          "Giáo dục - Giáo trình",
    "sach giao khoa":      "Giáo khoa - Tham khảo",
    "sach tham khao":      "Giáo khoa - Tham khảo",
    # ── Sức khỏe / Y tế ───────────────────────────────────────────────────────
    "suc khoe":            "Kỹ năng sống - Phát triển bản thân",
    "dinh duong":          "Kỹ năng sống - Phát triển bản thân",
    "nau an":              "Kỹ năng sống - Phát triển bản thân",
    "am thuc":             "Kỹ năng sống - Phát triển bản thân",
    # Y học – thêm alias riêng để bắt button click "Y học"
    "y hoc":               "Y học",
    "y te":                "Y học",
    "sach y hoc":          "Y học",
    "benh hoc":            "Y học",
    "cham soc suc khoe":   "Y học",
    # ── Triết học / Tôn giáo ─────────────────────────────────────────────────
    # DB: 'Triết học'(15), 'Tôn giáo - Tâm linh'(18)
    "triet hoc":           "Triết học",
    "philosophy":          "Triết học",
    "ton giao":            "Tôn giáo - Tâm linh",
    "phat giao":           "Tôn giáo - Tâm linh",
    "tam linh":            "Tôn giáo - Tâm linh",
    # ── Pháp luật ─────────────────────────────────────────────────────────────
    # DB: 'Pháp luật - Chính trị' (category_id=11)
    "phap luat":           "Pháp luật - Chính trị",
    "chinh tri":           "Pháp luật - Chính trị",
    # ── Truyện tranh / Comics mở rộng (Fix B) ────────────────────────────────
    # DB: 'Truyện tranh' (category_id=14)
    "manhwa":              "Truyện tranh",
    "manhua":              "Truyện tranh",
    "comic":               "Truyện tranh",
    "webtoon":             "Truyện tranh",
    "truyen tranh mau":    "Truyện tranh",
    # ── Light novel / Web novel (Fix B) ──────────────────────────────────────
    "light novel":         "Tiểu Thuyết",
    "lightnovel":          "Tiểu Thuyết",
    "ln":                  "Tiểu Thuyết",
    "web novel":           "Tiểu Thuyết",
    "webnovel":            "Tiểu Thuyết",
    "novel":               "Văn học - Tiểu thuyết",
    # ── Thể loại bổ sung phổ biến (Fix B) ────────────────────────────────────
    "nau an nuong":        "Kỹ năng sống - Phát triển bản thân",
    "mon an":              "Kỹ năng sống - Phát triển bản thân",
    "am thuc nuoc ngoai":  "Kỹ năng sống - Phát triển bản thân",
    "the thao":            "Khoa học tự nhiên",
    "the duc":             "Kỹ năng sống - Phát triển bản thân",
    "yoga":                "Kỹ năng sống - Phát triển bản thân",
    "du lich":             "Kỹ năng sống - Phát triển bản thân",
    "travel":              "Kỹ năng sống - Phát triển bản thân",
    "phong trao nu quyen": "Khoa học xã hội",
    "nu quyen":            "Khoa học xã hội",
    "feminism":            "Khoa học xã hội",
    "lam dep":             "Kỹ năng sống - Phát triển bản thân",
    "doi song":            "Kỹ năng sống - Phát triển bản thân",
    "kinh di tam ly":      "Kinh dị - Thriller",
    "horror":              "Kinh dị - Thriller",
    "ghost story":         "Kinh dị - Thriller",
    "truyen ma":           "Kinh dị - Thriller",
}


def _normalize(text: str) -> str:
    """Chuẩn hóa text: đ→d, NFD strip diacritics, lowercase."""
    text = text.replace("đ", "d").replace("Đ", "D")
    nfd = unicodedata.normalize("NFD", text)
    ascii_text = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return ascii_text.lower().strip()


# Các phrase cần KHÔNG bị nhận dạng thành genre (false positive guards)
_GENRE_NEGATIVE_PATTERNS = [
    r"lich su mua hang",
    r"lich su don hang",
    r"lich su giao dich",
    r"lich su (cua|toi|minh|ban)",
    r"xem lich su",
    r"upload.*\.(jpg|png|jpeg|gif|webp)",
    r"\[upload:",
]

# Compile negative patterns
_GENRE_NEG_RE = [re.compile(p, re.IGNORECASE) for p in _GENRE_NEGATIVE_PATTERNS]


def resolve_genre_alias(message: str) -> str | None:
    """
    Tìm alias thể loại trong message và trả về tên thể loại chuẩn.

    Ví dụ:
        "Gợi ý sách giảm stress cho dân văn phòng" → "Tâm lý học"
        "Tìm sách học tiếng Anh cho người mới" → "Ngoại ngữ"
        "Muốn đọc kinh dị" → "Kinh dị"

    Returns:
        str: Tên thể loại chuẩn nếu tìm thấy alias
        None: Nếu không khớp
    """
    norm = _normalize(message)

    # Guard 1: OCR filename hoặc Upload message → không phân loại genre
    if (
        "[upload:" in norm
        or ".jpg" in norm or ".jpeg" in norm
        or ".png" in norm or ".webp" in norm
    ):
        return None

    # Guard 2: Các cụm từ dễ bị nhận nhầm (false positive)
    for neg_re in _GENRE_NEG_RE:
        if neg_re.search(norm):
            return None

    # Guard 3: Alias quá ngắn (≤3 ký tự) phải match word boundary
    for alias, genre in sorted(_GENRE_ALIAS.items(), key=lambda x: -len(x[0])):
        if len(alias) <= 3:
            # Word boundary check: không match trong từ ghép
            if re.search(r'(?<![a-z])' + re.escape(alias) + r'(?![a-z])', norm):
                return genre
        else:
            if alias in norm:
                return genre
    return None


def is_garbled_query(text: str) -> bool:
    """
    Phát hiện text bị lỗi encoding hoặc garbled (ký tự lạ chiếm > 30%).

    Ví dụ:
        "T¼m s§ch" → True (garbled)
        "Tìm sách" → False (normal)
    """
    if not text:
        return False
    # Đếm ký tự printable hợp lệ (ASCII + Vietnamese)
    valid_chars = sum(
        1 for c in text
        if (ord(c) < 128 or  # ASCII
            '\u00c0' <= c <= '\u1ef9' or  # Latin Extended (Vietnamese)
            c in ' \t\n\r')
    )
    ratio = valid_chars / max(len(text), 1)
    return ratio < 0.7  # Hơn 30% ký tự lạ → garbled


def is_ocr_message(message: str) -> bool:
    """
    Phát hiện message đến từ OCR pipeline.

    OCR messages thường có tiền tố đặc biệt hoặc chứa metadata ảnh.
    """
    OCR_PREFIXES = [
        "[Ảnh OCR:",
        "[anh ocr:",
        "📷 Tìm sách từ ảnh:",
        "📷 Tôi vừa gửi ảnh sách",
        "📷 [",
        "[nguoi dung goi anh",
        "[hinh anh ocr",
    ]
    msg_lower = message.lower()
    return any(prefix.lower() in msg_lower for prefix in OCR_PREFIXES)


def extract_ocr_book_name(message: str) -> str | None:
    """
    Trích xuất tên sách từ message OCR.

    Ví dụ:
        '📷 Tìm sách từ ảnh: "Đắc Nhân Tâm"' → "Đắc Nhân Tâm"
        '[Ảnh OCR: Nhà Giả Kim]' → "Nhà Giả Kim"
        '📷 [Nhà Giả Kim] Cuốn này còn không' → "Nhà Giả Kim"

    Returns:
        str: Tên sách nếu tìm thấy
        None: Nếu không trích xuất được
    """
    # Pattern: 📷 [Tên sách] ...
    m = re.search(r'📷 \[([^\]]+)\]', message)
    if m:
        return m.group(1).strip()

    # Pattern: 📷 Tìm sách từ ảnh: "..."
    m = re.search(r'Tìm sách từ ảnh[:\s]+"?([^"]+)"?', message)
    if m:
        return m.group(1).strip().strip('"')

    # Pattern: [Ảnh OCR: ...]
    m = re.search(r'\[(?:Ảnh OCR|anh ocr)[:\s]+([^\]]+)\]', message, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    # Pattern: tên sách trong dấu ngoặc kép
    m = re.search(r'"([^"]{5,})"', message)
    if m:
        return m.group(1).strip()

    return None
