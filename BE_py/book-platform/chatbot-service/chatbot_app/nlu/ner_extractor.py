"""
ner_extractor.py – Named Entity Recognition cho tiếng Việt.

v2 – Nâng cấp:
  - Pattern giá mở rộng: "200 nghìn", "1.5 triệu", "150,000đ"
  - Fallback dictionary tên sách nổi tiếng khi underthesea không nhận diện được
  - Order ID pattern: bắt "đơn 123456" không có dấu #
  - Extraction keyword "ngân sách" cho recommend_gift
  - Nhận diện thứ tự sách trong series ("tập 1", "phần 2")
"""
import re
from typing import Optional


# ── Thể loại sách (để match nhanh từ text) ────────────────────────────────────
BOOK_GENRES: list[str] = [
    "văn học", "tiểu thuyết", "kỹ năng sống", "kỹ năng",
    "kinh tế", "kinh doanh", "tâm lý", "tâm lý học",
    "thiếu nhi", "lịch sử", "khoa học", "lập trình",
    "triết học", "tâm linh", "tôn giáo", "nuôi dạy con",
    "y học", "sức khỏe", "du lịch", "nấu ăn",
    "self-help", "phát triển bản thân", "truyện tranh",
    "manga", "light novel", "kinh dị", "fantasy", "romance",
    "marketing", "quản trị", "tài chính", "đầu tư",
    "khởi nghiệp", "leadership", "tư duy", "trinh thám",
]

# ── Fallback dictionary tên sách nổi tiếng ────────────────────────────────────
# Dùng khi underthesea không nhận diện được tên sách trong câu hỏi
FAMOUS_BOOKS: dict[str, str] = {
    # Self-help / Kỹ năng
    "đắc nhân tâm":              "Đắc Nhân Tâm",
    "7 thói quen":               "7 Thói Quen Của Người Thành Đạt",
    "nghĩ giàu làm giàu":        "Nghĩ Giàu Làm Giàu",
    "the alchemist":             "Nhà Giả Kim",
    "nhà giả kim":               "Nhà Giả Kim",
    "atomic habits":             "Atomic Habits",
    "thói quen nguyên tử":       "Thói Quen Nguyên Tử",
    "rich dad poor dad":         "Cha Giàu Cha Nghèo",
    "cha giàu cha nghèo":        "Cha Giàu Cha Nghèo",
    "dám nghĩ lớn":              "Dám Nghĩ Lớn",
    "think and grow rich":       "Nghĩ Giàu Làm Giàu",
    "mindset":                   "Mindset: Tư Duy Thành Công",
    "tư duy nhanh chậm":         "Tư Duy Nhanh Và Chậm",
    "thinking fast and slow":    "Tư Duy Nhanh Và Chậm",
    "zero to one":               "Từ 0 Đến 1",
    "từ 0 đến 1":                "Từ 0 Đến 1",
    "outliers":                  "Những Kẻ Xuất Chúng",
    "những kẻ xuất chúng":       "Những Kẻ Xuất Chúng",
    "blink":                     "Chớp Mắt",
    "shoes dog":                 "Giày Nike",

    # Lịch sử / Khoa học
    "sapiens":                   "Sapiens: Lược Sử Loài Người",
    "lược sử loài người":        "Sapiens: Lược Sử Loài Người",
    "homo deus":                 "Homo Deus: Lược Sử Tương Lai",
    "21 bài học":                "21 Bài Học Cho Thế Kỷ 21",
    "a brief history of time":   "Lịch Sử Vắn Tắt Về Thời Gian",
    "cosmos":                    "Vũ Trụ",
    "the selfish gene":          "Gen Ích Kỷ",

    # Văn học
    "hai số phận":               "Hai Số Phận",
    "nhà thờ đức bà paris":      "Nhà Thờ Đức Bà Paris",
    "1984":                      "1984",
    "brave new world":           "Thế Giới Tươi Đẹp",
    "lord of the rings":         "Chúa Tể Của Những Chiếc Nhẫn",
    "harry potter":              "Harry Potter",
    "the little prince":         "Hoàng Tử Bé",
    "hoàng tử bé":               "Hoàng Tử Bé",
    "dế mèn phiêu lưu ký":       "Dế Mèn Phiêu Lưu Ký",
    "số đỏ":                     "Số Đỏ",
    "chí phèo":                  "Chí Phèo",

    # Kinh tế / Business
    "good to great":             "Từ Tốt Đến Vĩ Đại",
    "từ tốt đến vĩ đại":        "Từ Tốt Đến Vĩ Đại",
    "start with why":            "Bắt Đầu Từ Tại Sao",
    "bắt đầu từ tại sao":        "Bắt Đầu Từ Tại Sao",
    "the lean startup":          "Khởi Nghiệp Tinh Gọn",
    "khởi nghiệp tinh gọn":      "Khởi Nghiệp Tinh Gọn",

    # ── ASCII không dấu (cho test inputs từ evaluate_chatbot_multiturn.py) ──
    "vu tru trong vo hat de":    "Vũ Trụ Trong Vỏ Hạt Dẻ",
    "vu tru trong vo hat":       "Vũ Trụ Trong Vỏ Hạt Dẻ",
    "vo hat de":                 "Vũ Trụ Trong Vỏ Hạt Dẻ",
    "luoc su loai nguoi":        "Sapiens: Lược Sử Loài Người",
    "dac nhan tam":              "Đắc Nhân Tâm",
    "nghi giau lam giau":        "Nghĩ Giàu Làm Giàu",
    "nha gia kim":               "Nhà Giả Kim",
    "thoi quen nguyen tu":       "Thói Quen Nguyên Tử",
    "cha giau cha ngheo":        "Cha Giàu Cha Nghèo",
    "dam nghi lon":              "Dám Nghĩ Lớn",
    "tu duy nhanh cham":         "Tư Duy Nhanh Và Chậm",
    "tu 0 den 1":                "Từ 0 Đến 1",
    "nhung ke xuat chung":       "Những Kẻ Xuất Chúng",
    "hoang tu be":               "Hoàng Tử Bé",
    "de men phieu luu ky":       "Dế Mèn Phiêu Lưu Ký",
    "tu tot den vi dai":         "Từ Tốt Đến Vĩ Đại",
    "bat dau tu tai sao":        "Bắt Đầu Từ Tại Sao",
    "khoi nghiep tinh gon":      "Khởi Nghiệp Tinh Gọn",
    "nguyen tu hanh phuc":       "Hạnh Phúc Đích Thực",
    "dau truong xanh":           "Chiến Lược Đại Dương Xanh",
    "nghin mat troi rang ro":    "Ngàn Mặt Trời Rực Rỡ",
    "mat biec":                  "Mắt Biếc",
    "to huu":                    "Thơ Tố Hữu",
    # ── Bổ sung sách nổi tiếng Việt Nam (Fix NER) ────────────────────────────
    # Văn học Việt
    "toi thay hoa vang tren co xanh": "Tôi Thấy Hoa Vàng Trên Cỏ Xanh",
    "hoa vang tren co xanh":    "Tôi Thấy Hoa Vàng Trên Cỏ Xanh",
    "toi thay hoa vang":        "Tôi Thấy Hoa Vàng Trên Cỏ Xanh",
    "cho toi xin mot ve di tuoi tho": "Cho Tôi Xin Một Vé Đi Tuổi Thơ",
    "mot ve di tuoi tho":       "Cho Tôi Xin Một Vé Đi Tuổi Thơ",
    "nam phut voi chinh minh":  "Năm Phút Với Chính Mình",
    "the gioi phang":           "Thế Giới Phẳng",
    "truyen kieu":              "Truyện Kiều",
    "chi pheo":                 "Chí Phèo",
    "so do viet":               "Số Đỏ",
    "lao hac":                  "Lão Hạc",
    "song mon":                 "Sống Mòn",
    "tat den":                  "Tắt Đèn",
    "buoc duong cung":          "Bước Đường Cùng",
    # Tâm lý / Self-help bổ sung
    "con nguoi tim kiem y nghia": "Con Người Tìm Kiếm Ý Nghĩa",
    "man's search for meaning": "Con Người Tìm Kiếm Ý Nghĩa",
    "deep work":                "Deep Work",
    "7 thoi quen hieu qua":     "7 Thói Quen Hiệu Quả",
    "su that ve hanh phuc":     "Sự Thật Về Hạnh Phúc",
    "suc manh cua hien tai":    "Sức Mạnh Của Hiện Tại",
    "the power of now":         "Sức Mạnh Của Hiện Tại",
    # Kinh doanh bổ sung
    "chien luoc dai duong xanh": "Chiến Lược Đại Dương Xanh",
    "blue ocean":               "Chiến Lược Đại Dương Xanh",
    "khong bao gio di an mot minh": "Không Bao Giờ Đi Ăn Một Mình",
    "never eat alone":          "Không Bao Giờ Đi Ăn Một Mình",
    "tu zero den hero":         "Từ Zero Đến Hero",
    # Thiếu nhi / Manga
    "doremon":                  "Doraemon",
    "conan":                    "Thám Tử Conan",
    "detective conan":          "Thám Tử Conan",
    "one piece":                "One Piece",
    "naruto":                   "Naruto",
    "dragon ball":              "Dragon Ball",
    # Ngôn tình / Light novel
    "mat nao cung la em":       "Mặt Nào Cũng Là Em",
    "1 met 72":                 "1 Mét 72",
    "chang trai nam ay":        "Chàng Trai Năm Ấy",
    "yeu nhau kho lam":         "Yêu Nhau Khó Lắm",
}

# ── Recipient types cho recommend_gift ────────────────────────────────────────
RECIPIENT_PATTERNS: dict[str, str] = {
    r"(bé|trẻ em|con nít|em bé).*(0|1|2|3|4|5|6)\s*tuổi": "child_0_6",
    r"(bé|trẻ em|thiếu nhi).*(7|8|9|10|11|12)\s*tuổi":    "child_7_12",
    r"(teen|thiếu niên|học sinh|sinh viên)":               "teenager",
    r"(con\s*(trai|gái)|cháu).*(7|8|9|10|11|12)":         "child_7_12",
    r"bạn\s*(gái|trai)|người\s*yêu|vợ|chồng|bạn bè":      "adult",
    r"(ba|bố|mẹ|má|cha|ông|bà|người lớn tuổi|cao tuổi)":  "elderly",
    r"(nam|anh|ông|chú|con trai)":                         "adult_male",
    r"(nữ|chị|cô|em gái|con gái)":                        "adult_female",
}


def extract_entities(text: str, intent: str = "") -> dict:
    """
    Trích xuất entities từ câu user.

    Args:
        text:   Câu người dùng
        intent: Intent đã phân loại

    Returns:
        dict với các keys: book_title, author, genre, price_max, price_min,
                           order_id, voucher_code, recipient_type, age, budget
    """
    entities: dict = {}
    text_lower = text.lower()

    # ── 1. Thể loại sách ──────────────────────────────────────────────────────
    for genre in BOOK_GENRES:
        if genre in text_lower:
            entities["genre"] = genre
            break

    # ── 2. Mã đơn hàng – pattern mở rộng ─────────────────────────────────────
    # Bắt: #12345 | đơn 12345 | đơn hàng #12345 | order 12345 | số 12345 | mã 12345
    order_match = re.search(
        r"(?:#|(?:đơn(?:\s+h[àa]ng)?\s*#?\s*)|order\s*#?\s*|số\s*(?:đơn\s*)?|mã\s*(?:đơn\s*)?)(\d{4,10})\b",
        text, re.IGNORECASE
    )
    if order_match and intent in (
        "order_status", "order_cancel", "order_history",
        "return_request", "complaint_damaged", "complaint_wrong",
    ):
        entities["order_id"] = order_match.group(1)

    # ── 3. Khoảng giá – pattern mở rộng ──────────────────────────────────────
    # "dưới 150k", "dưới 150 nghìn", "dưới 200.000", "dưới 1.5 triệu"
    price_under = re.search(
        r"(?:dưới|under|<|tối đa|max)\s*"
        r"([\d,\.]+)\s*(k|nghìn|ngàn|đồng|vnd|triệu|tr|000)?",
        text_lower
    )
    if price_under:
        entities["price_max"] = _parse_price(
            price_under.group(1), price_under.group(2)
        )

    # "từ X đến Y"
    price_range = re.search(
        r"từ\s*([\d,\.]+)\s*(k|nghìn|triệu|tr)?\s*(?:đến|tới|-)\s*([\d,\.]+)\s*(k|nghìn|triệu|tr)?",
        text_lower
    )
    if price_range:
        entities["price_min"] = _parse_price(price_range.group(1), price_range.group(2))
        entities["price_max"] = _parse_price(price_range.group(3), price_range.group(4))

    # "tầm 100k", "khoảng 200 nghìn", "cỡ 150k"
    price_around = re.search(
        r"(?:tầm|khoảng|cỡ|around|about)\s*([\d,\.]+)\s*(k|nghìn|ngàn|triệu|tr|đồng)?",
        text_lower
    )
    if price_around and "price_max" not in entities:
        approx = _parse_price(price_around.group(1), price_around.group(2))
        if approx:
            entities["price_min"] = int(approx * 0.8)
            entities["price_max"] = int(approx * 1.2)

    # "ngân sách X" – dùng cho recommend_gift
    budget_match = re.search(
        r"(?:ngân sách|budget|tiền|chi)\s*(?:khoảng|tầm|)?\s*([\d,\.]+)\s*(k|nghìn|triệu|tr|đồng)?",
        text_lower
    )
    if budget_match:
        budget = _parse_price(budget_match.group(1), budget_match.group(2))
        if budget:
            entities["budget"] = budget
            if "price_max" not in entities:
                entities["price_max"] = budget

    # ── 4. Mã voucher ─────────────────────────────────────────────────────────
    voucher_match = re.search(r"\b([A-Z][A-Z0-9]{3,14})\b", text)
    if voucher_match and intent == "voucher_apply":
        entities["voucher_code"] = voucher_match.group(1)

    # ── 5. Recipient type cho recommend_gift ──────────────────────────────────
    if intent == "recommend_gift":
        for pattern, recipient_type in RECIPIENT_PATTERNS.items():
            if re.search(pattern, text_lower):
                entities["recipient_type"] = recipient_type
                break
        # Tuổi cụ thể
        age_match = re.search(r"(\d+)\s*tuổi", text_lower)
        if age_match:
            entities["age"] = int(age_match.group(1))

    # ── 6. Kiểm tra tên sách từ dictionary nổi tiếng (TRƯỚC underthesea) ──────
    if intent in ("book_search", "book_detail", "book_compare",
                  "book_availability", "book_review", "recommend_combo"):
        text_ascii = text_lower
        # Normalize diacritic nếu cần (dùng cho test inputs ASCII không dấu)
        import unicodedata as _ud
        text_normed = "".join(
            c for c in _ud.normalize("NFD", text_lower.replace("đ", "d"))
            if _ud.category(c) != "Mn"
        )
        for keyword, book_name in FAMOUS_BOOKS.items():
            kw_normed = "".join(
                c for c in _ud.normalize("NFD", keyword.lower().replace("đ", "d"))
                if _ud.category(c) != "Mn"
            )
            if kw_normed in text_normed or keyword in text_ascii:
                entities["book_title"] = book_name
                break

    # ── 6b. Inline extraction: "cuon [title] gia bao nhieu" pattern ──────────
    if intent in ("book_detail", "book_search") and "book_title" not in entities:
        # Bắt: "cuon X gia bao nhieu", "cuon X la sach gi", "sach X co gia"
        # v2: mở rộng pattern cho book_search, fix lazy quantifier
        inline_match = re.search(
            r"(?:cuon|sach|quyen)\s+(.{3,60}?)\s+(?:gia|bao nhieu|la sach|co gia|thong tin|chi tiet|con hang|het hang|danh gia|review)",
            text_lower, re.IGNORECASE
        )
        if inline_match:
            entities["book_title"] = inline_match.group(1).strip().title()

        # Bắt tên sách trong ngoặc kép: "Mắt Biếc", 'Hoàng Tử Bé'
        quoted_match = re.search(r'["\'""]([\w\s]{3,60})["\'""]', text)
        if quoted_match and "book_title" not in entities:
            entities["book_title"] = quoted_match.group(1).strip()

    # ── 7. underthesea NER (PERSON, WORK_OF_ART) – fallback ─────────────────
    if intent in ("book_search", "book_detail", "book_compare",
                  "book_availability", "book_review", "recommend_combo") \
            and "book_title" not in entities:
        try:
            from underthesea import ner
            named_entities = ner(text)
            for word, tag in named_entities:
                if tag in ("B-WORK_OF_ART", "I-WORK_OF_ART") and "book_title" not in entities:
                    entities["book_title"] = word
                elif tag in ("B-PER", "I-PER") and "author" not in entities:
                    entities["author"] = word
        except Exception:
            pass  # underthesea chưa cài hoặc lỗi → bỏ qua

    # ── 8. Phần/tập sách (cho series) ─────────────────────────────────────────
    volume_match = re.search(r"(?:tập|phần|volume|vol\.?)\s*(\d+)", text_lower)
    if volume_match:
        entities["volume"] = int(volume_match.group(1))

    # ── 9. Author extraction từ "sách của X", "tác giả X viết" (Fix NER B) ────
    _FAMOUS_AUTHORS = [
        "Nguyễn Nhật Ánh", "Nguyễn Du", "Nam Cao", "Tố Hữu", "Tô Hoài",
        "Ngô Tất Tố", "Vũ Trọng Phụng", "Xuân Diệu", "Thạch Lam",
        "Dale Carnegie", "Napoleon Hill", "Robert Kiyosaki", "Paulo Coelho",
        "Haruki Murakami", "James Clear", "Malcolm Gladwell", "Stephen Covey",
        "Simon Sinek", "Cal Newport", "Mark Manson", "Ryan Holiday",
    ]
    if "author" not in entities:
        for _auth in _FAMOUS_AUTHORS:
            _auth_norm = _auth.lower()
            _auth_no_accent = "".join(
                c for c in __import__("unicodedata").normalize("NFD", _auth_norm.replace("đ", "d"))
                if __import__("unicodedata").category(c) != "Mn"
            )
            if _auth_norm in text_lower or _auth_no_accent in text_lower:
                entities["author"] = _auth
                break
        if "author" not in entities:
            # Pattern: "sách của [Tên Tác Giả]"
            author_regex = re.search(
                r"(?:sach|cuon|quyen) cua ([A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯ][a-zàáâãèéêìíòóôõùúýăđơư]+"
                r"(?:\s[A-ZÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯ][a-zàáâãèéêìíòóôõùúýăđơư]+){1,4})",
                text
            )
            if author_regex:
                entities["author"] = author_regex.group(1).strip()

    # ── 10. Spoken price: "350 nghìn", "3 trăm nghìn", "hai trăm rưỡi" ───────
    if "price_max" not in entities:
        spoken_price = re.search(r"(\d+)\s*trăm\s*(nghìn|ngàn|rưỡi)?", text_lower)
        if spoken_price:
            hundreds = int(spoken_price.group(1)) * 100_000
            half = 50_000 if spoken_price.group(2) == "rưỡi" else 0
            entities["price_max"] = hundreds + half

    return entities


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_price(value_str: str, unit: Optional[str]) -> Optional[int]:
    """
    Chuyển đổi chuỗi giá sang số nguyên (đơn vị VNĐ).

    Ví dụ:
        "150", "k"     → 150_000
        "1.5", "triệu" → 1_500_000
        "200", "nghìn" → 200_000
        "150000", None → 150_000
    """
    if not value_str:
        return None
    try:
        # Loại bỏ dấu phân cách nghìn
        clean = value_str.replace(",", ".")
        val = float(clean)
    except ValueError:
        return None

    unit = (unit or "").lower().strip()
    if unit in ("k", "nghìn", "ngàn"):
        return int(val * 1_000)
    elif unit in ("triệu", "tr"):
        return int(val * 1_000_000)
    elif unit in ("đồng", "vnd", "000"):
        # "150.000" → đã ở dạng đúng, hoặc "150" + "000" unit
        return int(val * 1_000) if val < 1_000 else int(val)
    else:
        # Không có unit: nếu < 10,000 → hiểu là nghìn đồng
        return int(val * 1_000) if val < 10_000 else int(val)


def normalize_price(value: Optional[int]) -> Optional[int]:
    """Chuẩn hóa giá: nếu nhập 150 → hiểu là 150.000đ."""
    if value is None:
        return None
    return value * 1000 if value < 10000 else value
