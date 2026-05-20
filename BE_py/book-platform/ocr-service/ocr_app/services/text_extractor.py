"""
text_extractor.py – NLP Text Extraction Module (v2 - Optimized)
================================================================
Cải tiến v2:
  1. _is_garbled_line: ngưỡng alpha ratio 0.55 (tăng từ 0.40) + real-word check
  2. extract_title: multi-pass (heuristic vị trí + BBox size + Capitalization pattern)
  3. _KNOWN_PUBLISHERS: mở rộng lên 50+ NXB Việt Nam
  4. extract_authors: validate tên người Việt (tiếng Việt ≥2 token)
  5. Compound confidence scoring (ocr_conf + title + entities + isbn bonus)
  6. clean_ocr_text: noise patterns bổ sung

Pipeline:
  raw_text → clean → isbn → title (multi-pass) → authors → publisher → score
"""
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

# ── NER (lazy load) ───────────────────────────────────────────────────────────
_ner_available = None


def _try_ner(text: str) -> list:
    """Thử chạy underthesea NER. Trả về list entities hoặc []."""
    global _ner_available
    if _ner_available is False:
        return []
    try:
        from underthesea import ner
        result = ner(text)
        _ner_available = True
        return result
    except Exception as e:
        logger.warning("underthesea NER không khả dụng: %s", e)
        _ner_available = False
        return []


def _extract_entities(ner_result: list, entity_type: str) -> List[str]:
    """Gom các token BIO cùng loại thành entity hoàn chỉnh."""
    entities = []
    current = []
    for token, tag in ner_result:
        if tag == f"B-{entity_type}":
            if current:
                entities.append(" ".join(current))
            current = [token]
        elif tag == f"I-{entity_type}" and current:
            current.append(token)
        else:
            if current:
                entities.append(" ".join(current))
                current = []
    if current:
        entities.append(" ".join(current))
    return entities


# ══════════════════════════════════════════════════════════════════════════════
# DATACLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ExtractedBookInfo:
    """Thông tin sách đã trích xuất từ OCR text."""
    title:            Optional[str]  = None
    authors:          List[str]      = field(default_factory=list)
    isbn:             Optional[str]  = None
    publisher:        Optional[str]  = None
    confidence:       float          = 0.0
    search_query:     str            = ""
    full_text_query:  str            = ""   # v4: full OCR text cho fallback search


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 1: Clean text
# ══════════════════════════════════════════════════════════════════════════════

_OCR_NOISE_PATTERNS = [
    (r"[|]{2,}", ""),
    (r"[_]{3,}", ""),
    (r"\.{3,}", "…"),
    (r"\s{2,}", " "),
    (r"^\s*[-=*#~]{2,}\s*$", "", re.MULTILINE),
    (r"[»«›‹·•~^`]{2,}", ""),
    # v2: thêm pattern xóa noise phổ biến của EasyOCR
    (r"[^\w\s\-.,!?():;/\u00C0-\u024F\u1E00-\u1EFF\u0300-\u036F]", ""),
    # v2: xóa khoảng trắng TRƯỚC punctuation
    (r"\s+([,.;:!?])", r"\1"),
]

# v3: giảm alpha ratio từ 0.55 → 0.40 để hỗ trợ tiêu đề có số/ký tự
# Ví dụ: "0852 Gửi Tống" có alpha ratio ≈ 0.54 (borderline khi tách riêng)
# Đặc biệt khi EasyOCR đọc "0852" và "Gửi Tống" thành 2 bbox riêng
_MIN_ALPHA_RATIO = 0.40
# v2: yêu cầu ít nhất 1 "real word" (≥3 ký tự alpha liên tiếp)
_MIN_REAL_WORD_LEN = 3


def _is_garbled_line(line: str) -> bool:
    """
    Kiểm tra xem dòng text có phải gibberish không (v2 cải thiện).

    Dòng bị coi là garbled nếu HOẶC:
    1. Tỉ lệ ký tự alpha/số < 55% (tăng từ 40%)
    2. Không có bất kỳ "real word" nào (≥3 ký tự alpha liên tiếp)
    3. Có hơn 3 ký tự đặc biệt kiểu symbol (=, ~, », +, |...)

    Ví dụ garbled:  '~» = ., -á =\"'  (pass=40% nhưng fail=55% và không có real word)
    Ví dụ OK:       'Đắc Nhân Tâm'  'Dale Carnegie'
    """
    stripped = line.strip()
    if not stripped:
        return True

    # Test 1: alpha ratio
    alpha_count = sum(1 for c in stripped if c.isalpha())
    if alpha_count / len(stripped) < _MIN_ALPHA_RATIO:
        return True

    # Test 2: phải có ít nhất 1 real word (liên tiếp ≥3 ký tự alpha)
    real_words = re.findall(r'[A-Za-zÀ-ɏḀ-ỿ\u0300-\u036f]{3,}', stripped, re.UNICODE)
    if not real_words:
        return True

    # Test 3: không quá nhiều ký tự symbol lạ
    symbol_count = sum(1 for c in stripped if c in '=<>~`^*+|\\{}[]@#$%&»«')
    if symbol_count > 3:
        return True

    return False


def clean_ocr_text(raw_text: str) -> str:
    """Làm sạch text thô từ OCR (v2 thêm patterns)."""
    text = raw_text.strip()
    for pattern_args in _OCR_NOISE_PATTERNS:
        if len(pattern_args) == 3:
            pattern, repl, flags = pattern_args
            text = re.sub(pattern, repl, text, flags=flags)
        else:
            pattern, repl = pattern_args
            text = re.sub(pattern, repl, text)
    return text.strip()


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 2: ISBN
# ══════════════════════════════════════════════════════════════════════════════

_ISBN_PATTERN = re.compile(
    # ISBN-13: starts with 978/979, with optional hyphens/spaces
    r"(?:ISBN[-:\s]?)?"
    r"((?:978|979)"
    r"[-\s]?\d{1,5}"
    r"[-\s]?\d{1,7}"
    r"[-\s]?\d{1,7}"
    r"[-\s]?\d)"
    # ISBN-10: 9 digits + digit/X, with optional hyphens between groups
    r"|(\d{1,5}[-\s]?\d{1,7}[-\s]?\d{1,7}[-\s]?[\dX])",
    re.IGNORECASE
)


def extract_isbn(text: str) -> Optional[str]:
    """Tìm ISBN-13 hoặc ISBN-10 trong text."""
    match = _ISBN_PATTERN.search(text)
    if not match:
        return None
    raw = match.group(1) or match.group(2) or ""
    clean = re.sub(r"[^\dX]", "", raw.upper())
    if len(clean) in (13, 10):
        return clean
    return None


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 3: Tiêu đề (v2 – Multi-pass)
# ══════════════════════════════════════════════════════════════════════════════

_NON_TITLE_KEYWORDS = {
    "isbn", "nhà xuất bản", "nxb", "tái bản", "lần xuất bản",
    "giá bìa", "giá:", "tác giả:", "author", "publisher",
    "copyright", "©", "printed", "first edition", "all rights",
    "www.", "http", ".com", ".vn",
    # v2: bổ sung
    "email:", "tel:", "fax:", "hotline", "biên soạn",
    "dịch:", "hiệu đính", "chịu trách nhiệm",
    # v5: Banner / label không phải title (hay xuất hiện trên bìa sách)
    "bestseller", "international bestseller", "#1 bestseller",
    "national bestseller", "worldwide bestseller", "new york times",
    "\"#1\"", "instant research", "research institute",
    "sổ tay", "sổ tay",   # subtitle marker
    "bên trong", "phần", "chương", "edition", "volume", "vol.",
    "phát hành", "xuất bản lần",
}

# v2: Pattern cho TitleCase – nhiều từ viết Hoa liên tiếp
_TITLECASE_PATTERN = re.compile(
    r'(?:[A-ZĐẮẰẲẴẶẮẤẦẨẪẬÉÈẺẼẸÊẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÚÙỦŨỤƯỨỪỬỮỰÝỲỶỸỴ]'
    r'[a-zđắằẳẵặắấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵ]+'
    r'\s?){2,}',
    re.UNICODE
)
# v3: Pattern phát hiện tên tác giả ở đầu bìa (ưu tiên bỏ qua)
# VD: "George S. Clarson", "Dale Carnegie", "J.K. Rowling"
_AUTHOR_LINE_PATTERN = re.compile(
    r'^[A-ZĐẮẰẲẴẶẮẤẦẨẪẬ][a-zđắằẳẵặắấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵ]+'
    r'(?:\s+[A-Z][a-z.]*)+'  # Họ tên với viết hoa
    r'$',
    re.UNICODE,
)

# Pattern: tên nước ngoài viết HOA HẾT (ví dụ "GEORGE S CLARSON")
_ALLCAPS_NAME_PATTERN = re.compile(
    r'^[A-Z]+(?:\s+[A-Z]+){0,3}$'
)


def _looks_like_author_name(line: str) -> bool:
    """
    Heuristic: kiểm tra dòng có giống tên tác giả không.
    Dùng để bỏ qua dòng đầu bìa thường là tên tác giả.
    """
    words = line.strip().split()
    # Quá ngắn: 1-2 từ, toàn chữ hoa hoặc TitleCase ngắn
    if len(words) <= 3 and len(line.strip()) <= 25:
        # Ví dụ: "GEORGE S", "Dale Carnegie", "J.K Rowling"
        if _ALLCAPS_NAME_PATTERN.match(line.strip()):
            return True
        if _AUTHOR_LINE_PATTERN.match(line.strip()):
            return True
    return False



def extract_title(
    lines: List[str],
    raw_results: Optional[list] = None,
) -> Optional[str]:
    """
    Trích xuất tiêu đề sách – v4 BBox Area Scoring.

    Nguyên lý cốt lõi:
      Tiêu đề bìa sách LUÔN là chữ to nhất trên ảnh.
      → Đo diện tích vật lý (bbox_area = width × height) của mỗi text block
        từ EasyOCR raw results, nhân với confidence.
      → Gom tất cả block lớn (≥ 60% max_score) → nối lại theo thứ tự Y → X.

    Ưu điểm so với heuristic v1-v3:
      ✅ Không quan tâm title ở dòng đầu hay cuối
      ✅ Không dùng blacklist tên tác giả (dễ sai)
      ✅ Xử lý title split nhiều dòng: "ĐẮC / NHÂN / TÂM"
      ✅ Xử lý title ở bất kỳ vị trí nào trên bìa

    Fallback (khi không có raw_results):
      → Tìm dòng dài nhất hợp lệ trong cleaned text (v0 baseline).
    """

    def _is_valid_token(text: str) -> bool:
        """Kiểm tra text block có hợp lệ để tính title không (v3)."""
        t = text.strip()
        if len(t) < 2:
            return False
        if len(t) > 120:
            return False
        ll = t.lower()
        if any(kw in ll for kw in _NON_TITLE_KEYWORDS):
            return False

        # v3: Cho phép token chỉ toàn số ≥ 2 chữ số (VD: "0852", "2024")
        # Đây có thể là phần đầu tiêu đề kiểu "0852 Gửi Tống" hoặc năm xuất bản
        # Chỉ loại nếu có THÊM ký tự đặc biệt (không phải số thuần)
        if re.match(r'^\d{2,}$', t):
            return True  # Pure digits ≥ 2 → có thể là số trong title

        # Loại nếu chỉ là ký tự đặc biệt (không có chữ/số thực sự)
        if re.match(r'^[\s\-.,/\\()\[\]]+$', t):
            return False

        # Với text hỗn hợp: dùng _is_garbled_line (ngưỡng 0.40)
        if _is_garbled_line(t):
            return False

        # Filter: toàn chữ HOA tiếng Anh (thường là banner/label, không phải title VN)
        # VD: "INSTANT RESEARCH INSTITUTE", "#1 INTERNATIONAL BESTSELLER"
        words = t.split()
        if len(words) >= 2:
            en_upper_words = sum(1 for w in words if re.match(r'^[A-Z#&0-9]+$', w))
            if en_upper_words == len(words):  # 100% all-caps English words
                logger.debug("Filter all-caps English banner: '%s'", t)
                return False
        return True

    # ══════════════════════════════════════════════════════════════════
    # PHƯƠNG PHÁP CHÍNH: BBox Area Scoring (chỉ dùng được khi có raw_results)
    # ══════════════════════════════════════════════════════════════════
    if raw_results:
        scored_blocks = []  # [(score, area, y_center, x_center, text)]

        # Tính chiều cao ảnh để biết vị trí tỷ lệ
        all_ys = []
        for item in raw_results:
            try:
                bbox, _, _ = item
                ys = [pt[1] for pt in bbox]
                all_ys.extend(ys)
            except Exception:
                pass
        img_height = max(all_ys) if all_ys else 1000

        for item in raw_results:
            try:
                bbox, text, conf = item
                text = text.strip()
                # v3: giảm ngưỡng conf từ 0.25 → 0.15
                # Font nghệ thuật/số stylized thường có EasyOCR conf thấp
                # nhưng vẫn là chữ thực sự (VD: "0852" font thiết kế)
                if conf < 0.15:
                    continue
                if not _is_valid_token(text):
                    continue

                # Tính diện tích bbox (width × height)
                xs = [pt[0] for pt in bbox]
                ys = [pt[1] for pt in bbox]
                w      = max(xs) - min(xs)
                h      = max(ys) - min(ys)
                area   = w * h
                y_cen  = (min(ys) + max(ys)) / 2
                x_cen  = (min(xs) + max(xs)) / 2
                y_ratio = y_cen / img_height  # 0.0 = top, 1.0 = bottom

                # Penalty vị trí:
                # Top 15%: thường là tên tác giả → penalty mạnh (×0.30)
                # Top 16-25%: có thể là subtitle header → penalty nhẹ (×0.70)
                # Bottom 8%: publisher info → penalty (×0.40)
                # Còn lại (25-92%): vùng title bình thường → không penalty
                if y_ratio < 0.15:
                    position_weight = 0.30
                elif y_ratio < 0.25:
                    position_weight = 0.70
                elif y_ratio > 0.92:
                    position_weight = 0.40
                else:
                    position_weight = 1.0

                # Score = height × confidence × position_weight
                # Lý do dùng HEIGHT thay AREA (width × height):
                #   Chữ tiêu đề LUÔN CAO hơn chữ phụ trên bìa sách
                #   Chữ phụ nhỏ nhưng trải rộng ngang → AREA lớn giả tạo,
                #   bị chọn nhầm làm title (lỗi "Đắc Nhân Tâm" → "Nỗ Lực...")
                score = h * conf * position_weight
                scored_blocks.append((score, h, y_cen, x_cen, text))

            except (ValueError, TypeError, IndexError):
                continue

        if scored_blocks:
            max_score = max(b[0] for b in scored_blocks)

            # Gom tất cả block có score ≥ 45% max → bắt được title nhiều dòng tốt hơn
            # Giảm từ 55% → 45% để gom được title khi dòng phụ font nhỏ hơn 1 chút
            # VD: "HÀNH TRÌNH" (to) + "VỀ PHƯƠNG ĐÔNG" (nhỏ hơn chút) vẫn gom đủ
            CLUSTER_RATIO = 0.45
            title_cluster = [
                b for b in scored_blocks if b[0] >= max_score * CLUSTER_RATIO
            ]

            # Sắp xếp theo vị trí: từ trên xuống (y), từ trái sang (x)
            title_cluster.sort(key=lambda b: (round(b[2] / 30) * 30, b[3]))
            # (làm tròn y về grid 30px để gom các từ cùng dòng)

            # Ghép các phần → tiêu đề hoàn chỉnh
            merged = " ".join(b[4] for b in title_cluster)
            merged = re.sub(r'\s{2,}', ' ', merged).strip()

            if merged and len(merged) >= 2:
                # v5: Reject all-digit "titles" (giá bìa, mã ISBN, số trang bị OCR nhầm làm title)
                # VD: "4852", "48500", "2024" → không phải tiêu đề hợp lệ
                # Tiêu đề thực sự luôn chứa chữ cái liên tiếp (≥2 ký tự)
                has_real_alpha = bool(re.search(r'[A-Za-zÀ-ỵ\u00C0-\u024F\u1E00-\u1EFF]{2,}', merged))
                if not has_real_alpha:
                    logger.warning("⚠️  Tiêu đề toàn số bị từ chối: '%s' → fallback heuristic", merged)
                    # Không return → rơi xuống fallback longest-line bên dưới
                else:
                    logger.info("✅ Title (BBox Area Score): '%s'  [%d blocks, max_score=%.0f]",
                                merged, len(title_cluster), max_score)
                    return merged

    # ══════════════════════════════════════════════════════════════════
    # FALLBACK: Tìm dòng dài + hợp lệ nhất trong text lines
    # (dùng khi không có raw_results từ EasyOCR)
    # ══════════════════════════════════════════════════════════════════
    valid_lines = [(len(l), l.strip()) for l in lines if _is_valid_token(l.strip())]
    if not valid_lines:
        logger.warning("⚠️  extract_title: không có dòng hợp lệ nào")
        return None

    # Ưu tiên dòng dài nhất (title thường dài hơn tên tác giả, publisher)
    valid_lines.sort(key=lambda x: -x[0])
    best_fallback = valid_lines[0][1]

    logger.info("✅ Title (fallback longest line): '%s'", best_fallback)
    return best_fallback



# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 4: Tác giả (v2 – Validate tên Việt)
# ══════════════════════════════════════════════════════════════════════════════

_AUTHOR_KEYWORD_RE = [re.compile(p, re.IGNORECASE) for p in [
    r"tác\s*giả\s*:\s*(.+)",
    r"author\s*:\s*(.+)",
    r"by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})",
    r"dịch\s*:\s*(.+)",
    r"biên\s*soạn\s*:\s*(.+)",
    # v2: bổ sung patterns
    r"soạn\s*giả\s*:\s*(.+)",
    r"chủ\s*biên\s*:\s*(.+)",
    r"người\s*dịch\s*:\s*(.+)",
]]

# v2: Regex validate tên người (Việt + Anh)
# Tên người VN: ≥2 token Titlecase, mỗi token 2-6 ký tự
_VN_NAME_PATTERN = re.compile(
    r'^(?:[A-ZĐẮẰẲẴẶẮẤẦẨẪẬÉÈẺẼẸÊẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÚÙỦŨỤƯỨỪỬỮỰÝỲỶỸỴ]'
    r'[a-zđắằẳẵặắấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵ]{1,8}'
    r'\s?){2,6}$',
    re.UNICODE
)

# Tên tác giả nước ngoài phổ biến (fallback)
_FOREIGN_NAME_PATTERN = re.compile(
    r'^[A-Z][a-z]+ (?:[A-Z]\. )?[A-Z][a-zA-Z]+$'
)


def _is_valid_author_name(name: str) -> bool:
    """
    Kiểm tra tên tác giả hợp lệ (v2).
    - Phải có ít nhất 2 token (họ + tên)
    - Mỗi token bắt đầu bằng chữ hoa (hoặc ký tự Hoa tiếng Việt)
    - Không chứa số
    - Độ dài hợp lý: 4-60 ký tự
    """
    name = name.strip()
    if len(name) < 4 or len(name) > 60:
        return False
    if any(c.isdigit() for c in name):
        return False
    tokens = name.split()
    if len(tokens) < 2:
        return False
    # Ít nhất 1 token phải bắt đầu bằng chữ hoa
    has_capital = any(t[0].isupper() for t in tokens if t)
    return has_capital


def extract_authors(text: str, lines: List[str]) -> List[str]:
    """
    Trích xuất tên tác giả (v2 với validate).

    Phương pháp 1: Keyword matching (nhanh, chính xác)
    Phương pháp 2: underthesea NER entity PER
    v2: Validate kết quả bằng _is_valid_author_name()
    """
    found_authors = []

    # ── Phương pháp 1: Keyword matching ──────────────────────────────────────
    for line in lines:
        for pattern in _AUTHOR_KEYWORD_RE:
            m = pattern.search(line)
            if m:
                author_text = m.group(1).strip()
                for sep in [",", "&", " và ", " and ", " - "]:
                    if sep in author_text:
                        found_authors.extend([a.strip() for a in author_text.split(sep) if a.strip()])
                        break
                else:
                    found_authors.append(author_text)

    # ── Phương pháp 2: underthesea NER ───────────────────────────────────────
    if not found_authors:
        try:
            ner_result = _try_ner(text)
            if ner_result:
                ner_authors = _extract_entities(ner_result, "PER")
                found_authors.extend(ner_authors)
        except Exception as e:
            logger.debug("NER PER lỗi: %s", e)

    # ── Validate + dedup + limit 3 ───────────────────────────────────────────
    seen = set()
    unique = []
    for a in found_authors:
        a_clean = a.strip()
        if not a_clean or a_clean.lower() in seen:
            continue
        if not _is_valid_author_name(a_clean):  # v2: validate
            logger.debug("Reject invalid author: '%s'", a_clean)
            continue
        seen.add(a_clean.lower())
        unique.append(a_clean)
        if len(unique) >= 3:
            break

    return unique


# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 5: Nhà xuất bản (v2 – Mở rộng 50+ NXB)
# ══════════════════════════════════════════════════════════════════════════════

_PUBLISHER_KEYWORD_RE = [re.compile(p, re.IGNORECASE) for p in [
    r"nhà\s*xuất\s*bản\s*(.+)",
    r"nxb\.?\s*(.+)",
    r"publisher\s*:\s*(.+)",
    r"published\s*by\s*(.+)",
    # v2: bổ sung
    r"xuất\s*bản\s*bởi\s*(.+)",
    r"ấn\s*hành\s*:\s*(.+)",
]]

# v2: Mở rộng từ 14 → 50+ NXB phổ biến
_KNOWN_PUBLISHERS = [
    # NXB Nhà nước
    "NXB Trẻ", "NXB Kim Đồng", "NXB Văn học", "NXB Lao động",
    "NXB Hội nhà văn", "NXB Dân trí", "NXB Thế giới", "NXB Phụ nữ",
    "NXB Giáo dục", "NXB Chính trị quốc gia", "NXB Thông tin truyền thông",
    "NXB Tổng hợp", "NXB Tổng hợp TP.HCM", "NXB Đại học Quốc gia",
    "NXB Thanh niên", "NXB Quân đội nhân dân", "NXB Từ điển Bách khoa",
    "NXB Khoa học xã hội", "NXB Khoa học kỹ thuật",
    "NXB Thể dục thể thao", "NXB Âm nhạc", "NXB Mỹ thuật",
    "NXB Sự thật", "NXB Công an nhân dân",
    # Đơn vị liên kết/tư nhân
    "First News", "Alphabooks", "Fahasa", "Đinh Tị", "Nhã Nam",
    "Omega Plus", "Skybooks", "Đông A", "Saigon Books", "Huy Hoàng",
    "Tre Books", "Minh Lam", "Đinh Tị Books", "Thái Hà Books",
    "1980 Books", "Gia Lai Books", "Book Hunter", "IPM",
    "Phanbook", "Chibooks", "Sbooks", "MCBooks", "Trúc Lâm",
    # Thương hiệu quốc tế phổ biến ở VN
    "Penguin", "HarperCollins", "Random House", "Simon & Schuster",
    "Oxford University Press", "Cambridge University Press",
    "Wiley", "Springer", "McGraw-Hill", "O'Reilly",
]


def extract_publisher(text: str, lines: List[str]) -> Optional[str]:
    """Trích xuất tên nhà xuất bản (v2 – 50+ NXB)."""

    # Phương pháp 1: Keyword matching
    for line in lines:
        for pattern in _PUBLISHER_KEYWORD_RE:
            m = pattern.search(line)
            if m:
                pub = m.group(1).strip()
                # Cắt ở dấu chấm/ xuống dòng đầu tiên nếu cần
                pub = re.split(r'[.\n]', pub)[0].strip()
                return pub[:60] if len(pub) > 60 else pub

    # Phương pháp 2: Known publishers list (v2: 50+ entries)
    text_lower = text.lower()
    for pub in _KNOWN_PUBLISHERS:
        if pub.lower() in text_lower:
            return pub

    # Phương pháp 3: underthesea NER ORG
    try:
        ner_result = _try_ner(text)
        if ner_result:
            orgs = _extract_entities(ner_result, "ORG")
            for org in orgs:
                if 4 <= len(org) <= 60:
                    return org
    except Exception as e:
        logger.debug("NER ORG lỗi: %s", e)

    return None


# ══════════════════════════════════════════════════════════════════════════════
# COMPOUND CONFIDENCE SCORING (v2)
# ══════════════════════════════════════════════════════════════════════════════

def _compute_compound_confidence(
    ocr_confidence: float,
    title: Optional[str],
    authors: List[str],
    isbn: Optional[str],
    publisher: Optional[str],
) -> float:
    """
    Tính confidence tổng hợp (v2) thay cho việc chỉ dùng OCR confidence thô.

    Formula:
      compound = w_ocr * ocr_conf
               + w_title * title_conf
               + w_entity * entity_score
               + w_isbn * isbn_bonus

    Weights: ocr=40%, title=30%, entity=20%, isbn=10%

    Rationale:
    - OCR confidence thô không phản ánh đúng chất lượng extraction
    - Nếu có title hợp lệ dài → rất có thể đúng sách
    - Có ISBN → bonus cao vì ISBN validate 100%
    """
    # Title score: 0.0 nếu không có, tăng theo chiều dài (max tại 20 ký tự)
    if title:
        title_len_score = min(len(title) / 20.0, 1.0)
        title_score = 0.7 + 0.3 * title_len_score  # Min 0.7 nếu có title
    else:
        title_score = 0.0

    # Entity score: 0.0 nếu không có author/publisher
    entity_count = len(authors) + (1 if publisher else 0)
    entity_score = min(entity_count / 3.0, 1.0)  # max tại 3 entities

    # ISBN bonus
    isbn_bonus = 1.0 if isbn else 0.0

    # Compound score (weights)
    compound = (
        0.40 * ocr_confidence +
        0.30 * title_score +
        0.20 * entity_score +
        0.10 * isbn_bonus
    )

    return round(min(compound, 1.0), 3)


# ══════════════════════════════════════════════════════════════════════════════
# API CHÍNH: extract_book_info() – v2
# ══════════════════════════════════════════════════════════════════════════════

def extract_book_info(
    raw_text: str,
    ocr_confidence: float = 0.0,
    raw_ocr_results: Optional[list] = None,  # v2: EasyOCR bbox results
) -> ExtractedBookInfo:
    """
    Hàm chính – trích xuất thông tin sách từ raw OCR text (v2).

    Args:
        raw_text:        Text thô từ OCR engine
        ocr_confidence:  Confidence score từ OCR engine (0.0-1.0)
        raw_ocr_results: List [(bbox, text, conf)] từ EasyOCR (v2: dùng cho multi-pass title)

    Returns ExtractedBookInfo với compound confidence score.

    Search query priority:
    1. title + author    → chính xác nhất
    2. title             → tốt
    3. isbn              → 100% match nhưng chỉ nếu sách có trong DB
    4. first valid line  → fallback
    """
    logger.info("📝 Extraction bắt đầu (%d ký tự, conf=%.3f)", len(raw_text), ocr_confidence)

    # [1] Clean
    clean_text = clean_ocr_text(raw_text)
    lines = [l.strip() for l in clean_text.split("\n") if l.strip()]

    if not lines:
        logger.warning("Không có text sau cleaning")
        return ExtractedBookInfo(confidence=0.0, search_query="")

    # [2] ISBN
    isbn = extract_isbn(clean_text)
    if isbn:
        logger.info("✅ ISBN: %s", isbn)

    # [3] Title (v2: multi-pass với bbox)
    title = extract_title(lines, raw_results=raw_ocr_results)
    if title:
        logger.info("✅ Title: %s", title)
    else:
        logger.info("⚠️  Không tìm được title")

    # [4] Authors
    authors = extract_authors(clean_text, lines)
    if authors:
        logger.info("✅ Authors: %s", authors)

    # [5] Publisher
    publisher = extract_publisher(clean_text, lines)
    if publisher:
        logger.info("✅ Publisher: %s", publisher)

    # [6] Compound confidence (v2)
    compound_conf = _compute_compound_confidence(
        ocr_confidence, title, authors, isbn, publisher
    )
    logger.info(
        "📊 Compound confidence: %.3f (ocr=%.3f, title=%s, authors=%d, isbn=%s)",
        compound_conf, ocr_confidence, bool(title), len(authors), bool(isbn)
    )

    # [7] Build search query – v4: Dùng TOÀN BỘ text hợp lệ, không chỉ title
    #
    # Chiến lược:
    # 1. Primary query: title + author (chính xác nhất, ngắn gọn)
    # 2. Extra query:   TẤT CẢ dòng hợp lệ ghép lại (dự phòng khi title OCR sai)
    #    → Search engine full-text ranking sẽ tìm đúng sách dù query dài
    # 3. Fallback: ISBN (nếu có)

    # Lấy tất cả dòng không garbled từ OCR (tối đa 12 dòng để có nhiều context hơn)
    all_valid_lines = [
        l for l in lines
        if not _is_garbled_line(l) and len(l) >= 3
    ][:12]

    # Primary query: title + author (ngắn, chính xác)
    if title and authors:
        primary_query = f"{title} {authors[0]}"
    elif title:
        primary_query = title
    elif isbn:
        primary_query = isbn
    elif all_valid_lines:
        primary_query = all_valid_lines[0][:60]
    else:
        primary_query = ""

    # Full-text query: ghép tất cả text hợp lệ → search engine tự rank
    # Tăng giới hạn lên 400 ky ́ tự để chứa đủ tiêu đề nhiều dòng + tác giả
    full_text_query = " ".join(all_valid_lines)[:400] if all_valid_lines else ""

    # Chọn search_query: ưu tiên primary, lưu full_text để dự phòng
    search_query = primary_query

    # Đính kèm full_text_query vào ExtractedBookInfo để integrate_search có thể dùng
    # khi primary query trả về ít kết quả
    logger.info("🔍 Primary query: '%s'", search_query[:60])
    logger.info("🔍 Full-text query: '%s'", full_text_query[:80])

    return ExtractedBookInfo(
        title=title,
        authors=authors,
        isbn=isbn,
        publisher=publisher,
        confidence=compound_conf,
        search_query=search_query,
        full_text_query=full_text_query,  # v4: thêm field mới
    )
