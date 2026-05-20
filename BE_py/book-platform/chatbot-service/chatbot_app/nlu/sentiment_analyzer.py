"""
sentiment_analyzer.py – Phân tích cảm xúc (Positive / Negative / Neutral).

v2 – Nâng cấp toàn diện:
  - Từ điển mở rộng 5x (NEGATIVE: 12→70+, POSITIVE: 12→50+)
  - Xử lý phủ định: "không hài lòng", "chẳng vui", "không ổn"
  - Intensifier: "cực kỳ tệ", "rất bực", "quá chậm"
  - Emoji cảm xúc tiêu cực/tích cực phổ biến
  - Scoring theo trọng số thay vì đếm đơn giản
"""
import re

# ── Intensifiers – tăng trọng số khi đứng trước từ cảm xúc ───────────────────
_INTENSIFIERS = re.compile(
    r"\b(rất|cực|cực kỳ|quá|vô cùng|thực sự|hoàn toàn|"
    r"siêu|mega|cực kỳ|hết sức|quá đỗi|vô cùng)\b",
    re.IGNORECASE
)

# ── Negation words – đảo nghĩa từ cảm xúc phía sau ──────────────────────────
_NEGATION = re.compile(
    r"\b(không|chẳng|chưa|chả|ko|k\b|không thể|chả thể|không hề|"
    r"không hề|không được|không có|chưa bao giờ)\b",
    re.IGNORECASE
)

# ── Từ điển NEGATIVE (trọng số 1.0 mỗi từ, intensifier nhân đôi) ─────────────
NEGATIVE_TERMS: list[tuple[str, float]] = [
    # Chất lượng sản phẩm
    ("tệ", 1.0), ("kém", 1.0), ("tồi", 1.0), ("xấu", 0.8), ("dở", 0.8),
    ("hỏng", 1.2), ("lỗi", 1.0), ("sai", 1.0), ("nhàu", 1.0), ("rách", 1.2),
    ("ố vàng", 1.0), ("mốc", 1.0), ("bẩn", 0.8), ("không đúng", 1.2),
    ("thiếu trang", 1.5), ("in mờ", 1.0), ("in sai", 1.2),

    # Cảm xúc tiêu cực
    ("thất vọng", 1.5), ("bực", 1.2), ("tức", 1.2), ("bực bội", 1.5),
    ("bực mình", 1.5), ("tức giận", 1.8), ("phẫn nộ", 2.0), ("nản", 1.0),
    ("chán", 0.8), ("chán nản", 1.2), ("khó chịu", 1.2), ("bực tức", 1.5),
    ("phàn nàn", 1.2), ("khiếu nại", 1.5), ("tố cáo", 1.8),
    ("không hài lòng", 1.5), ("không thỏa mãn", 1.5), ("không vừa ý", 1.2),

    # Dịch vụ
    ("chậm", 1.0), ("trễ", 1.0), ("muộn", 0.8), ("lâu quá", 1.2),
    ("mãi chưa", 1.2), ("vẫn chưa", 1.0), ("bao giờ mới", 1.0),
    ("không nhận được", 1.5), ("chưa nhận", 1.0), ("thất lạc", 1.5),
    ("bị mất", 1.5), ("không giao", 1.2),

    # Vấn đề tài chính
    ("bị trừ tiền", 1.5), ("tính sai tiền", 1.5), ("bị charge", 1.5),
    ("mất tiền", 1.8), ("không hoàn tiền", 1.5),

    # Từ tiếng Anh phổ biến trong chat
    ("bad", 0.8), ("terrible", 1.5), ("awful", 1.5), ("horrible", 2.0),
    ("worst", 2.0), ("disappointed", 1.5), ("frustrated", 1.5),
    ("unacceptable", 1.8), ("ridiculous", 1.5), ("poor quality", 1.5),

    # Emoji tiêu cực
    ("😡", 2.0), ("😤", 1.5), ("💢", 1.5), ("😠", 1.8), ("🤬", 2.0),
    ("😞", 1.2), ("😢", 1.2), ("😣", 1.2), ("😩", 1.5), ("🤦", 1.2),
]

# ── Từ điển POSITIVE (trọng số 1.0 mỗi từ) ───────────────────────────────────
POSITIVE_TERMS: list[tuple[str, float]] = [
    # Chất lượng
    ("tốt", 0.8), ("hay", 0.8), ("đẹp", 0.8), ("ổn", 0.7), ("hoàn hảo", 1.5),
    ("tuyệt", 1.2), ("tuyệt vời", 1.5), ("xuất sắc", 1.8), ("chuẩn", 1.0),
    ("chất lượng", 0.8), ("đúng", 0.5), ("ngon", 0.8), ("xịn", 1.0),

    # Cảm xúc tích cực
    ("hài lòng", 1.5), ("thích", 1.0), ("yêu thích", 1.2), ("vui", 0.8),
    ("thỏa mãn", 1.5), ("vừa ý", 1.2), ("hạnh phúc", 1.5), ("sung sướng", 1.5),
    ("phấn khởi", 1.2), ("ưng", 1.0), ("ưng ý", 1.2),

    # Khen ngợi dịch vụ
    ("nhanh", 0.8), ("giao nhanh", 1.2), ("đúng hẹn", 1.2), ("đúng giờ", 1.0),
    ("đúng sản phẩm", 1.0), ("đúng mô tả", 1.2), ("như mô tả", 1.0),
    ("hỗ trợ tốt", 1.5), ("nhiệt tình", 1.2), ("thân thiện", 1.0),

    # Cảm ơn / kết thúc tích cực
    ("cảm ơn", 1.2), ("cám ơn", 1.2), ("thanks", 1.0), ("thank you", 1.2),
    ("awesome", 1.5), ("perfect", 1.5), ("excellent", 1.8), ("love it", 1.5),
    ("great", 1.2), ("fantastic", 1.8), ("amazing", 1.8),
    ("recommend", 1.0), ("sẽ mua lại", 1.5), ("mua lại", 1.2),

    # Emoji tích cực
    ("😊", 1.2), ("😍", 1.5), ("🥰", 1.5), ("👍", 1.2), ("🎉", 1.5),
    ("❤️", 1.2), ("💯", 1.5), ("✨", 1.0), ("😄", 1.2), ("🌟", 1.2),
]

# ── Cụm phủ định cảm xúc POSITIVE thành NEGATIVE ────────────────────────────
NEGATED_POSITIVE: list[str] = [
    "không tốt", "không hay", "không ổn", "không hài lòng", "không thích",
    "không vừa ý", "chẳng tốt", "chẳng hay", "chưa tốt", "không đẹp",
    "không thỏa mãn", "không ưng", "chẳng ưng", "không hoàn hảo",
]

# ── Cụm phủ định cảm xúc NEGATIVE thành POSITIVE ────────────────────────────
NEGATED_NEGATIVE: list[str] = [
    "không tệ", "không kém", "không lỗi", "không sai", "không tồi",
    "chẳng tệ", "không bị lỗi", "không hỏng",
]


def analyze_sentiment(text: str) -> str:
    """
    Phân tích sentiment câu text.

    Returns:
        "POSITIVE" | "NEGATIVE" | "NEUTRAL"
    """
    text_lower = text.lower()

    # ── Bước 1: Kiểm tra cụm phủ định cố định ────────────────────────────────
    neg_boost  = sum(1.0 for phrase in NEGATED_POSITIVE if phrase in text_lower)
    pos_boost  = sum(1.0 for phrase in NEGATED_NEGATIVE if phrase in text_lower)

    # ── Bước 2: Phát hiện intensifier trong chuỗi ────────────────────────────
    intensifier_count = len(_INTENSIFIERS.findall(text_lower))

    # ── Bước 3: Tính điểm NEGATIVE ───────────────────────────────────────────
    neg_score = neg_boost
    for term, weight in NEGATIVE_TERMS:
        if term.lower() in text_lower:
            neg_score += weight

    # ── Bước 4: Tính điểm POSITIVE ───────────────────────────────────────────
    pos_score = pos_boost
    for term, weight in POSITIVE_TERMS:
        if term.lower() in text_lower:
            pos_score += weight

    # ── Bước 5: Áp dụng intensifier (tăng tất cả điểm 30% nếu có intensifier)
    if intensifier_count > 0:
        multiplier = 1.0 + (intensifier_count * 0.3)
        if neg_score > pos_score:
            neg_score *= multiplier
        else:
            pos_score *= multiplier

    # ── Bước 6: Quyết định ───────────────────────────────────────────────────
    if neg_score > pos_score and neg_score >= 1.0:
        return "NEGATIVE"
    if pos_score > neg_score and pos_score >= 0.8:
        return "POSITIVE"
    return "NEUTRAL"
