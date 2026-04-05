"""
sentiment_analyzer.py – Phân tích cảm xúc (Positive / Negative / Neutral).
Dùng để điều chỉnh tone trả lời: nếu khách đang tức giận → xin lỗi trước.
"""
import re

POSITIVE_WORDS = [
    "cảm ơn", "tuyệt", "tốt", "hay", "thích", "hài lòng",
    "ổn", "hoàn hảo", "xuất sắc", "tuyệt vời", "chuẩn", "ngon"
]
NEGATIVE_WORDS = [
    "tệ", "kém", "lỗi", "hỏng", "sai", "không đúng", "thất vọng",
    "bực", "tức", "chậm", "mãi chưa", "không nhận được",
    "rách", "bị hỏng", "không hài lòng", "phàn nàn", "khiếu nại"
]

def analyze_sentiment(text: str) -> str:
    text_lower = text.lower()
    neg = sum(1 for w in NEGATIVE_WORDS if w in text_lower)
    pos = sum(1 for w in POSITIVE_WORDS if w in text_lower)
    if neg > pos:
        return "NEGATIVE"
    if pos > neg:
        return "POSITIVE"
    return "NEUTRAL"
