"""
intent_classifier.py – Phân loại Intent người dùng.

Thuật toán: TF-IDF + Logistic Regression (scikit-learn)
  - Ưu điểm so với keyword matching: học được pattern, xử lý
    câu chưa gặp bao giờ nếu có pattern tương tự trong training data
  - Fallback: nếu confidence < ngưỡng → chuyển sang LLM xử lý

Workflow:
  1. Lần đầu: dùng rule-based (không cần train)
  2. Nếu có training/intent_model.pkl → dùng ML model
"""
import re
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from chatbot_app.config import CLASSIFIER_PATH, VECTORIZER_PATH

CONFIDENCE_THRESHOLD = 0.55   # Dưới ngưỡng này → fallback LLM


@dataclass
class NLUResult:
    intent:     str
    confidence: float = 1.0
    entities:   dict  = field(default_factory=dict)
    sentiment:  str   = "NEUTRAL"


# ──────────────────────────────────────────────────────────────
# Rule-based fallback (không cần train, chạy ngay)
# ──────────────────────────────────────────────────────────────
INTENT_PATTERNS = {
    "greeting": [
        r"\bxin chào\b", r"\bchào\b", r"\bhello\b", r"\bhi\b", r"\bhey\b"
    ],
    "farewell": [
        r"\btạm biệt\b", r"\bbye\b", r"\bcảm ơn\b", r"\bthank\b"
    ],
    "book_recommendation": [
        r"gợi[_\s]?ý", r"đề[_\s]?xuất", r"recommend", r"sách hay",
        r"nên đọc", r"muốn đọc", r"sách nào", r"tư[_\s]?vấn sách"
    ],
    "book_search": [
        r"tìm sách", r"tìm kiếm", r"có sách", r"bán sách",
        r"giá (sách|cuốn)", r"sách của", r"tác giả", r"tìm tên"
    ],
    "order_status": [
        r"đơn hàng", r"theo dõi (đơn|giao)", r"giao hàng đến đâu",
        r"trạng thái", r"vận chuyển", r"ship", r"nhận hàng chưa"
    ],
    "order_history": [
        r"lịch sử mua", r"đã mua", r"mua gì", r"mua những", r"danh sách đơn"
    ],
    "policy_return": [
        r"đổi (trả|hàng)", r"hoàn hàng", r"trả lại", r"refund",
        r"hàng lỗi", r"sách bị lỗi", r"đổi sách"
    ],
    "policy_shipping": [
        r"phí (ship|vận chuyển)", r"miễn phí ship",
        r"thời gian giao", r"bao (lâu|nhiêu ngày)", r"mấy ngày"
    ],
    "policy_payment": [
        r"thanh toán", r"payment", r"chuyển khoản",
        r"\bcod\b", r"vnpay", r"trả tiền", r"phương thức"
    ],
}


def _rule_based_intent(text: str) -> tuple[str, float]:
    text_lower = text.lower()
    for intent, patterns in INTENT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                return intent, 0.90
    return "general_query", 0.40


# ──────────────────────────────────────────────────────────────
# ML-based (nếu đã train model)
# ──────────────────────────────────────────────────────────────
_classifier  = None
_vectorizer  = None

def _load_ml_model():
    global _classifier, _vectorizer
    if CLASSIFIER_PATH.exists() and VECTORIZER_PATH.exists():
        with open(CLASSIFIER_PATH, "rb") as f:
            _classifier = pickle.load(f)
        with open(VECTORIZER_PATH, "rb") as f:
            _vectorizer = pickle.load(f)
        print("✅ Intent classifier (ML) loaded.")
    else:
        print("⚠️  ML model not found → dùng rule-based fallback")


def _ml_intent(text: str) -> tuple[str, float]:
    if _classifier is None or _vectorizer is None:
        return None, 0.0
    vec = _vectorizer.transform([text])
    intent = _classifier.predict(vec)[0]
    proba  = _classifier.predict_proba(vec).max()
    return intent, float(proba)


# ──────────────────────────────────────────────────────────────
# Entity extraction (đơn giản, không cần ML)
# ──────────────────────────────────────────────────────────────
BOOK_GENRES = [
    "văn học", "tiểu thuyết", "kỹ năng sống", "kỹ năng",
    "kinh tế", "kinh doanh", "tâm lý", "tâm lý học",
    "thiếu nhi", "lịch sử", "khoa học", "lập trình",
    "triết học", "tâm linh", "tôn giáo", "nuôi dạy con",
    "y học", "sức khỏe", "du lịch", "nấu ăn",
]

def _extract_entities(text: str, intent: str) -> dict:
    entities = {}
    text_lower = text.lower()

    # Thể loại sách
    for genre in BOOK_GENRES:
        if genre in text_lower:
            entities["genre"] = genre
            break

    # Mã đơn hàng (#12345 hoặc 12345)
    order_match = re.search(r"#?(\d{5,10})\b", text)
    if order_match:
        entities["order_id"] = order_match.group(1)

    # Khoảng giá (dưới 100k, từ 50 đến 200 nghìn...)
    price_match = re.search(r"(dưới|trên|từ|khoảng)\s*(\d+)\s*(k|nghìn|đồng|ngàn)", text_lower)
    if price_match:
        entities["price_ref"] = price_match.group(0)

    return entities


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────
_model_loaded = False

def detect_intent(text: str) -> NLUResult:
    global _model_loaded
    if not _model_loaded:
        _load_ml_model()
        _model_loaded = True

    # Thử ML model trước
    ml_intent, ml_conf = _ml_intent(text)
    if ml_intent and ml_conf >= CONFIDENCE_THRESHOLD:
        intent, confidence = ml_intent, ml_conf
    else:
        # Fallback rule-based
        intent, confidence = _rule_based_intent(text)

    entities = _extract_entities(text, intent)

    return NLUResult(
        intent=intent,
        confidence=confidence,
        entities=entities,
    )
