"""
utils/sbert_model.py
---------------------
Module-level Singleton cho Sentence-BERT model.

Lý do dùng Singleton:
  - Model keepitreal/vietnamese-sbert nặng ~500MB.
  - Load 1 lần lúc app startup, dùng lại cho mọi request.
  - Đảm bảo không bao giờ load nhiều lần (tốn RAM + chậm).

Cách dùng:
  from search_app.utils.sbert_model import get_sbert_model
  model = get_sbert_model()
  vector = model.encode("Nhà giả kim", normalize_embeddings=True).tolist()
"""

import logging
from typing import Optional

logger = logging.getLogger("search.sbert_model")

_MODEL = None  # Singleton instance


def get_sbert_model():
    """Trả về instance SBERT đã load sẵn. Load lần đầu nếu chưa có."""
    global _MODEL
    if _MODEL is None:
        try:
            from sentence_transformers import SentenceTransformer   # noqa: PLC0415
            MODEL_NAME = "keepitreal/vietnamese-sbert"
            logger.info("[SBERT] Đang load model '%s' lần đầu tiên...", MODEL_NAME)
            _MODEL = SentenceTransformer(MODEL_NAME)
            logger.info("[SBERT] Model đã sẵn sàng. Chiều Vector: %d", _MODEL.get_sentence_embedding_dimension())
        except ImportError:
            logger.error(
                "[SBERT] Thư viện 'sentence-transformers' chưa được cài. "
                "Hãy chạy: pip install sentence-transformers"
            )
            raise
    return _MODEL


def encode_book_text(title: str, categories: list, description: str) -> list[float]:
    """
    Build chuỗi văn bản đại diện cho 1 cuốn sách và encode thành Vector.

    Chiến lược ghép text (ảnh hưởng lớn đến chất lượng gợi ý):
      [Thể loại] Tựa đề. Mô tả nội dung...
    - Đặt Thể loại lên đầu giúp model định hướng không gian ngữ nghĩa.
    - Tựa đề ngắn gọn ở vị trí trọng tâm nhất.
    - Mô tả cung cấp ngữ cảnh, nhưng bị cắt ở 300 ký tự để tránh nhiễu.

    Returns:
      list[float]: Vector 768 chiều, đã chuẩn hóa L2 (Cosine-ready).
    """
    cats = " ".join(categories)[:100] if categories else ""
    desc_snippet = (description or "")[:300]

    text = f"[{cats}] {title}. {desc_snippet}".strip()

    model = get_sbert_model()
    vector = model.encode(text, normalize_embeddings=True)  # shape: (768,)
    return vector.tolist()
