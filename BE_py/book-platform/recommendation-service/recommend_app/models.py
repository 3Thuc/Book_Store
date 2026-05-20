# Chứa Pydantic model trả về API.
# app/models.py
from typing import Optional
from pydantic import BaseModel


class BookRecommendation(BaseModel):
    book_id: int
    title: str
    price: float
    main_image: Optional[str] = None
    author_name: Optional[str]
    avg_rating: float = 0
    rating_count: int = 0

    # Lý do / kiểu gợi ý
    reason: Optional[str] = None

    # Cac field diem / thong ke (tuy proc co hay khong)
    total_sold: Optional[int] = None
    final_score: Optional[float] = None
    co_purchase_count: Optional[int] = None
    view_count: Optional[int] = None
    # P2.3: category de MMR diversity grouping
    category_name: Optional[str] = None
