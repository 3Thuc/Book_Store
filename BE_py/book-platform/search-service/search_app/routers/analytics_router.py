"""
Analytics Router – P2.1
Endpoints xem thong ke search: top queries, miss queries, summary.
"""
from fastapi import APIRouter, Query
from search_app.analytics.logger import get_top_queries, get_miss_queries, get_summary

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/search-top")
def analytics_top_queries(n: int = Query(20, ge=1, le=100)):
    """
    Top N query pho bien nhat.
    Dung de biet user dang tim gi nhieu nhat.
    """
    return {
        "top_queries": get_top_queries(n),
        "note": "Sorted by frequency (highest first)"
    }


@router.get("/search-miss")
def analytics_miss_queries(n: int = Query(20, ge=1, le=100)):
    """
    Top N query khong co ket qua (total = 0).
    Dung de phat hien gap du lieu: sach chua duoc index, ten sach bi sai.
    """
    return {
        "miss_queries": get_miss_queries(n),
        "note": "Queries returning 0 results – consider adding missing books or aliases"
    }


@router.get("/search-summary")
def analytics_summary():
    """
    Tong hop toan bo thong ke search:
    - Tong so luong query
    - Miss rate (%)
    - Latency trung binh
    - So unique queries
    - So events dang luu trong Redis
    """
    return get_summary()
