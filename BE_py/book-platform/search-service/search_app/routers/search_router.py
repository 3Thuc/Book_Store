import time
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Query, Request
from search_app.middleware.rate_limiter import limiter
from search_app.search.service import suggest_books, search_books, search_books_ocr
from search_app.analytics.logger import log_search_event

router = APIRouter(prefix="/books", tags=["Search"])


@router.get("/suggest")
@limiter.limit("120/minute")
def suggest(
    request: Request,                    # slowapi cần Request object
    q: str = Query("", description="Từ khóa gợi ý"),
    limit: int = Query(10, ge=1, le=20),
):
    """Autocomplete (lightweight) – 120 req/phút."""
    return suggest_books(q=q, limit=limit)


@router.get("/search")
@limiter.limit("60/minute")
def search(
    request: Request,
    background_tasks: BackgroundTasks,   # Fix #3: analytics chạy nền, không block response
    q: str = Query("", description="Từ khóa tìm kiếm (có thể để trống để lấy tất cả)"),
    page: int = Query(1, ge=1, le=50, description="Giới hạn tối đa 50 trang"),
    limit: int = Query(20, ge=1, le=50),
    in_stock: Optional[bool] = Query(None),
    category: Optional[str] = Query(
        None, description="category_name (vd: 'Văn học')"),
    language: Optional[str] = Query(None),
    fmt: Optional[str] = Query(None, description="ebook|paperback|hardcover"),
    sort: str = Query(
        "relevance", pattern="^(relevance|price_asc|price_desc|newest|rating_desc)$"),
):
    """Full-text search – 60 req/phút/IP."""
    t0 = time.perf_counter()
    result = search_books(
        q=q,
        page=page,
        limit=limit,
        in_stock=in_stock,
        category=category,
        language=language,
        fmt=fmt,
        sort=sort,
    )
    latency_ms = int((time.perf_counter() - t0) * 1000)

    # Fix #3: Chạy log_search_event trong background → response trả về ngay,
    # không bị block dù Redis chậm. Graceful skip nếu Redis unavailable.
    background_tasks.add_task(
        log_search_event,
        q=q,
        total=result.get("total", 0),
        latency_ms=latency_ms,
    )
    return result


@router.get("/ocr-search")
@limiter.limit("20/minute")
def ocr_search(
    request: Request,
    q: str = Query(..., min_length=1, description="OCR text (có thể sai chính tả)"),
    limit: int = Query(15, ge=1, le=20),
):
    """
    Search đặc biệt cho OCR – chấp nhận văn bản sai chính tả nặng.
    minimum_should_match=30%, fuzziness=2, fold tiếng Việt.
    Rate limit: 20 req/phút (OCR downstream nặng).
    """
    return search_books_ocr(q=q, limit=limit)
