from typing import Optional
from fastapi import APIRouter, Query

from search_app.search.service import suggest_books, search_books

router = APIRouter(prefix="/books", tags=["Search"])


@router.get("/suggest")
def suggest(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=20),
):
    return suggest_books(q=q, limit=limit)


@router.get("/search")
def search(
    q: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=50),
    in_stock: Optional[bool] = Query(None),
    category: Optional[str] = Query(
        None, description="category_name (vd: 'Văn học')"),
    language: Optional[str] = Query(None),
    fmt: Optional[str] = Query(None, description="ebook|paperback|hardcover"),
    sort: str = Query(
        "relevance", pattern="^(relevance|price_asc|price_desc|newest|rating_desc)$"),
):
    return search_books(
        q=q,
        page=page,
        limit=limit,
        in_stock=in_stock,
        category=category,
        language=language,
        fmt=fmt,
        sort=sort,
    )
