# app/routers/recommend_router.py
from fastapi import HTTPException
from typing import List
from fastapi import APIRouter, Query
import os

from recommend_app.utils import _call_proc, _rows_to_recommendations
from recommend_app.models import BookRecommendation

# Import cb_service – gọi OpenSearch k-NN (SBERT Vector Search)
# cb_service connect tới search-service qua OpenSearch trực tiếp
try:
    from recommend_app.services.content_based.cb_service import get_similar_books as _get_cb_knn
    _CB_KNN_AVAILABLE = True
except Exception as _e:
    print(f"[WARN] cb_service k-NN không khởi tạo được: {_e}")
    _CB_KNN_AVAILABLE = False

router = APIRouter(prefix="/recommend")


# -------------------------
# HOME
# -------------------------
@router.get(
    "/popular",
    tags=["Home"],
    response_model=List[BookRecommendation],
    summary="Popular books (bán chạy)",
)
def recommend_popular(limit: int = Query(10, ge=1, le=100)):
    rows = _call_proc("sp_recommend_popular", [limit])
    return _rows_to_recommendations(rows, default_reason="popular")


@router.get(
    "/trending",
    tags=["Home"],
    response_model=List[BookRecommendation],
    summary="Trending books (theo view/add_to_cart gần đây)",
)
def recommend_trending_views(
    days: int = Query(7, ge=1, le=60),
    limit: int = Query(10, ge=1, le=100),
):
    rows = _call_proc("sp_recommend_trending_views", [days, limit])
    return _rows_to_recommendations(rows, default_reason="trending")


@router.get(
    "/top-rated",
    tags=["Home"],
    response_model=List[BookRecommendation],
    summary="Top rated books (đánh giá cao)",
)
def recommend_top_rated(limit: int = Query(10, ge=1, le=100)):
    rows = _call_proc("sp_recommend_top_rated", [limit])
    return _rows_to_recommendations(rows, default_reason="top_rated")


# -------------------------
# ITEM-BASED: RULE
# -------------------------
@router.get(
    "/book/{book_id}/rule",
    tags=["Item-based: Rule"],
    response_model=List[BookRecommendation],
    summary="Similar books (rule-based: category/author)",
)
def recommend_for_book_rule_based(
    book_id: int,
    limit: int = Query(10, ge=1, le=100),
):
    rows = _call_proc("sp_recommend_for_book_rule_based", [book_id, limit])
    return _rows_to_recommendations(rows, default_reason="similar_book_rule_based")


# -------------------------
# ITEM-BASED: CO-PURCHASE
# -------------------------
@router.get(
    "/book/{book_id}/also",
    tags=["Item-based: Co-purchase"],
    response_model=List[BookRecommendation],
    summary="Also bought (co-purchase + fallback)",
)
def recommend_also_bought_for_book(
    book_id: int,
    limit: int = Query(10, ge=1, le=100),
):
    rows = _call_proc("sp_recommend_also_bought_for_book", [book_id, limit])
    return _rows_to_recommendations(rows, default_reason="also_bought")


# -------------------------
# ITEM-BASED: CONTENT-BASED (SBERT k-NN OpenSearch)
# -------------------------
@router.get(
    "/book/{book_id}/cb",
    tags=["Item-based: Content"],
    response_model=List[BookRecommendation],
    summary="Similar books (SBERT AI Semantic – OpenSearch k-NN)",
)
def recommend_for_book_cb(
    book_id: int,
    limit: int = Query(10, ge=1, le=100),
    only_in_stock: bool = Query(True, description="Chỉ gợi ý sách đang còn hàng"),
):
    """
    Gợi ý sách tương đồng ngữ nghĩa dùng AI SBERT + OpenSearch k-NN.
    Không đọc bảng similar_books MySQL (TF-IDF cũ đã bị loại bỏ).
    """
    if not _CB_KNN_AVAILABLE:
        raise HTTPException(503, detail="Content-Based k-NN service chưa sẵn sàng.")

    items = _get_cb_knn(book_id=book_id, limit=limit, only_in_stock=only_in_stock)
    if not items:
        return []

    return [
        BookRecommendation(
            book_id=item["book_id"],
            title=item["title"],
            price=float(item.get("price") or 0),
            main_image=item.get("main_image_url"),
            author_name=item.get("author_name"),
            avg_rating=float(item.get("avg_rating") or 0),
            rating_count=0,
            reason=f"sbert_knn (similarity={item.get('similarity', 0):.2f})",
            final_score=item.get("similarity"),
        )
        for item in items
    ]


@router.get(
    "/book/{book_id}/cb-fallback",
    tags=["Item-based: Content"],
    response_model=List[BookRecommendation],
    summary="Similar books SBERT k-NN + fallback popular nếu thiếu kết quả",
)
def recommend_for_book_cb_with_fallback(
    book_id: int,
    limit: int = Query(10, ge=1, le=100),
):
    """
    SBERT k-NN + fallback popular nếu không đủ kết quả.
    Bao gồm cả sách Hết Hàng trong fallback để đảm bảo luôn đủ limit.
    """
    results = []
    if _CB_KNN_AVAILABLE:
        items = _get_cb_knn(book_id=book_id, limit=limit, only_in_stock=False)
        results = [
            BookRecommendation(
                book_id=item["book_id"],
                title=item["title"],
                price=float(item.get("price") or 0),
                main_image=item.get("main_image_url"),
                author_name=item.get("author_name"),
                avg_rating=float(item.get("avg_rating") or 0),
                rating_count=0,
                reason="sbert_knn",
                final_score=item.get("similarity"),
            )
            for item in items
        ]

    # Fallback popular nếu k-NN trả về ít hơn limit
    if len(results) < limit:
        rows = _call_proc("sp_recommend_popular", [limit - len(results)])
        fallback = _rows_to_recommendations(rows, default_reason="popular_fallback")
        seen = {r.book_id for r in results}
        results += [r for r in fallback if r.book_id not in seen]

    return results[:limit]


# -------------------------
# ITEM-BASED: CF (item-item similarity)
# -------------------------
@router.get(
    "/book/{book_id}/cf",
    tags=["Item-based: CF"],
    response_model=List[BookRecommendation],
    summary="Similar books (CF item-item)",
)
def recommend_for_book_cf(
    book_id: int,
    algo: str = Query(
        "CF_IMPLICIT", description="CF_IMPLICIT hoặc CF_PURCHASE"),
    limit: int = Query(10, ge=1, le=100),
):
    rows = _call_proc("sp_recommend_for_book_cf", [book_id, algo, limit])
    return _rows_to_recommendations(rows, default_reason="cf_similar")


# -------------------------
# PERSONALIZED: RULE (user-based)
# -------------------------
@router.get(
    "/user/{user_id}/rule",
    tags=["Personalized: Rule"],
    response_model=List[BookRecommendation],
    summary="User rule-based recommendations (top categories purchased)",
)
def recommend_for_user_rule_based(
    user_id: int,
    limit: int = Query(10, ge=1, le=100),
):
    rows = _call_proc("sp_recommend_for_user_rule_based", [user_id, limit])
    return _rows_to_recommendations(rows, default_reason="popular_in_user_categories")


# -------------------------
# PERSONALIZED: CF (cached in recommendations)
# -------------------------
@router.post(
    "/user/{user_id}/cf/rebuild",
    tags=["Admin/Maintenance"],
    summary="Rebuild CF cache for a user",
)
def rebuild_user_cf(
    user_id: int,
    days: int = Query(90, ge=1, le=365),
    topn: int = Query(50, ge=1, le=200),
):
    _call_proc("sp_rebuild_user_cf_implicit", [user_id, days, topn])
    return {"ok": True, "user_id": user_id, "days": days, "topn": topn}


@router.get(
    "/user/{user_id}/cf",
    tags=["Personalized: CF"],
    response_model=List[BookRecommendation],
    summary="User CF recommendations (read cache)",
)
def recommend_for_user_cf(
    user_id: int,
    limit: int = Query(20, ge=1, le=100),
):
    rows = _call_proc("sp_recommend_for_user_from_cache",
                      [user_id, "CF", limit])
    return _rows_to_recommendations(rows, default_reason="cf_cached")


@router.get(
    "/user/{user_id}/for-you",
    tags=["Personalized: For You"],
    response_model=List[BookRecommendation],
    summary="For you (CF + fallback rule + popular)",
)
def recommend_for_you(
    user_id: int,
    limit: int = Query(20, ge=1, le=100),
):
    # 1) CF cache
    rows = _call_proc("sp_recommend_for_user_from_cache",
                      [user_id, "CF", limit])
    recs = _rows_to_recommendations(rows, default_reason="cf_cached")

    # Nếu cache rỗng thì rebuild nhanh rồi lấy lại
    if len(recs) == 0:
        _call_proc("sp_rebuild_user_cf_implicit", [user_id, 90, 50])
        rows = _call_proc("sp_recommend_for_user_from_cache",
                          [user_id, "CF", limit])
        recs = _rows_to_recommendations(rows, default_reason="cf_cached")

    # 2) fallback rule-based theo category mua
    if len(recs) < limit:
        rows2 = _call_proc("sp_recommend_for_user_rule_based", [
                           user_id, limit - len(recs)])
        recs += _rows_to_recommendations(rows2,
                                         default_reason="popular_in_user_categories")

    # 3) fallback popular
    if len(recs) < limit:
        rows3 = _call_proc("sp_recommend_popular", [limit - len(recs)])
        recs += _rows_to_recommendations(rows3, default_reason="popular")

    # dedupe theo book_id
    seen = set()
    out = []
    for r in recs:
        if r.book_id in seen:
            continue
        seen.add(r.book_id)
        out.append(r)
        if len(out) >= limit:
            break
    return out


# -------------------------
# ADMIN / MAINTENANCE
# -------------------------
@router.post(
    "/book/{book_id}/cb/clear-cache",
    tags=["Admin/Maintenance"],
    summary="Clear similar_books cache for a book",
)
def clear_cb_cache_for_book(book_id: int):
    try:
        _call_proc("sp_clear_similar_cache_for_book", [book_id])
        return {"ok": True, "book_id": book_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
