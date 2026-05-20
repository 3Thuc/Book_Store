# recommend_app/routers/recommend_router.py
# v2.1.0 — Optimizations:
#   Fix #1: X-Admin-Key authentication for admin endpoints
#   Fix #2: CF rebuild as BackgroundTask (non-blocking)
#   Fix #3: Redis cache 5 min for Popular / Trending / Top-Rated
import os
import time
import logging
from typing import List, Optional

from fastapi import APIRouter, Query, BackgroundTasks, Header, HTTPException, Depends

from recommend_app.utils import _call_proc, _rows_to_recommendations
from recommend_app.models import BookRecommendation
from recommend_app.services.diversity import mmr_diversify
from recommend_app.services.ab_testing import (
    get_bucket, log_ab_event, get_ab_stats, reset_ab_stats,
    log_ab_click, get_ab_ctr_stats,
    _get_redis,  # reuse same Redis client from ab_testing
)

logger = logging.getLogger("recommend.router")

# ─── Import content-based k-NN (SBERT via OpenSearch) ──────────────────────
try:
    from recommend_app.services.content_based.cb_service import get_similar_books as _get_cb_knn
    _CB_KNN_AVAILABLE = True
except Exception as _e:
    logger.warning("[WARN] cb_service k-NN unavailable: %s", _e)
    _CB_KNN_AVAILABLE = False

router = APIRouter(prefix="/recommend")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Fix #1 — Admin API Key Authentication                          ║
# ║  Tất cả admin endpoints đều phải gửi header X-Admin-Key        ║
# ╚══════════════════════════════════════════════════════════════════╝
def verify_admin_key(x_admin_key: str = Header(..., description="Admin API Key bảo mật endpoint quản trị")):
    """
    FastAPI Dependency — kiểm tra X-Admin-Key header.
    Key đọc từ biến môi trường ADMIN_API_KEY.
    Trả 422 nếu thiếu header, 401 nếu sai key.
    """
    expected = os.getenv("ADMIN_API_KEY", "")
    if not expected:
        # Chưa cấu hình → cho qua (dev mode), nhưng log cảnh báo
        logger.warning("[SECURITY] ADMIN_API_KEY chưa được cấu hình trong .env!")
        return
    if x_admin_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized — X-Admin-Key không hợp lệ")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  Fix #3 — Simple Redis Cache Layer                              ║
# ║  Cache Popular / Trending / Top-Rated 5 phút → giảm MySQL load ║
# ╚══════════════════════════════════════════════════════════════════╝
_CACHE_TTL = 300  # 5 phút

# In-memory fallback cache khi Redis không có
_mem_cache: dict = {}  # key -> (data, expire_at)


def _cache_get(key: str):
    """Đọc cache: Redis trước, in-memory fallback."""
    # Thử Redis
    r = _get_redis()
    if r:
        try:
            import json
            raw = r.get(f"recommend:cache:{key}")
            if raw:
                return json.loads(raw)
        except Exception:
            pass

    # Fallback in-memory
    entry = _mem_cache.get(key)
    if entry:
        data, expire_at = entry
        if time.monotonic() < expire_at:
            return data
        del _mem_cache[key]

    return None


def _cache_set(key: str, data, ttl: int = _CACHE_TTL):
    """Ghi cache: Redis trước, in-memory fallback."""
    # Thử Redis
    r = _get_redis()
    if r:
        try:
            import json
            r.setex(f"recommend:cache:{key}", ttl, json.dumps(data, default=str))
            return
        except Exception:
            pass

    # Fallback in-memory
    _mem_cache[key] = (data, time.monotonic() + ttl)


def _recs_to_json(recs: List[BookRecommendation]) -> list:
    """Chuyển Pydantic list → JSON serializable list."""
    return [r.model_dump() for r in recs]


def _json_to_recs(data: list) -> List[BookRecommendation]:
    """Khôi phục từ JSON cache → Pydantic list."""
    return [BookRecommendation(**item) for item in data]


# ─────────────────────────────────────────────────────────────────────────────
# HOME ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/popular",
    tags=["Home"],
    response_model=List[BookRecommendation],
    summary="Popular books (bán chạy) — Cached 5 phút",
)
def recommend_popular(limit: int = Query(10, ge=1, le=100)):
    # Fix #3: Cache hit → trả ngay, không query MySQL
    cache_key = f"popular:{limit}"
    cached = _cache_get(cache_key)
    if cached:
        logger.debug("[Cache HIT] popular limit=%s", limit)
        return _json_to_recs(cached)

    rows = _call_proc("sp_recommend_popular", [limit])
    recs = _rows_to_recommendations(rows, default_reason="popular")
    _cache_set(cache_key, _recs_to_json(recs))
    return recs


@router.get(
    "/trending",
    tags=["Home"],
    response_model=List[BookRecommendation],
    summary="Trending books (theo view/add_to_cart gần đây) — Cached 5 phút",
)
def recommend_trending_views(
    days: int = Query(7, ge=1, le=60),
    limit: int = Query(10, ge=1, le=100),
):
    # Fix #3: Cache hit
    cache_key = f"trending:{days}:{limit}"
    cached = _cache_get(cache_key)
    if cached:
        logger.debug("[Cache HIT] trending days=%s limit=%s", days, limit)
        return _json_to_recs(cached)

    rows = _call_proc("sp_recommend_trending_views", [days, limit])
    results = _rows_to_recommendations(rows, default_reason="trending")

    # Fallback mượn sách Popular khi trending thiếu kết quả
    if len(results) < limit:
        fb_rows = _call_proc("sp_recommend_popular", [limit * 3])
        fallback_recs = _rows_to_recommendations(fb_rows, default_reason="popular_fallback")

        seen = {r.book_id for r in results}
        offset = limit  # bỏ top N đầu popular để tránh trùng "for-you"
        for r in fallback_recs[offset:]:
            if r.book_id not in seen:
                results.append(r)
                seen.add(r.book_id)
            if len(results) >= limit:
                break

    result_page = results[:limit]
    _cache_set(cache_key, _recs_to_json(result_page))
    return result_page


@router.get(
    "/top-rated",
    tags=["Home"],
    response_model=List[BookRecommendation],
    summary="Top rated books (đánh giá cao) — Cached 5 phút",
)
def recommend_top_rated(limit: int = Query(10, ge=1, le=100)):
    # Fix #3: Cache hit
    cache_key = f"top_rated:{limit}"
    cached = _cache_get(cache_key)
    if cached:
        logger.debug("[Cache HIT] top_rated limit=%s", limit)
        return _json_to_recs(cached)

    rows = _call_proc("sp_recommend_top_rated", [limit])
    recs = _rows_to_recommendations(rows, default_reason="top_rated")
    _cache_set(cache_key, _recs_to_json(recs))
    return recs


# ─────────────────────────────────────────────────────────────────────────────
# ITEM-BASED: RULE
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# ITEM-BASED: CO-PURCHASE
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# ITEM-BASED: CONTENT-BASED (SBERT k-NN)
# ─────────────────────────────────────────────────────────────────────────────

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
    summary="Similar books SBERT k-NN + fallback popular",
)
def recommend_for_book_cb_with_fallback(
    book_id: int,
    limit: int = Query(10, ge=1, le=100),
):
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


# ─────────────────────────────────────────────────────────────────────────────
# ITEM-BASED: CF
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/book/{book_id}/cf",
    tags=["Item-based: CF"],
    response_model=List[BookRecommendation],
    summary="Similar books (CF item-item)",
)
def recommend_for_book_cf(
    book_id: int,
    algo: str = Query("CF_IMPLICIT", description="CF_IMPLICIT hoac CF_PURCHASE"),
    limit: int = Query(10, ge=1, le=100),
    diverse: bool = Query(False, description="Bat MMR diversity filter (opt-in)"),
):
    rows = _call_proc("sp_recommend_for_book_cf", [book_id, algo, limit])
    recs = _rows_to_recommendations(rows, default_reason="cf_similar")
    if diverse and recs:
        recs = mmr_diversify(recs, k=limit, lambda_=0.7, max_same_category=2)
    return recs


# ─────────────────────────────────────────────────────────────────────────────
# PERSONALIZED: RULE
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# PERSONALIZED: CF CACHE
# ─────────────────────────────────────────────────────────────────────────────

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
    rows = _call_proc("sp_recommend_for_user_from_cache", [user_id, "CF", limit])
    return _rows_to_recommendations(rows, default_reason="cf_cached")


# ─────────────────────────────────────────────────────────────────────────────
# PERSONALIZED: FOR YOU (Cascade + A/B + MMR)
# ─────────────────────────────────────────────────────────────────────────────

def _rebuild_user_cf_background(user_id: int, days: int = 90, topn: int = 50):
    """
    Fix #2: Chạy rebuild CF trong background — không block HTTP response.
    Chỉ log lỗi, không raise exception (background task không có response).
    """
    try:
        logger.info("[BG] Rebuilding CF cache for user %s...", user_id)
        _call_proc("sp_rebuild_user_cf_implicit", [user_id, days, topn])
        logger.info("[BG] CF rebuild done for user %s", user_id)
    except Exception as e:
        logger.error("[BG] CF rebuild failed for user %s: %s", user_id, e)


@router.get(
    "/user/{user_id}/for-you",
    tags=["Personalized: For You"],
    response_model=List[BookRecommendation],
    summary="For you (CF + fallback rule + popular) — v2.1.0: rebuild non-blocking",
)
def recommend_for_you(
    user_id: int,
    background_tasks: BackgroundTasks,
    limit: int = Query(20, ge=1, le=100),
    diverse: bool = Query(True, description="MMR diversity (default=True)."),
    ab_override: Optional[str] = Query(None, description="Override bucket: sbert_knn|cf_als|rule_based"),
):
    # A/B bucket xác định
    bucket = ab_override if ab_override in ("sbert_knn", "cf_als", "rule_based") else get_bucket(user_id)
    log_ab_event(user_id, bucket, endpoint="for_you")

    # 1) Đọc CF cache
    rows = _call_proc("sp_recommend_for_user_from_cache", [user_id, "CF", limit])
    recs = _rows_to_recommendations(rows, default_reason="cf_cached")

    # Fix #2: Nếu cache rỗng (user mới) → schedule rebuild trong background,
    # KHÔNG chờ → trả về fallback ngay lập tức
    if len(recs) == 0:
        logger.info("[ForYou] User %s: CF cache empty, scheduling BG rebuild", user_id)
        background_tasks.add_task(_rebuild_user_cf_background, user_id, 90, 50)
        # Không chờ rebuild → tiếp tục fallback bên dưới ngay

    # 2) Fallback rule-based theo category đã mua
    if len(recs) < limit:
        rows2 = _call_proc("sp_recommend_for_user_rule_based", [user_id, limit - len(recs)])
        recs += _rows_to_recommendations(rows2, default_reason="popular_in_user_categories")

    # 3) Fallback popular
    if len(recs) < limit:
        rows3 = _call_proc("sp_recommend_popular", [limit - len(recs)])
        recs += _rows_to_recommendations(rows3, default_reason="popular")

    # Dedup theo book_id
    seen: set = set()
    out: List[BookRecommendation] = []
    for r in recs:
        if r.book_id in seen:
            continue
        seen.add(r.book_id)
        out.append(r)
        if len(out) >= limit:
            break

    # MMR Diversity (default=True)
    if diverse and out:
        out = mmr_diversify(out, k=limit, lambda_=0.7, max_same_category=3)

    return out


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN / MAINTENANCE — Fix #1: Tất cả đều yêu cầu X-Admin-Key
# ─────────────────────────────────────────────────────────────────────────────

@router.post(
    "/user/{user_id}/cf/rebuild",
    tags=["Admin/Maintenance"],
    summary="[Admin] Rebuild CF cache for a user — yêu cầu X-Admin-Key",
    dependencies=[Depends(verify_admin_key)],
)
def rebuild_user_cf(
    user_id: int,
    days: int = Query(90, ge=1, le=365),
    topn: int = Query(50, ge=1, le=200),
):
    """
    Fix #1: Yêu cầu header X-Admin-Key.
    Rebuild CF cache đồng bộ vì đây là admin action có chủ đích.
    """
    _call_proc("sp_rebuild_user_cf_implicit", [user_id, days, topn])
    return {"ok": True, "user_id": user_id, "days": days, "topn": topn}


@router.post(
    "/book/{book_id}/cb/clear-cache",
    tags=["Admin/Maintenance"],
    summary="[Admin] Clear similar_books cache for a book — yêu cầu X-Admin-Key",
    dependencies=[Depends(verify_admin_key)],
)
def clear_cb_cache_for_book(book_id: int):
    """Fix #1: Yêu cầu header X-Admin-Key."""
    try:
        _call_proc("sp_clear_similar_cache_for_book", [book_id])
        return {"ok": True, "book_id": book_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# A/B TESTING ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/ab/stats",
    tags=["Admin/A-B Testing"],
    summary="Xem thống kê phân bổ A/B Testing",
)
def ab_stats():
    return get_ab_stats()


@router.get(
    "/ab/my-bucket/{user_id}",
    tags=["Admin/A-B Testing"],
    summary="Xem bucket của 1 user cụ thể",
)
def ab_user_bucket(user_id: int):
    bucket = get_bucket(user_id)
    from recommend_app.services.ab_testing import BUCKET_CONFIG
    return {
        "user_id":    user_id,
        "bucket":     bucket,
        "slot":       user_id % 100,
        "label":      BUCKET_CONFIG[bucket]["label"],
        "config_pct": BUCKET_CONFIG[bucket]["range"],
    }


@router.post(
    "/ab/reset",
    tags=["Admin/A-B Testing"],
    summary="[Admin] Reset stats A/B — yêu cầu X-Admin-Key",
    dependencies=[Depends(verify_admin_key)],  # Fix #1
)
def ab_reset():
    """Fix #1: Reset A/B data cần xác thực — tránh mất data thực nghiệm."""
    return reset_ab_stats()


@router.post(
    "/ab/click",
    tags=["Admin/A-B Testing"],
    summary="Ghi click khi user click vào sách được gợi ý",
)
def ab_track_click(
    user_id: int = Query(..., description="ID người dùng"),
    book_id: int = Query(..., description="ID sách vừa được click"),
    position: int = Query(0, description="Vị trí sách trong list (0=đầu tiên)"),
):
    """
    Frontend gọi endpoint này khi user click vào sách trong phần 'For You'.
    Không cần Admin Key — đây là public action của user.
    """
    log_ab_click(user_id=user_id, book_id=book_id, position=position)
    bucket = get_bucket(user_id)
    return {
        "ok": True,
        "user_id": user_id,
        "book_id": book_id,
        "bucket": bucket,
        "position": position,
        "message": f"Click recorded for bucket '{bucket}'"
    }


@router.get(
    "/ab/ctr",
    tags=["Admin/A-B Testing"],
    summary="Xem CTR (Click-Through Rate) từng bucket A/B",
)
def ab_ctr_stats():
    """CTR = clicks / exposures. Bucket CTR cao → thuật toán tốt hơn."""
    return get_ab_ctr_stats()
