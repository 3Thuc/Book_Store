"""
P3.1 – A/B Testing Framework cho Recommendation
================================================
Chia user vao 3 bucket tu dong theo user_id % 3:
  - Bucket 0 (50%): SBERT k-NN (content-based, AI semantic)
  - Bucket 1 (30%): CF ALS    (collaborative filtering)
  - Bucket 2 (20%): Rule-based (category/author popularity)

Tracking:
  - Moi lan goi endpoint /user/{id}/for-you, ghi bucket vao Redis
  - Analytics endpoint /recommend/ab/stats de xem phan bo

Thiet ke don gian (khong can tracking CTR phuc tap):
  - Phase 3: Chi bucket assignment + log
  - Future: Them click tracking, conversion rate de quyet dinh winner
"""
import logging
import time
import os
from typing import Optional

logger = logging.getLogger("recommend.ab")

# ── Cau hinh bucket ─────────────────────────────────────────────────────────
# Phan tram theo user_id % 100
BUCKET_CONFIG = {
    "sbert_knn":   {"range": (0,  49), "label": "SBERT k-NN (semantic AI)"},
    "cf_als":      {"range": (50, 79), "label": "CF ALS (collaborative filtering)"},
    "rule_based":  {"range": (80, 99), "label": "Rule-based (category/author)"},
}

# ── Redis — Fix #4: Retry with cooldown thay vì cache failure vĩnh viễn ─────
# Vấn đề cũ: _redis_client = False → False is not None → return False mãi mãi
# Fix: dùng _redis_last_fail timestamp + cooldown 30s → tự phục hồi sau restart
_redis_client = None
_redis_last_fail: float = 0.0
_REDIS_RETRY_INTERVAL: float = 30.0  # Retry sau 30 giây kể từ lần fail cuối


def _get_redis():
    """
    Lấy Redis client với cơ chế retry thông minh.
    - Nếu đang connected → trả về client (tái sử dụng)
    - Nếu fail gần đây (< 30s) → bỏ qua để tránh spam retry
    - Sau 30s → thử kết nối lại tự động (graceful recovery)
    """
    global _redis_client, _redis_last_fail

    # Client đang live → dùng lại
    if _redis_client:  # None và False đều falsy → xử lý đúng cả hai
        return _redis_client

    # Trong cooldown → không thử lại
    if time.monotonic() - _redis_last_fail < _REDIS_RETRY_INTERVAL:
        return None

    # Hết cooldown → thử kết nối
    try:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = redis.from_url(url, socket_connect_timeout=1, decode_responses=True)
        client.ping()
        _redis_client = client  # Thành công → cache client
        logger.info("[A/B] Redis connected: %s", url)
        return _redis_client
    except Exception as e:
        _redis_last_fail = time.monotonic()  # Ghi thời điểm fail
        _redis_client = None  # Không cache False → sẽ retry sau 30s
        logger.debug("[A/B] Redis unavailable (retry in 30s): %s", e)
        return None


# ── Public API ───────────────────────────────────────────────────────────────
def get_bucket(user_id: int) -> str:
    """
    Xac dinh bucket A/B cua user dua theo user_id % 100.
    Stable: cung user_id luon nhan cung bucket.

    Returns:
        "sbert_knn" | "cf_als" | "rule_based"
    """
    slot = user_id % 100
    if slot <= 49:
        return "sbert_knn"
    if slot <= 79:
        return "cf_als"
    return "rule_based"


def log_ab_event(user_id: int, bucket: str, endpoint: str = "for_you") -> None:
    """
    Ghi 1 A/B exposure event vao Redis.
    Non-blocking: exception bi suppress.

    Keys:
      recommend:ab:counts:{bucket}   -> INCR (dem tong exposure)
      recommend:ab:users:{bucket}    -> SADD user_id (dem unique users)
    """
    r = _get_redis()
    if not r:
        return
    try:
        pipe = r.pipeline(transaction=False)
        pipe.incr(f"recommend:ab:counts:{bucket}")
        pipe.sadd(f"recommend:ab:users:{bucket}", str(user_id))
        pipe.execute()
    except Exception as e:
        logger.debug("[A/B] log_ab_event failed: %s", e)


def get_ab_stats() -> dict:
    """
    Tra ve thong ke phan bo A/B testing.
    """
    r = _get_redis()
    if not r:
        return {"status": "redis_unavailable"}

    try:
        stats = {}
        total_exposures = 0
        total_users     = 0

        for bucket_name, cfg in BUCKET_CONFIG.items():
            count    = int(r.get(f"recommend:ab:counts:{bucket_name}") or 0)
            uniq     = r.scard(f"recommend:ab:users:{bucket_name}")
            lo, hi   = cfg["range"]
            pct_conf = hi - lo + 1  # Phan tram cau hinh

            stats[bucket_name] = {
                "label":           cfg["label"],
                "config_pct":      pct_conf,
                "exposures":       count,
                "unique_users":    uniq,
                "user_id_range":   f"{lo}-{hi} (mod 100)",
            }
            total_exposures += count
            total_users     += uniq

        # Tinh phan tram thuc te
        for bucket_name in stats:
            exp = stats[bucket_name]["exposures"]
            stats[bucket_name]["actual_pct"] = (
                round(exp / total_exposures * 100, 1) if total_exposures > 0 else 0
            )

        return {
            "buckets":         stats,
            "total_exposures": total_exposures,
            "total_users":     total_users,
            "note":            "Bucket phan cong theo user_id % 100. Stable: cung user -> cung bucket.",
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def reset_ab_stats() -> dict:
    """Xoa toan bo stats A/B (dung khi bat dau experiment moi)."""
    r = _get_redis()
    if not r:
        return {"status": "redis_unavailable"}
    try:
        keys = []
        for b in BUCKET_CONFIG:
            keys += [
                f"recommend:ab:counts:{b}",
                f"recommend:ab:users:{b}",
                f"recommend:ab:clicks:{b}",      # P4 CTR
                f"recommend:ab:click_users:{b}",  # P4 CTR unique clickers
            ]
        if keys:
            r.delete(*keys)
        return {"status": "ok", "deleted_keys": len(keys)}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── P4: CTR Tracking ─────────────────────────────────────────────────────────
def log_ab_click(user_id: int, book_id: int, position: int = 0) -> None:
    """
    Ghi 1 click event khi user click vao sach duoc goi y.
    Dung de tinh CTR (Click-Through Rate) cho moi bucket.

    Args:
        user_id:  ID nguoi dung click
        book_id:  ID sach duoc click
        position: Vi tri sach trong danh sach (0=dau tien)

    Redis keys:
        recommend:ab:clicks:{bucket}       -> INCR (dem tong click)
        recommend:ab:click_users:{bucket}  -> SADD (unique clickers)
    """
    r = _get_redis()
    if not r:
        return
    try:
        bucket = get_bucket(user_id)
        pipe = r.pipeline(transaction=False)
        pipe.incr(f"recommend:ab:clicks:{bucket}")
        pipe.sadd(f"recommend:ab:click_users:{bucket}", str(user_id))
        # Ghi click detail la sorted set (book_id -> click count)
        pipe.zincrby(f"recommend:ab:top_clicked:{bucket}", 1, str(book_id))
        pipe.execute()
        logger.debug("[A/B CTR] user=%s bucket=%s book=%s pos=%s", user_id, bucket, book_id, position)
    except Exception as e:
        logger.debug("[A/B CTR] log_ab_click failed: %s", e)


def get_ab_ctr_stats() -> dict:
    """
    Tra ve CTR chi tiet cho tung bucket.
    CTR = clicks / exposures * 100%
    """
    r = _get_redis()
    if not r:
        return {"status": "redis_unavailable"}

    try:
        result = {}
        for bucket_name, cfg in BUCKET_CONFIG.items():
            exposures  = int(r.get(f"recommend:ab:counts:{bucket_name}") or 0)
            clicks     = int(r.get(f"recommend:ab:clicks:{bucket_name}") or 0)
            clickers   = r.scard(f"recommend:ab:click_users:{bucket_name}")
            ctr        = round(clicks / exposures * 100, 2) if exposures > 0 else 0.0
            # Top 5 sach duoc click nhieu nhat trong bucket nay
            top_books  = r.zrevrange(f"recommend:ab:top_clicked:{bucket_name}", 0, 4, withscores=True)

            result[bucket_name] = {
                "label":        cfg["label"],
                "exposures":    exposures,
                "clicks":       clicks,
                "ctr_pct":      ctr,
                "unique_clickers": clickers,
                "top_clicked_books": [
                    {"book_id": int(bid), "clicks": int(cnt)}
                    for bid, cnt in top_books
                ],
            }

        total_clicks    = sum(v["clicks"] for v in result.values())
        total_exposures = sum(v["exposures"] for v in result.values())

        return {
            "buckets":          result,
            "total_exposures":  total_exposures,
            "total_clicks":     total_clicks,
            "overall_ctr_pct":  round(total_clicks / total_exposures * 100, 2) if total_exposures > 0 else 0.0,
            "note": "CTR = clicks / exposures. Bucket co CTR cao hon = thuat toan tot hon.",
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}

