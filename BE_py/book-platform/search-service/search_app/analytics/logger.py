"""
Search Analytics Logger – P2.1
Ghi log non-blocking vao Redis DB0 sau moi search query.
Graceful skip neu Redis khong chay.

Fix #6: Redis retry logic — thay vì cache False vĩnh viễn khi Redis fail,
nay chờ _REDIS_RETRY_INTERVAL giây trước khi thử lại.
Điều này giúp analytics tự động phục hồi khi Redis khởi động chậm.
"""
import json
import logging
import time
from typing import Optional

logger = logging.getLogger("search.analytics")

_redis_client = None
_redis_last_fail: float = 0.0
_REDIS_RETRY_INTERVAL: float = 30.0  # Thử kết nối lại sau 30 giây


def _get_redis():
    """
    Lazy init Redis, trả None nếu không kết nối được.

    Fix #6: Không cache False vĩnh viễn. Sau _REDIS_RETRY_INTERVAL giây
    sẽ tự động thử lại → analytics phục hồi khi Redis restart/khởi động chậm.
    """
    global _redis_client, _redis_last_fail

    # Đã kết nối thành công → dùng lại
    if _redis_client and _redis_client is not False:
        return _redis_client

    # Cooldown: chưa đủ thời gian retry → bỏ qua
    if time.monotonic() - _redis_last_fail < _REDIS_RETRY_INTERVAL:
        return None

    # Thử kết nối / kết nối lại
    try:
        import redis
        import os
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        client = redis.from_url(url, socket_connect_timeout=1,
                                socket_timeout=1, decode_responses=True)
        client.ping()
        _redis_client = client
        logger.info("[Analytics] Redis connected – analytics enabled")
        return _redis_client
    except Exception as e:
        _redis_last_fail = time.monotonic()  # Ghi lại thời điểm fail
        _redis_client = None                  # Không cache False, để retry sau
        logger.warning("[Analytics] Redis unavailable – sẽ thử lại sau %.0fs (%s)",
                       _REDIS_RETRY_INTERVAL, e)
        return None


# Keys
_KEY_EVENTS   = "search:analytics:events"    # List cac event (newest first)
_KEY_COUNTERS = "search:analytics:query_cnt" # Hash: query -> count
_KEY_MISS     = "search:analytics:misses"    # Hash: query -> miss_count
_MAX_EVENTS   = 10_000


def log_search_event(
    q: str,
    total: int,
    latency_ms: int,
    user_id: Optional[int] = None,
):
    """
    Ghi 1 search event vao Redis. Non-blocking: exception bi suppress.
    - DB0 key search:analytics:events     -> list JSON events (moi nhat truoc)
    - DB0 key search:analytics:query_cnt  -> hash dem tan suat query
    - DB0 key search:analytics:misses     -> hash dem miss (total=0)
    """
    r = _get_redis()
    if not r:
        return

    q_norm = q.strip().lower()[:100]  # Chuan hoa de group cac query giong nhau
    is_miss = total == 0

    try:
        event = {
            "q":    q_norm,
            "n":    total,
            "ms":   latency_ms,
            "miss": is_miss,
            "ts":   round(time.time(), 3),
        }
        if user_id:
            event["uid"] = user_id

        pipe = r.pipeline(transaction=False)
        # Ghi event vao list (newest first)
        pipe.lpush(_KEY_EVENTS, json.dumps(event))
        pipe.ltrim(_KEY_EVENTS, 0, _MAX_EVENTS - 1)
        # Tang counter tan suat
        pipe.hincrby(_KEY_COUNTERS, q_norm, 1)
        # Neu miss, tang miss counter
        if is_miss:
            pipe.hincrby(_KEY_MISS, q_norm, 1)
        pipe.execute()

    except Exception as exc:
        # Tuyet doi khong lam cham search chinh
        logger.debug("[Analytics] log_search_event failed (silent): %s", exc)


def get_top_queries(n: int = 20) -> list:
    """Tra ve top-n query pho bien nhat."""
    r = _get_redis()
    if not r:
        return []
    try:
        items = r.hgetall(_KEY_COUNTERS)
        sorted_items = sorted(items.items(), key=lambda x: int(x[1]), reverse=True)
        return [{"query": k, "count": int(v)} for k, v in sorted_items[:n]]
    except Exception:
        return []


def get_miss_queries(n: int = 20) -> list:
    """Tra ve top-n query khong co ket qua (miss)."""
    r = _get_redis()
    if not r:
        return []
    try:
        items = r.hgetall(_KEY_MISS)
        sorted_items = sorted(items.items(), key=lambda x: int(x[1]), reverse=True)
        return [{"query": k, "miss_count": int(v)} for k, v in sorted_items[:n]]
    except Exception:
        return []


def get_summary() -> dict:
    """Tong hop: tong search, miss rate, top queries, latency trung binh."""
    r = _get_redis()
    if not r:
        return {"status": "redis_unavailable"}
    try:
        total_queries = sum(int(v) for v in r.hvals(_KEY_COUNTERS))
        total_misses  = sum(int(v) for v in r.hvals(_KEY_MISS))
        miss_rate = round(total_misses / total_queries * 100, 2) if total_queries > 0 else 0

        # Doc 100 event gan nhat de tinh latency trung binh
        raw_events = r.lrange(_KEY_EVENTS, 0, 99)
        events = [json.loads(e) for e in raw_events]
        avg_ms = round(sum(e["ms"] for e in events) / len(events), 1) if events else 0

        return {
            "total_queries":     total_queries,
            "total_misses":      total_misses,
            "miss_rate_pct":     miss_rate,
            "avg_latency_ms":    avg_ms,
            "unique_queries":    r.hlen(_KEY_COUNTERS),
            "events_stored":     r.llen(_KEY_EVENTS),
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
