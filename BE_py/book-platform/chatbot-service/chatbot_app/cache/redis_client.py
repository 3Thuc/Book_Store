"""
redis_client.py – Redis client cho Chatbot Session Store (Phase 1.3)

Chiến lược:
  1. Kết nối Redis qua REDIS_URL env (redis://redis:6379/1 trong Docker)
  2. Nếu Redis không sẵn sàng → fallback về in-memory (giống cũ)
  3. Serialize/Deserialize context dict bằng JSON

Tại sao Redis thay vì chỉ in-memory?
  - Multi-instance deployment: nhiều uvicorn worker dùng chung session
  - Persist qua restart: context không mất khi redeploy
  - TTL tự động: Redis tự xóa session cũ, không cần eviction logic

Keys format:
  chat:session:{session_id}  → JSON context (TTL: 2 giờ)
"""
import json
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from chatbot_app.config import REDIS_URL
except ImportError:
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")
SESSION_TTL = 7200   # 2 giờ – đủ dài cho 1 phiên chat dù gián đoạn
_PREFIX = "chat:session:"

# ── Lazy singleton ────────────────────────────────────────────────────────────
_redis_pool = None
_redis_available = False


def _get_pool():
    """Khởi tạo Redis connection pool (lazy, thread-safe import)."""
    global _redis_pool, _redis_available
    if _redis_pool is not None:
        return _redis_pool, _redis_available

    try:
        import redis
        pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=1,    # fail fast nếu Redis chưa chạy
            socket_timeout=1,
            max_connections=20,
        )
        # Test connection
        client = redis.Redis(connection_pool=pool)
        client.ping()

        _redis_pool = pool
        _redis_available = True
        logger.info("✅ [redis_client] Kết nối Redis thành công: %s", REDIS_URL)
    except Exception as e:
        _redis_pool = None
        _redis_available = False
        logger.warning(
            "⚠️ [redis_client] Redis không sẵn sàng (%s). "
            "Dùng in-memory fallback.", e
        )

    return _redis_pool, _redis_available


def _get_client():
    """Trả về Redis client hoặc None nếu không sẵn sàng."""
    pool, available = _get_pool()
    if not available or pool is None:
        return None
    try:
        import redis
        return redis.Redis(connection_pool=pool)
    except Exception:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def redis_get_session(session_id: str) -> Optional[dict]:
    """
    Lấy context từ Redis.
    Trả về dict nếu tìm thấy, None nếu miss hoặc Redis lỗi.
    """
    client = _get_client()
    if client is None:
        return None
    try:
        raw = client.get(f"{_PREFIX}{session_id}")
        if raw:
            return json.loads(raw)
    except Exception as e:
        logger.debug("[redis_get_session] error: %s", e)
    return None


def redis_set_session(session_id: str, context: dict, ttl: int = SESSION_TTL) -> bool:
    """
    Lưu context vào Redis với TTL.
    Trả về True nếu thành công.
    """
    client = _get_client()
    if client is None:
        return False
    try:
        # Bỏ runtime-only fields trước khi serialize
        ctx_to_save = {
            k: v for k, v in context.items()
            if k not in ("is_guest", "user_id")
        }
        client.setex(
            f"{_PREFIX}{session_id}",
            ttl,
            json.dumps(ctx_to_save, ensure_ascii=False),
        )
        return True
    except Exception as e:
        logger.debug("[redis_set_session] error: %s", e)
        return False


def redis_delete_session(session_id: str) -> bool:
    """Xóa session khỏi Redis (logout / force refresh)."""
    client = _get_client()
    if client is None:
        return False
    try:
        client.delete(f"{_PREFIX}{session_id}")
        return True
    except Exception as e:
        logger.debug("[redis_delete_session] error: %s", e)
        return False


def redis_health() -> dict:
    """Health check endpoint info."""
    _, available = _get_pool()
    if not available:
        return {"redis": "unavailable", "fallback": "in-memory"}
    try:
        client = _get_client()
        if client:
            info = client.info("memory")
            return {
                "redis": "ok",
                "url": REDIS_URL,
                "used_memory_human": info.get("used_memory_human", "?"),
            }
    except Exception as e:
        return {"redis": "error", "detail": str(e)}
    return {"redis": "unavailable"}
