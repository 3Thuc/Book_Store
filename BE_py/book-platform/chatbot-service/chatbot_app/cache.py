"""
cache.py – In-memory response cache cho các câu trả lời static/semi-static.

Mục đích:
- Giảm latency cho các intent không đổi thường xuyên (store_info, return_policy, payment_method)
- Tránh gọi Ollama LLM cho cùng một câu hỏi đã trả lời trong 5 phút gần đây
- Thread-safe với TTL (time-to-live) để tránh stale data

Cách dùng:
    from chatbot_app.cache import response_cache

    # Thử lấy từ cache
    cached = response_cache.get("store_info", "hotline contact")
    if cached:
        return cached

    # Tính toán và lưu cache
    result = await compute_something()
    response_cache.set("store_info", "hotline contact", result, ttl=300)
    return result
"""
import time
import hashlib
from threading import Lock
from typing import Any


class SimpleCache:
    """
    Thread-safe in-memory cache với TTL (Time-To-Live).

    Design:
    - Key = (intent, normalized_query) → giảm collision
    - TTL mặc định 5 phút (300s) cho semi-static content
    - Max 200 entries (FIFO khi đầy) – tránh memory leak
    """

    def __init__(self, default_ttl: int = 300, max_size: int = 200):
        self._store: dict[str, tuple[Any, float]] = {}  # key → (value, expire_at)
        self._lock  = Lock()
        self.default_ttl = default_ttl
        self.max_size    = max_size
        self._hits   = 0
        self._misses = 0

    # ── Public API ────────────────────────────────────────────────────────

    def get(self, intent: str, query: str) -> Any | None:
        """Lấy giá trị từ cache. Trả về None nếu không có hoặc đã hết hạn."""
        key = self._make_key(intent, query)
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            value, expire_at = entry
            if time.time() > expire_at:
                del self._store[key]
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, intent: str, query: str, value: Any, ttl: int | None = None) -> None:
        """Lưu giá trị vào cache với TTL giây."""
        if value is None:
            return
        key     = self._make_key(intent, query)
        ttl     = ttl or self.default_ttl
        expire  = time.time() + ttl
        with self._lock:
            # FIFO eviction khi đầy
            if len(self._store) >= self.max_size and key not in self._store:
                oldest_key = next(iter(self._store))
                del self._store[oldest_key]
            self._store[key] = (value, expire)

    def invalidate(self, intent: str | None = None) -> int:
        """Xóa cache theo intent. Nếu intent=None → xóa tất cả."""
        count = 0
        with self._lock:
            if intent is None:
                count = len(self._store)
                self._store.clear()
            else:
                prefix = f"{intent}:"
                keys_to_del = [k for k in self._store if k.startswith(prefix)]
                for k in keys_to_del:
                    del self._store[k]
                count = len(keys_to_del)
        return count

    def stats(self) -> dict:
        """Trả về hit rate và kích thước cache hiện tại."""
        total = self._hits + self._misses
        return {
            "size":     len(self._store),
            "max_size": self.max_size,
            "hits":     self._hits,
            "misses":   self._misses,
            "hit_rate": f"{self._hits/total:.1%}" if total else "N/A",
        }

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _make_key(intent: str, query: str) -> str:
        """Tạo key từ intent + hash của query (tránh key quá dài)."""
        # Normalize: lowercase + strip + bỏ dấu câu thừa
        normalized = query.lower().strip()
        q_hash = hashlib.md5(normalized.encode("utf-8")).hexdigest()[:8]
        return f"{intent}:{q_hash}"


# ── Singleton instance ─────────────────────────────────────────────────────────

# TTL theo loại intent (giây):
#   store_info     → 600s (10 phút) – thông tin liên hệ, giờ làm việc ít đổi
#   return_policy  → 600s           – chính sách đổi trả ít thay đổi
#   payment_method → 600s           – phương thức thanh toán ổn định
#   promotion      →  60s (1 phút)  – khuyến mãi có thể thay đổi nhanh
#   book_search    →   0s           – KHÔNG cache (mỗi query khác nhau)

CACHE_TTL: dict[str, int] = {
    "store_info":        600,
    "return_policy":     600,
    "payment_method":    600,
    "payment_info":      600,   # ← MỚI: hỏi thanh toán nhanh nhất
    "promotion_info":    120,   # ← MỚI: voucher / khuyến mãi (cập nhật nhanh hơn)
    "promotion_current":  60,
}

# Intents được phép cache – chỉ static content
CACHEABLE_INTENTS = set(CACHE_TTL.keys())

response_cache = SimpleCache(default_ttl=300, max_size=200)
