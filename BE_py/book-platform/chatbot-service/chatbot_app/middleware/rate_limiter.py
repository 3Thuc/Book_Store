"""
middleware/rate_limiter.py – SlowAPI rate limiter cho Chatbot Service.

Giới hạn request để chống:
  - Spam câu hỏi liên tục → Ollama CPU/VRAM 100%
  - Brute-force qua Staff/Admin chatbot để lấy dữ liệu
  - DoS đơn giản từ script tự động

Chiến lược theo role:
  Customer  → 20/minute  (thoải mái cho UX thông thường)
  Staff     → 20/minute  (tra cứu nội bộ nhanh)
  Admin     → 10/minute  (query phức tạp hơn, cần giới hạn chặt hơn)
  History   → 30/minute  (GET, nhẹ hơn POST)

Key: IP-based (X-Forwarded-For → fallback client host)
"""
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from fastapi.responses import JSONResponse


# ── Limiter instance (dùng chung toàn service) ──────────────────────────────
limiter = Limiter(key_func=get_remote_address)


# ── Custom error handler ─────────────────────────────────────────────────────
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """
    Trả về lỗi thân thiện thay vì HTTP 429 mặc định của slowapi.
    Frontend hiển thị thông báo này cho user.
    """
    return JSONResponse(
        status_code=429,
        content={
            "detail": (
                "Bạn đang gửi quá nhiều tin nhắn. "
                "Vui lòng chờ 1 phút rồi thử lại! 🙏"
            ),
            "error": "rate_limit_exceeded",
            "limit": str(exc.detail),
        },
        headers={"Retry-After": "60"},
    )
