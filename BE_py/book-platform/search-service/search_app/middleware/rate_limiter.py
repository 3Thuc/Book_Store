"""
rate_limiter.py – Cấu hình Rate Limiting cho Search Service (Phase 1.2)

Dùng slowapi (wrapper của limits library cho FastAPI).

Giới hạn:
  - /books/search       : 60 requests/phút (1 req/giây trung bình)
  - /books/ocr-search   : 20 requests/phút (OCR downstream nặng hơn)
  - /books/suggest      : 120 requests/phút (lightweight autocomplete)

Chiến lược:
  - Key = IP address (get_remote_address)
  - Storage = in-memory (không cần Redis cho rate limit nhỏ)
  - Response khi vượt limit: HTTP 429 Too Many Requests
"""
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

# Singleton limiter – import từ router để dùng chung
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["200/minute"],   # global ceiling cho toàn service
)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Custom 429 response thay vì mặc định của slowapi."""
    logger.warning(
        "Rate limit exceeded: %s %s – IP: %s",
        request.method, request.url.path,
        get_remote_address(request),
    )
    return JSONResponse(
        status_code=429,
        content={
            "error": "rate_limit_exceeded",
            "message": (
                "Bạn đã gửi quá nhiều yêu cầu. "
                "Vui lòng đợi vài giây rồi thử lại."
            ),
            "retry_after": str(exc.limit.limit),
        },
        headers={"Retry-After": "60"},
    )
