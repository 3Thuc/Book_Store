"""
rate_limiter.py – Rate Limiting cho OCR Service (Phase 1.2)

Giới hạn:
  - POST /api/ocr/search-by-cover    : 10 requests/phút/IP (OCR nặng nhất)
  - POST /api/ocr/extract-book-info  : 15 requests/phút/IP
  - POST /api/ocr/scan-receipt       : 10 requests/phút/IP
  - GET  /api/ocr/health             : unlimited (health check không cần limit)

Lý do giới hạn chặt OCR:
  - EasyOCR CPU mode: 1-3 giây/ảnh, 3 worker threads
  - Nếu không có rate limit: burst 50 req/giây → OOM crash
  - 10 req/phút = đủ cho user bình thường, không ảnh hưởng UX
"""
import logging
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["30/minute"],    # global ceiling cho toàn OCR service
)


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Custom 429 response."""
    logger.warning(
        "OCR Rate limit exceeded: %s – IP: %s",
        request.url.path,
        get_remote_address(request),
    )
    return JSONResponse(
        status_code=429,
        content={
            "error": "rate_limit_exceeded",
            "message": (
                "Bạn đã gửi quá nhiều ảnh trong thời gian ngắn. "
                "Vui lòng đợi 1 phút rồi thử lại (giới hạn 10 ảnh/phút)."
            ),
        },
        headers={"Retry-After": "60"},
    )
