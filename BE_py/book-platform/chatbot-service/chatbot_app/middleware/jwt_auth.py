"""
middleware/jwt_auth.py – JWT Authentication + Role Authorization dependency.

Mục đích:
  - Đảm bảo chỉ user có JWT hợp lệ mới gọi được Staff/Admin chatbot
  - Đọc role từ JWT claims → đối chiếu với endpoint yêu cầu
  - Tránh tình trạng bất kỳ ai tự xưng role="admin" trong request body

Cơ chế:
  Header: Authorization: Bearer <JWT>
  Payload: {sub: user_id, role: "staff"|"admin", exp: ...}

  Dependency FastAPI:
    async def verify_staff(payload = Depends(require_staff_role)):  → 401/403 nếu sai
    async def verify_admin(payload = Depends(require_admin_role)):  → 401/403 nếu sai

Cấu hình:
  JWT_SECRET_KEY: đọc từ env var (phải khớp với Spring Boot BookStorage)
  JWT_ALGORITHM:  HS256 (mặc định của Spring Boot jjwt)

Fallback khi JWT_SECRET_KEY chưa set:
  Dev mode → log WARNING, cho qua (KHÔNG chặn)
  Production → nên set JWT_SECRET_KEY trong .env
"""
import logging
import os
from typing import Optional

from fastapi import Header, HTTPException, status

logger = logging.getLogger("chatbot.auth")

# ── Đọc JWT secret từ env (khớp cùng key với Spring Boot) ───────────────────
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "")
JWT_ALGORITHM  = os.getenv("JWT_ALGORITHM", "HS256")


def _decode_jwt(token: str) -> Optional[dict]:
    """
    Decode và verify JWT token.
    Trả về payload dict nếu hợp lệ, None nếu lỗi.
    """
    try:
        import jwt as pyjwt   # PyJWT library
        payload = pyjwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
        )
        return payload
    except Exception as e:
        logger.warning("JWT decode failed: %s", e)
        return None


async def _get_jwt_payload(authorization: Optional[str] = Header(None)) -> Optional[dict]:
    """
    Trích xuất và decode JWT từ Authorization header.
    Trả về payload nếu hợp lệ.
    """
    if not JWT_SECRET_KEY:
        # Dev mode: chưa cấu hình secret key → warn + skip (không chặn)
        logger.warning(
            "JWT_SECRET_KEY chưa được cấu hình trong .env – bỏ qua auth check (dev mode)"
        )
        return {"sub": "dev", "role": "admin", "dev_mode": True}

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Yêu cầu đăng nhập. Vui lòng cung cấp JWT token hợp lệ.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization.removeprefix("Bearer ").strip()
    payload = _decode_jwt(token)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token không hợp lệ hoặc đã hết hạn. Vui lòng đăng nhập lại.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload


async def require_staff_role(authorization: Optional[str] = Header(None)) -> dict:
    """
    FastAPI Dependency – Yêu cầu role staff HOẶC admin.
    Dùng cho: POST /api/staff/chat/message, GET /api/staff/chat/history/{id}

    Returns: JWT payload dict (có sub, role, exp...)
    Raises: HTTP 401 nếu không có token, HTTP 403 nếu sai role
    """
    payload = await _get_jwt_payload(authorization)
    role = payload.get("role", "").lower()

    # Admin cũng được phép vào Staff endpoints
    if role not in ("staff", "admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Truy cập bị từ chối. Endpoint này yêu cầu role Staff hoặc Admin (role hiện tại: {role}).",
        )
    return payload


async def require_admin_role(authorization: Optional[str] = Header(None)) -> dict:
    """
    FastAPI Dependency – Yêu cầu role admin.
    Dùng cho: POST /api/admin/chat/message, GET /api/admin/chat/history/{id}

    Returns: JWT payload dict
    Raises: HTTP 401 nếu không có token, HTTP 403 nếu sai role
    """
    payload = await _get_jwt_payload(authorization)
    role = payload.get("role", "").lower()

    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Truy cập bị từ chối. Endpoint này chỉ dành cho Admin (role hiện tại: {role}).",
        )
    return payload
