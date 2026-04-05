# app/middleware.py
from fastapi import Request
from fastapi.responses import JSONResponse


async def log_errors_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        print(f"[UNCAUGHT ERROR] {request.method} {request.url} -> {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error (uncaught)."}
        )
