from pathlib import Path
import sys
import importlib
import asyncio
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

logger = logging.getLogger("platform_api")

BASE_DIR = Path(__file__).resolve().parents[1]

# add path trước
sys.path.insert(0, str(BASE_DIR / "search-service"))
sys.path.insert(0, str(BASE_DIR / "recommendation-service"))
sys.path.insert(0, str(BASE_DIR / "chatbot-service"))       # ← CHATBOT

load_dotenv(BASE_DIR / ".env")

search_router = importlib.import_module(
    "search_app.routers.search_router").router
admin_search_router = importlib.import_module(
    "search_app.routers.admin_search_router").router
analytics_router = importlib.import_module(
    "search_app.routers.analytics_router").router   # P2.1 Search Analytics

recommend_router = importlib.import_module(
    "recommend_app.routers.recommend_router").router

start_worker = importlib.import_module(
    "search_app.workers.index_queue").start_worker

customer_chat_router = importlib.import_module("chatbot_app.routers.customer_chat_router").router
staff_chat_router = importlib.import_module("chatbot_app.routers.staff_chat_router").router
admin_chat_router_chat = importlib.import_module("chatbot_app.routers.admin_chat_router").router

app = FastAPI(title="BOOK-PLATFORM API")

# Enable CORS để FE gọi được
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://localhost:3000",
        "http://localhost:5173",
        "https://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(recommend_router)
app.include_router(search_router)
app.include_router(admin_search_router)
app.include_router(analytics_router)          # P2.1 Search Analytics
app.include_router(customer_chat_router)
app.include_router(staff_chat_router)
app.include_router(admin_chat_router_chat)


# ──────────────────────────────────────────────
# Health Check – dùng cho Docker HEALTHCHECK
# GET /health → {"status": "ok"} với HTTP 200
# ──────────────────────────────────────────────
@app.get("/health", tags=["System"], summary="Health check endpoint")
def health_check():
    return {"status": "ok", "service": "platform_api"}


# ──────────────────────────────────────────────
# Startup: Pre-warm recommendation cache
# Gọi /for-you cho top users trước khi có request thật
# → Giảm cold-start latency từ 755ms → <10ms
# ──────────────────────────────────────────────
async def _prewarm_recommendation_cache():
    """Pre-warm Redis cache cho top 5 users phổ biến nhất."""
    await asyncio.sleep(10)  # Đợi service và DB connection pool ổn định
    try:
        import urllib.request
        TOP_USERS = list(range(1, 6))  # Chỉ warm 5 users, tránh pool exhausted
        warmed = 0
        for uid in TOP_USERS:
            try:
                url = f"http://localhost:8000/recommend/user/{uid}/for-you?limit=10"
                with urllib.request.urlopen(url, timeout=15):
                    warmed += 1
                await asyncio.sleep(1)  # Delay 1s giữa mỗi request, tránh quá tải MySQL pool
            except Exception:
                pass  # Bỏ qua nếu user không tồn tại
        logger.info(f"[PREWARM] Recommendation cache warmed for {warmed}/{len(TOP_USERS)} users")
    except Exception as e:
        logger.warning(f"[PREWARM] Cache pre-warm failed (non-critical): {e}")



@app.on_event("startup")
async def startup_event():
    logger.info("[STARTUP] Platform API is starting...")
    # Khởi động AsyncIO Index Queue Worker để sync sách mới vào OpenSearch
    start_worker()
    logger.info("[STARTUP] OpenSearch Index Queue Worker started.")
    # Chạy pre-warm trong background, không block startup
    asyncio.create_task(_prewarm_recommendation_cache())


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("[SHUTDOWN] Platform API is shutting down...")
