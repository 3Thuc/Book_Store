# app/main.py — Recommendation Service v2.1.0
# Changes: Admin Auth, Non-blocking CF Rebuild, Redis Cache, Redis Retry
import logging
from fastapi import FastAPI
from recommend_app.routers.recommend_router import router as recommend_router
from recommend_app.middleware import log_errors_middleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

tags_metadata = [
    {"name": "Home", "description": "Gợi ý cho trang chủ (không cần user)."},
    {"name": "Item-based: Content",
        "description": "Gợi ý theo nội dung (TF-IDF) và fallback."},
    {"name": "Item-based: CF",
        "description": "Gợi ý tương tự theo Collaborative Filtering (item-item)."},
    {"name": "Item-based: Co-purchase",
        "description": "Khách mua sách này cũng mua (orders-based)."},
    {"name": "Item-based: Rule",
        "description": "Rule-based (category/author)."},
    {"name": "Personalized: CF",
        "description": "Gợi ý cá nhân hoá theo user (cached trong recommendations)."},
    {"name": "Personalized: For You",
        "description": "Endpoint tổng hợp: CF + fallback."},
    {"name": "Personalized: Rule",
        "description": "Rule-based theo lịch sử mua của user."},
    {"name": "Admin/Maintenance", "description": "API phục vụ rebuild/clear cache."},
]

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Bookstore Recommendation Service",
    version="2.1.0",
    openapi_tags=tags_metadata,
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://localhost:3000",
        "http://localhost:5173", 
        "https://localhost:5173",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# middleware
app.middleware("http")(log_errors_middleware)


@app.get("/health")
def health_check():
    return {"status": "ok"}


# Router
app.include_router(recommend_router)
