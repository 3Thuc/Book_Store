# app/main.py
from fastapi import FastAPI
from recommend_app.routers.recommend_router import router as recommend_router
from recommend_app.middleware import log_errors_middleware

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

app = FastAPI(
    title="Bookstore Recommendation Service",
    version="1.0.0",
    openapi_tags=tags_metadata,
)
# middleware
app.middleware("http")(log_errors_middleware)


@app.get("/health")
def health_check():
    return {"status": "ok"}


# Router
app.include_router(recommend_router)
