"""
Search Service – FastAPI Entry Point
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from search_app.routers.search_router import router as search_router
from search_app.routers.admin_search_router import router as admin_router
from search_app.routers.image_search_router import router as image_search_router
from search_app.workers.index_queue import start_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("search.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: khởi động background index worker."""
    logger.info("[Startup] Khởi động AsyncIO Index Queue Worker...")
    start_worker()
    logger.info("[Startup] Worker sẵn sàng.")
    yield
    logger.info("[Shutdown] Search Service đang tắt.")


app = FastAPI(
    title="Search Service – VnBook Platform",
    description=(
        "Full-text search + autocomplete tiếng Việt dùng OpenSearch.\n\n"
        "Real-time sync qua **Event-Driven In-Process Queue** (không dùng Cronjob)."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# ── CORS Middleware - Allow Frontend to call API ────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",
        "https://localhost:5173",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API Routers (phải đăng ký TRƯỚC static mount) ────────────────────────────
app.include_router(search_router)
app.include_router(admin_router)
app.include_router(image_search_router)


@app.get("/", include_in_schema=False)
def root():
    """Redirect về trang Search UI."""
    return RedirectResponse(url="/ui/")


# ── Static files: Search UI (index.html) ─────────────────────────────────────
# Đúng đường dẫn: search_app/static/ (path tương đối từ thư mục search-service)
app.mount(
    "/ui",
    StaticFiles(directory="search_app/static", html=True),
    name="static",
)
