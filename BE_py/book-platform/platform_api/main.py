from pathlib import Path
import sys
import importlib

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]

# add path trước
sys.path.insert(0, str(BASE_DIR / "search-service"))
sys.path.insert(0, str(BASE_DIR / "recommendation-service"))
sys.path.insert(0, str(BASE_DIR / "chatbot-service"))       # ← CHATBOT

load_dotenv(BASE_DIR / ".env")


# Import search routers
search_router = importlib.import_module(
    "search_app.routers.search_router").router
admin_search_router = importlib.import_module(
    "search_app.routers.admin_search_router").router
# Import image search router
try:
    image_search_router = importlib.import_module(
        "search_app.routers.image_search_router").router
except Exception as e:
    print(f"⚠️  Image Search service unavailable: {e}")
    image_search_router = None

recommend_router = importlib.import_module(
    "recommend_app.routers.recommend_router").router

# Try to load chatbot, but don't fail if dependencies missing
try:
    chat_router = importlib.import_module(                       # ← CHATBOT
        "chatbot_app.routers.chat_router").router                # ← CHATBOT
except Exception as e:
    print(f"⚠️  Chatbot service unavailable: {e}")
    chat_router = None

app = FastAPI(title="BOOK-PLATFORM API")

# Enable CORS để FE gọi được
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "https://localhost:3000",
        "http://localhost:5173", 
        "https://localhost:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(recommend_router)
app.include_router(search_router)
app.include_router(admin_search_router)
if image_search_router:
    app.include_router(image_search_router)
    # Also register adapter endpoints (non-/api) that some frontend code may call.
    try:
        img_mod = importlib.import_module("search_app.routers.image_search_router")
        # The original handler is defined as `search_by_image` in that module.
        if hasattr(img_mod, "search_by_image"):
            # Mount same handler at /books/search-by-image (no /api prefix)
            app.post("/books/search-by-image")(getattr(img_mod, "search_by_image"))
            # Also add health endpoint without /api prefix
            if hasattr(img_mod, "health_check"):
                app.get("/books/search-by-image/health")(getattr(img_mod, "health_check"))
    except Exception:
        # If adapter mount fails, ignore (router already included above)
        pass
if chat_router:
    app.include_router(chat_router)                          # ← CHATBOT
