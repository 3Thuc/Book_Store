"""
main.py – FastAPI entry point cho Chatbot Service.

Khởi động:
  uvicorn chatbot_app.main:app --host 0.0.0.0 --port 8004 --reload

Giải thích startup:
  - Lifespan function: load SBERT model 1 lần vào RAM khi server bật
  - Singleton pattern: các module khác gọi get_sbert_model() → cùng 1 instance
  - Không reload model giữa request → tiết kiệm thời gian
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi.errors import RateLimitExceeded

from chatbot_app.routers.customer_chat_router import router as customer_chat_router
from chatbot_app.routers.staff_chat_router import router as staff_chat_router
from chatbot_app.routers.admin_chat_router import router as admin_chat_router
from chatbot_app.nlu.customer_intent_classifier import load_sbert_model
from chatbot_app.db import test_connection, ensure_tables
from chatbot_app.middleware.rate_limiter import limiter, rate_limit_exceeded_handler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Chạy 1 lần khi server khởi động (trước khi nhận request).
    Dùng để pre-load model nặng vào RAM.
    """
    print("=" * 55)
    print("🚀 Chatbot Service đang khởi động...")
    print("   📌 Customer + Staff + Admin chatbot")
    print("=" * 55)

    # 1. Kiểm tra kết nối MySQL + tạo bảng nếu chưa có
    print("📡 [1/4] Kiểm tra kết nối MySQL...")
    if test_connection():
        print("   ✅ MySQL OK")
        ensure_tables()
    else:
        print("   ❌ MySQL FAILED – kiểm tra .env và DB!")

    # 2. Load SBERT model vào RAM (chỉ 1 lần)
    # Lý do: model ~400MB, load 1 lần mất 5-10s
    # Nếu load trong từng request → chậm không chịu được
    # FIX: chạy trong thread executor để không block event loop (tránh startup timeout)
    print("🧠 [2/4] Loading SBERT model (keepitreal/vietnamese-sbert)...")
    try:
        import asyncio
        await asyncio.to_thread(load_sbert_model)
        print("   ✅ SBERT ready!")
    except Exception as e:
        print(f"   ⚠️  SBERT load error (non-critical, will lazy-load): {e}")

    # 3. Pre-warm SBERT cache: encode 1 query hay dùng để nạp vào lru_cache
    # Lý do: lần encode đầu tiên luôn chậm hơn 50-100ms do JIT / tensor warmup
    print("🔥 [3/4] Pre-warming SBERT embedding cache...")
    try:
        from chatbot_app.retrieval.opensearch_retriever import _encode_query_cached
        import asyncio as _asyncio
        await _asyncio.to_thread(_encode_query_cached, "sách bán chạy hay nhất")
        await _asyncio.to_thread(_encode_query_cached, "sách kỹ năng sống")
        print("   ✅ SBERT cache warmed (2 queries)")
    except Exception as e:
        print(f"   ⚠️  SBERT cache warm error (non-critical): {e}")

    # 4. Pre-warm Ollama: gửi 1 request nhỏ để nạp model vào VRAM/RAM
    # Lý do: lần đầu Ollama cần load model vào bộ nhớ GPU/CPU, mất 10-30s
    # User đầu tiên sẽ không phải chờ nữa!
    print("🤖 [4/4] Pre-warming Ollama LLM...")
    try:
        import httpx
        import asyncio
        from chatbot_app.generation.llm_client import OLLAMA_HOST, OLLAMA_MODEL
        async def _warm_ollama():
            async with httpx.AsyncClient(timeout=90.0) as client:
                await client.post(
                    f"{OLLAMA_HOST}/api/chat",
                    json={
                        "model": OLLAMA_MODEL,
                        "messages": [{"role": "user", "content": "hi"}],
                        "stream": False,
                        "options": {"num_predict": 1},  # chỉ 1 token để nạp model nhanh
                    },
                )
        await _warm_ollama()
        print(f"   ✅ Ollama ({OLLAMA_MODEL}) ready!")
    except Exception as e:
        print(f"   ⚠️  Ollama pre-warm failed (non-critical): {e}")

    print("=" * 55)
    print("✅ Chatbot Service sẵn sàng tại: http://localhost:8004")
    print("📄 Docs:     http://localhost:8004/docs")
    print("⚡ Stream:   POST /api/chat/stream  (SSE)")
    print("=" * 55)

    yield  # Server đang chạy và nhận request

    # Phần dưới yield chạy khi server shutdown
    print("🔴 Chatbot Service đang tắt...")


# ── Tạo FastAPI app ─────────────────────────────────────────────
app = FastAPI(
    title="BookStore Chatbot Service",
    description="Hybrid Intelligent Chatbot – SBERT + Ollama + OpenSearch",
    version="3.1.0",
    lifespan=lifespan,
)

# ── SlowAPI Rate Limiter ───────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)

# ── CORS Middleware ───────────────────────────────────────────────
# Origins kiểm soát qua env var ALLOWED_ORIGINS trong .env
# Mặc định: localhost:3000 (React dev server)
import os as _os
_allowed_origins_raw = _os.getenv("ALLOWED_ORIGINS", "http://localhost:3000")
_allowed_origins = [o.strip() for o in _allowed_origins_raw.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Mount routers ──────────────────────────────────────────────
app.include_router(customer_chat_router)  # Customer: /api/chat/...
app.include_router(staff_chat_router)     # Staff:    /api/staff/chat/...
app.include_router(admin_chat_router)     # Admin:    /api/admin/chat/...


@app.get("/")
def root():
    return {
        "service": "BookStore Chatbot",
        "version": "3.0",
        "roles_supported": ["customer", "staff", "admin"],
        "endpoints": {
            # Customer chatbot
            "customer_chat":         "POST /api/chat/message",
            "customer_stream":       "POST /api/chat/stream",
            "customer_upload_image": "POST /api/chat/upload-image  ← OCR+Chat tích hợp",
            "customer_history":      "GET  /api/chat/history/{session_id}",
            "customer_health":       "GET  /api/chat/health",
            # Staff chatbot
            "staff_chat":       "POST /api/staff/chat/message",
            "staff_history":    "GET  /api/staff/chat/history/{session_id}",
            "staff_health":     "GET  /api/staff/chat/health",
            # Admin chatbot
            "admin_chat":       "POST /api/admin/chat/message",
            "admin_history":    "GET  /api/admin/chat/history/{session_id}",
            "admin_health":     "GET  /api/admin/chat/health",
            # Docs
            "docs":             "GET  /docs",
        }
    }
