import os
from pathlib import Path
from dotenv import load_dotenv

# Đọc .env từ book-platform/
# config.py nằm ở: chatbot-service/chatbot_app/config.py
# parents[0] = chatbot_app/  | parents[1] = chatbot-service/
# parents[2] = book-platform/ ← .env ở đây | parents[3] = BE_py/
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

# ── MySQL (dùng chung DB bookstore) ──────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB       = os.getenv("MYSQL_DB",       "bookstore")

# ── OpenSearch (dùng chung với recommendation/search service) ─
OPENSEARCH_HOST        = os.getenv("OPENSEARCH_HOST",        "localhost")
OPENSEARCH_PORT        = int(os.getenv("OPENSEARCH_PORT",    "9200"))
OPENSEARCH_USER        = os.getenv("OPENSEARCH_USER",        "admin")
OPENSEARCH_PASSWORD    = os.getenv("OPENSEARCH_PASSWORD",    "")
OPENSEARCH_USE_SSL     = os.getenv("OPENSEARCH_USE_SSL",     "true").lower() == "true"
OPENSEARCH_VERIFY_CERTS= os.getenv("OPENSEARCH_VERIFY_CERTS","false").lower() == "true"
OPENSEARCH_BOOKS_INDEX = os.getenv("OPENSEARCH_INDEX",       "books_current")
OPENSEARCH_KB_INDEX    = "chatbot_kb"   # index FAQ / chính sách

# ── Ollama (LLM server local) ─────────────────────────────────
# Local dev:  OLLAMA_HOST=http://localhost:11434
# Docker:     OLLAMA_HOST=http://ollama:11434
OLLAMA_HOST  = os.getenv("OLLAMA_HOST",  "http://ollama:11434").strip()
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")  # qwen2.5:3b – RTX 3050 4GB VRAM

# ── SBERT Embedding Model ─────────────────────────────────────
# Dùng chung với recommendation-service (keepitreal/vietnamese-sbert)
# Không cần load lại → gọi get_sbert_model() từ intent_classifier
SBERT_MODEL_NAME = "keepitreal/vietnamese-sbert"

# ── Knowledge Base ─────────────────────────────────────────────
CHATBOT_DIR = Path(__file__).resolve().parents[1]
KB_DIR      = CHATBOT_DIR / "chatbot_app" / "knowledge_base"

# ── Chatbot Settings ──────────────────────────────────────────
# Task 7: Tune thresholds
INTENT_CONFIDENCE_THRESHOLD = 0.58   # ↑ 0.55→0.58: bớt trigger hỏi xác nhận với câu rõ ràng
CONTEXT_WINDOW_SIZE         = 12     # ↑ 10→12: giữ context lâu hơn cho hội thoại dài
MAX_BOOK_RESULTS            = 5      # kết quả tìm sách
LLM_MAX_TOKENS              = 1500   # ↑ 1200→1500: tránh truncation response giữa câu
LLM_TEMPERATURE             = 0.35   # ↑ 0.3→0.35: tự nhiên hơn, đồng bộ với llm_client.py
OLLAMA_NUM_PREDICT          = 450    # FIX: tăng num_predict 256→450 để không bị cắt câu
# Chỉ hỏi confirm khi confidence < threshold NÀY (tránh confirm loop lạm dụng)
CONFIRM_CONFIDENCE_THRESHOLD = 0.60  # Dưới ngưỡng này mới hỏi confirm

# ── Port ──────────────────────────────────────────────────────
# ── Port ──────────────────────────────────────────────────────
CHATBOT_PORT = int(os.getenv("CHATBOT_PORT", "8004"))

# ── Java Spring Boot Backend API (cho cache eviction) ─────────
# Sau khi Python UPDATE trực tiếp vào MySQL, Java Spring Cache không biết.
# Gọi nội bộ Java API để trigger @CacheEvict và đảm bảo FE nhận data mới.
JAVA_API_BASE  = os.getenv("JAVA_API_BASE",  "https://localhost:8443/bookdb")
JAVA_BOT_EMAIL = os.getenv("JAVA_BOT_EMAIL", "")
JAVA_BOT_PASS  = os.getenv("JAVA_BOT_PASS",  "")
ADMIN_API_KEY  = os.getenv("ADMIN_API_KEY",  "bookstore-internal-key")


