import os
from pathlib import Path
from dotenv import load_dotenv

# Đọc .env từ book-platform/
BASE_DIR = Path(__file__).resolve().parents[3]
load_dotenv(BASE_DIR / ".env")

# ── MySQL (dùng chung DB bookstore) ──────────────────────────
MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB       = os.getenv("MYSQL_DB", "bookstore")

# ── Ollama (LLM server) ───────────────────────────────────────
# Khi chạy Docker: OLLAMA_HOST=http://ollama:11434
# Khi chạy local:  OLLAMA_HOST=http://localhost:11434
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b")

# ── FAISS Knowledge Base ──────────────────────────────────────
CHATBOT_DIR       = Path(__file__).resolve().parents[1]
FAISS_INDEX_PATH  = CHATBOT_DIR / "faiss_index" / "kb.index"
FAISS_CHUNKS_PATH = CHATBOT_DIR / "faiss_index" / "kb_chunks.json"
KB_DIR            = CHATBOT_DIR / "chatbot_app" / "knowledge_base"

# ── Embedding model (đa ngôn ngữ, hỗ trợ tiếng Việt) ─────────
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"

# ── Intent Classifier Model ───────────────────────────────────
CLASSIFIER_PATH = CHATBOT_DIR / "training" / "intent_model.pkl"
VECTORIZER_PATH = CHATBOT_DIR / "training" / "intent_vectorizer.pkl"
