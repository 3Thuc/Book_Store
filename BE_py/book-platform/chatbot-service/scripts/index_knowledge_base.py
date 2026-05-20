"""
scripts/index_knowledge_base.py
Đọc các file .txt trong knowledge_base/ → encode SBERT → index vào OpenSearch chatbot_kb.

Chạy 1 lần trước khi khởi động chatbot service:
  cd chatbot-service
  python scripts/index_knowledge_base.py

Giải thích:
  - Mỗi file .txt được chia thành các "chunk" (đoạn văn)
  - Mỗi chunk được encode thành vector 768D bằng SBERT
  - Vector được lưu vào OpenSearch index "chatbot_kb"
  - Khi user hỏi → encode câu hỏi → k-NN tìm chunk gần nhất → đưa vào LLM
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pathlib import Path
from opensearchpy import OpenSearch, RequestsHttpConnection
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# Load .env từ book-platform/
BASE_DIR = Path(__file__).resolve().parents[2]   # scripts/ → chatbot-service/ → book-platform/
load_dotenv(BASE_DIR / ".env")

OPENSEARCH_HOST     = os.getenv("OPENSEARCH_HOST",     "localhost")
OPENSEARCH_PORT     = int(os.getenv("OPENSEARCH_PORT", "9200"))
OPENSEARCH_USER     = os.getenv("OPENSEARCH_USER",     "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "")
USE_SSL             = os.getenv("OPENSEARCH_USE_SSL",  "true").lower() == "true"

KB_DIR       = Path(__file__).parents[1] / "chatbot_app" / "knowledge_base"
SBERT_MODEL  = "keepitreal/vietnamese-sbert"
KB_INDEX     = "chatbot_kb"
CHUNK_SIZE   = 3  # số dòng mỗi chunk


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE) -> list[str]:
    """Chia text thành các đoạn nhỏ theo số dòng."""
    lines  = [l.strip() for l in text.splitlines() if l.strip()]
    chunks = []
    for i in range(0, len(lines), chunk_size):
        chunk = " ".join(lines[i:i + chunk_size])
        if chunk:
            chunks.append(chunk)
    return chunks


def main():
    # ── 0. Debug: verify .env đã load đúng ────────────────────
    print(f"📂 BASE_DIR  : {BASE_DIR}")
    print(f"📄 .env path : {BASE_DIR / '.env'} (exists: {(BASE_DIR / '.env').exists()})")
    print(f"🔑 OS_HOST   : {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    print(f"🔑 OS_USER   : {OPENSEARCH_USER}")
    print(f"🔑 OS_PASS   : {'SET (' + str(len(OPENSEARCH_PASSWORD)) + ' chars)' if OPENSEARCH_PASSWORD else 'EMPTY!'}")
    print(f"🔒 USE_SSL   : {USE_SSL}")
    print()

    # ── 1. Kết nối OpenSearch ──────────────────────────────────
    print("📡 Kết nối OpenSearch...")
    client = OpenSearch(
        hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
        http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
        use_ssl=USE_SSL,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
    )
    print(f"   ✅ Kết nối thành công: {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")


    # ── 2. Tạo index chatbot_kb (nếu chưa có) ────────────────
    index_body = {
        "settings": {
            "index": {"knn": True, "knn.algo_param.ef_search": 100}
        },
        "mappings": {
            "properties": {
                "text":             {"type": "text", "analyzer": "standard"},
                "category":         {"type": "keyword"},
                "source_file":      {"type": "keyword"},
                "sbert_embedding":  {
                    "type": "knn_vector",
                    "dimension": 768,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "lucene",
                        "parameters": {"m": 16, "ef_construction": 100}
                    }
                }
            }
        }
    }

    if client.indices.exists(index=KB_INDEX):
        print(f"🗑️  Xóa index cũ '{KB_INDEX}'...")
        client.indices.delete(index=KB_INDEX)

    client.indices.create(index=KB_INDEX, body=index_body)
    print(f"✅ Tạo index '{KB_INDEX}' thành công")

    # ── 3. Load SBERT model ───────────────────────────────────
    print(f"🧠 Loading SBERT model: {SBERT_MODEL}...")
    model = SentenceTransformer(SBERT_MODEL)
    print("   ✅ Model loaded")

    # ── 4. Đọc files KB → chunk → encode → index ─────────────
    total_docs = 0
    txt_files  = list(KB_DIR.glob("*.txt"))
    print(f"\n📚 Tìm thấy {len(txt_files)} file KB:")

    for txt_file in txt_files:
        category = txt_file.stem.replace("chinh_sach_", "").replace("_", " ")
        text     = txt_file.read_text(encoding="utf-8")
        chunks   = chunk_text(text)
        print(f"  📄 {txt_file.name}: {len(chunks)} chunks")

        for i, chunk in enumerate(chunks):
            vec = model.encode(chunk, normalize_embeddings=True).tolist()
            doc = {
                "text":            chunk,
                "category":        category,
                "source_file":     txt_file.name,
                "sbert_embedding": vec,
            }
            doc_id = f"{txt_file.stem}_{i}"
            client.index(index=KB_INDEX, id=doc_id, body=doc)
            total_docs += 1

    print(f"\n🎉 Hoàn tất! Đã index {total_docs} documents vào '{KB_INDEX}'")
    print(f"   Sẵn sàng dùng cho chatbot RAG query!")


if __name__ == "__main__":
    main()
