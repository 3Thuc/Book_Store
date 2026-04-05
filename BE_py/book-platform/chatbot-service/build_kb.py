"""
build_kb.py – Xây dựng FAISS index từ knowledge base files.

Chạy 1 lần trước khi khởi động server:
  cd chatbot-service
  python build_kb.py

Hoặc trong Docker:
  docker compose exec fastapi python /app/chatbot-service/build_kb.py
"""
import json
import sys
from pathlib import Path

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

BASE_DIR  = Path(__file__).resolve().parent
KB_DIR    = BASE_DIR / "chatbot_app" / "knowledge_base"
IDX_DIR   = BASE_DIR / "faiss_index"
IDX_DIR.mkdir(exist_ok=True)

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"


def load_chunks() -> list[dict]:
    """Đọc tất cả *.txt trong knowledge_base/, chia thành chunks theo đoạn."""
    chunks = []
    for kb_file in sorted(KB_DIR.glob("*.txt")):
        text = kb_file.read_text(encoding="utf-8")
        for para in text.split("\n\n"):
            para = para.strip()
            if len(para) > 20:          # bỏ đoạn quá ngắn
                chunks.append({
                    "source": kb_file.name,
                    "text":   para,
                })
    return chunks


def build_index(chunks: list[dict]) -> None:
    print(f"📖  {len(chunks)} chunks, loading embedding model...")
    model  = SentenceTransformer(MODEL_NAME)
    texts  = [c["text"] for c in chunks]
    embeds = model.encode(texts, show_progress_bar=True,
                          normalize_embeddings=True).astype(np.float32)

    dim   = embeds.shape[1]
    index = faiss.IndexFlatIP(dim)    # Inner Product = Cosine (đã normalize)
    index.add(embeds)

    faiss.write_index(index, str(IDX_DIR / "kb.index"))
    (IDX_DIR / "kb_chunks.json").write_text(
        json.dumps(chunks, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"✅  FAISS index saved → {IDX_DIR}/")
    print(f"     kb.index  ({index.ntotal} vectors, dim={dim})")
    print(f"     kb_chunks.json")


if __name__ == "__main__":
    chunks = load_chunks()
    if not chunks:
        print("❌  Không tìm thấy file .txt trong knowledge_base/")
        sys.exit(1)
    build_index(chunks)
