"""
rag_retriever.py – FAISS Knowledge Base Retrieval.

Embedding: paraphrase-multilingual-MiniLM-L12-v2 (đa ngôn ngữ, hỗ trợ tiếng Việt)
Index:     FAISS IndexFlatIP (cosine similarity sau khi normalize)

Dữ liệu: chatbot_app/knowledge_base/*.txt → build_kb.py → faiss_index/
"""
import json
import faiss
import numpy as np
from pathlib import Path
from sentence_transformers import SentenceTransformer
from chatbot_app.config import FAISS_INDEX_PATH, FAISS_CHUNKS_PATH, EMBEDDING_MODEL


class RAGRetriever:
    _instance = None

    def __init__(self):
        print(f"🔄 Loading FAISS index from {FAISS_INDEX_PATH}...")
        if not FAISS_INDEX_PATH.exists():
            raise FileNotFoundError(
                f"FAISS index not found: {FAISS_INDEX_PATH}\n"
                "→ Chạy: python build_kb.py"
            )
        self.index  = faiss.read_index(str(FAISS_INDEX_PATH))
        with open(FAISS_CHUNKS_PATH, encoding="utf-8") as f:
            self.chunks = json.load(f)
        print(f"🔄 Loading embedding model: {EMBEDDING_MODEL}")
        self.model  = SentenceTransformer(EMBEDDING_MODEL)
        print(f"✅ RAG ready ({self.index.ntotal} vectors)")

    @classmethod
    def get(cls) -> "RAGRetriever":
        """Singleton – load 1 lần khi startup."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def retrieve(self, query: str, top_k: int = 3) -> list[dict]:
        """
        Tìm top_k chunks liên quan nhất theo cosine similarity.
        Returns: [{"text": ..., "source": ..., "score": ...}]
        """
        vec = self.model.encode(
            [query], normalize_embeddings=True
        ).astype(np.float32)

        scores, indices = self.index.search(vec, top_k)

        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < 0:
                continue
            results.append({
                "text":   self.chunks[idx]["text"],
                "source": self.chunks[idx]["source"],
                "score":  float(score),
            })
        return results
