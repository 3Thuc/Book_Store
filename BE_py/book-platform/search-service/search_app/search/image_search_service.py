"""
Image Search Service - Search for similar books using image embeddings
"""
import os
import logging
from typing import List, Dict, Any, Optional
import numpy as np
from search_app.search.client import get_os_client
from search_app.search.clip_service import get_clip_service


logger = logging.getLogger(__name__)

# Index chứa image embeddings (có thể khác books_current)
IMAGE_INDEX = os.getenv("OPENSEARCH_IMAGE_INDEX", "books_current")
# Index chứa thông tin đầy đủ của sách (text search index)
BOOKS_INDEX = os.getenv("OPENSEARCH_INDEX", "books_current")


class ImageSearchService:
    """Service to search for similar books using image embeddings"""

    def __init__(self):
        self.client = get_os_client()
        self.clip_service = get_clip_service()
        self.image_index = IMAGE_INDEX
        self.books_index = BOOKS_INDEX

    def search_by_image(
        self,
        image_data: bytes,
        k: int = 20,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        logger.info(f"🖼️ Starting image search with k={k}")

        # --- Generate CLIP embedding ---
        logger.info("📊 Generating CLIP embedding...")
        embedding = self.clip_service.get_image_embedding(image_data)
        embedding_list = embedding.tolist()
        logger.info(f"✅ Embedding generated (dimension: {len(embedding_list)})")

        filter_clauses = self._build_filter_clauses(filters)
        errors: Dict[str, str] = {}

        inner_query: Dict[str, Any] = {"match_all": {}}
        if filter_clauses:
            inner_query = {
                "bool": {
                    "must": [{"match_all": {}}],
                    "filter": filter_clauses,
                }
            }

        # ----------------------------------------------------------------
        # Strategy 1: k-NN query
        # ----------------------------------------------------------------
        knn_query: Dict[str, Any] = {
            "size": k,
            "_source": ["book_id"],
            "query": {
                "knn": {
                    "image_embedding": {
                        "vector": embedding_list,
                        "k": k,
                    }
                }
            },
        }
        if filter_clauses:
            knn_query["query"] = {
                "bool": {
                    "must": [knn_query["query"]],
                    "filter": filter_clauses,
                }
            }
        try:
            logger.info("🔍 [Strategy 1] Trying k-NN query...")
            response = self.client.search(index=self.image_index, body=knn_query)
            logger.info("✅ k-NN query succeeded.")
            return self._enrich_and_return(response, mode="knn")
        except Exception as exc:
            errors["knn"] = str(exc)
            logger.warning("⚠️  k-NN failed: %s", exc)

        # ----------------------------------------------------------------
        # Strategy 2: cosineSimilarity script_score
        # ----------------------------------------------------------------
        cosine_query: Dict[str, Any] = {
            "size": k,
            "_source": ["book_id"],
            "query": {
                "script_score": {
                    "query": inner_query,
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'image_embedding') + 1.0",
                        "params": {"query_vector": embedding_list},
                    },
                }
            },
        }
        try:
            logger.info("🔍 [Strategy 2] Trying cosineSimilarity...")
            response = self.client.search(index=self.image_index, body=cosine_query)
            logger.info("✅ cosineSimilarity succeeded.")
            return self._enrich_and_return(response, mode="cosine")
        except Exception as exc:
            errors["cosine"] = str(exc)
            logger.warning("⚠️  cosineSimilarity failed: %s", exc)

        # ----------------------------------------------------------------
        # Strategy 3: Python fallback - fetch all embeddings, compute in Python
        # ----------------------------------------------------------------
        try:
            logger.info("🔍 [Strategy 3] Python cosine fallback...")
            return self._python_similarity_search(embedding_list, k, filter_clauses)
        except Exception as exc:
            errors["python"] = str(exc)
            logger.error("❌ Python fallback failed: %s", exc)

        error_summary = " | ".join(f"[{k}]: {v}" for k, v in errors.items())
        raise RuntimeError(
            f"Image search failed. Check 'image_embedding' mapping. Errors: {error_summary}"
        )

    # ------------------------------------------------------------------
    # Enrich: lấy book_ids từ kết quả similarity, query books_current
    # để lấy đầy đủ thông tin (title, author, image_url, ...)
    # ------------------------------------------------------------------
    def _enrich_and_return(self, similarity_response: Dict, mode: str = "knn") -> Dict:
        """
        Nhận response từ similarity search (chỉ có book_id + score),
        query lại books_current để lấy đầy đủ thông tin sách.
        """
        hits = similarity_response.get("hits", {}).get("hits", [])
        if not hits:
            return {"success": True, "total": 0, "count": 0, "books": [], "took_ms": 0}

        # Map book_id → similarity score
        score_map: Dict[str, float] = {}
        for hit in hits:
            bid = hit.get("_source", {}).get("book_id") or hit.get("_id")
            raw_score = hit.get("_score", 0) or 0
            # Normalize cosine score (offset +1.0 → range [0,2]) về [0,1]
            if mode == "cosine":
                score = (raw_score - 1.0)
            else:
                score = raw_score
            score_map[str(bid)] = round(float(score), 4)

        book_ids = list(score_map.keys())
        logger.info("🔍 Enriching %d books from '%s'...", len(book_ids), self.books_index)

        # Query books_current bằng book_ids
        enrich_query = {
            "size": len(book_ids),
            "_source": [
                "book_id", "title", "author_name", "price",
                "categories", "main_image_url", "avg_rating",
                "in_stock", "publisher_name", "stock_quantity",
                "rating_count", "publication_year",
            ],
            "query": {
                "terms": {"book_id": book_ids}
            }
        }

        try:
            enrich_response = self.client.search(
                index=self.books_index, body=enrich_query
            )
        except Exception as exc:
            logger.error("❌ Enrich query failed: %s", exc)
            # Fallback: trả về data tối giản từ similarity hits
            books = [
                {
                    "book_id": bid,
                    "title": "",
                    "author": "",
                    "price": 0,
                    "category": "",
                    "image_url": "",
                    "rating": 0,
                    "in_stock": True,
                    "similarity_score": score,
                    "similarity_percentage": round(score * 100, 1),
                }
                for bid, score in score_map.items()
            ]
            return {"success": True, "total": len(books), "count": len(books), "books": books, "took_ms": 0}

        # Build result — giữ thứ tự theo similarity score
        book_map: Dict[str, Dict] = {}
        for hit in enrich_response.get("hits", {}).get("hits", []):
            src = hit.get("_source", {})
            bid = str(src.get("book_id") or hit.get("_id", ""))
            cats = src.get("categories") or []
            category = cats[0] if cats else "Khác"
            book_map[bid] = {
                "book_id": bid,
                "title": src.get("title", ""),
                "author": src.get("author_name", ""),
                "price": src.get("price", 0),
                "category": category,
                "image_url": src.get("main_image_url", ""),  # path dạng covers/books/xxx/xxx.jpg
                "rating": src.get("avg_rating", 0),
                "in_stock": src.get("in_stock", False),
                "publisher_name": src.get("publisher_name", ""),
                "rating_count": src.get("rating_count", 0),
                "publication_year": src.get("publication_year"),
            }

        # Sắp xếp theo similarity score giảm dần, gắn score vào
        books = []
        for bid in sorted(score_map, key=lambda x: score_map[x], reverse=True):
            if bid in book_map:
                entry = book_map[bid].copy()
                score = score_map[bid]
                entry["similarity_score"] = score
                entry["similarity_percentage"] = round(score * 100, 1)
                books.append(entry)

        logger.info("✅ Enriched %d books", len(books))
        return {
            "success": True,
            "total": len(books),
            "count": len(books),
            "books": books,
            "took_ms": similarity_response.get("took", 0),
        }

    def _python_similarity_search(
        self,
        query_vector: List[float],
        k: int,
        filter_clauses: List[Dict],
    ) -> Dict[str, Any]:
        """Fetch image_embedding từ OpenSearch, tính cosine similarity bằng Python."""
        fetch_query: Dict[str, Any] = {
            "size": 500,
            "_source": ["book_id", "image_embedding"],
            "query": {"match_all": {}} if not filter_clauses else {
                "bool": {"filter": filter_clauses}
            },
        }
        response = self.client.search(index=self.image_index, body=fetch_query)
        hits = response.get("hits", {}).get("hits", [])

        docs_with_emb = sum(1 for h in hits if h.get("_source", {}).get("image_embedding") is not None)
        logger.info("📦 Fetched %d docs, %d have image_embedding", len(hits), docs_with_emb)

        if docs_with_emb == 0:
            logger.warning("⚠️  No image_embedding found. Returning top books by rating instead.")
            # Fallback: trả sách phổ biến từ books_current
            fallback = self.client.search(
                index=self.books_index,
                body={
                    "size": k,
                    "_source": [
                        "book_id", "title", "author_name", "price",
                        "categories", "main_image_url", "avg_rating",
                        "in_stock", "publisher_name", "rating_count",
                    ],
                    "query": {"match_all": {}},
                    "sort": [{"avg_rating": "desc"}, {"rating_count": "desc"}],
                }
            )
            books = []
            for hit in fallback.get("hits", {}).get("hits", []):
                src = hit.get("_source", {})
                cats = src.get("categories") or []
                books.append({
                    "book_id": str(src.get("book_id", "")),
                    "title": src.get("title", ""),
                    "author": src.get("author_name", ""),
                    "price": src.get("price", 0),
                    "category": cats[0] if cats else "Khác",
                    "image_url": src.get("main_image_url", ""),
                    "rating": src.get("avg_rating", 0),
                    "in_stock": src.get("in_stock", False),
                    "similarity_score": 0.0,
                    "similarity_percentage": 0.0,
                })
            return {
                "success": True,
                "total": len(books),
                "count": len(books),
                "books": books,
                "took_ms": response.get("took", 0),
                "warning": "image_embedding not found; showing top-rated books instead.",
            }

        # Compute cosine similarity
        qv = np.array(query_vector, dtype=np.float32)
        qv_norm = np.linalg.norm(qv)
        scored: List[tuple] = []
        for hit in hits:
            emb = hit.get("_source", {}).get("image_embedding")
            if emb is None:
                continue
            bid = str(hit.get("_source", {}).get("book_id") or hit.get("_id", ""))
            ev = np.array(emb, dtype=np.float32)
            ev_norm = np.linalg.norm(ev)
            score = float(np.dot(qv, ev) / (qv_norm * ev_norm + 1e-10)) if ev_norm > 0 else 0.0
            scored.append((score, bid))

        scored.sort(reverse=True)
        top = scored[:k]

        # Build fake similarity response to reuse _enrich_and_return
        fake_hits = [
            {"_id": bid, "_score": score, "_source": {"book_id": bid}}
            for score, bid in top
        ]
        fake_response = {
            "hits": {"hits": fake_hits, "total": {"value": len(fake_hits)}},
            "took": response.get("took", 0),
        }
        return self._enrich_and_return(fake_response, mode="python")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_filter_clauses(self, filters: Optional[Dict[str, Any]]) -> List[Dict]:
        if not filters:
            return []
        clauses: List[Dict] = []
        if filters.get("category"):
            clauses.append({"term": {"category.keyword": filters["category"]}})
        range_filter: Dict[str, Any] = {}
        if "min_price" in filters:
            range_filter["gte"] = filters["min_price"]
        if "max_price" in filters:
            range_filter["lte"] = filters["max_price"]
        if range_filter:
            clauses.append({"range": {"price": range_filter}})
        if filters.get("in_stock"):
            clauses.append({"term": {"in_stock": True}})
        return clauses


def get_image_search_service() -> ImageSearchService:
    return ImageSearchService()