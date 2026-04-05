"""
cb_service.py – Content-Based Recommendation qua OpenSearch k-NN Vector Search

Kiến trúc mới (thay thế MySQL similar_books):
  1. Lấy Vector SBERT của cuốn sách đang xem từ OpenSearch.
  2. Gửi lệnh k-NN query (Approximate Nearest Neighbor) vào OpenSearch.
  3. OpenSearch dùng HNSW Index (C++) tính Cosine Distance trong <10ms.
  4. Trả về Top-N sách có ngữ nghĩa tương đồng nhất, kèm Filter thông minh.

Ưu điểm so sánh:
  ┌─────────────────────┬──────────────────────┬───────────────────────────┐
  │ Tiêu chí            │ MySQL similar_books   │ OpenSearch k-NN (Cái này) │
  ├─────────────────────┼──────────────────────┼───────────────────────────┤
  │ Tốc độ truy vấn     │ 50-200ms (Index scan) │ 3-10ms (HNSW Graph)       │
  │ Filter linh hoạt    │ Không (Top-20 cứng)   │ Có (còn hàng, giá, ...)   │
  │ Dung lượng DB phụ   │ 20M rows rác          │ 0 rows (Vector trực tiếp) │
  │ Khi thêm sách mới   │ Phải rebuild lại      │ Tự động (indexer.py)       │
  └─────────────────────┴──────────────────────┴───────────────────────────┘
"""

import os
import logging
from dotenv import load_dotenv

from search_app.search.client import get_os_client

load_dotenv()
logger = logging.getLogger("search.cb_service")
INDEX  = os.getenv("OPENSEARCH_INDEX", "books_current")


def get_similar_books(
    book_id: int,
    limit: int = 10,
    only_in_stock: bool = True,
) -> list[dict]:
    """
    Trả về danh sách sách gợi ý tương đồng theo ngữ nghĩa AI (SBERT k-NN).

    Luồng hoạt động:
      Bước 1: Lấy sbert_embedding của sách đang xem từ OpenSearch (_source).
      Bước 2: Gửi k-NN query với vector đó → OpenSearch HNSW Graph tìm láng giềng gần nhất.
      Bước 3: Lọc bỏ chính cuốn sách đang xem (khỏi tự gợi ý chính nó).
      Bước 4: Trả về danh sách chuẩn hóa cho Frontend.

    Args:
      book_id       – ID sách đang xem.
      limit         – Số lượng gợi ý muốn lấy (mặc định 10).
      only_in_stock – Chỉ gợi ý sách đang còn hàng (mặc định True).
    """
    client = get_os_client()

    # Bước 1: Lấy Vector của cuốn sách hiện tại
    try:
        source = client.get(
            index=INDEX, id=str(book_id),
            _source=["sbert_embedding", "title"],
        )
        vector = source["_source"].get("sbert_embedding")
    except Exception as e:
        logger.warning("[cb_service] Không tìm thấy book_id=%s: %s", book_id, e)
        return []

    if not vector:
        logger.warning("[cb_service] book_id=%s chưa có SBERT Vector. Hãy chạy reindex_full.", book_id)
        return []

    # Bước 2: Xây dựng k-NN query với Filter kết hợp
    filter_clauses = [
        {"term": {"status": "active"}},
        {"bool": {"must_not": [{"term": {"book_id": str(book_id)}}]}},  # Loại sách đang xem
    ]
    if only_in_stock:
        filter_clauses.append({"term": {"in_stock": True}})

    # Lấy limit+5 để sau khi lọc vẫn đủ limit sách
    k = limit + 5
    query = {
        "size": k,
        "query": {
            "knn": {
                "sbert_embedding": {
                    "vector": vector,
                    "k": k,
                    "filter": {
                        "bool": {"must": filter_clauses}
                    }
                }
            }
        },
        "_source": [
            "book_id", "title", "author_name", "price",
            "avg_rating", "main_image_url", "in_stock",
            "categories", "trending_score",
        ],
    }

    # Bước 3: Gửi lệnh và thu hoạch kết quả
    resp = client.search(index=INDEX, body=query)
    hits = resp.get("hits", {}).get("hits", [])

    results = []
    for hit in hits[:limit]:
        src   = hit["_source"]
        score = hit["_score"]          # Cosine Similarity score (0.0 → 1.0)
        results.append({
            "book_id":       int(src.get("book_id", 0)),
            "title":         src.get("title", ""),
            "author_name":   src.get("author_name", ""),
            "price":         src.get("price"),
            "avg_rating":    src.get("avg_rating"),
            "main_image_url":src.get("main_image_url"),
            "in_stock":      src.get("in_stock"),
            "categories":    src.get("categories", []),
            "similarity":    round(float(score), 4),   # Điểm tương đồng ngữ nghĩa AI
            "reason":        "sbert_knn",              # Giải thích tại sao được gợi ý
        })

    logger.info("[cb_service] book_id=%s → %d sách gợi ý", book_id, len(results))
    return results
