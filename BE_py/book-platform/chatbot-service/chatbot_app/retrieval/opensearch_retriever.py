"""
opensearch_retriever.py – Retrieval bằng OpenSearch k-NN SBERT.

Thay thế FAISS bằng OpenSearch đang chạy sẵn trong hệ thống.

Lý do chuyển từ FAISS sang OpenSearch:
  - OpenSearch đã chạy sẵn cho recommendation-service
  - Không cần build lại index riêng (dùng lại chatbot_kb index)
  - HNSW của OpenSearch nhanh hơn FAISS IndexFlatIP cho large-scale
  - Hỗ trợ filter kết hợp vector + metadata (category, price...)

Index đang dùng:
  - "books_current": index sách đã có SBERT embedding (từ recommendation-service)
  - "chatbot_kb":    index FAQ, chính sách (tạo mới bằng scripts/index_kb.py)
"""
from opensearchpy import OpenSearch, RequestsHttpConnection
from chatbot_app.retrieval.sql_retriever import get_real_stock
from chatbot_app.config import (
    OPENSEARCH_HOST, OPENSEARCH_PORT,
    OPENSEARCH_USER, OPENSEARCH_PASSWORD,
    OPENSEARCH_USE_SSL, OPENSEARCH_VERIFY_CERTS,
)
from chatbot_app.nlu.customer_intent_classifier import get_sbert_model
from functools import lru_cache


# ── Singleton OpenSearch client ────────────────────────────────
_os_client: OpenSearch | None = None


def get_os_client() -> OpenSearch:
    global _os_client
    if _os_client is None:
        _os_client = OpenSearch(
            hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
            http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
            use_ssl=OPENSEARCH_USE_SSL,
            verify_certs=OPENSEARCH_VERIFY_CERTS,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            timeout=10,
        )
    return _os_client


# ── LRU Cache cho SBERT encoding ─────────────────────────────────
# Lý do: SBERT model encode mỗi query mất ~80-150ms (chạy CPU).
# Khi user hỏi lại cùng từ khóa (VD: bấm lại tag), không cần tính lại.
# maxsize=256: cache tối đa 256 query khác nhau trong RAM.
@lru_cache(maxsize=256)
def _encode_query_cached(query: str) -> tuple:
    """Encode query bằng SBERT và có cache theo từ khóa. Thread-safe (GIL)."""
    model = get_sbert_model()
    vec = model.encode(query, normalize_embeddings=True)
    return tuple(vec.tolist())  # tuple là hashable → dùng được làm cache key


# ── Tìm kiếm sách bằng Hybrid (SBERT k-NN + BM25) ────────────────────────────
def search_books_semantic(
    query: str,
    top_k:     int   = 5,
    price_max: int   = None,
    price_min: int   = None,
    genre:     str   = None,
    min_score: float = 0.5,
) -> list[dict]:
    """
    Tìm sách bằng phương pháp Hỗn Hợp (Hybrid Search): k-NN SBERT kết hợp BM25 Keyword Search.
    
    Quy trình:
      1. Tách và mã hóa Query thành Vector bằng SBERT.
      2. Truy vấn Elasticsearch qua 'bool/should': kết hợp Text ('title', 'author') và Vector.

    Args:
        query:     Câu tìm kiếm (VD: "kỹ năng lãnh đạo")
        top_k:     Số kết quả trả về
        price_max: Giá tối đa (đồng)
        price_min: Giá tối thiểu
        genre:     Thể loại sách (filter)

    Returns:
        List[{title, author, price, stock, category, score}]
    """
    model     = get_sbert_model()
    # Dùng hàm có cache thay vì encode trực tiếp → tiết kiệm ~100ms nếu query đã từng dùng
    query_vec = list(_encode_query_cached(query))

    # Build filter
    filters = [
        {"term": {"status": "active"}}
    ]
    if price_max:
        filters.append({"range": {"price": {"lte": price_max}}})
    if price_min:
        filters.append({"range": {"price": {"gte": price_min}}})
    if genre:
        filters.append({"match": {"category_name": genre}})

    body = {
        "size": top_k * 3,  # Lấy nhiều hơn để trừ hao khi stock ở MySQL = 0
        "_source": ["title", "author_name", "price", "stock_quantity",
                    "category_name", "cover_image", "book_id"],
        "query": {
            "bool": {
                "should": [
                    {"knn": {"sbert_embedding": {"vector": query_vec, "k": top_k}}},
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["title^3", "author_name^2", "category_name"],
                            "type": "best_fields"
                        }
                    }
                ],
                "minimum_should_match": 1,
                "filter": filters,
            }
        }
    }

    client = get_os_client()
    try:
        resp = client.search(index="books_current", body=body)

        results = []
        for hit in resp["hits"]["hits"]:
            src = hit["_source"]
            results.append({
                "book_id":  src.get("book_id"),
                "title":    src.get("title", ""),
                "author":   src.get("author_name", ""),
                "price":    src.get("price", 0),
                "stock":    src.get("stock_quantity", 0),
                "category": src.get("category_name", ""),
                "image":    src.get("cover_image", ""),
                "score":    round(hit["_score"], 4),
            })

        # XÁC NHẬN LẠI VỚI MYSQL ĐỂ LẤY SỐ LƯỢNG KHO THỰC TẾ
        book_ids = [b["book_id"] for b in results if b.get("book_id")]
        real_stocks = get_real_stock(book_ids)
        
        filtered_results = []
        for b in results:
            bid = b.get("book_id")
            if not bid: continue
            
            # Cập nhật số lượng kho mới nhất
            b["stock"] = real_stocks.get(int(bid), 0)
            
            # Ta KHÔNG loại bỏ sách có stock <= 0. Sách hết hàng VẪN được list ra
            # để Chatbot có thể nhắc tới (tư duy Lạc loài/Tư vấn thay thế).
            
            # Ngăn chặn Retrieval Hallucination bằng Ngưỡng Điểm Vector Rất Thấp
            if b.get("score", 0) < min_score:
                continue
                
            filtered_results.append(b)
            if len(filtered_results) >= top_k:
                break
                
        return filtered_results
    except Exception as e:
        print(f"⚠️  search_books_semantic error: {e}")
        return []


# ── Tìm trong Chatbot KB (FAQ, chính sách) ───────────────────
def search_knowledge_base(query: str, top_k: int = 3) -> list[dict]:
    """
    Tìm kiếm ngữ nghĩa trong chatbot_kb index.
    Dùng cho: policy_return, payment_method, store_info, general_query.

    Returns:
        List[{id, text, category, score}]
    """
    model     = get_sbert_model()
    query_vec = model.encode(query, normalize_embeddings=True).tolist()

    body = {
        "size": top_k,
        "_source": ["text", "category"],
        "query": {
            "knn": {
                "sbert_embedding": {"vector": query_vec, "k": top_k}
            }
        }
    }

    client = get_os_client()
    try:
        resp   = client.search(index="chatbot_kb", body=body)
        return [
            {
                "id":       hit["_id"],
                "text":     hit["_source"].get("text", ""),
                "category": hit["_source"].get("category", ""),
                "score":    round(hit["_score"], 4),
            }
            for hit in resp["hits"]["hits"]
        ]
    except Exception as e:
        print(f"⚠️  chatbot_kb search error: {e}")
        return []


# ── Kiểm tra chatbot_kb tồn tại ──────────────────────────────
def kb_index_exists() -> bool:
    """Kiểm tra index chatbot_kb đã được tạo chưa."""
    try:
        return get_os_client().indices.exists(index="chatbot_kb")
    except Exception:
        return False
