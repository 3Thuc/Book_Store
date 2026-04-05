from typing import Optional, Dict, Any, List
import os
from dotenv import load_dotenv
from search_app.search.client import get_os_client
from search_app.utils.text_normalize import fold_vi

load_dotenv()
INDEX = os.getenv("OPENSEARCH_INDEX", "books_current")


def suggest_books(q: str, limit: int = 10):
    client = get_os_client()

    q_folded = fold_vi(q)

    body = {
        "size": limit,
        "_source": ["book_id", "title", "author_name", "main_image_url", "avg_rating", "price"],
        "query": {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": q,
                            "type": "bool_prefix",
                            "fields": [
                                "title_suggest^2",
                                "title_suggest._2gram",
                                "title_suggest._3gram",
                            ],
                        }
                    },
                    {
                        "multi_match": {
                            "query": q_folded,
                            "type": "bool_prefix",
                            "fields": [
                                "title_suggest_folded^3",
                                "title_suggest_folded._2gram",
                                "title_suggest_folded._3gram",
                            ],
                        }
                    },
                ],
                "minimum_should_match": 1
            }
        }
    }

    res = client.search(index=INDEX, body=body)
    hits = res.get("hits", {}).get("hits", [])
    return [h["_source"] for h in hits]


def search_books(
    q: str,
    page: int = 1,
    limit: int = 20,
    in_stock: Optional[bool] = None,
    category: Optional[str] = None,
    language: Optional[str] = None,
    fmt: Optional[str] = None,
    sort: str = "relevance",
) -> Dict[str, Any]:
    client = get_os_client()
    q_folded = fold_vi(q)

    page = max(1, page)
    limit = max(1, min(50, limit))
    from_ = (page - 1) * limit

    filters: List[Dict[str, Any]] = [
        {"terms": {"status": ["active", "out_of_stock", "inactive"]}}]
    if in_stock is True:
        filters.append({"term": {"in_stock": True}})
    elif in_stock is False:
        filters.append({"term": {"in_stock": False}})
    if category:
        filters.append({"term": {"categories": category}})
    if language:
        filters.append({"term": {"language": language}})
    if fmt:
        filters.append({"term": {"format": fmt}})

    # Sort (tùy chọn)
    sort_clause = None
    if sort == "price_asc":
        sort_clause = [{"price": {"order": "asc"}}]
    elif sort == "price_desc":
        sort_clause = [{"price": {"order": "desc"}}]
    elif sort == "newest":
        sort_clause = [{"updated_at": {"order": "desc"}}]
    elif sort == "rating_desc":
        sort_clause = [{"avg_rating": {"order": "desc"}},
                       {"rating_count": {"order": "desc"}}]

    # Query: kết hợp nhánh có dấu + nhánh folded (không dấu)
    # If query is empty or whitespace, return all books with filters applied
    if not q or not q.strip():
        text_query = {
            "bool": {
                "must": {"match_all": {}},
                "filter": filters,
            }
        }
    else:
        text_query = {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": q,
                            "type": "best_fields",
                            "fields": [
                                "title^6",
                                "author_name^3",
                                "publisher_name^1",
                                "description^1",
                            ],
                            "fuzziness": "AUTO",
                            "prefix_length": 1,
                            "max_expansions": 50,
                        }
                    },
                    {
                        "multi_match": {
                            "query": q_folded,
                            "type": "best_fields",
                            "fields": [
                                "title_folded^5",
                                "author_name_folded^2.5",
                                "publisher_name_folded^1",
                                "description_folded^1",
                            ],
                            "fuzziness": "AUTO",
                            "prefix_length": 1,
                            "max_expansions": 50,
                        }
                    },
                ],
                "minimum_should_match": 1,
                "filter": filters,
            }
        }

    body: Dict[str, Any] = {
        "size": limit,
        "from": from_,
        "_source": [
            "book_id", "title", "author_name", "publisher_name", "main_image_url",
            "price", "avg_rating", "rating_count", "stock_quantity", "in_stock",
            "categories", "format", "language", "publication_year", "status"
        ],
        "highlight": {"fields": {"title": {}, "author_name": {}}},
    }
    
    # Add query with function_score only if there's a search term
    if not q or not q.strip():
        body["query"] = text_query
    else:
        body["query"] = {
            "function_score": {
                "query": text_query,
                "functions": [
                    {"field_value_factor": {"field": "trending_score",
                                            "factor": 0.4, "missing": 0}},
                    {"field_value_factor": {"field": "popularity",
                                            "factor": 0.2, "missing": 0}},
                    {"field_value_factor": {"field": "avg_rating",
                                            "factor": 0.2, "missing": 0}},
                ],
                "score_mode": "sum",
                "boost_mode": "sum",
            }
        }

    if sort_clause:
        body["sort"] = sort_clause

    res = client.search(index=INDEX, body=body)

    total_obj = res.get("hits", {}).get("total", 0)
    total = total_obj.get("value", 0) if isinstance(
        total_obj, dict) else int(total_obj)

    items = []
    for h in res.get("hits", {}).get("hits", []):
        src = h.get("_source", {})
        src["_score"] = h.get("_score")
        src["highlight"] = h.get("highlight", {})
        items.append(src)

    return {"page": page, "limit": limit, "total": total, "items": items}
