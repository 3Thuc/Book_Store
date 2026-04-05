import os
import json
import time
from dotenv import load_dotenv

from search_app.db.mysql import get_mysql_conn
from search_app.search.client import get_os_client
from search_app.utils.text_normalize import fold_vi
from search_app.utils.sbert_model import get_sbert_model   # ← SBERT

load_dotenv()

INDEX      = os.getenv("OPENSEARCH_INDEX", "books_current")
BATCH_SIZE = int(os.getenv("REINDEX_BATCH_SIZE", "200"))

SQL_BOOKS = """
SELECT
  b.book_id, b.title, a.author_name, p.publisher_name,
  b.price, b.stock_quantity, b.description, b.publication_year,
  b.isbn, b.avg_rating, b.rating_count, b.language, b.format, b.status, b.updated_at,
  GROUP_CONCAT(DISTINCT c.category_name SEPARATOR '||') AS categories,
  MAX(CASE WHEN bi.is_main = 1 THEN bi.image_url END) AS main_image_url
FROM books b
LEFT JOIN authors a ON b.author_id = a.author_id
LEFT JOIN publishers p ON b.publisher_id = p.publisher_id
LEFT JOIN book_categories bc ON b.book_id = bc.book_id
LEFT JOIN categories c ON bc.category_id = c.category_id
LEFT JOIN book_images bi ON b.book_id = bi.book_id
WHERE b.status <> 'deleted'
GROUP BY b.book_id;
"""

SQL_TREND_7D = """
SELECT
  book_id,
  SUM(action_type='view') AS views_7d,
  SUM(action_type='add_to_cart') AS atc_7d,
  SUM(action_type='purchase') AS purchase_7d
FROM user_actions
WHERE action_date >= NOW() - INTERVAL 7 DAY
GROUP BY book_id;
"""


def calc_scores(row):
    v = int(row.get("views_7d") or 0)
    a = int(row.get("atc_7d") or 0)
    p = int(row.get("purchase_7d") or 0)
    popularity = v + 3 * a + 10 * p
    trending   = 0.2 * v + 1.0 * a + 3.0 * p
    return float(popularity), float(trending)


def build_ndjson_batch(books: list, trend_map: dict, model) -> str:
    """
    Build NDJSON cho 1 batch, tích hợp SBERT encoding hiệu quả.

    Bí quyết tối ưu tốc độ:
      Thay vì gọi model.encode() cho từng sách (1 lần/sách = N lần gọi),
      ta gom tất cả text trong batch lại → gọi model.encode(list) 1 lần.
      Sentence-Transformers tự xử lý batching nội bộ trên GPU/CPU → nhanh hơn 5-10x.
    """
    texts = []
    for b in books:
        title = (b.get("title") or "").strip()
        cats  = b["categories"].split("||") if b.get("categories") else []
        desc  = (b.get("description") or "").strip()[:300]
        texts.append(f"[{' '.join(cats)}] {title}. {desc}".strip())

    # Batch encode toàn bộ – trả về numpy array (N, 768)
    vectors = model.encode(texts, normalize_embeddings=True, show_progress_bar=False)

    lines = []
    for i, b in enumerate(books):
        bid       = int(b["book_id"])
        pop, trnd = calc_scores(trend_map.get(bid, {}))

        title     = (b.get("title")          or "").strip()
        author    = (b.get("author_name")    or "").strip()
        publisher = (b.get("publisher_name") or "").strip()
        desc      = (b.get("description")   or "").strip()
        cats      = b["categories"].split("||") if b.get("categories") else []

        doc = {
            "book_id":               str(bid),
            "title":                 title,
            "author_name":           author,
            "publisher_name":        publisher,
            "description":           desc,
            "title_folded":          fold_vi(title),
            "author_name_folded":    fold_vi(author),
            "publisher_name_folded": fold_vi(publisher),
            "description_folded":    fold_vi(desc),
            "title_suggest":         title,
            "title_suggest_folded":  fold_vi(title),
            "categories":            cats,
            "isbn":                  b.get("isbn"),
            "language":              b.get("language"),
            "format":                b.get("format"),
            "publication_year":      b.get("publication_year"),
            "price":                 float(b["price"]),
            "stock_quantity":        int(b["stock_quantity"]),
            "in_stock":              (int(b["stock_quantity"]) > 0 and b["status"] == "active"),
            "status":                b["status"],
            "avg_rating":            float(b.get("avg_rating")  or 0),
            "rating_count":          int(b.get("rating_count")  or 0),
            "popularity":            pop,
            "trending_score":        trnd,
            "main_image_url":        b.get("main_image_url"),
            "updated_at":            b["updated_at"].isoformat() if b.get("updated_at") else None,
            # Vector AI 768 chiều, chuẩn hóa L2 (cosine-ready)
            "sbert_embedding":       vectors[i].tolist(),
        }
        action = json.dumps({"index": {"_index": INDEX, "_id": str(bid)}})
        lines.append(action)
        lines.append(json.dumps(doc, ensure_ascii=False))

    return "\n".join(lines) + "\n"


def main():
    print("[SBERT] Đang load AI model (lần đầu tiên sẽ tải về ~500MB)...")
    model = get_sbert_model()
    print(f"[SBERT] Model sẵn sàng. Dimension: {model.get_sentence_embedding_dimension()}D")

    conn = get_mysql_conn()
    with conn.cursor() as cur:
        cur.execute(SQL_BOOKS)
        books = cur.fetchall()
        cur.execute(SQL_TREND_7D)
        trends = cur.fetchall()

    trend_map = {int(x["book_id"]): x for x in trends}
    client    = get_os_client()
    total     = len(books)
    indexed   = 0
    err_count = 0

    print(f"[START] Reindex {total} sách | batch={BATCH_SIZE} | SBERT enabled")

    for start in range(0, total, BATCH_SIZE):
        batch = books[start: start + BATCH_SIZE]

        body = build_ndjson_batch(batch, trend_map, model)
        resp = client.bulk(body=body)

        if resp.get("errors"):
            failed = [
                item for item in resp.get("items", [])
                if item.get("index", {}).get("status", 200) >= 400
            ]
            err_count += len(failed)
            print(f"  [WARN] Batch {start}-{start+len(batch)-1}: {len(failed)} lỗi")
        else:
            indexed += len(batch)

        done = min(start + BATCH_SIZE, total)
        print(f"  [{done}/{total}] ok={indexed}  errors={err_count}")
        time.sleep(0.3)    # Giảm áp lực memory OpenSearch giữa các batch

    client.indices.refresh(index=INDEX)
    print(f"\n[DONE] Indexed: {indexed}/{total}  |  SBERT Vectors: {indexed}  |  Errors: {err_count}")


if __name__ == "__main__":
    main()
