import os
import json
from datetime import datetime
from dotenv import load_dotenv
from search_app.utils.text_normalize import fold_vi
from search_app.db.mysql import get_mysql_conn
from search_app.search.client import get_os_client

load_dotenv()

INDEX = os.getenv("OPENSEARCH_INDEX", "books_v1")
STATE_PATH = os.getenv("SYNC_STATE_PATH", "search_app/jobs/state.json")

BATCH_LIMIT = int(os.getenv("SYNC_BATCH_LIMIT", "2000")
                  )  # mỗi lần xử lý tối đa N sách


def read_state() -> datetime:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        s = data.get("last_synced_at", "1970-01-01T00:00:00")
        # hỗ trợ cả "YYYY-MM-DD HH:MM:SS"
        s = s.replace("Z", "")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except FileNotFoundError:
        return datetime.fromisoformat("1970-01-01T00:00:00")


def write_state(dt: datetime):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {"last_synced_at": dt.isoformat(timespec="seconds")},
            f,
            ensure_ascii=False,
            indent=2,
        )



def calc_scores(row):
    v = int(row.get("views_7d") or 0)
    a = int(row.get("atc_7d") or 0)
    p = int(row.get("purchase_7d") or 0)
    popularity = v + 3 * a + 10 * p
    trending = 0.2 * v + 1.0 * a + 3.0 * p
    return float(popularity), float(trending)


def main():
    last_synced = read_state()
    print("[STATE] last_synced_at =",
          last_synced.isoformat(timespec="seconds"))

    conn = get_mysql_conn()
    with conn.cursor() as cur:
        # 1) lấy danh sách book_id thay đổi
        cur.execute(
            """
            SELECT book_id, updated_at
            FROM books
            WHERE updated_at > %s AND status <> 'deleted'
            ORDER BY updated_at ASC
            LIMIT %s
            """,
            (last_synced.strftime("%Y-%m-%d %H:%M:%S"), BATCH_LIMIT),
        )
        changed = cur.fetchall()

        if not changed:
            print("[OK] No changes.")
            return

        book_ids = [int(x["book_id"]) for x in changed]
        max_updated_at = max(x["updated_at"] for x in changed)

        # 2) lấy full data cho các book_id đó
        # Note: IN (...) cần placeholder đúng số lượng
        placeholders = ",".join(["%s"] * len(book_ids))

        sql_books = f"""
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
        WHERE b.book_id IN ({placeholders})
        GROUP BY b.book_id
        """
        cur.execute(sql_books, book_ids)
        books = cur.fetchall()

        # 3) tính trend 7 ngày cho các book_id đó (nhanh hơn query toàn bảng)
        sql_trend = f"""
        SELECT
          book_id,
          SUM(action_type='view') AS views_7d,
          SUM(action_type='add_to_cart') AS atc_7d,
          SUM(action_type='purchase') AS purchase_7d
        FROM user_actions
        WHERE action_date >= NOW() - INTERVAL 7 DAY
          AND book_id IN ({placeholders})
        GROUP BY book_id
        """
        cur.execute(sql_trend, book_ids)
        trends = cur.fetchall()

    trend_map = {int(x["book_id"]): x for x in trends}

    # 4) build bulk upsert
    lines = []
    for b in books:
        bid = int(b["book_id"])
        popularity, trending = calc_scores(trend_map.get(bid, {}))

        title = (b.get("title") or "").strip()
        author = (b.get("author_name") or "").strip()
        publisher = (b.get("publisher_name") or "").strip()
        desc = (b.get("description") or "").strip()

        doc = {
            "book_id": str(bid),

            # có dấu
            "title": title,
            "author_name": author,
            "publisher_name": publisher,
            "description": desc,

            # không dấu (folded)
            "title_folded": fold_vi(title),
            "author_name_folded": fold_vi(author),
            "publisher_name_folded": fold_vi(publisher),
            "description_folded": fold_vi(desc),

            # autocomplete
            "title_suggest": title,
            "title_suggest_folded": fold_vi(title),

            "categories": (b["categories"].split("||") if b.get("categories") else []),

            "isbn": b.get("isbn"),
            "language": b.get("language"),
            "format": b.get("format"),
            "publication_year": b.get("publication_year"),

            "price": float(b["price"]),
            "stock_quantity": int(b["stock_quantity"]),
            "in_stock": (int(b["stock_quantity"]) > 0 and b["status"] == "active"),
            "status": b["status"],

            "avg_rating": float(b.get("avg_rating") or 0),
            "rating_count": int(b.get("rating_count") or 0),

            "popularity": popularity,
            "trending_score": trending,

            "main_image_url": b.get("main_image_url"),
            "updated_at": b["updated_at"].isoformat() if b.get("updated_at") else None,
        }

        lines.append(json.dumps({"index": {"_index": INDEX, "_id": str(bid)}}))
        lines.append(json.dumps(doc))

    body = "\n".join(lines) + "\n"

    # 5) bulk
    client = get_os_client()
    resp = client.bulk(body=body)
    client.indices.refresh(index=INDEX)

    errors = resp.get("errors")
    print("[BULK] books =", len(books), "errors =", errors)

    # 6) update state chỉ khi bulk không lỗi
    if errors:
        # In ra 1 vài item lỗi để debug nhanh
        items = resp.get("items", [])[:3]
        print("[WARN] bulk error sample:", json.dumps(
            items, ensure_ascii=False)[:1000])
        print("[STOP] Not updating state because bulk errors=True")
        return

    write_state(max_updated_at)
    print("[OK] Updated state to:", max_updated_at.isoformat(timespec="seconds"))


if __name__ == "__main__":
    main()
