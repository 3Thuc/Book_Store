import requests
import pandas as pd
from time import sleep

BASE_URL = "https://openlibrary.org/search.json"


def extract_book_from_doc(doc):
    title = doc.get("title") or ""
    authors = doc.get("author_name") or []
    author_name = ", ".join(authors)

    subjects = doc.get("subject") or []
    subjects_str = ", ".join(subjects)

    first_publish_year = doc.get("first_publish_year") or None

    cover_id = doc.get("cover_i")
    cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg" if cover_id else ""

    work_key = doc.get("key") or ""
    openlibrary_url = f"https://openlibrary.org{work_key}" if work_key else ""

    return {
        "title": title,
        "author_name": author_name,
        "subjects": subjects_str,
        "first_publish_year": first_publish_year,
        "cover_url": cover_url,
        "openlibrary_url": openlibrary_url,
    }


def search_books(query, max_pages=5, max_results=2000, sleep_seconds=1.0):
    all_rows = []
    for page in range(1, max_pages + 1):
        params = {"q": query, "page": page}
        print(f"[{query}] Page {page}...")
        resp = requests.get(BASE_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        docs = data.get("docs", [])
        print("  Docs:", len(docs))
        if not docs:
            break

        for doc in docs:
            row = extract_book_from_doc(doc)
            all_rows.append(row)
            if len(all_rows) >= max_results:
                return all_rows
        sleep(sleep_seconds)
    return all_rows


if __name__ == "__main__":
    queries = ["programming", "novel", "history", "science", "mathematics"]
    all_rows = []

    for q in queries:
        rows = search_books(q, max_pages=5, max_results=2000)
        all_rows.extend(rows)
        print(f"==> Query '{q}' lấy được {len(rows)} sách")

    df = pd.DataFrame(all_rows)

    # Loại trùng theo title + author + year (đơn giản)
    df.drop_duplicates(
        subset=["title", "author_name", "first_publish_year"], inplace=True)

    print("Tổng sau khi loại trùng:", len(df))
    df.to_excel("openlibrary_multi_queries.xlsx", index=False)
    print("Đã lưu openlibrary_multi_queries.xlsx")
