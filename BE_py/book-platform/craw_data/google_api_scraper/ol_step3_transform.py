import requests
import pandas as pd

BASE_URL = "https://openlibrary.org/search.json"


def extract_book_from_doc(doc):
    """Chuyển 1 doc Open Library thành 1 record gọn."""
    title = doc.get("title") or ""
    authors = doc.get("author_name") or []
    author_name = ", ".join(authors)

    subjects = doc.get("subject") or []
    subjects_str = ", ".join(subjects)

    first_publish_year = doc.get("first_publish_year") or None

    # cover image
    cover_id = doc.get("cover_i")
    if cover_id:
        cover_url = f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
    else:
        cover_url = ""

    # link tới trang sách trên Open Library
    work_key = doc.get("key") or ""       # ví dụ "/works/OL12345W"
    if work_key:
        openlibrary_url = f"https://openlibrary.org{work_key}"
    else:
        openlibrary_url = ""

    return {
        "title": title,
        "author_name": author_name,
        "subjects": subjects_str,
        "first_publish_year": first_publish_year,
        "cover_url": cover_url,
        "openlibrary_url": openlibrary_url,
    }


def test_transform():
    params = {
        "q": "programming",
        "page": 1
    }
    resp = requests.get(BASE_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    docs = data.get("docs", [])
    rows = [extract_book_from_doc(doc) for doc in docs[:5]]

    df = pd.DataFrame(rows)
    print(df)


if __name__ == "__main__":
    test_transform()
