import requests
import pandas as pd
from time import sleep

API_KEY = "AIzaSyCRX14XD2udNBdUlmxfpIgLoKetVxbtqQ4"
BASE_URL = "https://www.googleapis.com/books/v1/volumes"


def extract_book_from_item(item):
    volume = item.get("volumeInfo", {})
    title = volume.get("title", "")
    authors = volume.get("authors", []) or []
    author_name = ", ".join(authors)

    categories = volume.get("categories", []) or []
    categories_str = ", ".join(categories)

    description = volume.get("description", "") or ""
    published_date = volume.get("publishedDate", "") or ""
    language = volume.get("language", "")

    image_links = volume.get("imageLinks", {}) or {}
    cover_url = (
        image_links.get("thumbnail")
        or image_links.get("smallThumbnail")
        or ""
    )

    info_link = volume.get("infoLink", "") or item.get("selfLink", "")

    return {
        "title": title,
        "author_name": author_name,
        "categories": categories_str,
        "description": description,
        "published_date": published_date,
        "language": language,
        "cover_url": cover_url,
        "info_link": info_link,
    }


def search_google_books_vi(query, max_results=400, sleep_seconds=1.0):
    all_rows = []
    max_per_req = 40

    for start in range(0, max_results, max_per_req):
        params = {
            "q": query,
            "langRestrict": "vi",
            "printType": "books",
            "startIndex": start,
            "maxResults": max_per_req,
            "key": API_KEY,
        }
        print(f"[{query}] startIndex={start}")
        resp = requests.get(BASE_URL, params=params, timeout=20)
        if resp.status_code != 200:
            print("  Lỗi:", resp.status_code, resp.text[:500])
            break

        data = resp.json()
        items = data.get("items", [])
        print("  Số items:", len(items))
        if not items:
            break

        for item in items:
            row = extract_book_from_item(item)
            all_rows.append(row)
            if len(all_rows) >= max_results:
                return all_rows

        sleep(sleep_seconds)

    return all_rows


if __name__ == "__main__":
    rows = search_google_books_vi("sách", max_results=300)
    print("Tổng sách lấy được:", len(rows))

    df = pd.DataFrame(rows)
    print(df.head())

    df.to_excel("googlebooks_vi_sach.xlsx", index=False)
    print("Đã lưu googlebooks_vi_sach.xlsx")
