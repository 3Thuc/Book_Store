import requests
import pandas as pd

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
    # ưu tiên ảnh lớn hơn nếu có
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


def test_transform():
    params = {
        "q": "lập trình",
        "langRestrict": "vi",
        "maxResults": 10,
        "printType": "books",
        "key": API_KEY,
    }

    resp = requests.get(BASE_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    rows = [extract_book_from_item(it) for it in items]

    df = pd.DataFrame(rows)
    print(df)


if __name__ == "__main__":
    test_transform()
