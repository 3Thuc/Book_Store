import os
import time
import html
import requests
import pandas as pd
from typing import Dict, Any, List, Tuple


# ================== CẤU HÌNH CƠ BẢN ==================

BASE_URL = "https://www.googleapis.com/books/v1/volumes"
MAX_PER_REQUEST = 40          # Google cho tối đa 40
DEFAULT_SLEEP_SECONDS = 1.0
DEFAULT_LANG = "vi"


# ================== HÀM TIỆN ÍCH ==================

def get_api_key() -> str:
    """
    Lấy API key từ biến môi trường GOOGLE_BOOKS_API_KEY.
    Nếu không có thì fallback sang key hard-code.
    """
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY", "").strip()
    if not api_key:
        # Fallback key bạn đang dùng để test
        api_key = "AIzaSyCRX14XD2udNBdUlmxfpIgLoKetVxbtqQ4"

    if not api_key:
        raise RuntimeError(
            "Chưa cấu hình GOOGLE_BOOKS_API_KEY.\n"
            "Hãy set biến môi trường hoặc sửa lại get_api_key()."
        )
    return api_key


def extract_isbns(volume: Dict[str, Any]) -> Tuple[str, str]:
    """Lấy ISBN_10, ISBN_13 từ volumeInfo.industryIdentifiers (nếu có)."""
    industry_ids = volume.get("industryIdentifiers") or []
    isbn_10, isbn_13 = "", ""

    for ident in industry_ids:
        id_type = ident.get("type") or ""
        identifier = ident.get("identifier") or ""
        if id_type == "ISBN_10":
            isbn_10 = identifier
        elif id_type == "ISBN_13":
            isbn_13 = identifier

    return isbn_10, isbn_13


def extract_cover_url(volume: Dict[str, Any]) -> str:
    """
    Lấy URL cover từ volumeInfo.imageLinks.
    Ưu tiên thumbnail -> smallThumbnail.
    """
    image_links = volume.get("imageLinks", {}) or {}

    cover_url = (
        image_links.get("thumbnail")
        or image_links.get("smallThumbnail")
        or ""
    )

    # Gỡ &amp; nếu có
    cover_url = html.unescape(cover_url or "")

    return cover_url


def extract_book_from_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Rút gọn 1 item Google Books thành 1 record dict (1 dòng DataFrame).

    cover_url: lấy từ imageLinks (thumbnail / smallThumbnail),
    KHÔNG tự build từ id -> tránh 404.
    """
    volume = item.get("volumeInfo", {}) or {}

    title = volume.get("title", "") or ""

    authors = volume.get("authors", []) or []
    author_name = ", ".join(authors)

    categories = volume.get("categories", []) or []
    categories_str = ", ".join(categories)

    description = volume.get("description", "") or ""
    published_date = volume.get("publishedDate", "") or ""
    language = volume.get("language", "") or ""
    publisher = volume.get("publisher", "") or ""

    page_count = volume.get("pageCount")
    average_rating = volume.get("averageRating")
    ratings_count = volume.get("ratingsCount")

    cover_url = extract_cover_url(volume)

    info_link = volume.get("infoLink", "") or item.get("selfLink", "") or ""

    isbn_10, isbn_13 = extract_isbns(volume)

    return {
        "title": title,
        "author_name": author_name,
        "categories": categories_str,
        "description": description,
        "published_date": published_date,
        "language": language,
        "publisher": publisher,
        "page_count": page_count,
        "average_rating": average_rating,
        "ratings_count": ratings_count,
        "isbn_10": isbn_10,
        "isbn_13": isbn_13,
        "cover_url": cover_url,
        "info_link": info_link,
    }


# ================== HÀM GỌI GOOGLE BOOKS API ==================

def search_google_books(
    query: str,
    *,
    lang: str = DEFAULT_LANG,
    max_results: int = 400,
    sleep_seconds: float = DEFAULT_SLEEP_SECONDS,
) -> List[Dict[str, Any]]:
    """
    Tìm sách với Google Books API cho 1 query.
    - lang: mã ngôn ngữ (vd: 'vi', 'en'...)
    - max_results: tối đa số sách muốn lấy cho riêng query này.

    CHỈ trả về các row có cover_url (các item không có imageLinks sẽ bị bỏ qua).
    """
    api_key = get_api_key()
    all_rows: List[Dict[str, Any]] = []

    for start in range(0, max_results, MAX_PER_REQUEST):
        remaining = max_results - len(all_rows)
        if remaining <= 0:
            break

        per_request = min(MAX_PER_REQUEST, remaining)

        params = {
            "q": query,
            "langRestrict": lang,
            "printType": "books",
            "startIndex": start,
            "maxResults": per_request,
            "key": api_key,
        }

        print(f"[{query}] startIndex={start}, maxResults={per_request}")

        try:
            resp = requests.get(
                BASE_URL,
                params=params,
                timeout=20,
                headers={"User-Agent": "book-crawler/1.0"},
            )
        except Exception as e:
            print(f"  Lỗi kết nối: {e}")
            break

        if resp.status_code != 200:
            print("  Lỗi HTTP:", resp.status_code)
            try:
                print("  Nội dung:", resp.text[:300])
            except Exception:
                pass
            break

        data = resp.json()
        total_items = data.get("totalItems", 0)
        items = data.get("items") or []

        print("  totalItems (cho query này):", total_items)
        print("  Số items nhận được:", len(items))

        if not items:
            break

        for item in items:
            row = extract_book_from_item(item)

            # CHỈ giữ sách có cover_url không rỗng
            cover_url = (row.get("cover_url") or "").strip()
            if not cover_url:
                continue

            all_rows.append(row)
            if len(all_rows) >= max_results:
                return all_rows

        time.sleep(sleep_seconds)

    return all_rows


def deduplicate_books(df: pd.DataFrame) -> pd.DataFrame:
    """
    Loại bỏ trùng:
    1. Với sách có ISBN_13 -> drop_duplicates theo isbn_13
    2. Với sách không có ISBN_13 -> drop_duplicates theo (title, author_name, published_date)
    """
    for col in ["isbn_13", "title", "author_name", "published_date"]:
        if col not in df.columns:
            df[col] = ""

    df["isbn_13"] = df["isbn_13"].astype(str).str.strip()
    df["title"] = df["title"].astype(str).str.strip()
    df["author_name"] = df["author_name"].astype(str).str.strip()
    df["published_date"] = df["published_date"].astype(str).str.strip()

    df_has_isbn = df[df["isbn_13"] != ""].copy()
    df_no_isbn = df[df["isbn_13"] == ""].copy()

    before_has = len(df_has_isbn)
    df_has_isbn.drop_duplicates(subset=["isbn_13"], inplace=True)
    after_has = len(df_has_isbn)
    print(f"Loại trùng theo ISBN_13: {before_has} -> {after_has}")

    before_no = len(df_no_isbn)
    df_no_isbn.drop_duplicates(
        subset=["title", "author_name", "published_date"], inplace=True
    )
    after_no = len(df_no_isbn)
    print(
        f"Loại trùng theo (title, author_name, published_date): "
        f"{before_no} -> {after_no}"
    )

    df_final = pd.concat([df_has_isbn, df_no_isbn], ignore_index=True)
    print("Tổng số sách sau khi gộp & loại trùng:", len(df_final))
    return df_final


# ================== HÀM TEST 1 SÁCH ==================

def test_one_book(query: str = "sách", lang: str = DEFAULT_LANG):
    """
    Tìm 1 sách (có cover_url) để test,
    in ra title, cover_url và HTTP status của cover_url.
    """
    print("=== TEST 1 SÁCH ===")
    rows = search_google_books(query, lang=lang, max_results=1)
    if not rows:
        print("Không tìm được sách nào có cover_url cho query:", query)
        return

    book = rows[0]
    title = book.get("title")
    cover_url = book.get("cover_url")

    print("Tiêu đề:", title)
    print("Cover URL:", cover_url)

    try:
        r = requests.get(cover_url, timeout=20)
        print("HTTP status cover_url:", r.status_code)
    except Exception as e:
        print("Lỗi khi request cover_url:", e)


# ================== MAIN CRAWL NHIỀU QUERY ==================

def main():
    queries = [
        # Văn học
        "tiểu thuyết",
        "truyện ngắn",
        "ngôn tình",
        "trinh thám",
        "văn học Việt Nam",
        "văn học nước ngoài",
        # Kinh tế – tài chính – kinh doanh
        "kinh tế",
        "marketing",
        "tài chính",
        "đầu tư",
        "quản trị kinh doanh",
        "khởi nghiệp",
        # Kỹ năng – tâm lý
        "kỹ năng sống",
        "tâm lý học",
        "phát triển bản thân",
        # Lịch sử – văn hóa – tôn giáo
        "lịch sử",
        "văn hóa",
        "tôn giáo",
        # Khoa học – công nghệ
        "khoa học",
        "khoa học tự nhiên",
        "khoa học xã hội",
        "công nghệ thông tin",
        "lập trình",
        # Thiếu nhi – giáo dục
        "thiếu nhi",
        "truyện tranh",
        "sách giáo khoa",
        "học ngoại ngữ",
        "tiếng Anh",
    ]

    all_rows: List[Dict[str, Any]] = []
    MAX_PER_QUERY = 400

    for q in queries:
        print("=" * 60)
        print(f"Đang chạy query: '{q}'")
        try:
            rows = search_google_books(
                q, lang=DEFAULT_LANG, max_results=MAX_PER_QUERY
            )
        except Exception as e:
            print(f"  Lỗi khi xử lý query '{q}': {e}")
            continue

        print(
            f"==> Query '{q}' lấy được {len(rows)} sách (đã lọc chỉ sách có cover)\n"
        )
        all_rows.extend(rows)

    if not all_rows:
        print("Không có dữ liệu nào được lấy. Thoát.")
        return

    df = pd.DataFrame(all_rows)

    # LỌC LẠI MỘT LẦN NỮA (CHO CHẮC): chỉ giữ sách có cover_url
    df["cover_url"] = df["cover_url"].astype(str).str.strip()
    df = df[df["cover_url"] != ""].copy()
    print("Sau khi đảm bảo chỉ giữ sách có cover_url:", len(df))

    df_final = deduplicate_books(df)

    # Nếu vẫn còn &amp; đâu đó (hiếm), thì gỡ luôn
    df_final["cover_url"] = (
        df_final["cover_url"].astype(str).str.replace(
            "&amp;", "&", regex=False)
    )

    output_file = "googlebooks_vi_multi_enriched.xlsx"
    df_final.to_excel(output_file, index=False)
    print(f"Đã lưu file: {output_file}")


if __name__ == "__main__":
    # 👉 ĐỂ TEST 1 SÁCH:
    main()

    # 👉 Khi crawl thật thì comment dòng trên và mở dòng dưới:
    # main()
