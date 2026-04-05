import requests
import json

API_KEY = "AIzaSyCRX14XD2udNBdUlmxfpIgLoKetVxbtqQ4"
BASE_URL = "https://www.googleapis.com/books/v1/volumes"


def test_google_books():
    params = {
        "q": "lập trình",      # từ khóa tiếng Việt
        "langRestrict": "vi",  # chỉ sách tiếng Việt
        "maxResults": 5,       # tối đa 5 kết quả / request
        "printType": "books",
        "key": API_KEY,
    }

    resp = requests.get(BASE_URL, params=params, timeout=20)
    print("Status code:", resp.status_code)
    print("URL thực tế:", resp.url)

    if resp.status_code != 200:
        print("Nội dung lỗi:", resp.text[:1000])
        return

    data = resp.json()
    print("Tổng items:", data.get("totalItems"))
    items = data.get("items", [])
    print("Số items trong response:", len(items))

    # In thử 1 item để xem cấu trúc:
    if items:
        first = items[0]
        print(json.dumps(first, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    test_google_books()
