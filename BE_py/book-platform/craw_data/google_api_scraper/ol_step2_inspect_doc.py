import requests
import json

BASE_URL = "https://openlibrary.org/search.json"


def test_one_page():
    params = {
        "q": "programming",
        "page": 1
    }
    resp = requests.get(BASE_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    print("numFound:", data.get("numFound"))
    print("số docs trong trang này:", len(data.get("docs", [])))

    # In thử 1 doc đầu tiên cho dễ nhìn
    first_doc = data["docs"][0]
    print(json.dumps(first_doc, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    test_one_page()
