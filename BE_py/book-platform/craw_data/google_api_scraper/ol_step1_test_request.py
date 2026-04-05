import requests

url = "https://openlibrary.org/search.json"

params = {
    "q": "programming",  # từ khóa, tạm dùng 'programming' cho dễ thấy dữ liệu
    "page": 1
}

resp = requests.get(url, params=params, timeout=20)
print("Status code:", resp.status_code)
print("URL thực tế:", resp.url)

# In thử một ít JSON
print("Độ dài nội dung:", len(resp.text))
print(resp.text[:1000])
