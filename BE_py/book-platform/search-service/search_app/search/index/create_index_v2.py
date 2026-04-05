"""
Tạo (hoặc kiểm tra) Index OpenSearch với mapping mới nhất.
Bao gồm trường knn_vector (SBERT 768D) để hỗ trợ Semantic Search.

Chạy sau khi xóa index cũ:
  python -m search_app.search.index.create_index_v2
"""
import os
import json
from pathlib import Path

from search_app.search.client import get_os_client

INDEX = os.getenv("OPENSEARCH_INDEX", "books_current")
MAPPING_PATH = Path(__file__).parent / "mapping_books_v2.json"


def main():
    client = get_os_client()

    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        body = json.load(f)

    if client.indices.exists(index=INDEX):
        print(f"[INFO] Index '{INDEX}' đã tồn tại.")
        print("[INFO] Nếu muốn rebuild từ đầu: xóa index trên OpenSearch Dashboards")
        print(f"       → DELETE /{INDEX}  →  sau đó chạy lại script này.")
    else:
        resp = client.indices.create(index=INDEX, body=body)
        print(f"[OK] Đã tạo index '{INDEX}' thành công.")
        print(f"     Acknowledged: {resp.get('acknowledged')}")


if __name__ == "__main__":
    main()
