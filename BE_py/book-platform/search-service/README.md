python -m venv venv
venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt

cd docker
docker compose up -d

curl -k -u admin:VnBook$2025!Qx9 https://localhost:9200

cd E:\TLCN\BE_py\book-platform\search-service
curl -k -u admin:VnBook$2025!Qx9 ^
-H "Content-Type: application/json" ^
-X PUT "https://localhost:9200/books_v2" ^
--data-binary "@search_app/search/index/mapping_books_v2.json"

curl -k -u admin:VnBook$2025!Qx9 https://localhost:9200/_cat/indices?v


curl -k -u admin:VnBook$2025!Qx9 -H "Content-Type: application/json" -X POST https://localhost:9200/_aliases -d "{\"actions\":[{\"add\":{\"index\":\"books_v2\",\"alias\":\"books_current\"}}]}"


venv\Scripts\activate
python -m search_app.jobs.reindex_full

curl -k -u admin:VnBook$2025!Qx9 https://localhost:9200/books_v2/\_count

python -m search_app.jobs.sync_incremental

uvicorn app.main:app --reload
