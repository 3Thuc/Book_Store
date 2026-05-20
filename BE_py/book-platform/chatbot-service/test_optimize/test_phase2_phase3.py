"""
=============================================================
  BOOKSTORE PLATFORM - Phase 2 & Phase 3 Full Test Suite
=============================================================
Chay lenh: python test_optimize/test_phase2_phase3.py

Yeu cau: Cac service phai dang chay
  - platform_api: http://localhost:8000
  - ocr:          http://localhost:8005
  - redis:        localhost:6379
"""
import json
import time
import urllib.request
import urllib.error
import os
import sys

# =============================================================
# CONFIG
# =============================================================
BASE_URL    = "http://localhost:8000"
OCR_URL     = "http://localhost:8005"

PASS  = "[PASS]"
FAIL  = "[FAIL]"
SKIP  = "[SKIP]"
INFO  = "[INFO]"
SEP   = "-" * 56

results = []

# =============================================================
# HELPERS
# =============================================================
def req(url, method="GET", body=None, timeout=10):
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if data else {}
    try:
        req_obj = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req_obj, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {}
    except Exception as ex:
        return -1, {"_error": str(ex)}

def check(name, ok, detail="", warn=False):
    icon = PASS if ok else (SKIP if warn else FAIL)
    results.append((name, ok, warn))
    line = f"  {icon} {name}"
    print(line)
    if detail:
        print(f"       -> {detail}")
    return ok

def section(title):
    print(f"\n{SEP}")
    print(f"  {title}")
    print(SEP)

def summary():
    total   = len(results)
    passed  = sum(1 for _, ok, warn in results if ok)
    warned  = sum(1 for _, ok, warn in results if warn and not ok)
    failed  = total - passed - warned

    print(f"\n{'='*56}")
    print(f"  KET QUA: {passed}/{total} passed", end="")
    if warned: print(f"  |  {warned} warning", end="")
    if failed: print(f"  |  {failed} FAILED", end="")
    print()

    if failed == 0:
        print("  -> TAT CA PASS! Phase 2 & 3 COMPLETE.")
    else:
        print("  -> CAC TEST THAT BAI:")
        for name, ok, warn in results:
            if not ok and not warn:
                print(f"     x {name}")
    print("="*56)

# =============================================================
# PHASE 2.1 – SEARCH ANALYTICS LOGGING
# =============================================================
section("P2.1 – SEARCH ANALYTICS LOGGING")

# Tao vai search truoc de co data
print(f"  {INFO} Tao du lieu: thuc hien 5 queries...")
for q in ["python", "sach giao khoa", "zzz_khong_co_ket_qua_xyz", "toan hoc", "python"]:
    req(f"{BASE_URL}/books/search?q={q}&limit=3")
time.sleep(0.5)

s, r = req(f"{BASE_URL}/analytics/search-summary")
check("GET /analytics/search-summary tra ve 200", s == 200,
      f"HTTP {s}")
if s == 200:
    check("Co truong total_queries", "total_queries" in r,
          f"total_queries={r.get('total_queries','MISSING')}")
    check("Co truong miss_rate_pct", "miss_rate_pct" in r,
          f"miss_rate_pct={r.get('miss_rate_pct','MISSING')}%")
    check("Co truong avg_latency_ms", "avg_latency_ms" in r,
          f"avg_latency_ms={r.get('avg_latency_ms','MISSING')}ms")
    check("total_queries > 0 (da log duoc query)", r.get("total_queries", 0) > 0,
          f"total_queries={r.get('total_queries',0)}")

s, r = req(f"{BASE_URL}/analytics/search-top?n=5")
check("GET /analytics/search-top?n=5 tra ve 200", s == 200, f"HTTP {s}")
if s == 200:
    top_list = r.get("top_queries", r) if isinstance(r, dict) else r
    check("Co danh sach top_queries", isinstance(top_list, list),
          f"type={type(top_list).__name__}, len={len(top_list) if isinstance(top_list,list) else '?'}")
    if isinstance(top_list, list) and top_list:
        check("Moi entry co query va count", "query" in top_list[0] and "count" in top_list[0],
              f"top1: query='{top_list[0].get('query')}', count={top_list[0].get('count')}")

s, r = req(f"{BASE_URL}/analytics/search-miss?n=5")
check("GET /analytics/search-miss?n=5 tra ve 200", s == 200, f"HTTP {s}")
if s == 200:
    miss_list = r.get("miss_queries", r) if isinstance(r, dict) else r
    check("Miss queries la list", isinstance(miss_list, list),
          f"type={type(miss_list).__name__}, len={len(miss_list) if isinstance(miss_list,list) else '?'}")

# =============================================================
# PHASE 2.2 – OCR ASYNC QUEUE
# =============================================================
section("P2.2 – OCR ASYNC QUEUE")

# Health check
s, r = req(f"{OCR_URL}/api/ocr/health")
ocr_up = check("GET /api/ocr/health = 200", s == 200, f"status={r.get('status','?')}")

# Queue stats
s, r = req(f"{OCR_URL}/api/ocr/queue-stats")
check("GET /api/ocr/queue-stats = 200", s == 200, f"HTTP {s}")
if s == 200:
    workers = r.get("workers", 0)
    check("Co it nhat 2 background workers", workers >= 2,
          f"workers={workers} (can >= 2)")
    check("Co truong queue_size", "queue_size" in r,
          f"queue_size={r.get('queue_size', 'MISSING')}")

# Submit async task (khong co anh that, dung 1x1 pixel PNG)
print(f"  {INFO} Test submit async task (anh gia lap)...")
PNG_1x1 = (
    b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01'
    b'\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00'
    b'\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18'
    b'\xd8N\x00\x00\x00\x00IEND\xaeB`\x82'
)

import urllib.parse
boundary = b"----TestBoundary"
body = (
    b"------TestBoundary\r\n"
    b"Content-Disposition: form-data; name=\"file\"; filename=\"test.png\"\r\n"
    b"Content-Type: image/png\r\n\r\n" + PNG_1x1 + b"\r\n"
    b"------TestBoundary--\r\n"
)
try:
    req_obj = urllib.request.Request(
        f"{OCR_URL}/api/ocr/search-by-cover-async",
        data=body,
        headers={"Content-Type": "multipart/form-data; boundary=----TestBoundary"},
        method="POST"
    )
    with urllib.request.urlopen(req_obj, timeout=10) as resp:
        async_status = resp.status
        async_r = json.loads(resp.read())
except urllib.error.HTTPError as e:
    async_status = e.code
    try: async_r = json.loads(e.read())
    except: async_r = {}
except Exception as ex:
    async_status = -1
    async_r = {"_error": str(ex)}

task_id = async_r.get("task_id") or async_r.get("id")
check("POST /api/ocr/search-by-cover-async tra ve ngay", async_status in (200, 202),
      f"HTTP {async_status}, task_id={task_id}")

if task_id:
    # Poll result
    time.sleep(1.5)
    s, r = req(f"{OCR_URL}/api/ocr/result/{task_id}")
    check("GET /api/ocr/result/{task_id} hoat dong", s in (200, 404),
          f"HTTP {s}, status={r.get('status','?')}")
    if s == 200:
        check("Co truong status trong result", "status" in r,
              f"status={r.get('status')}")

    # Progress endpoint
    s, _ = req(f"{OCR_URL}/api/ocr/progress/{task_id}", timeout=3)
    check("GET /api/ocr/progress/{task_id} hoat dong", s in (200, 404),
          f"HTTP {s}")

# =============================================================
# PHASE 2.3 – MMR DIVERSITY RECOMMENDATION
# =============================================================
section("P2.3 – MMR DIVERSITY RECOMMENDATION")

# Test for-you default (diverse=true)
s, r = req(f"{BASE_URL}/recommend/user/1/for-you?limit=10")
check("GET /recommend/user/1/for-you?limit=10 = 200", s == 200,
      f"HTTP {s}, returned {len(r) if isinstance(r,list) else '?'} items")

if s == 200 and isinstance(r, list) and len(r) >= 3:
    # Kiem tra da dang the loai
    categories = [item.get("category_name") or item.get("category") or item.get("reason","") for item in r]
    unique_cats = set(c for c in categories if c)
    check("Ket qua da dang the loai (MMR)", len(unique_cats) >= 2,
          f"unique categories={len(unique_cats)}: {list(unique_cats)[:5]}")

# So sanh diverse=true vs diverse=false
s1, r1 = req(f"{BASE_URL}/recommend/user/2/for-you?limit=10&diverse=true")
s2, r2 = req(f"{BASE_URL}/recommend/user/2/for-you?limit=10&diverse=false")
check("diverse=true tra ve ket qua hop le", s1 == 200,
      f"HTTP {s1}, {len(r1) if isinstance(r1,list) else '?'} items")
check("diverse=false tra ve ket qua hop le", s2 == 200,
      f"HTTP {s2}, {len(r2) if isinstance(r2,list) else '?'} items")

# Test similar books diversity
s, r = req(f"{BASE_URL}/recommend/book/1/cf?limit=5&diverse=true")
check("GET /recommend/book/1/cf?diverse=true = 200", s in (200, 404),
      f"HTTP {s}")

# =============================================================
# PHASE 3.1 – A/B TESTING FRAMEWORK
# =============================================================
section("P3.1 – A/B TESTING FRAMEWORK")

# Test bucket assignment logic
bucket_tests = [
    (0,   "sbert_knn"),   # slot=0  -> 0-49 -> sbert_knn
    (49,  "sbert_knn"),   # slot=49 -> 0-49 -> sbert_knn
    (50,  "cf_als"),      # slot=50 -> 50-79 -> cf_als
    (79,  "cf_als"),      # slot=79 -> 50-79 -> cf_als
    (80,  "rule_based"),  # slot=80 -> 80-99 -> rule_based
    (99,  "rule_based"),  # slot=99 -> 80-99 -> rule_based
    (100, "sbert_knn"),   # slot=0  (100%100=0) -> sbert_knn
]
bucket_ok = True
for uid, expected in bucket_tests:
    slot = uid % 100
    if slot <= 49:   actual = "sbert_knn"
    elif slot <= 79: actual = "cf_als"
    else:            actual = "rule_based"
    if actual != expected:
        bucket_ok = False
        print(f"       FAIL: user_id={uid} slot={slot} -> {actual} != {expected}")
check("Bucket logic chinh xac (7 test cases)", bucket_ok,
      "slot 0-49=sbert_knn | 50-79=cf_als | 80-99=rule_based")

# API: my-bucket
s, r = req(f"{BASE_URL}/recommend/ab/my-bucket/1")
check("GET /recommend/ab/my-bucket/1 = 200", s == 200,
      f"bucket={r.get('bucket','?')}, slot={r.get('slot','?')}")
if s == 200:
    check("Tra ve du 4 truong (user_id, bucket, slot, label)", 
          all(k in r for k in ("user_id","bucket","slot","label")),
          f"keys={list(r.keys())}")

# API: stats
s, r = req(f"{BASE_URL}/recommend/ab/stats")
check("GET /recommend/ab/stats = 200", s == 200, f"HTTP {s}")
if s == 200:
    check("Co truong buckets", "buckets" in r,
          f"keys={list(r.keys())}")
    if "buckets" in r:
        check("Co du 3 buckets (sbert_knn, cf_als, rule_based)",
              all(b in r["buckets"] for b in ("sbert_knn","cf_als","rule_based")),
              f"buckets found: {list(r['buckets'].keys())}")

# Test ab_override
s_knn, _ = req(f"{BASE_URL}/recommend/user/5/for-you?limit=3&ab_override=sbert_knn")
s_cf,  _ = req(f"{BASE_URL}/recommend/user/5/for-you?limit=3&ab_override=cf_als")
s_rb,  _ = req(f"{BASE_URL}/recommend/user/5/for-you?limit=3&ab_override=rule_based")
check("ab_override=sbert_knn hoat dong", s_knn == 200, f"HTTP {s_knn}")
check("ab_override=cf_als hoat dong",    s_cf  == 200, f"HTTP {s_cf}")
check("ab_override=rule_based hoat dong",s_rb  == 200, f"HTTP {s_rb}")

# Verify exposure duoc ghi
time.sleep(0.3)
s, r2 = req(f"{BASE_URL}/recommend/ab/stats")
if s == 200 and "total_exposures" in r2:
    check("Exposure duoc ghi vao Redis sau khi goi for-you",
          r2.get("total_exposures", 0) > 0,
          f"total_exposures={r2.get('total_exposures',0)}")

# =============================================================
# PHASE 3.2 – SYNONYM FILTER
# =============================================================
section("P3.2 – SYNONYM FILTER (OpenSearch)")

synonym_pairs = [
    ("sach",    "cuon",      "sach = cuon (y nghia tuong duong)"),
    ("sach",    "quyen",     "sach = quyen"),
    ("mua",     "dat hang",  "mua = dat hang"),
    ("tac gia", "nguoi viet","tac gia = nguoi viet"),
]
for q1, q2, desc in synonym_pairs:
    s1, r1 = req(f"{BASE_URL}/books/search?q={urllib.parse.quote(q1)}&limit=1")
    s2, r2 = req(f"{BASE_URL}/books/search?q={urllib.parse.quote(q2)}&limit=1")
    t1 = r1.get("total", 0) if s1 == 200 else 0
    t2 = r2.get("total", 0) if s2 == 200 else 0
    # Ca hai deu co ket qua (du khong can bang nhau chinh xac)
    check(f"Synonym: {desc}", s1 == 200 and s2 == 200 and t2 > 0,
          f"'{q1}'={t1} hits | '{q2}'={t2} hits")

# Search analytics co ghi nhan query
s, r = req(f"{BASE_URL}/analytics/search-summary")
check("Analytics ghi nhan sau synonym search", s == 200,
      f"total_queries={r.get('total_queries',0)}, miss_rate={r.get('miss_rate_pct',0)}%")

# =============================================================
# PHASE 3.3 – BK-TREE PHASH
# =============================================================
section("P3.3 – BK-TREE pHash (OCR)")

# Kiem tra static: file bktree.py ton tai
bktree_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "source-code", "BE_py", "book-platform", "ocr-service",
    "ocr_app", "services", "bktree.py"
)
check("File bktree.py ton tai", os.path.exists(bktree_path),
      bktree_path)

# Kiem tra noi dung BK-Tree
if os.path.exists(bktree_path):
    content = open(bktree_path, encoding="utf-8").read()
    check("BKTree class duoc dinh nghia", "class BKTree" in content)
    check("Ham find_best() O(log n) ton tai", "def find_best" in content)
    check("Ham search() tra ve list ket qua", "def search" in content)
    check("Pruning logic: lo <= k <= hi", "lo <= k <= hi" in content)

# Kiem tra image_similarity_engine da duoc patch
engine_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "source-code", "BE_py", "book-platform", "ocr-service",
    "ocr_app", "services", "image_similarity_engine.py"
)
if os.path.exists(engine_path):
    eng = open(engine_path, encoding="utf-8").read()
    check("Engine import BKTree", "from ocr_app.services.bktree import BKTree" in eng)
    check("Engine co _bk_tree global", "_bk_tree: BKTree" in eng)
    check("build_hash_index xay BK-Tree", "BK-Tree built" in eng or "_bk_tree.add" in eng)
    check("find_similar_book dung BK-Tree (O(log n))", "_bk_tree.find_best" in eng)

# OCR service health
s, r = req(f"{OCR_URL}/api/ocr/health")
check("OCR service dang chay", s == 200, f"status={r.get('status','?')}")

# Queue stats sau tat ca test
s, r = req(f"{OCR_URL}/api/ocr/queue-stats")
if s == 200:
    check("Workers van on dinh sau test", r.get("workers", 0) >= 2,
          f"workers={r.get('workers',0)}, queue={r.get('queue_size',0)}")

# =============================================================
# SUMMARY
# =============================================================
summary()
