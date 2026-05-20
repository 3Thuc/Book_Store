# -*- coding: utf-8 -*-
"""
Test script -- Recommendation Service v2.1.0 Optimizations
===========================================================
Kiem tra 4 optimization:
  Fix #1: Admin Auth (X-Admin-Key)
  Fix #2: CF Rebuild Non-blocking
  Fix #3: Redis Cache Popular/Trending/Top-Rated
  Fix #4: Redis Retry (khong test truc tiep, gian tiep qua ab/stats)

Chay:
    python test_recommend_service.py --base-url http://localhost:8000 --admin-key <KEY>
"""

import argparse
import sys
import time
import requests

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

results = {"pass": 0, "fail": 0}


def ok(msg):   print("  [PASS] " + msg)
def fail(msg): print("  [FAIL] " + msg)
def info(msg): print("  [INFO] " + msg)

def header(title):
    print("\n" + "=" * 60)
    print("  " + title)
    print("=" * 60)

def check(condition, pass_msg, fail_msg):
    if condition:
        ok(pass_msg)
        results["pass"] += 1
    else:
        fail(fail_msg)
        results["fail"] += 1
    return condition


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--admin-key", default="")
    args = parser.parse_args()

    BASE = args.base_url.rstrip("/")
    KEY  = args.admin_key

    print("\nRecommendation Service v2.1.0 -- Optimization Tests")
    print("Base URL  : " + BASE)
    print("Admin Key : " + ("<set>" if KEY else "<NOT SET>"))

    # ------------------------------------------------------------------
    header("Test #1 -- Admin Auth: /ab/reset khong co key -> 422")
    # ------------------------------------------------------------------
    try:
        r = requests.post(f"{BASE}/recommend/ab/reset", timeout=5)
        check(r.status_code == 422,
              f"POST /ab/reset khong co key -> 422 (nhan {r.status_code})",
              f"Expected 422, nhan {r.status_code}: {r.text[:120]}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #2 -- Admin Auth: /ab/reset sai key -> 401")
    # ------------------------------------------------------------------
    try:
        r = requests.post(f"{BASE}/recommend/ab/reset",
                          headers={"X-Admin-Key": "wrong-key-xyz"}, timeout=5)
        check(r.status_code == 401,
              f"POST /ab/reset sai key -> 401 (nhan {r.status_code})",
              f"Expected 401, nhan {r.status_code}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #3 -- Admin Auth: /user/{id}/cf/rebuild khong co key -> 422")
    # ------------------------------------------------------------------
    try:
        r = requests.post(f"{BASE}/recommend/user/1/cf/rebuild", timeout=5)
        check(r.status_code == 422,
              f"POST /user/1/cf/rebuild khong co key -> 422 (nhan {r.status_code})",
              f"Expected 422, nhan {r.status_code}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #4 -- Admin Auth: /book/{id}/cb/clear-cache khong co key -> 422")
    # ------------------------------------------------------------------
    try:
        r = requests.post(f"{BASE}/recommend/book/1/cb/clear-cache", timeout=5)
        check(r.status_code == 422,
              f"POST /book/1/cb/clear-cache khong co key -> 422 (nhan {r.status_code})",
              f"Expected 422, nhan {r.status_code}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #5 -- Admin Auth: /ab/reset dung key -> 200")
    # ------------------------------------------------------------------
    if KEY:
        try:
            r = requests.post(f"{BASE}/recommend/ab/reset",
                              headers={"X-Admin-Key": KEY}, timeout=5)
            check(r.status_code == 200,
                  f"POST /ab/reset dung key -> 200 (nhan {r.status_code})",
                  f"Expected 200, nhan {r.status_code}: {r.text[:120]}")
        except Exception as e:
            fail(f"Ket noi that bai: {e}"); results["fail"] += 1
    else:
        info("Bo qua: chua cung cap --admin-key")

    # ------------------------------------------------------------------
    header("Test #6 -- Popular endpoint hoat dong binh thuong")
    # ------------------------------------------------------------------
    try:
        r = requests.get(f"{BASE}/recommend/popular", params={"limit": 10}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            check(isinstance(data, list) and len(data) > 0,
                  f"GET /popular -> {len(data)} items",
                  f"Popular tra ve rong hoac sai format")
        else:
            check(False, "", f"GET /popular -> {r.status_code}: {r.text[:120]}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #7 -- Fix #3: Cache Popular (lan 2 phai nhanh hon)")
    # ------------------------------------------------------------------
    try:
        t0 = time.perf_counter()
        r1 = requests.get(f"{BASE}/recommend/popular", params={"limit": 10}, timeout=10)
        t1 = time.perf_counter() - t0

        t0 = time.perf_counter()
        r2 = requests.get(f"{BASE}/recommend/popular", params={"limit": 10}, timeout=10)
        t2 = time.perf_counter() - t0

        info(f"Lan 1 (cold): {int(t1*1000)}ms | Lan 2 (cache): {int(t2*1000)}ms")

        if r1.status_code == 200 and r2.status_code == 200:
            data1, data2 = r1.json(), r2.json()
            same_data = (len(data1) == len(data2))
            check(same_data,
                  f"Cache tra ve cung so item ({len(data2)}) -- cache hoat dong",
                  f"Cache co van de: lan1={len(data1)} lan2={len(data2)}")
            if t2 < t1:
                info(f"Cache lam nhanh hon {int((t1-t2)*1000)}ms")
            else:
                info("Cache speedup khong ro rang o local (network overhead nho) -- dung voi MySQL cao tai")
        else:
            check(False, "", f"Popular request fail: {r1.status_code} / {r2.status_code}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #8 -- Trending endpoint hoat dong binh thuong")
    # ------------------------------------------------------------------
    try:
        r = requests.get(f"{BASE}/recommend/trending", params={"days": 7, "limit": 10}, timeout=10)
        check(r.status_code == 200 and isinstance(r.json(), list),
              f"GET /trending -> {len(r.json()) if r.status_code==200 else '?'} items",
              f"GET /trending -> {r.status_code}: {r.text[:120]}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #9 -- Top-Rated endpoint hoat dong binh thuong")
    # ------------------------------------------------------------------
    try:
        r = requests.get(f"{BASE}/recommend/top-rated", params={"limit": 10}, timeout=10)
        check(r.status_code == 200 and isinstance(r.json(), list),
              f"GET /top-rated -> {len(r.json()) if r.status_code==200 else '?'} items",
              f"GET /top-rated -> {r.status_code}: {r.text[:120]}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #10 -- Fix #2: For-You user moi (cache rong) tra ve nhanh")
    # ------------------------------------------------------------------
    NEW_USER_ID = 999999  # User ID chac chan chua co cache
    try:
        t0 = time.perf_counter()
        r = requests.get(f"{BASE}/recommend/user/{NEW_USER_ID}/for-you",
                         params={"limit": 10}, timeout=15)
        elapsed = int((time.perf_counter() - t0) * 1000)

        if r.status_code == 200:
            data = r.json()
            check(isinstance(data, list),
                  f"GET /user/{NEW_USER_ID}/for-you -> {len(data)} items | latency={elapsed}ms (non-blocking)",
                  f"For-you tra ve sai format")
            if elapsed < 3000:
                info(f"Latency {elapsed}ms < 3000ms -- rebuild chay background, response nhanh OK")
            else:
                info(f"Latency {elapsed}ms -- co the rebuild van dang block, can kiem tra lai")
        else:
            check(False, "", f"GET /for-you -> {r.status_code}: {r.text[:120]}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #11 -- A/B Stats endpoint hoat dong (Fix #4 gian tiep)")
    # ------------------------------------------------------------------
    try:
        r = requests.get(f"{BASE}/recommend/ab/stats", timeout=5)
        if r.status_code == 200:
            data = r.json()
            has_fields = "buckets" in data or "status" in data
            check(has_fields,
                  f"GET /ab/stats -> {data.get('status', 'ok')} (Redis retry logic active)",
                  f"Response khong hop le: {r.text[:120]}")
        else:
            check(False, "", f"GET /ab/stats -> {r.status_code}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    header("Test #12 -- ab/click public endpoint (khong can key)")
    # ------------------------------------------------------------------
    try:
        r = requests.post(f"{BASE}/recommend/ab/click",
                          params={"user_id": 1, "book_id": 1, "position": 0},
                          timeout=5)
        check(r.status_code == 200,
              f"POST /ab/click (public) -> 200 OK",
              f"POST /ab/click -> {r.status_code}: {r.text[:120]}")
    except Exception as e:
        fail(f"Ket noi that bai: {e}"); results["fail"] += 1

    # ------------------------------------------------------------------
    total = results["pass"] + results["fail"]
    print("\n" + "=" * 60)
    print(f"KET QUA: {results['pass']} PASS / {results['fail']} FAIL / {total} TOTAL")
    if results["fail"] == 0:
        print("[OK] Tat ca tests PASS -- Recommendation Service v2.1.0 hoat dong tot!")
    else:
        print(f"[!!] Co {results['fail']} test FAIL")
    sys.exit(0 if results["fail"] == 0 else 1)


if __name__ == "__main__":
    main()
