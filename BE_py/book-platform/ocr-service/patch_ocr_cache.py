"""
patch_ocr_cache.py - Inject MD5 cache + stats vao ocr_router.py
Chay trong container: python /app/patch_ocr_cache.py
"""
import sys

filepath = "/app/ocr_app/routers/ocr_router.py"

with open(filepath, "r", encoding="utf-8") as f:
    src = f.read()

if "_ocr_cache" in src:
    print("Already patched, skip.")
    sys.exit(0)

# 1. Them hashlib + threading vao imports
src = src.replace(
    "import logging\nimport time\nfrom typing import",
    "import hashlib\nimport logging\nimport threading\nimport time\nfrom typing import"
)
print("Step 1: Added imports")

# 2. Them cache vars sau router = APIRouter(...)
cache_code = '''

# ==================== MD5 RESULT CACHE (Task 3.3) ====================
# Van de: User upload cung 1 anh 2 lan -> EasyOCR chay lai 3-8s
# Giai phap: hash MD5 anh bytes -> cache ket qua 1h -> lan sau <1ms
_ocr_cache = {}  # {md5: (OCRResponse, expire_at)}
_ocr_cache_lock = threading.Lock()
_OCR_CACHE_TTL = 3600   # 1 gio
_OCR_CACHE_MAX = 200    # toi da 200 anh

# Performance counters (Task 5.2)
import time as _time_mod
_perf = {"total": 0, "hits": 0, "visual": 0, "ocr_runs": 0, "total_ms": 0.0, "start": _time_mod.time()}
_perf_lock = threading.Lock()


def _ocr_cache_get(h):
    with _ocr_cache_lock:
        e = _ocr_cache.get(h)
        if e is None:
            return None
        r, exp = e
        if _time_mod.time() > exp:
            del _ocr_cache[h]
            return None
        return r


def _ocr_cache_set(h, r):
    with _ocr_cache_lock:
        if len(_ocr_cache) >= _OCR_CACHE_MAX and h not in _ocr_cache:
            del _ocr_cache[next(iter(_ocr_cache))]
        _ocr_cache[h] = (r, _time_mod.time() + _OCR_CACHE_TTL)


def get_ocr_stats():
    with _perf_lock:
        t = _perf["total"] or 1
        with _ocr_cache_lock:
            cc = len(_ocr_cache)
        return {
            "total_requests": _perf["total"],
            "cache_hits": _perf["hits"],
            "cache_hit_rate": f"{round(_perf['hits'] / t * 100, 1)}%",
            "visual_hits": _perf["visual"],
            "ocr_runs": _perf["ocr_runs"],
            "avg_latency_ms": round(_perf["total_ms"] / t, 1),
            "cached_images": cc,
            "cache_capacity": _OCR_CACHE_MAX,
            "uptime_seconds": round(_time_mod.time() - _perf["start"]),
        }

# =====================================================================
'''

src = src.replace(
    'router = APIRouter(prefix="/api/ocr", tags=["OCR"])',
    'router = APIRouter(prefix="/api/ocr", tags=["OCR"])' + cache_code
)
print("Step 2: Added cache vars")

# 3. Inject MD5 check vao search_by_cover truoc TANG 0
# Tim doan "relevance = assess_image_relevance(image_bytes)"
target_line = "    relevance = assess_image_relevance(image_bytes)"
md5_inject = """    # ── MD5 Cache Check (truoc ca garbage detection) ─────────────────────
    _md5 = hashlib.md5(image_bytes).hexdigest()
    _cached_result = _ocr_cache_get(_md5)
    if _cached_result is not None:
        with _perf_lock:
            _perf["hits"] += 1
            _perf["total"] += 1
            _perf["total_ms"] += 1
        logger.info("MD5 cache HIT (%s...) <1ms", _md5[:8])
        _cached_result.processing_time_ms = 1
        return _cached_result
    with _perf_lock:
        _perf["total"] += 1\n\n"""

if target_line in src:
    src = src.replace(target_line, md5_inject + target_line, 1)
    print("Step 3: Injected MD5 check before garbage detection")
else:
    print("WARNING: target_line not found, skipping step 3")

# 4. Cache ket qua thanh cong cuoi search_by_cover
# Tim doan return cuoi cung cua search_by_cover
old_return = '''    return OCRResponse(
        success=True if search_results else len(raw_text.strip()) > 0,
        processing_time_ms=elapsed,
        extracted_text=raw_text,
        book_info=book_info_schema,
        search_results=search_results,
        total_results=len(search_results),
        engine_used=engine_used,
        match_method="ocr_search" if search_results else "not_found",
        image_quality=quality["quality"],
        error=quality["message"] if quality["quality"] == "low" and not search_results else None,
    )'''

new_return = '''    _final = OCRResponse(
        success=True if search_results else len(raw_text.strip()) > 0,
        processing_time_ms=elapsed,
        extracted_text=raw_text,
        book_info=book_info_schema,
        search_results=search_results,
        total_results=len(search_results),
        engine_used=engine_used,
        match_method="ocr_search" if search_results else "not_found",
        image_quality=quality["quality"],
        error=quality["message"] if quality["quality"] == "low" and not search_results else None,
    )
    with _perf_lock:
        _perf["ocr_runs"] += 1
        _perf["total_ms"] += elapsed
    if _final.success and search_results:
        _ocr_cache_set(_md5, _final)
        logger.info("MD5 cache SET (%s...) %d results", _md5[:8], len(search_results))
    return _final'''

if old_return in src:
    src = src.replace(old_return, new_return, 1)
    print("Step 4: Added cache set + perf logging at return")
else:
    print("WARNING: old_return not found exact match")

# 5. Add /stats endpoint at the end (before ENDPOINT 2)
stats_endpoint = '''

# ==================== ENDPOINT: /stats (Task 5.1) ====================
@router.get("/stats", summary="Thong ke hieu suat OCR")
def ocr_stats():
    """Tra ve thong ke cache hit rate, latency, so lan OCR chay."""
    stats = get_ocr_stats()
    from ocr_app.services.image_similarity_engine import get_index_stats
    stats["visual_index"] = get_index_stats()
    return stats

'''

# Them vao truoc ENDPOINT 2
target_ep2 = '''# ==============================
# ENDPOINT 2: EXTRACT BOOK INFO'''
if target_ep2 not in src:
    target_ep2 = '''# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 2: EXTRACT BOOK INFO'''

if target_ep2 in src:
    src = src.replace(target_ep2, stats_endpoint + target_ep2, 1)
    print("Step 5: Added /stats endpoint")
else:
    # Them vao cuoi file truoc scan-receipt
    src += stats_endpoint
    print("Step 5: Added /stats endpoint at end")

with open(filepath, "w", encoding="utf-8") as f:
    f.write(src)

print("Patch complete. Running syntax check...")
import py_compile
try:
    py_compile.compile(filepath, doraise=True)
    print("Syntax OK!")
except py_compile.PyCompileError as e:
    print(f"Syntax ERROR: {e}")
    sys.exit(1)
