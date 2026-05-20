"""
search_integrator.py – Search Integration Module
=================================================
Gọi Search Service (:8000) và Google Books API (optional) để tìm sách
sau khi OCR đã trích xuất được thông tin từ ảnh.

Pattern: httpx AsyncClient với timeout 10s và retry 1 lần.

Tại sao dùng httpx?
- async native: không block FastAPI event loop (khác requests.get())
- Timeout rõ ràng: không để request bị treo vô thời hạn
- Connection pooling: tái dùng TCP connection → giảm latency
"""
import logging
import re
import unicodedata
from typing import List, Optional

import httpx

from ocr_app.config import GOOGLE_BOOKS_API_KEY, SEARCH_SERVICE_URL
from ocr_app.models.schemas import BookResult
from ocr_app.services.text_extractor import ExtractedBookInfo

logger = logging.getLogger(__name__)


def _fold_vi(text: str) -> str:
    """Chuẩn hóa tiếng Việt: bỏ dấu + lowercase để so sánh không phân biệt dấu.

    Dùng cho re-ranking: "đắc nhân tâm" ↔ "dac nhan tam"
    Giúp bắt được kết quả đúng khi OCR có lỗi dấu.
    """
    if not text:
        return ""
    s = text.strip().lower()
    s = s.replace("đ", "d").replace("Đ", "d")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s

# Timeout cho HTTP requests đến Search Service
# 10s là đủ cho OpenSearch query (thường <500ms trong Docker network)
_HTTP_TIMEOUT = httpx.Timeout(connect=3.0, read=10.0, write=5.0, pool=5.0)

# Giới hạn kết quả trả về: tăng lên 15 để cho user nhiều lựa chọn hơn
_SEARCH_LIMIT = 15


# ══════════════════════════════════════════════════════════════════════════════
# SEARCH SERVICE (:8000)
# ══════════════════════════════════════════════════════════════════════════════

async def search_via_service(query: str, limit: int = _SEARCH_LIMIT) -> List[BookResult]:
    """
    Gọi Search Service nội bộ: GET /books/search?q={query}

    Search Service đang chạy tại SEARCH_SERVICE_URL (mặc định: http://fastapi:8000).
    Trong Docker network, "fastapi" là tên container của service Platform API.

    Response format từ search_books():
    {
      "page": 1,
      "limit": 10,
      "total": 5,
      "items": [
        {
          "book_id": 42,
          "title": "Đắc Nhân Tâm",
          "author_name": "Dale Carnegie",
          "price": 79000.0,
          "main_image_url": "https://...",
          "_score": 12.5,
          ...
        },
        ...
      ]
    }
    """
    if not query or not query.strip():
        logger.warning("search_via_service: query rỗng, bỏ qua")
        return []

    url = f"{SEARCH_SERVICE_URL}/books/search"
    params = {"q": query.strip(), "limit": limit}

    logger.info("🔍 Gọi Search Service: %s?q=%s", url, query[:50])

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            logger.info("Search Service trả về %d kết quả", len(items))

            # Chuyển đổi sang BookResult schema của OCR Service
            results = []
            for item in items:
                try:
                    results.append(BookResult(
                        book_id      = int(item.get("book_id", 0)),
                        title        = str(item.get("title", "")),
                        author_name  = item.get("author_name"),
                        price        = item.get("price"),
                        image_url    = item.get("main_image_url") or item.get("image_url"),
                        score        = item.get("_score"),
                        avg_rating   = item.get("avg_rating", 0.0) or 0.0,
                        rating_count = item.get("rating_count", 0) or 0,
                        categories   = item.get("categories"),
                        stock_quantity = item.get("stock_quantity"),
                    ))
                except (ValueError, TypeError) as e:
                    logger.debug("Bỏ qua item không hợp lệ: %s – %s", item, e)

            return results

    except httpx.ConnectError:
        logger.error("❌ Không kết nối được Search Service tại %s", SEARCH_SERVICE_URL)
        logger.error("   Kiểm tra: container 'fastapi' đang chạy chưa?")
        return []
    except httpx.TimeoutException:
        logger.error("❌ Search Service timeout sau 10s")
        return []
    except httpx.HTTPStatusError as e:
        logger.error("❌ Search Service lỗi HTTP %d: %s", e.response.status_code, e.response.text[:200])
        return []
    except Exception as e:
        logger.exception("❌ search_via_service lỗi bất ngờ: %s", e)
        return []


async def search_via_service_ocr(query: str, limit: int = 15) -> List[BookResult]:
    """
    Gọi endpoint /books/ocr-search – tolerant với lỗi OCR nặng.

    Endpoint này dùng:
    - minimum_should_match: 30%  → chỉ cần 30% từ khớp
    - fuzziness: 2               → sai tối đa 2 ký tự/từ
    - fold_vi (không dấu)        → bỏ qua lỗi dấu tiếng Việt
    """
    if not query or not query.strip():
        return []

    url = f"{SEARCH_SERVICE_URL}/books/ocr-search"
    params = {"q": query.strip(), "limit": limit}

    logger.info("🔍 [OCR-Search] %s?q=%s", url, query[:50])

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            logger.info("OCR-Search trả về %d kết quả", len(items))

            results = []
            for item in items:
                try:
                    results.append(BookResult(
                        book_id      = int(item.get("book_id", 0)),
                        title        = str(item.get("title", "")),
                        author_name  = item.get("author_name"),
                        price        = item.get("price"),
                        image_url    = item.get("main_image_url") or item.get("image_url"),
                        score        = item.get("_score"),
                        avg_rating   = item.get("avg_rating", 0.0) or 0.0,
                        rating_count = item.get("rating_count", 0) or 0,
                        categories   = item.get("categories"),
                        stock_quantity = item.get("stock_quantity"),
                    ))
                except (ValueError, TypeError):
                    pass
            return results

    except Exception as e:
        logger.warning("❌ search_via_service_ocr lỗi: %s – fallback sang search thường", e)
        # Fallback về search thường nếu endpoint mới chưa deploy
        return await search_via_service(query, limit=limit)



# ══════════════════════════════════════════════════════════════════════════════
# GOOGLE BOOKS API (Fallback)
# ══════════════════════════════════════════════════════════════════════════════

async def search_google_books(isbn: Optional[str] = None, title: Optional[str] = None) -> List[BookResult]:
    """
    Tìm kiếm qua Google Books API (fallback khi Search Service không có kết quả).

    Ưu tiên: ISBN > title (ISBN tìm chính xác 100%)

    Google Books API không cần key cho free tier (giới hạn 1000 req/day).
    Có key → 1 triệu req/day.

    Response format:
    {
      "totalItems": 1,
      "items": [
        {
          "id": "...",
          "volumeInfo": {
            "title": "Đắc Nhân Tâm",
            "authors": ["Dale Carnegie"],
            "publisher": "NXB Tổng hợp",
            "industryIdentifiers": [{"type": "ISBN_13", "identifier": "..."}],
            "imageLinks": {"thumbnail": "..."},
            ...
          }
        }
      ]
    }
    """
    if not isbn and not title:
        return []

    # Build query
    if isbn:
        query = f"isbn:{isbn}"
    else:
        query = title

    params: dict = {"q": query, "maxResults": 5, "langRestrict": "vi"}
    if GOOGLE_BOOKS_API_KEY:
        params["key"] = GOOGLE_BOOKS_API_KEY

    url = "https://www.googleapis.com/books/v1/volumes"

    logger.info("📚 Gọi Google Books API: q=%s", query[:50])

    try:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            items = data.get("items", [])
            if not items:
                logger.info("Google Books: không tìm thấy kết quả")
                return []

            results = []
            for item in items[:5]:
                vol = item.get("volumeInfo", {})
                ident = vol.get("industryIdentifiers", [])

                # Lấy ISBN từ identifiers
                book_isbn = None
                for id_obj in ident:
                    if id_obj.get("type") in ("ISBN_13", "ISBN_10"):
                        book_isbn = id_obj.get("identifier")
                        break

                # Google Books không có book_id trong hệ thống → dùng 0
                # Frontend không navigate từ Google Books result, chỉ hiển thị metadata
                results.append(BookResult(
                    book_id    = 0,  # Không có trong local DB
                    title      = vol.get("title", ""),
                    author_name= ", ".join(vol.get("authors", [])) or None,
                    price      = None,  # Google Books không có giá VND
                    image_url  = vol.get("imageLinks", {}).get("thumbnail"),
                    score      = None,
                ))

            logger.info("Google Books trả về %d kết quả", len(results))
            return results

    except httpx.ConnectError:
        logger.warning("Google Books API không thể kết nối (offline?)")
        return []
    except httpx.TimeoutException:
        logger.warning("Google Books API timeout")
        return []
    except Exception as e:
        logger.warning("Google Books API lỗi: %s", e)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# API CHÍNH: integrate_search()
# ══════════════════════════════════════════════════════════════════════════════

async def integrate_search(book_info: ExtractedBookInfo) -> List[BookResult]:
    """
    Orchestrator v5: Dual-query parallel + Title-Match Re-ranking.

    Chiến lược mới (v5):
    1. Chạy SONG SONG primary_query và full_text_query
    2. Merge kết quả, dedup theo book_id
    3. Re-rank: sách nào có title xuất hiện trong OCR full_text → boost lên đầu
       → Giải quyết trường hợp title OCR sai nhưng full_text vẫn chứa title đúng
    4. Google Books fallback nếu vẫn không có kết quả

    Tại sao cần Re-rank?
    - Cũ: primary_query sai → trả 10 sách sai, full_text chưa bao giờ chạy
    - Mới: full_text luôn chạy → sách đúng xuất hiện trong kết quả,
           re-rank đẩy sách có title khớp OCR text lên đầu
    """
    logger.info("🔗 integrate_search v5: primary='%s', full_text='%s'",
                book_info.search_query[:50], book_info.full_text_query[:50])

    full_text_lower = (book_info.full_text_query or "").lower()

    # ── Bước 1: Chạy SONG SONG primary_query và full_text_query ─────────────
    import asyncio

    tasks = []
    if book_info.search_query:
        tasks.append(search_via_service(book_info.search_query, limit=_SEARCH_LIMIT))
    else:
        tasks.append(asyncio.sleep(0, result=[]))  # dummy

    if book_info.full_text_query and book_info.full_text_query != book_info.search_query:
        # Dùng OCR-search tolerant thay vì search thông thường
        # → minimum_should_match=30%, fuzziness=2, fold_vi loại dấu
        # → Bắt được "NGUÒl QIÀU cO /HẤT THÀNH BABYLOIY" → "Người Giàu Nhất Thành Babylon"
        tasks.append(search_via_service_ocr(book_info.full_text_query, limit=_SEARCH_LIMIT))
    else:
        tasks.append(asyncio.sleep(0, result=[]))  # dummy

    results_list = await asyncio.gather(*tasks)
    primary_results  = results_list[0] if results_list[0] else []
    fulltext_results = results_list[1] if results_list[1] else []

    logger.info("Primary query: %d kết quả | Full-text: %d kết quả",
                len(primary_results), len(fulltext_results))

    # ── Bước 2: Merge + Dedup ────────────────────────────────────────────────
    merged_map: dict = {}  # book_id → (BookResult, base_score)

    for r in primary_results:
        if r.book_id not in merged_map:
            merged_map[r.book_id] = (r, float(r.score or 0))

    for r in fulltext_results:
        if r.book_id not in merged_map:
            merged_map[r.book_id] = (r, float(r.score or 0))
        else:
            # Xuất hiện trong cả 2 kết quả → cộng điểm
            old_r, old_score = merged_map[r.book_id]
            merged_map[r.book_id] = (old_r, old_score + float(r.score or 0) * 0.5)

    # ── Bước 3: Re-rank v2 – boost sách có title khớp qua accent-insensitive ─
    # v2 cải tiến:
    # 1. _fold_vi() so sánh không phân biệt dấu ("dac nhan tam" ↔ "Đắc Nhân Tâm")
    # 2. Giảm min_word_len từ 4 → 3 (từ VN như "tâm", "nhân", "đắc" chỉ 3 ký tự)
    # 3. Dual matching: thử cả original lẫn folded → tăng recall
    full_text_folded = _fold_vi(book_info.full_text_query or "")

    ranked = []
    for book_id, (r, score) in merged_map.items():
        boost = 0.0
        if r.title and (full_text_lower or full_text_folded):
            title_lower  = r.title.lower()
            title_folded = _fold_vi(r.title)

            # Nếu title sách xuất hiện nguyên vẹn trong OCR text → boost tối đa
            if title_lower in full_text_lower or title_folded in full_text_folded:
                boost = 100.0
                logger.info("🎯 Title full-match boost: '%s'", r.title)
            else:
                # So sánh từng từ ≥3 ký tự (tăng từ 4 để bắt "tâm", "nhân", "đắc")
                # Thử cả so sánh có dấu và không dấu (dual matching)
                words_orig   = [w for w in title_lower.split()  if len(w) >= 3]
                words_folded = [w for w in title_folded.split() if len(w) >= 3]

                if words_orig:
                    # Khớp từng từ trong cả 2 phiên bản (dấu + không dấu)
                    matched_orig   = sum(1 for w in words_orig   if w in full_text_lower)
                    matched_folded = sum(1 for w in words_folded if w in full_text_folded)
                    # Lấy max của 2 phương pháp
                    best_matched = max(matched_orig, matched_folded)
                    ratio = best_matched / len(words_orig)
                    boost = ratio * 60.0  # tăng ceiling từ 50 → 60

                    if ratio >= 0.5:
                        logger.info(
                            "🎯 Word-match boost: '%s' (%.0f%% words matched, +%.1f)",
                            r.title, ratio * 100, boost
                        )
        ranked.append((score + boost, r))

    ranked.sort(key=lambda x: -x[0])
    final_results = [r for _, r in ranked]

    # ── Bước 4: Google Books fallback nếu không có kết quả ──────────────────
    if not final_results:
        google_results = []
        if book_info.isbn or book_info.title:
            google_results = await search_google_books(
                isbn=book_info.isbn,
                title=book_info.title,
            )
            logger.info("Google Books fallback: %d kết quả", len(google_results))
        final_results = google_results

    logger.info("✅ Tổng kết quả sau re-rank: %d", len(final_results))
    return final_results[:_SEARCH_LIMIT]
