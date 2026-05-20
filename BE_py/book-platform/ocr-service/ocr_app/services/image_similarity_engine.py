"""
image_similarity_engine.py – Visual Book Cover Matching (Stage 0)
=================================================================
So sánh ảnh bìa upload với toàn bộ ảnh bìa sách trong hệ thống bằng
Perceptual Hashing (pHash). Nếu khớp → trả kết quả NGAY, không cần OCR.

Tại sao pHash?
- Nhanh: tính hash O(1), so sánh O(n) với Hamming distance
- Robust: chịu được resize, JPEG compression, nhẹ độ sáng/tương phản
- Lightweight: mỗi hash chỉ 8 bytes (64 bits) → cache 10.000 sách = 80KB RAM
- Không cần model AI: thuần toán học

Thư viện: imagehash (pip install imagehash)

Luồng:
  1. Startup: build_hash_index() – lấy tất cả book image URLs từ Search Service
                                  → tính pHash → cache vào RAM
  2. Request: find_similar_book(img_bytes) – tính pHash của ảnh upload
                                            → so Hamming distance với index
                                            → nếu dist ≤ THRESHOLD → trả BookResult

Refresh: Mỗi CACHE_TTL giây (mặc định 6h), tự động rebuild index trong background.
"""
import asyncio
import logging
import os
import pickle
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import httpx
import numpy as np
from ocr_app.services.bktree import BKTree  # P3.3

from ocr_app.config import SEARCH_SERVICE_URL, PHASH_INDEX_PATH

logger = logging.getLogger(__name__)

# ── Cấu hình ─────────────────────────────────────────────────────────────────
# Hamming distance ≤ 8 trên 64-bit pHash ≈ similarity ≥ 87.5%
# (Thực nghiệm: cùng ảnh resize/compress thường < 5, ảnh khác thường > 15)
# ⚠️ Đừng tăng quá 12 – sẽ gây collision giữa bìa sách có tông màu tương tự
SIMILARITY_THRESHOLD = 6    # Ngưỡng max Hamming distance để coi là "khớp"
                             # 6/64 bits = similarity ≥ 90.6%
                             # (Thực nghiệm: cùng ảnh resize/compress thường < 4, ảnh khác thường > 10)
                             # ⚠️ Threshold 10 quá lỏng → bìa sách tông màu tương tự bị nhầm nhau
                             # ⚠️ Threshold 6 = chỉ match khi ảnh gần giống hệt (chụp lại, chụp từ màn hình)
CACHE_TTL            = 6 * 3600  # 6 giờ – refresh index này thường xuyên
MAX_BOOKS_TO_INDEX   = 25000      # Giới hạn số sách index (performance bound)
HTTP_TIMEOUT         = httpx.Timeout(connect=3.0, read=8.0, write=3.0, pool=3.0)

# MinIO base URL – dùng để xây URL đầy đủ từ relative path từ Search Service
# VD: "covers/books/123/123.jpg" → "http://localhost:9000/bookstore/covers/books/123/123.jpg"
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_BUCKET        = os.getenv("MINIO_BUCKET",     "bookstore")
MINIO_USE_HTTPS     = os.getenv("MINIO_USE_HTTPS",  "false").lower() == "true"

def _build_image_url(raw_url: str) -> str:
    """
    Chuẩn hóa image URL tù Search Service:
    - Nếu đã là URL đầy đủ (bắt đầu http/https) → giữ nguyên
    - Nếu là relative path (VD: "covers/books/123/123.jpg") → thêm MinIO base
    """
    if not raw_url:
        return ""
    if raw_url.startswith(("http://", "https://")):
        return raw_url
    # Relative path → build MinIO URL
    scheme = "https" if MINIO_USE_HTTPS else "http"
    # Normalize: bỏ leading slash nếu có
    path = raw_url.lstrip("/")
    return f"{scheme}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{path}"


# ── Hash Index (in-memory) ────────────────────────────────────────────────────
@dataclass
class BookHashEntry:
    """Một entry trong hash index."""
    book_id:     int
    title:       str
    author_name: Optional[str]
    price:       Optional[float]
    image_url:   Optional[str]
    phash:       int   # 64-bit integer (pHash value)


_hash_index: list[BookHashEntry] = []
_index_built_at: float = 0.0
_index_building: bool  = False
_bk_tree: BKTree = BKTree()  # P3.3: BK-Tree index


# ── pHash Index Persistence ─────────────────────────────────────────────────────

def _save_index_to_disk(index: list, built_at: float) -> None:
    """
    Serialize pHash index ra file pickle khi build xong.
    Mục đích: khi server restart, load từ file (<1s) thay vì rebuild từ đầu (30-60s).
    """
    try:
        payload = {"index": index, "built_at": built_at}
        with open(PHASH_INDEX_PATH, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
        size_mb = os.path.getsize(PHASH_INDEX_PATH) / (1024 * 1024)
        logger.info(
            "[pHash] Index đã lưu ra disk: %s (%.1fMB, %d books)",
            PHASH_INDEX_PATH, size_mb, len(index)
        )
    except Exception as e:
        logger.warning("[pHash] Không thể lưu index ra disk: %s", e)


def _load_index_from_disk() -> tuple[list, float]:
    """
    Load pHash index từ file pickle khi startup.
    Chỉ load nếu file còn có hiệu lực (< CACHE_TTL).

    Returns:
        (index_list, built_at) nếu đọc được, hoặc ([], 0.0) nếu không có.
    """
    try:
        if not os.path.exists(PHASH_INDEX_PATH):
            return [], 0.0
        mtime = os.path.getmtime(PHASH_INDEX_PATH)
        age = time.time() - mtime
        if age > CACHE_TTL:
            logger.info("[pHash] File index cũ (%.0fh), bỏ qua, sẽ rebuild", age / 3600)
            return [], 0.0
        with open(PHASH_INDEX_PATH, "rb") as f:
            payload = pickle.load(f)
        index = payload.get("index", [])
        built_at = payload.get("built_at", mtime)
        size_mb = os.path.getsize(PHASH_INDEX_PATH) / (1024 * 1024)
        logger.info(
            "[pHash] Load index từ disk: %d books, tuổi=%.0fh, size=%.1fMB",
            len(index), age / 3600, size_mb
        )
        return index, built_at
    except Exception as e:
        logger.warning("[pHash] Không thể load index từ disk: %s", e)
        return [], 0.0


async def load_index_from_disk_async() -> bool:
    """
    Async wrapper – gọi từ lifespan startup để không block event loop.
    Trả về True nếu load thành công, False nếu cần rebuild.
    """
    global _hash_index, _index_built_at, _bk_tree

    index, built_at = await asyncio.to_thread(_load_index_from_disk)
    if not index:
        return False

    _hash_index = index
    _index_built_at = built_at

    # Rebuild BK-Tree từ index đã load
    t0 = time.perf_counter()
    _bk_tree.__init__()
    for entry in index:
        _bk_tree.add(entry.phash, entry)
    bk_ms = (time.perf_counter() - t0) * 1000
    logger.info("[pHash] BK-Tree rebuilt từ disk index: %d nodes in %.1fms", len(index), bk_ms)
    return True



# ══════════════════════════════════════════════════════════════════════════════
# pHash Implementation (không dùng thư viện imagehash để tránh dependency)
# ══════════════════════════════════════════════════════════════════════════════

def _compute_phash(img_bytes: bytes, hash_size: int = 8) -> Optional[int]:
    """
    Tính Perceptual Hash (pHash) của ảnh từ bytes.

    Thuật toán pHash:
    1. Resize ảnh về hash_size*4 × hash_size*4 px (mặc định 32x32)
    2. Grayscale
    3. DCT (Discrete Cosine Transform) 2D
    4. Lấy góc trên-trái (hash_size × hash_size) – vùng tần số thấp
    5. So với giá trị trung bình: pixel > mean → 1, ≤ mean → 0
    6. Pack bits thành 1 số nguyên 64 bits

    Nếu cùng ảnh nhưng resize/crop nhẹ → hash gần như giống hệt.
    Ảnh khác hoàn toàn → hash khác xa (Hamming distance cao).

    Args:
        img_bytes:  raw bytes của ảnh (JPEG/PNG/WebP)
        hash_size:  kích thước hash (8 = 64-bit, 16 = 256-bit)

    Returns:
        64-bit integer pHash, hoặc None nếu ảnh không hợp lệ.
    """
    try:
        from PIL import Image
        import io

        # Load ảnh
        pil_img = Image.open(io.BytesIO(img_bytes)).convert("L")  # Grayscale

        # Resize về hash_size*4 để DCT có đủ dữ liệu
        high_freq_size = hash_size * 4
        pil_img = pil_img.resize((high_freq_size, high_freq_size), Image.LANCZOS)

        # Numpy array
        pixels = np.array(pil_img, dtype=np.float32)

        # 2D DCT: dùng scipy nếu có, fallback numpy
        try:
            from scipy.fft import dct as scipy_dct
            dct_rows = scipy_dct(pixels, axis=1, norm='ortho')
            dct_2d   = scipy_dct(dct_rows, axis=0, norm='ortho')
        except ImportError:
            # Fallback: approximation bằng numpy FFT (ít chính xác hơn nhưng OK)
            dct_2d = np.fft.fft2(pixels).real

        # Lấy góc trên-trái (low frequency)
        low_freq = dct_2d[:hash_size, :hash_size]

        # Tính bit: pixel > median → 1, ≤ median → 0
        median = np.median(low_freq)
        bits = (low_freq > median).flatten()  # hash_size * hash_size bits

        # Pack thành integer
        hash_val = 0
        for bit in bits[:64]:   # Chỉ lấy 64 bits đầu
            hash_val = (hash_val << 1) | (1 if bit else 0)

        return hash_val

    except Exception as e:
        logger.debug("pHash computation lỗi: %s", e)
        return None


def _hamming_distance(hash1: int, hash2: int) -> int:
    """
    Tính Hamming distance giữa 2 hash (số bit khác nhau).

    Dùng XOR: bit khác nhau → 1, giống → 0
    Đếm số bit 1 trong kết quả XOR = số bit khác nhau.

    Ví dụ: 0b1010 XOR 0b1001 = 0b0011 → 2 bit khác nhau.
    Câu lệnh bit_count() (Python 3.10+) hoặc bin(x).count('1').
    """
    xor = hash1 ^ hash2
    return bin(xor).count('1')


# ══════════════════════════════════════════════════════════════════════════════
# BUILD HASH INDEX
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_book_images() -> list[dict]:
    """
    Lấy danh sách tất cả sách có ảnh bìa trực tiếp từ MySQL Database để vét trọn 100% (20.210 sách),
    vượt mặt giới hạn phân trang 10.000 max_result_window của OpenSearch.
    """
    all_books = []
    
    # Hàm chạy đồng bộ để truy vấn DB (sẽ được bọc bằng to_thread)
    def _query_db_sync():
        import mysql.connector
        from ocr_app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
        
        books = []
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
                connection_timeout=10
            )
            cursor = conn.cursor(dictionary=True)
            # Lấy sách có ảnh chính từ bảng books và book_images
            query = """
                SELECT b.book_id, b.title, b.price, i.image_url
                FROM books b
                JOIN book_images i ON b.book_id = i.book_id
                WHERE i.is_main = 1 AND i.image_url IS NOT NULL AND i.image_url != ''
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                raw_url = row.get("image_url") or ""
                if not raw_url:
                    continue
                full_url = _build_image_url(raw_url)
                
                # Giá có thể là float hoặc decimal tuỳ DB driver trả về
                price_val = row.get("price")
                if price_val is not None:
                    price_val = float(price_val)
                    
                books.append({
                    "book_id":     row["book_id"],
                    "title":       str(row.get("title") or ""),
                    "author_name": row.get("author_name"),
                    "price":       price_val,
                    "image_url":   full_url,
                })
            
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"Lỗi truy vấn MySQL khi _fetch_book_images: {e}")
        return books

    try:
        all_books = await asyncio.to_thread(_query_db_sync)
    except Exception as e:
        logger.error(f"Lỗi chạy luồng DB _fetch_book_images: {e}")

    logger.info("Fetched %d sách có ảnh bìa từ MySQL (Bypass OpenSearch Limit)", len(all_books))
    
    # Nếu vì lý do nào đó DB sập trả về <=0 quyển, fallback thử dùng OpenSearch API nhưng chỉ lấy 50 cuốn tượng trưng
    if not all_books:
        logger.warning("Truy vấn DB thất bại, fallback sang Search Service...")
        # Lấy trang 1 tượng trưng để app không chết
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(f"{SEARCH_SERVICE_URL}/books/search", params={"q": " ", "limit": 50})
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("items", []):
                    raw_url = item.get("main_image_url") or item.get("image_url") or item.get("cover_image") or ""
                    if raw_url:
                        all_books.append({
                            "book_id":     int(item.get("book_id", 0)),
                            "title":       str(item.get("title", "")),
                            "author_name": item.get("author_name"),
                            "price":       item.get("price"),
                            "image_url":   _build_image_url(raw_url)
                        })

    return all_books



async def _download_and_hash(
    client: httpx.AsyncClient,
    book: dict,
) -> Optional[BookHashEntry]:
    """Tải ảnh bìa và tính pHash. Trả về None nếu thất bại."""
    try:
        img_bytes = await _fetch_image_bytes(book["image_url"])
        if img_bytes is None or len(img_bytes) < 1000:
            return None

        import asyncio
        phash = await asyncio.to_thread(_compute_phash, img_bytes)
        if phash is None:
            return None

        return BookHashEntry(
            book_id=book["book_id"],
            title=book["title"],
            author_name=book.get("author_name"),
            price=book.get("price"),
            image_url=book["image_url"],
            phash=phash,
        )
    except Exception:
        return None


async def _fetch_image_bytes(url: str) -> Optional[bytes]:
    """
    Tải ảnh từ URL, hỗ trợ cả public URL và MinIO (với credentials).
    Chiến lược: MinIO SDK trước (nếu là MinIO URL), plain HTTP làm fallback.
    Timeout ngắn (2s) để tránh block cả index build.
    """
    minio_prefix       = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/"
    minio_prefix_https = f"https://{MINIO_ENDPOINT}/{MINIO_BUCKET}/"

    is_minio_url = url.startswith(minio_prefix) or url.startswith(minio_prefix_https) or (f"/{MINIO_BUCKET}/" in url)

    if is_minio_url:
        try:
            parts = url.split(f"/{MINIO_BUCKET}/")
            object_name = parts[-1] if len(parts) >= 2 else url.replace(minio_prefix, "").replace(minio_prefix_https, "")

            data = await asyncio.to_thread(_fetch_from_minio_sync, object_name)
            if data and len(data) >= 1000:
                return data
        except Exception:
            pass  # Fall through to HTTP

    # Plain HTTP fallback – NGẮN timeout (1.5s) để không block cả index build
    try:
        async with httpx.AsyncClient(timeout=1.5) as c:
            resp = await c.get(url)
            if resp.status_code == 200 and len(resp.content) >= 1000:
                return resp.content
    except Exception:
        pass

    return None


# Global MinioClient để reuse connection pool (tránh port exhaustion trên Windows)
_minio_client = None

def _get_minio_client():
    global _minio_client
    if _minio_client is None:
        from minio import Minio as MinioClient
        _minio_client = MinioClient(
            endpoint=MINIO_ENDPOINT,
            access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "admin123456789"),
            secure=MINIO_USE_HTTPS,
        )
    return _minio_client


def _fetch_from_minio_sync(object_name: str) -> Optional[bytes]:
    """Synchronous MinIO SDK download – chạy trong thread pool qua asyncio.to_thread."""
    try:
        client = _get_minio_client()
        response = client.get_object(bucket_name=MINIO_BUCKET, object_name=object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except Exception as e:
        logger.debug("MinIO SDK download lỗi %s: %s", object_name, e)
        return None


async def build_hash_index(force: bool = False) -> int:
    """
    Build (hoặc rebuild) perceptual hash index từ tất cả book cover images.

    Args:
        force: True = rebuild dù index còn mới

    Returns:
        Số entry được index thành công.
    """
    global _hash_index, _index_built_at, _index_building

    if _index_building:
        logger.debug("Index đang được build, bỏ qua request trùng lặp")
        return len(_hash_index)

    # Kiểm tra TTL
    age = time.time() - _index_built_at
    if not force and age < CACHE_TTL and _hash_index:
        logger.debug("Index còn mới (%.0fs tuổi), không rebuild", age)
        return len(_hash_index)

    _index_building = True
    t0 = time.perf_counter()
    logger.info("🔍 Bắt đầu build visual hash index...")

    try:
        # [1] Lấy danh sách sách có ảnh
        books = await _fetch_book_images()
        if not books:
            logger.warning("Không có sách nào có ảnh bìa – index rỗng")
            return 0

        # [2] Download + hash song song (max 20 concurrent, giới hạn global 5 phút)
        semaphore = asyncio.Semaphore(20)
        new_index = []
        lock = asyncio.Lock()

        async def bounded_hash(book: dict) -> None:
            async with semaphore:
                try:
                    # Giới hạn 3s mỗi book – tránh 1 ảnh chậm block cả process
                    entry = await asyncio.wait_for(
                        _download_and_hash(None, book),
                        timeout=3.0
                    )
                    if entry:
                        async with lock:
                            new_index.append(entry)
                except asyncio.TimeoutError:
                    pass  # Ảnh này quá chậm → bỏ qua
                except Exception:
                    pass

        await asyncio.gather(*[bounded_hash(b) for b in books])

        # [3] Cap nhat global index + build BK-Tree (P3.3)
        _hash_index = new_index
        _index_built_at = time.time()

        # Build BK-Tree tu index list
        t_bk = time.perf_counter()
        _bk_tree.__init__()  # Reset
        for _e in new_index:
            _bk_tree.add(_e.phash, _e)
        bk_ms = (time.perf_counter() - t_bk) * 1000
        logger.info("[P3.3] BK-Tree built: %d nodes in %.1fms", len(_bk_tree), bk_ms)

        # [4] Persist index ra disk – giúp restart nhanh hơn
        await asyncio.to_thread(_save_index_to_disk, new_index, _index_built_at)

        elapsed = (time.perf_counter() - t0) * 1000
        logger.info(
            "✅ Visual hash index: %d/%d sách (%.0fms)",
            len(new_index), len(books), elapsed
        )
        return len(new_index)

    except Exception as e:
        logger.error("Build hash index thất bại: %s", e)
        return len(_hash_index)  # Giữ index cũ nếu có
    finally:
        _index_building = False


# ══════════════════════════════════════════════════════════════════════════════
# INCREMENTAL INDEX UPDATE (thêm/sửa/xóa 1 sách mà không rebuild toàn bộ)
# ══════════════════════════════════════════════════════════════════════════════

async def upsert_book_in_hash_index(book_id: int) -> dict:
    """
    Thêm hoặc cập nhật 1 sách vào pHash index mà KHÔNG cần rebuild toàn bộ.
    Được gọi từ Java Backend (qua HTTP) ngay sau khi thêm/sửa sách.

    Luồng:
      Java POST /api/ocr/index-book/{book_id}
        → Query MySQL lấy image_url của book_id
        → Download ảnh từ MinIO
        → Tính pHash
        → Upsert vào _hash_index + BK-Tree
        → Persist ra disk

    Returns:
        dict với action ("inserted"|"updated"|"skipped"), book_id, title
    """
    global _hash_index, _bk_tree

    # [1] Query MySQL để lấy thông tin ảnh của book_id
    def _query_one_book_sync():
        import mysql.connector
        from ocr_app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
        try:
            conn = mysql.connector.connect(
                host=MYSQL_HOST, port=MYSQL_PORT,
                user=MYSQL_USER, password=MYSQL_PASSWORD,
                database=MYSQL_DB, connection_timeout=8,
            )
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT b.book_id, b.title, a.author_name, b.price, i.image_url
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                JOIN book_images i ON b.book_id = i.book_id
                WHERE b.book_id = %s AND i.is_main = 1
                  AND i.image_url IS NOT NULL AND i.image_url != ''
                LIMIT 1
            """, (book_id,))
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            return row
        except Exception as e:
            logger.error("[upsert_book] MySQL query lỗi: %s", e)
            return None

    row = await asyncio.to_thread(_query_one_book_sync)

    if not row:
        logger.warning("[upsert_book] book_id=%d không có ảnh trong DB – bỏ qua", book_id)
        return {"action": "skipped", "book_id": book_id, "reason": "no_image_in_db"}

    raw_url  = row.get("image_url") or ""
    full_url = _build_image_url(raw_url)
    book_data = {
        "book_id":     int(row["book_id"]),
        "title":       str(row.get("title") or ""),
        "author_name": row.get("author_name"),
        "price":       float(row["price"]) if row.get("price") is not None else None,
        "image_url":   full_url,
    }

    # [2] Download ảnh + tính pHash (timeout 5s)
    try:
        entry = await asyncio.wait_for(
            _download_and_hash(None, book_data),
            timeout=5.0,
        )
    except asyncio.TimeoutError:
        logger.warning("[upsert_book] Timeout khi download ảnh book_id=%d", book_id)
        return {"action": "skipped", "book_id": book_id, "reason": "image_timeout"}

    if entry is None:
        logger.warning("[upsert_book] Không tính được pHash cho book_id=%d", book_id)
        return {"action": "skipped", "book_id": book_id, "reason": "phash_failed"}

    # [3] Upsert vào _hash_index (xóa entry cũ nếu có, thêm entry mới)
    action = "inserted"
    new_index = [e for e in _hash_index if e.book_id != book_id]
    if len(new_index) < len(_hash_index):
        action = "updated"
    new_index.append(entry)
    _hash_index = new_index

    # [4] Rebuild BK-Tree (nhanh: O(n), ~5ms cho 20k sách)
    t0 = time.perf_counter()
    _bk_tree.__init__()
    for e in _hash_index:
        _bk_tree.add(e.phash, e)
    logger.info(
        "[upsert_book] BK-Tree rebuilt in %.1fms (%d entries)",
        (time.perf_counter() - t0) * 1000, len(_hash_index)
    )

    # [5] Persist ra disk (không chờ)
    asyncio.ensure_future(
        asyncio.to_thread(_save_index_to_disk, _hash_index, _index_built_at or time.time())
    )

    logger.info(
        "[upsert_book] %s book_id=%d title='%s'",
        action.upper(), book_id, entry.title
    )
    return {"action": action, "book_id": book_id, "title": entry.title}


def remove_book_from_hash_index(book_id: int) -> dict:
    """
    Xóa 1 sách khỏi pHash index (đồng bộ, gọi khi xóa sách).
    Rebuild BK-Tree sau khi xóa.

    Returns:
        dict với action ("removed"|"not_found"), book_id
    """
    global _hash_index, _bk_tree

    new_index = [e for e in _hash_index if e.book_id != book_id]
    if len(new_index) == len(_hash_index):
        logger.debug("[remove_book] book_id=%d không có trong pHash index", book_id)
        return {"action": "not_found", "book_id": book_id}

    _hash_index = new_index

    # Rebuild BK-Tree
    _bk_tree.__init__()
    for e in _hash_index:
        _bk_tree.add(e.phash, e)

    logger.info("[remove_book] Đã xóa book_id=%d khỏi pHash index (%d còn lại)", book_id, len(_hash_index))
    return {"action": "removed", "book_id": book_id}


# ══════════════════════════════════════════════════════════════════════════════
# VISUAL SEARCH
# ══════════════════════════════════════════════════════════════════════════════

def find_similar_book(
    img_bytes: bytes,
    threshold: int = SIMILARITY_THRESHOLD,
) -> Optional[tuple[BookHashEntry, float]]:
    """
    Tìm sách có ảnh bìa giống nhất với ảnh upload.

    Returns:
        (BookHashEntry, similarity_score [0.0-1.0]) nếu tìm thấy khớp,
        hoặc None nếu không có sách nào khớp đủ gần.

    Complexity: O(n) với n = số sách trong index.
    Với 2000 sách: ~0.5ms (rất nhanh).
    """
    if not _hash_index:
        logger.debug("Hash index rỗng, bỏ qua visual search")
        return None

    query_hash = _compute_phash(img_bytes)
    if query_hash is None:
        return None

    # P3.3: BK-Tree search O(log n) thay vi linear scan O(n)
    if _bk_tree.size > 0:
        result = _bk_tree.find_best(query_hash, threshold)
        if result is None:
            logger.info("Visual search: khong khop (BK-Tree, threshold=%d)", threshold)
            return None
        best_dist, best_entry = result
    else:
        # Fallback: linear scan (khi BK-Tree chua duoc build)
        best_entry = None
        best_dist  = threshold + 1
        for entry in _hash_index:
            dist = _hamming_distance(query_hash, entry.phash)
            if dist < best_dist:
                best_dist  = dist
                best_entry = entry

    if best_entry is None or best_dist > threshold:
        logger.info(
            "Visual search: không khớp (best_dist=%d > threshold=%d)",
            best_dist, threshold
        )
        return None

    # Similarity score: 0 distance = 1.0, threshold distance = 0.0
    similarity = 1.0 - (best_dist / 64.0)  # 64 bits tổng

    logger.info(
        "✅ Visual match! '%s' (dist=%d, sim=%.2f%%)",
        best_entry.title, best_dist, similarity * 100
    )
    return best_entry, similarity


# ══════════════════════════════════════════════════════════════════════════════
# GARBAGE / UNRELATED IMAGE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def assess_image_relevance(img_bytes: bytes) -> dict:
    """
    Đánh giá xem ảnh upload có "liên quan đến sách" không.
    Trả về score và verdict để caller có thể từ chối sớm.

    Các dấu hiệu ảnh KHÔNG liên quan đến sách:
    1. Ảnh có quá ít cạnh (edge density thấp) → ảnh trắng/đen thuần, ảnh mờ
    2. Saturation quá cao → ảnh chụp cảnh ngoài trời, màu sặc sỡ (không phải bìa sách)
    3. Kích thước ảnh quá nhỏ → thumbnail 50x50px không đủ thông tin
    4. Ảnh không có vùng text nào (đánh giá bằng edge detection)

    Note: Đây là heuristic, không phải classifier. False positive có thể xảy ra.
    Dùng ngưỡng loose để không từ chối nhầm bìa sách có thiết kế đặc biệt.

    Returns:
        {
            "is_relevant": bool,
            "confidence": float [0.0-1.0] – độ chắc chắn về irrelevance
            "reason": str – lý do từ chối (nếu is_relevant=False)
        }
    """
    try:
        from PIL import Image
        import io
        import cv2

        pil_img = Image.open(io.BytesIO(img_bytes))
        w, h = pil_img.size

        # Test 1: Kích thước quá nhỏ
        if w < 50 or h < 50:
            return {
                "is_relevant": False,
                "confidence": 0.95,
                "reason": f"Ảnh quá nhỏ ({w}x{h}px). Vui lòng upload ảnh rõ hơn.",
            }

        # Test 2: OpenCV edge analysis
        cv_gray = np.array(pil_img.convert("L"))
        edges = cv2.Canny(cv_gray, 50, 150)
        edge_density = float(np.count_nonzero(edges)) / (w * h)

        # Ảnh hợp lệ (bìa sách, tài liệu) luôn có ít nhất 0.3% cạnh
        # 0.003 = ngưỡng rất thấp – chỉ reject ảnh trắng/đen hoàn toàn hoặc quá mờ (chụp lối, nhòe)
        if edge_density < 0.003:
            return {
                "is_relevant": False,
                "confidence": 0.85,
                "reason": "Ảnh không có nội dung rõ ràng (quá mờ hoặc trắng/đen thuần túy).",
            }

        # Test 3: Color sanity check
        # Ảnh toàn một màu (screenshot chụp nhầm background) → S ≈ 0 hoặc V ≈ const
        cv_bgr = np.array(pil_img.convert("RGB"))[:, :, ::-1]  # RGB→BGR
        hsv = cv2.cvtColor(cv_bgr, cv2.COLOR_BGR2HSV)
        sat_std  = float(np.std(hsv[:, :, 1]))   # Std deviation của Saturation
        sat_mean = float(np.mean(hsv[:, :, 1]))  # Mean Saturation
        hue_std  = float(np.std(hsv[:, :, 0]))   # Std deviation của Hue

        # Ảnh toàn một màu (screenshot desktop, gradient thuần) → S ≈ 0, edge rất thấp
        # NOTE: bìa sách grayscale (minh họa xám trắng) có sat_std thấp NHƯĐG edge_density > 0.008
        # Nên tighten edge threshold ← 0.02 xuống 0.008 để không reject nhầm bìa illustration
        if sat_std < 5 and edge_density < 0.008:
            return {
                "is_relevant": False,
                "confidence": 0.75,
                "reason": "Ảnh không giống bìa sách (màu sắc đơn điệu, thiếu chi tiết).",
            }

        # Test 4: Aspect ratio – bìa sách thường DỌNG, nhưng ảnh sản phẩm/promotional
        # có thể có nền trắng rộng hơn → chỉ reject ảnh NGANG RÕ RÀNG (h < 60% width)
        # 0.80 quá chặt → false positive trên ảnh artcity/product shot
        # 0.60 = cho phép landscape nhẹ (4:3 → h/w=0.75 PASS, 5:3 → h/w=0.60 PASS)
        # chỉ reject panorama thật sự (h/w < 0.60: ảnh chụp cảnh, banner, v.v.)
        aspect = h / w if w > 0 else 1.0
        if aspect < 0.60:
            return {
                "is_relevant": False,
                "confidence": 0.70,
                "reason": "Ảnh nằm ngang không giống bìa sách. Vui lòng chụp thẳng bìa sách.",
            }

        # Test 5: Random noise / TV static detection
        # Ảnh nhiễu (static, white noise) có edge_density rất cao (mọi pixel đều tạo cạnh)
        # và hue_std rất cao (màu ngẫu nhiên khắp nơi).
        #
        # Thực nghiệm:
        #  - Bìa sách phức tạp (đông người, cartoon chi tiết): edge_density ≈ 0.08-0.14
        #  - Random noise / TV static: edge_density ≈ 0.18-0.30 (cao hơn hẳn)
        #  - hue_std bìa sách phức tạp: ≈ 35-48  |  noise: ≈ 55-85
        #
        # Ngưỡng 0.16 + hue_std > 50: chỉ bắt được static/noise thật sự,
        # KHÔNG bắt bìa sách minh họa đông người (Tỷ Phú Bán Giày, v.v.)
        if edge_density > 0.16 and float(np.std(hsv[:, :, 0])) > 50:
            return {
                "is_relevant": False,
                "confidence": 0.88,
                "reason": (
                    "Ảnh nhiễu hoặc static, không phải bìa sách. "
                    "Vui lòng chụp thẳng mặt trước bìa sách."
                ),
            }

        # Passed all tests → probably a book-related image
        return {"is_relevant": True, "confidence": 0.0, "reason": ""}

    except Exception as e:
        logger.debug("assess_image_relevance lỗi: %s", e)
        # Lỗi không xác định → giả sử ảnh hợp lệ (tránh từ chối nhầm)
        return {"is_relevant": True, "confidence": 0.0, "reason": ""}


def assess_ocr_result_quality(
    ocr_confidence: float,
    search_results: list,
    extracted_text: str,
    compound_confidence: float,
) -> dict:
    """
    Đánh giá chất lượng kết quả OCR sau khi đã chạy xong.
    Dùng để phát hiện "ảnh bậy lung tung" qua output (không qua input).

    Trả về verdict để frontend hiển thị thông báo phù hợp.

    Tại sao cần 2 lớp check (input + output)?
    - Input check (assess_image_relevance): nhanh (~1ms), heuristic, bắt được ảnh rõ ràng sai
    - Output check (hàm này): sau OCR, biết chắc hơn – ảnh thực sự không có text sách

    Returns:
        {
            "quality": "good" | "low" | "irrelevant",
            "message": str – thông báo cho user
        }
    """
    has_text     = bool(extracted_text.strip()) and len(extracted_text.strip()) >= 5
    has_results  = len(search_results) > 0
    text_words   = len([w for w in extracted_text.split() if len(w) >= 2])

    # OCR không đọc được text (font trang trí, nền phức tạp, ảnh mờ)
    # KHÔNG đánh là "irrelevant" vì ảnh có thể là bìa sách hợp lệ với font khó đọc.
    # Dùng "low" → frontend hiện "0 kết quả" + gợi ý tìm thủ công (không phải red error box).
    if not has_text and ocr_confidence < 0.1:
        return {
            "quality": "low",
            "message": (
                "Không thể đọc được chữ trên ảnh này (font trang trí hoặc ảnh mờ). "
                "Hãy dùng nút 🔍 Tìm bên dưới để tìm kiếm thủ công."
            ),
        }

    # OCR đọc được text nhưng text quá ít và không tìm thấy sách
    if text_words < 3 and not has_results:
        return {
            "quality": "low",
            "message": (
                "OCR đọc được ít thông tin từ ảnh này. "
                "Thử chụp ảnh bìa phía trước sách với độ phân giải cao hơn."
            ),
        }

    # Có text nhưng confidence quá thấp và không có kết quả
    if compound_confidence < 0.15 and not has_results:
        return {
            "quality": "low",
            "message": (
                "Không tìm thấy sách phù hợp. "
                "Ảnh có thể không phải bìa sách, hoặc sách chưa có trong hệ thống."
            ),
        }

    # ── Kiểm tra keyword non-book (ID card, thẻ ngân hàng, CCCD...) ──────────────
    # Chạy SAU OCR – là tờ bảo vệ thứ 2 cho các ảnh lọt qua pre-OCR check
    _NON_BOOK_KEYWORDS = [
        # Ngân hàng / thẻ
        "bidv", "vietcombank", "agribank", "techcombank", "vpbank",
        "mbbank", "mb bank", "sacombank", "hdbank", "ocb", "tpbank",
        "visa", "mastercard", "napas", "chip", "atm",
        # Thẻ sinh viên / giấy tờ
        "thẻ sinh viên", "student id", "student card", "mã số sinh viên",
        "mssv", "học sinh", "học viên", "sinh viên", "hcmute", "hcmus",
        "bkhn", "neu", "ftu", "ueh", "hutech",
        # Giấy tờ tùy thân
        "cccd", "cmnd", "chứng minh", "căn cước",
        "passport", "hộ chiếu", "visa",
        # Hợp đồng / tài liệu pháp lý
        "hợp đồng", "phụ lục", "điều khoản", "bên a", "bên b",
        "thoả thuận", "thỏa thuận", "giao kết", "toà án",
        "cộng hòa xã hội", "chứng thực", "công chứng",
        # Báo cáo / biểu đồ
        "báo cáo", "doanh thu", "kế hoạch", "tống kết",
        "số liệu", "thống kê", "dashboard", "chart",
        # — Hóa đơn / biên lai / shopping —
        "invoice", "receipt", "billing", "order id", "order no",
        "hóa đơn", "biên lai", "đơn hàng", "thanh toán",
        "subtotal", "grand total", "amount due", "tax invoice",
        "quantity", "unit price", "item", "paid", "payment",
        "customer", "customer name", "khach hang", "khách hàng",
        # Sản phẩm công nghệ
        "laptop", "iphone", "macbook", "ipad", "samsung", "dell xps",
        "dell", "asus", "lenovo", "hp laptop", "acer", "microsoft surface",
        # Sêu tả sản phẩm không phải sách
        "processor", "ram", "ssd", "warranty", "serial number",
        "model", "sku",
    ]
    extracted_lower = extracted_text.lower()
    matched_kw = next(
        (kw for kw in _NON_BOOK_KEYWORDS if kw in extracted_lower), None
    )
    if matched_kw:
        return {
            "quality": "irrelevant",
            "message": (
                "Ảnh không phải bìa sách (phát hiện nội dung thẻ/giấy tờ/hóa đơn). "
                "Vui lòng chụp thẳng mặt trước bìa sách."
            ),
        }

    return {"quality": "good", "message": ""}


def get_index_stats() -> dict:
    """Trả về thống kê của hash index (dùng cho health check)."""
    age = time.time() - _index_built_at if _index_built_at > 0 else -1
    return {
        "indexed_books": len(_hash_index),
        "index_age_seconds": int(age),
        "index_fresh": age < CACHE_TTL if age >= 0 else False,
        "similarity_threshold": SIMILARITY_THRESHOLD,
    }
