"""
OCR Async Task Queue – P2.2
asyncio.Queue noi bo, khong can Celery.

Flow:
  1. Client POST /api/ocr/search-by-cover-async   → nhan task_id ngay (<50ms)
  2. Background worker (2 workers) lay task tu queue, chay OCR pipeline
  3. Ket qua luu vao Redis DB1 voi TTL 5 phut
  4. Client GET /api/ocr/result/{task_id}          → tra ket qua khi xong
  5. Client GET /api/ocr/progress/{task_id}        → SSE stream trang thai

Uu diem so voi sync endpoint:
  - Khong bao gio timeout (request tra ve ngay)
  - Server xu ly max 2 anh cung luc (tranh CPU 100%)
  - Queue maxsize=20 tu dong reject khi qua tai
"""
import asyncio
import logging
import time
import uuid
from typing import Optional

logger = logging.getLogger("ocr.queue")

# ── Internal state ──────────────────────────────────────────────────────────
_task_queue: asyncio.Queue = asyncio.Queue(maxsize=20)
_results: dict = {}          # task_id -> result dict (fallback khi Redis unavailable)
_workers: list = []
_redis_client = None

STATUS_QUEUED     = "queued"
STATUS_PROCESSING = "processing"
STATUS_DONE       = "done"
STATUS_ERROR      = "error"


# ── Redis helpers ────────────────────────────────────────────────────────────
def _get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        import redis, os
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        # Dung DB1 (chatbot session DB) vi OCR result cung la tam thoi
        url_db1 = url.rstrip("/0").rstrip("/1") + "/1"
        _redis_client = redis.from_url(url_db1, socket_connect_timeout=1,
                                       decode_responses=True)
        _redis_client.ping()
        logger.info("[OCR Queue] Redis DB1 connected")
    except Exception as e:
        logger.warning("[OCR Queue] Redis unavailable, using in-memory fallback: %s", e)
        _redis_client = False
    return _redis_client


def _save_result(task_id: str, data: dict, ttl: int = 300):
    """Luu ket qua vao Redis (TTL 5 phut) hoac fallback in-memory."""
    import json
    r = _get_redis()
    if r:
        try:
            r.setex(f"ocr:result:{task_id}", ttl, json.dumps(data))
            return
        except Exception:
            pass
    # Fallback: in-memory (mat khi restart)
    _results[task_id] = data


def get_result(task_id: str) -> Optional[dict]:
    """Doc ket qua tu Redis hoac in-memory."""
    import json
    r = _get_redis()
    if r:
        try:
            raw = r.get(f"ocr:result:{task_id}")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    # Fallback in-memory
    return _results.get(task_id)


# ── Public API ────────────────────────────────────────────────────────────────
async def submit_task(img_bytes: bytes, filename: str) -> str:
    """
    Them task OCR vao queue. Tra ve task_id ngay lap tuc.
    Raise QueueFull neu hang doi qua tai (>20 tasks).
    """
    task_id = str(uuid.uuid4())[:8]
    task = {
        "id":       task_id,
        "img":      img_bytes,
        "file":     filename,
        "queued_at": time.time(),
    }
    try:
        _task_queue.put_nowait(task)
    except asyncio.QueueFull:
        raise RuntimeError("OCR queue qua tai. Vui long thu lai sau it giay.")

    # Luu trang thai initial
    _save_result(task_id, {
        "status":     STATUS_QUEUED,
        "task_id":    task_id,
        "queued_at":  task["queued_at"],
        "queue_size": _task_queue.qsize(),
    })
    logger.info("[OCR Queue] Task %s queued (queue size=%d)", task_id, _task_queue.qsize())
    return task_id


async def _run_ocr_on_bytes(img_bytes: bytes, filename: str) -> dict:
    """
    Chay OCR pipeline tren img_bytes (anh upload).
    Tai su dung logic tu ocr_router nhung goi truc tiep service functions.
    """
    import io
    from PIL import Image
    from ocr_app.services.ocr_engine import get_easyocr_reader
    from ocr_app.services.preprocessor import preprocess_image
    from ocr_app.services.search_integrator import search_books_by_text

    try:
        # Preprocess
        pil_img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
        processed_img = preprocess_image(pil_img)

        # OCR
        reader = get_easyocr_reader()
        import numpy as np
        img_np = np.array(processed_img)
        raw_results = reader.readtext(img_np, detail=1, paragraph=False)
        extracted_text = " ".join(r[1] for r in raw_results if r[2] > 0.3)

        if not extracted_text.strip():
            return {"books": [], "ocr_text": "", "message": "Khong doc duoc chu"}

        # Search
        books = await search_books_by_text(extracted_text)
        return {
            "ocr_text": extracted_text[:300],
            "books":    books,
            "count":    len(books),
        }
    except Exception as e:
        logger.error("[OCR Worker] Pipeline loi: %s", e)
        raise


async def _worker_loop(worker_id: int):
    """Background worker loop: lay task tu queue va xu ly."""
    logger.info("[OCR Worker %d] Started", worker_id)
    while True:
        try:
            task = await _task_queue.get()
            task_id = task["id"]
            wait_ms = int((time.time() - task["queued_at"]) * 1000)

            logger.info("[OCR Worker %d] Processing task %s (waited %dms)",
                        worker_id, task_id, wait_ms)

            # Cap nhat trang thai -> processing
            _save_result(task_id, {
                "status":      STATUS_PROCESSING,
                "task_id":     task_id,
                "worker_id":   worker_id,
                "started_at":  time.time(),
                "wait_ms":     wait_ms,
            })

            t0 = time.perf_counter()
            try:
                result = await _run_ocr_on_bytes(task["img"], task["file"])
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                _save_result(task_id, {
                    "status":      STATUS_DONE,
                    "task_id":     task_id,
                    "elapsed_ms":  elapsed_ms,
                    "wait_ms":     wait_ms,
                    **result,
                })
                logger.info("[OCR Worker %d] Task %s DONE in %dms",
                            worker_id, task_id, elapsed_ms)
            except Exception as e:
                _save_result(task_id, {
                    "status":    STATUS_ERROR,
                    "task_id":   task_id,
                    "error":     str(e),
                })
                logger.error("[OCR Worker %d] Task %s FAILED: %s", worker_id, task_id, e)

        except asyncio.CancelledError:
            logger.info("[OCR Worker %d] Cancelled", worker_id)
            break
        except Exception as e:
            logger.error("[OCR Worker %d] Unexpected error: %s", worker_id, e)
        finally:
            try:
                _task_queue.task_done()
            except Exception:
                pass


def start_workers(n: int = 2):
    """Khoi dong n worker trong event loop hien tai."""
    global _workers
    _workers = [asyncio.ensure_future(_worker_loop(i)) for i in range(1, n + 1)]
    logger.info("[OCR Queue] %d workers started", n)


def queue_stats() -> dict:
    return {
        "queue_size":    _task_queue.qsize(),
        "queue_maxsize": _task_queue.maxsize,
        "workers":       len(_workers),
        "redis_enabled": bool(_get_redis()),
    }
