"""
workers/index_queue.py
-----------------------
In-Process AsyncIO Queue Worker – thay thế hoàn toàn Cronjob.

Luồng hoạt động:
  Backend API insert/update sách
       ↓
  POST /admin/books/{book_id}/sync   (admin_search_router)
       ↓
  enqueue(book_id)   ← non-blocking, trả về ngay < 1ms
       ↓
  asyncio.Queue  ← buffer tối đa 5000 book_ids
       ↓
  _worker()   ← coroutine chạy ngầm suốt vòng đời app
       ↓
  index_one_book(book_id)   ← chạy trong threadpool (không block event loop)
       ↓
  OpenSearch   ← upsert document, refresh=True
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

logger = logging.getLogger("search.queue_worker")

# Hàng chờ tối đa 5000 book_id (nếu tràn thì put_nowait raises QueueFull)
_queue: asyncio.Queue[int] = asyncio.Queue(maxsize=5000)

# Thread pool riêng để chạy index_one_book (code blocking) không block event loop
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="os_indexer")

# Task hiện đang chạy (dùng để kiểm tra trạng thái)
_worker_task: Optional[asyncio.Task] = None


async def enqueue(book_id: int) -> bool:
    """Push book_id vào queue để worker xử lý ngầm.

    Returns:
        True  – đã thêm vào queue.
        False – queue đầy (5000 chỗ), bỏ qua.
    """
    try:
        _queue.put_nowait(book_id)
        return True
    except asyncio.QueueFull:
        logger.warning("[IndexQueue] Queue FULL – dropped book_id=%s", book_id)
        return False


async def enqueue_many(book_ids: list[int]) -> dict:
    """Push nhiều book_id một lúc (dùng sau bulk import CSV)."""
    queued, dropped = 0, 0
    for bid in book_ids:
        ok = await enqueue(bid)
        if ok:
            queued += 1
        else:
            dropped += 1
    return {"queued": queued, "dropped": dropped}


async def _worker():
    """Coroutine chạy vô tận – lấy book_id từ queue và index vào OpenSearch.

    Kỹ thuật: dùng run_in_executor để chạy code blocking (index_one_book)
    trong ThreadPoolExecutor, không block event loop của FastAPI.
    """
    # Import ở đây tránh circular import
    from search_app.search.indexer import index_one_book  # noqa: PLC0415

    loop = asyncio.get_event_loop()
    logger.info("[IndexQueue] Worker started, waiting for events...")

    while True:
        book_id = await _queue.get()
        try:
            result = await loop.run_in_executor(
                _executor, index_one_book, book_id
            )
            logger.info("[IndexQueue] Indexed book_id=%s action=%s",
                        book_id, result.get("action", "?"))
        except Exception as exc:  # noqa: BLE001
            logger.error("[IndexQueue] Failed book_id=%s error=%s", book_id, exc)
        finally:
            _queue.task_done()


def start_worker() -> asyncio.Task:
    """Khởi động background worker. Gọi từ @app.on_event("startup")."""
    global _worker_task
    _worker_task = asyncio.create_task(_worker(), name="os_index_worker")
    logger.info("[IndexQueue] Background worker task created.")
    return _worker_task


def queue_stats() -> dict:
    """Trả về thống kê hàng chờ hiện tại (dùng cho healthcheck endpoint)."""
    return {
        "queue_size": _queue.qsize(),
        "queue_maxsize": _queue.maxsize,
        "worker_alive": _worker_task is not None and not _worker_task.done(),
    }
