"""
Admin Webhook Router – nhận sự kiện từ Backend API và enqueue vào queue.

Endpoints (tất cả không chặn event loop – response ngay < 1ms):
  POST /admin/books/{book_id}/sync      – sync 1 cuốn sách
  POST /admin/books/bulk-sync           – sync nhiều cuốn cùng lúc
  POST /admin/reindex-full              – trigger full reindex chạy nền
  GET  /admin/healthcheck               – kiểm tra trạng thái queue worker
"""

import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from search_app.workers.index_queue import enqueue, enqueue_many, queue_stats
from search_app.search.indexer import index_one_book

logger = logging.getLogger("search.admin_router")

router = APIRouter(prefix="/admin", tags=["Admin – Search Sync"])


# ── Schema ───────────────────────────────────────────────────────────────────

class BulkSyncBody(BaseModel):
    book_ids: list[int]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/books/{book_id}/sync",
             summary="Sync 1 sách vào OpenSearch (non-blocking)",
             response_description="Xác nhận đã đưa vào queue")
async def sync_one_book(book_id: int):
    """Enqueue book_id vào AsyncIO queue. Response trả về ngay (< 1ms).
    Worker nền sẽ gọi index_one_book() và push lên OpenSearch sau tối đa vài giây.
    """
    ok = await enqueue(book_id)
    if not ok:
        raise HTTPException(503, detail="Index queue đầy (5000), thử lại sau.")
    return {"queued": True, "book_id": book_id}


@router.post("/books/bulk-sync",
             summary="Sync nhiều sách (gọi sau khi import CSV hàng loạt)",
             response_description="Số lượng đã enqueue / bị bỏ (queue đầy)")
async def bulk_sync_books(body: BulkSyncBody):
    """Push nhiều book_id vào queue một lúc.
    - Dùng ngay sau khi chạy csv_to_mysql.py để cập nhật OpenSearch.
    - Worker xử lý lần lượt, mỗi cuốn ~50–200ms.
    """
    if not body.book_ids:
        raise HTTPException(422, detail="book_ids không được rỗng.")
    if len(body.book_ids) > 5000:
        raise HTTPException(422, detail="Tối đa 5000 book_ids mỗi lần.")

    stats = await enqueue_many(body.book_ids)
    logger.info("[AdminRouter] bulk-sync: %s", stats)
    return stats


@router.post("/reindex-full",
             summary="Trigger full reindex toàn bộ sách (chạy nền)")
async def reindex_full():
    """Chạy reindex_full trong background thread, trả response ngay.
    Dùng sau khi import lượng lớn dữ liệu hoặc rebuild OpenSearch index từ đầu.
    """
    from search_app.jobs.reindex_full import main as _reindex_main  # noqa: PLC0415

    async def _run():
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _reindex_main)
            logger.info("[AdminRouter] reindex-full DONE")
        except Exception as exc:  # noqa: BLE001
            logger.error("[AdminRouter] reindex-full FAILED: %s", exc)

    asyncio.create_task(_run(), name="reindex_full_task")
    return {"started": True, "message": "Full reindex đang chạy nền, xem log để theo dõi."}


@router.get("/healthcheck",
            summary="Kiểm tra trạng thái Queue Worker + OpenSearch")
async def healthcheck():
    """Trả về:
    - Trạng thái background worker (có đang sống không).
    - Số book đang chờ trong queue.
    - Ping OpenSearch.
    """
    stats = queue_stats()

    # Ping OpenSearch
    try:
        from search_app.search.client import get_os_client  # noqa: PLC0415
        client = get_os_client()
        os_ok = client.ping()
    except Exception:  # noqa: BLE001
        os_ok = False

    return {
        **stats,
        "opensearch_reachable": os_ok,
    }
