"""
Admin Webhook Router – nhận sự kiện từ Backend API và enqueue vào queue.

Endpoints (tất cả không chặn event loop – response ngay < 1ms):
  POST /admin/books/{book_id}/sync      – sync 1 cuốn sách
  POST /admin/books/bulk-sync           – sync nhiều cuốn cùng lúc
  POST /admin/reindex-full              – trigger full reindex chạy nền
  GET  /admin/healthcheck               – kiểm tra trạng thái queue worker

Security: Tất cả endpoints yêu cầu header X-Admin-Key khớp với env ADMIN_API_KEY.
"""

import asyncio
import logging
import os

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from search_app.workers.index_queue import enqueue, enqueue_many, queue_stats
from search_app.search.indexer import index_one_book

logger = logging.getLogger("search.admin_router")

router = APIRouter(prefix="/admin", tags=["Admin – Search Sync"])


# ── Security ──────────────────────────────────────────────────────────────────

def verify_admin_key(x_admin_key: str = Header(..., description="Admin API Key (env: ADMIN_API_KEY)")):
    """
    Dependency kiểm tra header X-Admin-Key.
    
    Nếu ADMIN_API_KEY chưa set trong môi trường → raise 500 (cấu hình thiếu).
    Nếu key không khớp → raise 401 Unauthorized.
    
    Dùng với Depends() trong từng endpoint:
        @router.post("/...", dependencies=[Depends(verify_admin_key)])
    """
    expected = os.getenv("ADMIN_API_KEY", "")
    if not expected:
        logger.error("[AdminRouter] ADMIN_API_KEY chưa được cấu hình trong env!")
        raise HTTPException(
            status_code=500,
            detail="Server chưa cấu hình ADMIN_API_KEY. Liên hệ administrator."
        )
    if x_admin_key != expected:
        logger.warning("[AdminRouter] API key không hợp lệ – từ chối truy cập.")
        raise HTTPException(
            status_code=401,
            detail="X-Admin-Key không hợp lệ."
        )


# ── Schema ───────────────────────────────────────────────────────────────────

class BulkSyncBody(BaseModel):
    book_ids: list[int]


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/books/{book_id}/sync",
             summary="Sync 1 sách vào OpenSearch (non-blocking)",
             response_description="Xác nhận đã đưa vào queue",
             dependencies=[Depends(verify_admin_key)])
async def sync_one_book(book_id: int):
    """Enqueue book_id vào AsyncIO queue. Response trả về ngay (< 1ms).
    Worker nền sẽ gọi index_one_book() và push lên OpenSearch sau tối đa vài giây.
    Yêu cầu header: X-Admin-Key
    """
    ok = await enqueue(book_id)
    if not ok:
        raise HTTPException(503, detail="Index queue đầy (5000), thử lại sau.")
    return {"queued": True, "book_id": book_id}


@router.post("/books/bulk-sync",
             summary="Sync nhiều sách (gọi sau khi import CSV hàng loạt)",
             response_description="Số lượng đã enqueue / bị bỏ (queue đầy)",
             dependencies=[Depends(verify_admin_key)])
async def bulk_sync_books(body: BulkSyncBody):
    """Push nhiều book_id vào queue một lúc.
    - Dùng ngay sau khi chạy csv_to_mysql.py để cập nhật OpenSearch.
    - Worker xử lý lần lượt, mỗi cuốn ~50–200ms.
    Yêu cầu header: X-Admin-Key
    """
    if not body.book_ids:
        raise HTTPException(422, detail="book_ids không được rỗng.")
    if len(body.book_ids) > 5000:
        raise HTTPException(422, detail="Tối đa 5000 book_ids mỗi lần.")

    stats = await enqueue_many(body.book_ids)
    logger.info("[AdminRouter] bulk-sync: %s", stats)
    return stats


# Lock tránh chạy reindex song song
_reindex_running: bool = False


@router.post("/reindex-full",
             summary="Trigger full reindex toàn bộ sách (chạy nền)",
             dependencies=[Depends(verify_admin_key)])
async def reindex_full():
    """Chạy reindex_full trong background thread, trả response ngay.
    Dùng sau khi import lượng lớn dữ liệu hoặc rebuild OpenSearch index từ đầu.
    - Nếu đang có reindex chạy → trả 409 Conflict thay vì spawn task thứ 2.
    Yêu cầu header: X-Admin-Key
    """
    global _reindex_running
    if _reindex_running:
        raise HTTPException(
            status_code=409,
            detail="Full reindex đang chạy. Vui lòng đợi cho đến khi hoàn tất.",
        )

    from search_app.jobs.reindex_full import main as _reindex_main  # noqa: PLC0415

    async def _run():
        global _reindex_running
        _reindex_running = True
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _reindex_main)
            logger.info("[AdminRouter] reindex-full DONE")
        except Exception as exc:  # noqa: BLE001
            logger.error("[AdminRouter] reindex-full FAILED: %s", exc)
        finally:
            _reindex_running = False  # Luôn unlock dù thành công hay thất bại

    asyncio.create_task(_run(), name="reindex_full_task")
    return {"started": True, "message": "Full reindex đang chạy nền, xem log để theo dõi."}


@router.get("/healthcheck",
            summary="Kiểm tra trạng thái Queue Worker + OpenSearch",
            dependencies=[Depends(verify_admin_key)])
async def healthcheck():
    """Trả về:
    - Trạng thái background worker (có đang sống không).
    - Số book đang chờ trong queue.
    - Trạng thái reindex (đang chạy hay không).
    - Ping OpenSearch.
    Yêu cầu header: X-Admin-Key
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
        "reindex_running": _reindex_running,
    }
