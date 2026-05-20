"""
Async OCR Router – P2.2
Endpoint khong dong bo: submit task → nhan task_id → poll ket qua.

Endpoint moi (song song voi endpoint cu):
  POST /api/ocr/search-by-cover-async  → submit, tra task_id ngay
  GET  /api/ocr/result/{task_id}        → lay ket qua (poll)
  GET  /api/ocr/progress/{task_id}      → SSE stream trang thai real-time
  GET  /api/ocr/queue-stats             → thong ke hang doi
"""
import asyncio
import json
import logging
import time

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from ocr_app.queue.task_queue import submit_task, get_result, queue_stats, STATUS_DONE

logger = logging.getLogger("ocr.async_router")

router = APIRouter(prefix="/api/ocr", tags=["OCR Async (P2.2)"])

# Gioi han kich thuoc file upload: 10MB
MAX_FILE_SIZE = 10 * 1024 * 1024


@router.post(
    "/search-by-cover-async",
    summary="Tim sach theo anh bi a (Async – khong blocking)",
    description=(
        "Upload anh bi a sach, nhan task_id ngay lap tuc (<50ms).\n\n"
        "Sau do dung GET /api/ocr/result/{task_id} de lay ket qua.\n\n"
        "**Uu diem:** Khong bao gio bi timeout, server khong bi overload."
    ),
)
async def search_by_cover_async(
    file: UploadFile = File(..., description="Anh bi a sach (JPG/PNG/WEBP, max 10MB)"),
):
    # Validate file type
    content_type = file.content_type or ""
    if not content_type.startswith("image/"):
        raise HTTPException(400, detail="Chi chap nhan file anh (image/*)")

    # Doc bytes va validate kich thuoc
    img_bytes = await file.read()
    if len(img_bytes) > MAX_FILE_SIZE:
        raise HTTPException(413, detail=f"File qua lon (max {MAX_FILE_SIZE // 1024 // 1024}MB)")

    if len(img_bytes) < 100:
        raise HTTPException(400, detail="File anh qua nho hoac bi hong")

    try:
        task_id = await submit_task(img_bytes, file.filename or "upload.jpg")
    except RuntimeError as e:
        raise HTTPException(503, detail=str(e))

    return {
        "task_id":    task_id,
        "status":     "queued",
        "message":    "Task da duoc tiep nhan. Dung GET /api/ocr/result/{task_id} de lay ket qua.",
        "poll_url":   f"/api/ocr/result/{task_id}",
        "stream_url": f"/api/ocr/progress/{task_id}",
        "estimated_wait_sec": "1-5",
    }


@router.get(
    "/result/{task_id}",
    summary="Lay ket qua OCR async",
    description="Poll ket qua cua task da submit. status: queued | processing | done | error",
)
async def get_ocr_result(task_id: str):
    result = get_result(task_id)
    if not result:
        raise HTTPException(
            404,
            detail=f"Task '{task_id}' khong tim thay. Co the da het han (TTL 5 phut) hoac task_id sai."
        )
    return result


@router.get(
    "/progress/{task_id}",
    summary="SSE stream trang thai OCR task theo thoi gian thuc",
    description=(
        "Server-Sent Events (SSE) stream – nhan update real-time.\n\n"
        "Stream tu dong dong khi task DONE hoac ERROR.\n\n"
        "Frontend: `const es = new EventSource('/api/ocr/progress/{task_id}')`"
    ),
    response_class=StreamingResponse,
)
async def stream_ocr_progress(task_id: str):
    """
    SSE stream tra ve event moi 500ms cho den khi task hoan thanh.
    """
    async def event_generator():
        max_wait = 30.0   # Toi da 30s cho 1 task
        start = time.time()
        last_status = None

        while time.time() - start < max_wait:
            result = get_result(task_id)

            if not result:
                yield f"data: {json.dumps({'status': 'not_found', 'task_id': task_id})}\n\n"
                break

            current_status = result.get("status", "unknown")

            # Chi gui event khi co thay doi
            if current_status != last_status:
                payload = {
                    "status":   current_status,
                    "task_id":  task_id,
                    "ts":       round(time.time(), 3),
                }
                if current_status == STATUS_DONE:
                    payload["count"] = result.get("count", 0)
                    payload["elapsed_ms"] = result.get("elapsed_ms")
                elif current_status == "error":
                    payload["error"] = result.get("error", "Unknown error")

                yield f"data: {json.dumps(payload)}\n\n"
                last_status = current_status

            # Neu xong -> dong stream
            if current_status in (STATUS_DONE, "error"):
                break

            await asyncio.sleep(0.5)

        else:
            # Timeout
            yield f"data: {json.dumps({'status': 'timeout', 'task_id': task_id, 'max_wait': max_wait})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":   "no-cache",
            "X-Accel-Buffering": "no",   # Nginx: tat buffering de SSE hoat dong
        },
    )


@router.get(
    "/queue-stats",
    summary="Thong ke hang doi OCR Async",
    tags=["Admin/Monitoring"],
)
async def get_queue_stats():
    """Xem trang thai hang doi: so task dang cho, so worker, Redis co san hay khong."""
    return queue_stats()
