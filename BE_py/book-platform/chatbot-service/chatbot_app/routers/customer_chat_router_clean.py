"""
chat_router.py – FastAPI router cho chatbot endpoints.

Endpoint chính: POST /api/chat/message
Streaming:      POST /api/chat/stream  (SSE)

Pipeline /message:
  1. NLU → 2. Context → 3. Profile → 4. Dialog Manager → 5. DB → 6. Response

Pipeline /stream:
  1. NLU → 2. Context → 3. process_for_stream (retrieval only) → 4. SSE stream LLM tokens → 5. done event → 6. DB
"""
import time
import json
import httpx
from typing import Optional
from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse, StreamingResponse
from chatbot_app.models import (
    ChatRequest, ChatResponse,
    UserRole, SessionHistoryResponse, HistoryMessage,
    NavigateButton,
)
from chatbot_app.nlu.customer_intent_classifier import detect_intent
from chatbot_app.nlu.sentiment_analyzer import analyze_sentiment
from chatbot_app.context.session_manager import (
    load_session, save_session, save_message,
    resolve_coref, update_context_with_entities,
    get_history_from_context,          # ← HOT PATH: dùng context thay DB query
    load_history_from_db,              # ← LEGACY: giữ cho backward compat
    get_session_info, get_all_messages, cache_stats,
)
from chatbot_app.user_intelligence.profile_builder import build_user_profile
from chatbot_app.generation.customer_dialog_manager import process, process_for_stream
from chatbot_app.generation.llm_client import check_ollama_health, generate_stream, _clean_llm_response

router = APIRouter(prefix="/api/chat", tags=["Chatbot"])


# ── POST /api/chat/message  (non-streaming, giữ nguyên để tương thích) ────────
@router.post("/message", response_model=ChatResponse)
async def chat_message(req: ChatRequest):
    """
    Endpoint chatbot truyền thống (không stream).
    Phù hợp với staff/admin hoặc khi client không hỗ trợ SSE.
    """
    t_start = time.perf_counter()
    role = req.role.value

    try:
        nlu_result = detect_intent(req.message)
        sentiment  = analyze_sentiment(req.message)
        nlu_result.sentiment = sentiment

        context = load_session(req.session_id, user_id=req.user_id, role=role)
        if req.history:
            # Client gửi history → dùng, bỏ qua DB
            history_dicts = [
                {"role": m.role.value, "content": m.content}
                for m in req.history[-10:]
            ]
        else:
            # ← THÀNH QUẢ TỐI Ư˜U (Phương án G):
            # history_window đã có trong context sau load_session()
            # KHÔNG cần gọi DB thêm lần nữa
            history_dicts = get_history_from_context(context, max_turns=10)

        resolved_message = resolve_coref(req.message, context)
        user_profile = build_user_profile(req.user_id)

        answer, sources, navigate_buttons = await process(
            message=resolved_message,
            nlu_result=nlu_result,
            user_id=req.user_id,
            context=context,
            history=history_dicts,
            user_profile=user_profile,
        )

        try:
            context = update_context_with_entities(context, nlu_result.entities, nlu_result.intent)
            context["user_role"] = role
            save_session(req.session_id, context)
            save_message(req.session_id, "user", req.message,
                         intent=nlu_result.intent, confidence=nlu_result.confidence,
                         sentiment=sentiment, entities=nlu_result.entities)
            save_message(req.session_id, "assistant", answer, sources=sources)
        except Exception as db_err:
            print(f"⚠️  DB save error (non-critical): {db_err}")

        latency_ms = round((time.perf_counter() - t_start) * 1000, 1)
        debug = None
        if role in ("staff", "admin"):
            debug = {
                "intent":     nlu_result.intent,
                "confidence": round(nlu_result.confidence, 3),
                "sentiment":  sentiment,
                "latency_ms": latency_ms,
                "sources":    sources,
            }
            if role == "admin":
                session_info = get_session_info(req.session_id)
                debug["entities"]     = nlu_result.entities
                debug["session_meta"] = {
                    "turn_count":  session_info.get("turn_count") if session_info else "?",
                    "last_active": str(session_info.get("last_active", "")) if session_info else "?",
                    "user_id":     req.user_id,
                    "role":        role,
                }

        return ChatResponse(
            session_id=req.session_id,
            answer=answer,
            intent=nlu_result.intent,
            confidence=nlu_result.confidence,
            sentiment=sentiment,
            sources=sources,
            navigate_buttons=navigate_buttons,
            debug=debug,
        )

    except Exception as e:
        import traceback
        print(f"❌ CHAT ERROR: {e}")
        traceback.print_exc()
        return ChatResponse(
            session_id=req.session_id,
            answer=(
                "😅 Xin lỗi, tôi gặp sự cố kỹ thuật.\n"
                "Vui lòng thử lại sau hoặc liên hệ hotline 0353260721."
            ),
            intent="error",
            confidence=0.0,
            sentiment="NEUTRAL",
            sources=[],
        )


# ── GET /api/chat/history/{session_id} ────────────────────────────────────────
@router.get("/history/{session_id}", response_model=SessionHistoryResponse)
async def get_chat_history(session_id: str):
    """Trả về toàn bộ lịch sử hội thoại của phiên."""
    messages_raw = get_all_messages(session_id)
    session_info = get_session_info(session_id)

    messages = [
        HistoryMessage(
            id=m["id"],
            role=m["role"],
            content=m["content"],
            intent=m.get("intent"),
            confidence=m.get("confidence"),
            sentiment=m.get("sentiment"),
            created_at=m.get("created_at"),
        )
        for m in messages_raw
    ]

    return SessionHistoryResponse(
        session_id=session_id,
        messages=messages,
        turn_count=session_info["turn_count"] if session_info else len(messages),
        last_active=str(session_info["last_active"]) if session_info and session_info.get("last_active") else None,
    )

@router.post("/stream")
async def chat_stream(req: ChatRequest):
    """
    SSE Streaming endpoint – POST /api/chat/stream

    Mỗi sự kiện SSE có format:
      data: {"type": "token", "content": "..."}\n\n
      data: {"type": "done", "btns": [...], "sources": [...]}\n\n

    Frontend nhận bằng fetch + ReadableStream (không dùng EventSource vì cần POST).
    """
    role = req.role.value

    async def event_generator():
        try:
            # 1. NLU
            nlu_result = detect_intent(req.message)
            sentiment  = analyze_sentiment(req.message)
            nlu_result.sentiment = sentiment

            # 2. Context + History
            context = load_session(req.session_id, user_id=req.user_id, role=role)
            if req.history:
                history_dicts = [
                    {"role": m.role.value, "content": m.content}
                    for m in req.history[-10:]
                ]
            else:
                # ← THÀNH QUẢ TỐI Ư˜U (Phương án G):
                # Không gọi SELECT chat_messages nữa
                history_dicts = get_history_from_context(context, max_turns=10)

            resolved_message = resolve_coref(req.message, context)
            user_profile = build_user_profile(req.user_id)

            # 3. Retrieval (không gọi LLM ở bước này)
            stream_ctx, tone, intent_str, navigate_buttons, sources, is_template = (
                await process_for_stream(
                    message=resolved_message,
                    nlu_result=nlu_result,
                    user_id=req.user_id,
                    context=context,
                    history=history_dicts,
                    user_profile=user_profile,
                )
            )

            if is_template:
                # Template không cần LLM, nhả 1 event rồi done
                ev = json.dumps({"type": "token", "content": stream_ctx}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"
                full_text = stream_ctx
            else:
                # Stream từng token LLM
                full_parts: list[str] = []
                async for token in generate_stream(
                    resolved_message, stream_ctx, history_dicts, tone, intent_str
                ):
                    full_parts.append(token)

                # ✅ Clean full text SAU khi collect xong (trước khi gửi FE)
                # Lý do: token stream từng mảnh nên cần full text để clean regex
                # (ví dụ: "[" và "CONTEXT]" có thể nằm trong 2 token khác nhau)
                raw_full   = "".join(full_parts)
                full_text  = _clean_llm_response(raw_full)

                # Gửi 1 token event với nội dung đã clean
                # (FE tự handle append, token này thay thế toàn bộ stream đã collect)
                ev = json.dumps({"type": "token", "content": full_text, "complete": True}, ensure_ascii=False)
                yield f"data: {ev}\n\n"

                # Done event với UI buttons + sources
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"

            # Lưu DB sau khi stream/event xong (không block)
            try:
                ctx_updated = update_context_with_entities(context, nlu_result.entities, nlu_result.intent)
                ctx_updated["user_role"] = role
                save_session(req.session_id, ctx_updated)
                save_message(
                    req.session_id, "user", req.message,
                    intent=nlu_result.intent, confidence=nlu_result.confidence,
                    sentiment=sentiment, entities=nlu_result.entities,
                )
                save_message(req.session_id, "assistant", full_text, sources=sources)
            except Exception as db_err:
                print(f"⚠️  DB save error (stream): {db_err}")

        except Exception as exc:
            import traceback
            traceback.print_exc()
            err_msg = "😅 Xin lỗi, tôi gặp sự cố kỹ thuật. Vui lòng thử lại sau!"
            ev_err = json.dumps({"type": "token", "content": err_msg}, ensure_ascii=False)
            yield f"data: {ev_err}\n\n"
            yield 'data: {"type":"done","btns":[],"sources":[]}\n\n'

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",   # tắt nginx buffer để token đến ngay browser
        },
    )


# ── POST /api/chat/upload-image  (OCR → Chatbot True Integration) ─────────────
# URL OCR Service – hostname Docker internal (bookstore_ocr) thay vì localhost
# localhost trong container = container itself, KHÔNG phải host machine
OCR_SERVICE_URL = "http://bookstore_ocr:8005"

@router.post("/upload-image")
async def chat_upload_image(
    file: UploadFile = File(..., description="Ảnh bìa sách (jpg/png/webp)"),
    session_id: str  = Form(...),
    message: str     = Form("", description="Câu hỏi kèm theo ảnh (tùy chọn)"),
    role: str        = Form("customer"),
    user_id: Optional[int] = Form(None),
):
    """
    POST /api/chat/upload-image – Kết nối thật sự giữa OCR Service và Chatbot.

    Luồng xử lý:
      1. Forward file ảnh lên OCR Service (:8005/api/ocr/search-by-cover)
      2. OCR Service trả về: book_title, authors, extracted_text, search_results[]
      3. Inject OCR kết quả vào context session dưới key _ocr_data
      4. Set NLU intent = image_search với confidence = 1.0 (bypass SBERT)
      5. Gọi process_for_stream() → Dialog Manager xử lý ảnh tìm sách
      6. Trả về SSE stream giống /api/chat/stream
    """
    async def event_generator():
        try:
            # ── Bước 1: Gọi OCR Service ────────────────────────────────────────
            # Yield loading indicator NGAY để user thấy phản hồi <50ms
            loading_ev = json.dumps({"type": "token", "content": "🔍 Đang nhận dạng ảnh bìa sách..."}, ensure_ascii=False)
            yield f"data: {loading_ev}\n\n"

            ocr_data: dict = {}
            try:
                file_bytes = await file.read()
                async with httpx.AsyncClient(timeout=10.0) as client:  # timeout 10s thôi, đủ cho OCR
                    ocr_resp = await client.post(
                        f"{OCR_SERVICE_URL}/api/ocr/search-by-cover",
                        files={"file": (file.filename, file_bytes, file.content_type or "image/jpeg")},
                    )
                if ocr_resp.status_code == 200:
                    raw = ocr_resp.json()
                    if raw.get("success"):
                        ocr_data = {
                            "book_title":     raw.get("book_info", {}).get("title", ""),
                            "authors":        raw.get("book_info", {}).get("authors", []),
                            "extracted_text": raw.get("extracted_text", ""),
                            "confidence":     raw.get("book_info", {}).get("confidence", 0),
                            "search_results": raw.get("search_results", []),
                        }
            except Exception as ocr_err:
                print(f"⚠️  OCR Service error (non-critical): {ocr_err}")
                # OCR failed → tiếp tục, dialog manager sẽ fallback

            # ── Bước 2: Inject OCR data vào context session ────────────────────
            from chatbot_app.nlu.customer_intent_classifier import NLUResult
            context = load_session(session_id, user_id=user_id, role=role)
            context["_ocr_data"] = ocr_data

            user_profile  = build_user_profile(user_id)
            # ← THÀNH QUẢ TỐI Ư˜U: history từ context, không query DB
            history_dicts = get_history_from_context(context, max_turns=10)

            # ── Bước 3: Force intent = image_search (bypass SBERT hoàn toàn) ──
            nlu_result = NLUResult(intent="image_search", confidence=1.0)
            nlu_result.sentiment = "NEUTRAL"

            book_title = ocr_data.get("book_title", "")
            # [FIX] Luôn đưa tên sách OCR vào display_message.
            # Nếu user gửi text kèm ảnh ("Cuốn này thì sao"), cần inject book_title để
            # dialog manager biết đang nói về sách mới, không phải sách cũ trong context.
            if book_title and message.strip():
                display_message = f'📷 [{book_title}] {message.strip()}'
            elif book_title:
                display_message = f'📷 Tìm sách từ ảnh: "{book_title}"'
            else:
                display_message = message.strip() or "📷 Tôi vừa gửi ảnh sách"

            # ── Bước 4: Dialog Manager xử lý ──────────────────────────────────
            stream_ctx, tone, intent_str, navigate_buttons, sources, is_template = (
                await process_for_stream(
                    message=display_message,
                    nlu_result=nlu_result,
                    user_id=user_id,
                    context=context,
                    history=history_dicts,
                    user_profile=user_profile,
                )
            )

            # ── Bước 5: SSE stream response ────────────────────────────────────
            if is_template:
                ev = json.dumps({"type": "token", "content": stream_ctx}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"
                full_text = stream_ctx
            else:
                full_parts: list[str] = []
                async for token in generate_stream(display_message, stream_ctx, history_dicts, tone, intent_str):
                    full_parts.append(token)
                full_text = _clean_llm_response("".join(full_parts))
                ev = json.dumps({"type": "token", "content": full_text, "complete": True}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"

            # ── Bước 6: Lưu DB + persist OCR context cho coref ────────────────
            try:
                ctx_updated = update_context_with_entities(context, nlu_result.entities, nlu_result.intent)
                ctx_updated["user_role"] = role
                # Phase 4.1: persist last_ocr_title để coref resolution hoạt động qua nhiều turn
                if book_title:
                    ctx_updated["last_found_title"] = book_title
                    ctx_updated["last_ocr_title"]   = book_title   # persistent key
                    ctx_updated["last_intent"]       = "image_search"
                ctx_updated.pop("_ocr_data", None)  # Không persist OCR blob vào DB
                save_session(session_id, ctx_updated)
                save_message(session_id, "user", display_message,
                             intent="image_search", confidence=1.0,
                             sentiment="NEUTRAL", entities={})
                save_message(session_id, "assistant", full_text, sources=sources)
            except Exception as db_err:
                print(f"⚠️  DB save error (upload-image): {db_err}")

        except Exception as exc:
            import traceback
            traceback.print_exc()
            err_msg = "😅 Xin lỗi, tôi gặp sự cố khi xử lý ảnh. Vui lòng thử lại!"
            ev_err = json.dumps({"type": "token", "content": err_msg}, ensure_ascii=False)
            yield f"data: {ev_err}\n\n"
            yield 'data: {"type":"done","btns":[],"sources":[]}\n\n'

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
