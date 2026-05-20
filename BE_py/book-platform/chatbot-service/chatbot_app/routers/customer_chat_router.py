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
    resolve_coref, update_context_with_entities, update_context_after_ocr,
    get_history_from_context,
    load_history_from_db,
    get_session_info, get_all_messages, cache_stats,
)
from chatbot_app.user_intelligence.profile_builder import build_user_profile
from chatbot_app.generation.customer_dialog_manager import process, process_for_stream
from chatbot_app.generation.llm_client import check_ollama_health, generate_stream, _clean_llm_response, _capitalize_first

router = APIRouter(prefix="/api/chat", tags=["Chatbot"])

# Intent mà guest cần đăng nhập mới thực hiện được
PURCHASE_INTENTS = {"cart_help", "checkout", "order_status", "order_cancel", "order_history",
                   "return_request", "payment_issue", "loyalty_points", "account_help",
                   "voucher_apply", "wishlist_add"}

# Intent trả lời trực tiếp KHÔNG cần hỏi confirm (confidence >= 0.70)
DIRECT_EXECUTE_INTENTS = {
    "book_search", "book_detail", "book_availability", "book_review", "book_compare",
    "recommend_category", "recommend_personal", "recommend_trending", "recommend_gift",
    "recommend_combo", "store_info", "return_policy", "promotion_current", "chitchat",
    "image_search", "farewell", "greeting", "order_status", "order_history",
    "staff_inventory_check", "staff_top_selling", "staff_revenue_today", "staff_order_list_pending",
    "admin_dashboard", "admin_user_stats", "admin_top_books",
}

# Security Guard – dùng module tập trung thay vì inline check cũ
from chatbot_app.middleware.security_guard import check_security as _guard_check

_SECURITY_BLOCK_ANSWER = (
    "⚠️ Tôi không thể thực hiện yêu cầu này.\n\n"
    "Hành động bạn yêu cầu vi phạm chính sách sử dụng của BookStore. "
    "Nếu bạn cần hỗ trợ, vui lòng liên hệ **0353260721** (8h–22h)."
)


def _login_required_response():
    """Response trả về khi guest cố truy cập chức năng yêu cầu đăng nhập."""
    from chatbot_app.models import NavigateButton
    return (
        "Để sử dụng chức năng này, bạn cần **đăng nhập** vào tài khoản BookStore!\n\n"
        "🔐 Một số tính năng chỉ dành cho thành viên: đặt hàng, theo dõi đơn, voucher...",
        [],
        [NavigateButton(label="🔑 Đăng nhập", url="/login"),
         NavigateButton(label="📖 Tiếp tục xem sách", url="/books")],
    )


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
        # ── 0. Security Guard ──────────────────────────────────
        context = load_session(req.session_id, user_id=req.user_id, role=role)
        _guard_role = "guest" if req.user_id is None else "customer"
        _sec = _guard_check(req.message, role=_guard_role, context=context)
        if _sec:
            save_session(req.session_id, context)
            return ChatResponse(
                session_id=req.session_id,
                answer=_sec["response"],
                intent=f"security_{_sec['reason']}",
                confidence=1.0,
                sentiment="NEGATIVE",
                sources=[],
            )

        # ── 1. NLU + History ───────────────────────────────────
        nlu_result = detect_intent(req.message)
        sentiment  = analyze_sentiment(req.message)
        nlu_result.sentiment = sentiment

        if req.history:
            history_dicts = [
                {"role": m.role.value, "content": m.content}
                for m in req.history[-10:]
            ]
        else:
            history_dicts = get_history_from_context(context, max_turns=10)

        resolved_message = resolve_coref(req.message, context)
        user_profile = build_user_profile(req.user_id)

        # [FIX] Genre rescue: tên thể loại bị NLU classify nhầm thành PURCHASE_INTENTS
        if req.user_id is None and nlu_result.intent in PURCHASE_INTENTS:
            from chatbot_app.generation.dialog_utils import resolve_genre_alias as _rga_msg
            _gr_msg = _rga_msg(req.message)
            if _gr_msg:
                nlu_result.intent = "recommend_category"
                nlu_result.entities["genre"] = _gr_msg
                nlu_result.confidence = 1.0

        # Guest login-gate: nếu không đăng nhập mà hỏi chức năng yêu cầu auth
        if req.user_id is None and nlu_result.intent in PURCHASE_INTENTS:
            answer, sources, navigate_buttons = _login_required_response()
        else:
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



# GET /api/chat/health
@router.get("/health")
async def chatbot_health():
    """Kiem tra chatbot service va Ollama."""
    from chatbot_app.generation.llm_client import check_ollama_health as _ck
    ollama_ok = await _ck()
    return {
        "status": "ok",
        "service": "chatbot",
        "ollama": "ready" if ollama_ok else "not_ready",
    }


# GET /api/chat/stats (Task 5.1)
@router.get("/stats")
async def chatbot_stats():
    """Thong ke hieu suat: L1 cache, DB, OCR."""
    from chatbot_app.context.session_manager import cache_stats as _cs
    import httpx as _httpx
    stats = _cs()
    ocr_data = None
    try:
        async with _httpx.AsyncClient(timeout=3.0) as cli:
            r = await cli.get("http://bookstore_ocr:8005/api/ocr/stats")
            if r.status_code == 200:
                ocr_data = r.json()
    except Exception:
        pass
    return {
        "chatbot": {
            "session_cache": stats,
            "db_optimization": {
                "blocking_db_calls_per_message": 0,
                "async_writes": 2,
                "history_source": "context_json (embedded)",
            },
        },
        "ocr": ocr_data,
    }

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
            from chatbot_app.nlu.customer_intent_classifier import NLUResult as _NLUResult

            # ── 0. Security Guard (trước mọi xử lý) ─────────────────
            context = load_session(req.session_id, user_id=req.user_id, role=role)
            _guard_role_s = "guest" if req.user_id is None else "customer"
            _sec_s = _guard_check(req.message, role=_guard_role_s, context=context)
            if _sec_s:
                save_session(req.session_id, context)
                ev_sec = json.dumps({"type": "token", "content": _sec_s["response"]}, ensure_ascii=False)
                yield f"data: {ev_sec}\n\n"
                yield 'data: {"type":"done","btns":[],"sources":[]}\n\n'
                return

            # ── [FAST QUICK-REPLY INTERCEPTOR] ──────────────────────────────
            # Bypass NLU + sentiment for predefined tags → saves 1-3 seconds.
            # ⚠️ Only Customer/Guest tags. Staff/Admin use separate pipeline.
            # ⚠️ Duplicate keys in a dict silently keep the LAST value → no dupes here.
            _FAST_TAGS: dict[str, tuple[str, dict]] = {
                # Customer tags
                "📚 Sách tâm lý hay nhất":       ("recommend_category", {"genre": "Tâm lý học"}),
                "🔥 Sách đang hot":               ("recommend_trending",  {}),
                "📦 Kiểm tra đơn hàng":           ("order_status",       {}),
                "🎁 Gợi ý sách tặng quà":         ("recommend_gift",     {}),
                "💡 Sách kinh tế & khởi nghiệp":  ("recommend_category", {"genre": "Kinh tế"}),
                # Guest tags
                "📖 Sách văn học hay":            ("recommend_category", {"genre": "Văn học"}),
                "💰 Khuyến mãi hiện tại":         ("promotion_current",  {}),
                "📚 Sách phát triển bản thân":    ("recommend_category", {"genre": "Kỹ năng sống"}),
            }
            _msg_clean = req.message.strip()
            if _msg_clean in _FAST_TAGS:
                _fast_intent, _fast_entities = _FAST_TAGS[_msg_clean]
                nlu_result = _NLUResult(intent=_fast_intent, entities=_fast_entities, confidence=1.0)
                sentiment  = "NEUTRAL"
                nlu_result.sentiment = sentiment
            else:
        # Normal path: full NLU + sentiment
                nlu_result = detect_intent(req.message)
                sentiment  = analyze_sentiment(req.message)
                nlu_result.sentiment = sentiment

            # ── 2. Context + History (always runs) ──────────────────────────
            context = context
            if req.history:
                history_dicts = [
                    {"role": m.role.value, "content": m.content}
                    for m in req.history[-10:]
                ]
            else:
                history_dicts = get_history_from_context(context, max_turns=10)

            resolved_message = resolve_coref(req.message, context)
            user_profile = build_user_profile(req.user_id)

            # [FIX] Genre rescue: nếu NLU classify nhầm tên thể loại thành PURCHASE_INTENTS
            # VD: "Lịch sử" → order_history → bị login gate chặn sai
            if req.user_id is None and nlu_result.intent in PURCHASE_INTENTS:
                from chatbot_app.generation.dialog_utils import resolve_genre_alias as _rga
                _genre_rescue = _rga(req.message)
                if _genre_rescue:
                    nlu_result.intent = "recommend_category"
                    nlu_result.entities["genre"] = _genre_rescue
                    nlu_result.confidence = 1.0

            # [SECURITY] Đã check ở trên, bỏ inline check cũ
            if req.user_id is None and nlu_result.intent in PURCHASE_INTENTS:
                answer_text, sources_list, navigate_buttons = _login_required_response()
                ev = json.dumps({"type": "token", "content": answer_text}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources_list}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"
                return

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
                ev = json.dumps({"type": "token", "content": stream_ctx}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"
                full_text = stream_ctx
            else:
                full_parts: list[str] = []
                async for token in generate_stream(
                    resolved_message, stream_ctx, history_dicts, tone, intent_str
                ):
                    full_parts.append(token)

                raw_full  = "".join(full_parts)
                full_text = _capitalize_first(_clean_llm_response(raw_full))

                ev = json.dumps({"type": "token", "content": full_text, "complete": True}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
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
            ocr_data: dict = {}
            try:
                file_bytes = await file.read()
                async with httpx.AsyncClient(timeout=20.0) as client:  # 20s: đủ cho OCR dual-mode (~10-15s worst case)
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

            # ── Bước 3 & 4: Route theo Role ───────────────────────────────────
            if role == "staff":
                from chatbot_app.nlu.staff_intent_classifier import detect_staff_intent
                from chatbot_app.generation.staff_dialog_manager import process_staff
                
                nlu_result = detect_staff_intent(display_message)
                if nlu_result.intent == "staff_out_of_scope" and not message.strip():
                    nlu_result.intent = "staff_book_lookup"
                    nlu_result.confidence = 1.0
                if book_title:
                    context["last_found_title"] = book_title
                
                stream_ctx, sources = await process_staff(display_message, nlu_result, user_id, context)
                nav_btns_raw = context.pop("pending_btns", [])
                navigate_buttons = [NavigateButton(**b) for b in nav_btns_raw]
                is_template = True
                
            elif role == "admin":
                from chatbot_app.nlu.admin_intent_classifier import detect_admin_intent
                from chatbot_app.generation.admin_dialog_manager import process_admin
                
                nlu_result = detect_admin_intent(display_message)
                if nlu_result.intent == "admin_out_of_scope" and not message.strip():
                    nlu_result.intent = "admin_book_lookup"
                    nlu_result.confidence = 1.0
                if book_title:
                    context["last_found_title"] = book_title
                
                stream_ctx, sources = await process_admin(display_message, nlu_result, user_id, context)
                navigate_buttons = []
                is_template = True
                
            else:
                # Customer / Guest
                nlu_result = NLUResult(intent="image_search", confidence=1.0)
                nlu_result.sentiment = "NEUTRAL"
                
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
                full_text = _capitalize_first(_clean_llm_response("".join(full_parts)))
                ev = json.dumps({"type": "token", "content": full_text, "complete": True}, ensure_ascii=False)
                yield f"data: {ev}\n\n"
                btns_data = [b.model_dump() for b in navigate_buttons]
                ev_done = json.dumps({"type": "done", "btns": btns_data, "sources": sources}, ensure_ascii=False)
                yield f"data: {ev_done}\n\n"

            # ── Bước 6: Lưu DB + persist OCR context cho coref ────────────────
            try:
                ctx_updated = update_context_with_entities(context, nlu_result.entities, nlu_result.intent)
                ctx_updated["user_role"] = role
                # Persist OCR info vào ocr_history để resolve "hai cuốn vừa upload"
                if book_title:
                    book_info_for_ctx = {
                        "title": book_title,
                        "price": ocr_data.get("search_results", [{}])[0].get("discounted_price", 0) if ocr_data.get("search_results") else 0,
                        "avg_rating": ocr_data.get("search_results", [{}])[0].get("avg_rating", 0) if ocr_data.get("search_results") else 0,
                        "id": ocr_data.get("search_results", [{}])[0].get("id", None) if ocr_data.get("search_results") else None,
                    }
                    ctx_updated = update_context_after_ocr(ctx_updated, book_info_for_ctx)
                    ctx_updated["last_intent"] = "image_search"
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
