"""
staff_chat_router.py – FastAPI router cho Staff Chatbot endpoints.

Endpoints:
  POST /api/staff/chat/message  – Gửi tin nhắn đến Staff chatbot
  GET  /api/staff/chat/history/{session_id} – Xem lịch sử chat của phiên

Pipeline:
  1. NLU: detect_staff_intent → intent, entities
  2. Context: load session
  3. Dialog Manager: process_staff → retrieval từ MySQL
  4. Lưu session + messages vào MySQL
  5. Build response (bao gồm debug info vì user là staff)

Lưu ý:
  - Staff chatbot KHÔNG dùng K-Means personalization
  - KHÔNG cần user_id bắt buộc (staff có thể tra cứu theo mã/email)
  - Debug info luôn hiển thị (intent, confidence, latency)
  - Confirm flow cho các actions nguy hiểm
"""
import time
from fastapi import APIRouter, Depends, Request
from chatbot_app.models import (
    ChatRequest, ChatResponse,
    SessionHistoryResponse, HistoryMessage,
)
from chatbot_app.nlu.staff_intent_classifier import detect_staff_intent
from chatbot_app.nlu.sentiment_analyzer import analyze_sentiment
from chatbot_app.context.session_manager import (
    load_session, save_session, save_message,
    load_history_from_db, get_session_info, get_all_messages,
)
from chatbot_app.generation.staff_dialog_manager import process_staff
from chatbot_app.middleware.rate_limiter import limiter
from chatbot_app.middleware.jwt_auth import require_staff_role
from chatbot_app.middleware.security_guard import check_security

router = APIRouter(prefix="/api/staff/chat", tags=["Staff Chatbot"])


@router.post("/message", response_model=ChatResponse)
@limiter.limit("20/minute")
async def staff_chat_message(
    request: Request,
    req: ChatRequest,
    _jwt: dict = Depends(require_staff_role),
):
    """
    Staff chatbot endpoint.
    POST /api/staff/chat/message

    Yêu cầu: Authorization: Bearer <JWT> với role staff hoặc admin.
    Rate limit: 20 requests/minute/IP.
    Response bao gồm debug info (intent, confidence, latency).
    """
    t_start = time.perf_counter()

    try:
        # ── 0. Security Guard ─────────────────────────────────
        context = load_session(req.session_id, user_id=req.user_id, role="staff")
        _guard = check_security(req.message, role="staff", context=context)
        if _guard:
            save_session(req.session_id, context)
            return ChatResponse(
                session_id=req.session_id,
                answer=_guard["response"],
                intent=f"security_{_guard['reason']}",
                confidence=1.0,
                sentiment="NEGATIVE",
                sources=[],
            )

        # ── 1. NLU – Staff intent ──────────────────────────────
        nlu_result = detect_staff_intent(req.message)
        sentiment  = analyze_sentiment(req.message)
        nlu_result.sentiment = sentiment

        # ── 2. Load context (đã load ở guard, dùng lại) ───────────
        # context đã được load từ bước 0 (security guard), dùng lại

        # Load lịch sử (tránh reload lại từ client nếu không cần)
        if req.history:
            history_dicts = [
                {"role": m.role.value, "content": m.content}
                for m in req.history[-6:]
            ]
        else:
            history_dicts = load_history_from_db(req.session_id, max_turns=6)

        # ── 3. Process (Staff Dialog Manager) ─────────────────
        answer, sources = await process_staff(
            message=req.message,
            nlu_result=nlu_result,
            user_id=req.user_id,
            context=context,
        )

        # ── 4. Lưu DB ─────────────────────────────────────────
        try:
            context["user_role"] = "staff"
            save_session(req.session_id, context)
            save_message(
                req.session_id, "user", req.message,
                intent=nlu_result.intent,
                confidence=nlu_result.confidence,
                sentiment=sentiment,
                entities=nlu_result.entities,
            )
            save_message(req.session_id, "assistant", answer, sources=sources)
        except Exception as db_err:
            print(f"⚠️ Staff DB save error: {db_err}")

        # ── 5. Build response (staff luôn thấy debug info) ────
        latency_ms = round((time.perf_counter() - t_start) * 1000, 1)
        debug = {
            "intent":     nlu_result.intent,
            "confidence": round(nlu_result.confidence, 3),
            "sentiment":  sentiment,
            "latency_ms": latency_ms,
            "sources":    sources,
            "entities":   nlu_result.entities,
        }

        # Lấy navigate_buttons (confirm gate, quick reply...) từ context nếu có
        nav_btns_raw = context.pop("pending_btns", [])
        from chatbot_app.models import NavigateButton
        nav_btns = [NavigateButton(**b) for b in nav_btns_raw]

        return ChatResponse(
            session_id=req.session_id,
            answer=answer,
            intent=nlu_result.intent,
            confidence=nlu_result.confidence,
            sentiment=sentiment,
            sources=sources,
            debug=debug,
            navigate_buttons=nav_btns,
        )

    except Exception as e:
        import traceback
        print(f"❌ STAFF CHAT ERROR: {e}")
        traceback.print_exc()
        return ChatResponse(
            session_id=req.session_id,
            answer=(
                "😅 Xin lỗi, tôi gặp sự cố kỹ thuật.\n"
                "Vui lòng thử lại sau."
            ),
            intent="error",
            confidence=0.0,
            sentiment="NEUTRAL",
            sources=[],
        )


@router.get("/history/{session_id}", response_model=SessionHistoryResponse)
@limiter.limit("30/minute")
async def get_staff_chat_history(
    request: Request,
    session_id: str,
    _jwt: dict = Depends(require_staff_role),
):
    """Lịch sử hội thoại của phiên staff chat. Yêu cầu JWT staff/admin."""
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


@router.get("/health")
async def staff_chatbot_health():
    """Staff chatbot health check."""
    return {
        "status":   "ok",
        "service":  "staff-chatbot",
        "version":  "1.0",
        "intents":  14,
    }
