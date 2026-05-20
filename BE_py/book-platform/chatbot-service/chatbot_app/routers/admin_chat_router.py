"""
admin_chat_router.py – FastAPI router cho Admin Chatbot endpoints.

Endpoints:
  POST /api/admin/chat/message  – Gửi tin nhắn đến Admin chatbot
  GET  /api/admin/chat/history/{session_id} – Lịch sử chat

Pipeline:
  1. NLU: detect_admin_intent → intent, entities
  2. Context: load session
  3. Dialog Manager: process_admin → queries MySQL + các service khác
  4. Lưu session + messages vào MySQL
  5. Build response với full debug info cho admin

Lưu ý:
  - Admin chatbot có confirm flow cực kỳ nghiêm ngặt
  - Full debug info (entities, session_meta, latency)
  - Actions nguy hiểm (lock user, update role) PHẢI confirm
"""
import time
from fastapi import APIRouter, Depends, Request
from chatbot_app.models import (
    ChatRequest, ChatResponse,
    SessionHistoryResponse, HistoryMessage,
)
from chatbot_app.nlu.admin_intent_classifier import detect_admin_intent
from chatbot_app.nlu.sentiment_analyzer import analyze_sentiment
from chatbot_app.context.session_manager import (
    load_session, save_session, save_message,
    load_history_from_db, get_session_info, get_all_messages,
)
from chatbot_app.generation.admin_dialog_manager import process_admin
from chatbot_app.middleware.rate_limiter import limiter
from chatbot_app.middleware.jwt_auth import require_admin_role
from chatbot_app.middleware.security_guard import check_security

router = APIRouter(prefix="/api/admin/chat", tags=["Admin Chatbot"])


@router.post("/message", response_model=ChatResponse)
@limiter.limit("60/minute")
async def admin_chat_message(
    request: Request,
    req: ChatRequest,
    _jwt: dict = Depends(require_admin_role),
):
    """
    Admin chatbot endpoint.
    POST /api/admin/chat/message

    Yêu cầu: Authorization: Bearer <JWT> với role admin.
    Rate limit: 10 requests/minute/IP.
    Response bao gồm full debug info (intent, entities, session_meta, latency).
    """
    t_start = time.perf_counter()

    try:
        # ── 0. Security Guard ─────────────────────────────────
        context_pre = load_session(req.session_id, user_id=req.user_id, role="admin")
        _guard = check_security(req.message, role="admin", context=context_pre)
        if _guard:
            save_session(req.session_id, context_pre)
            return ChatResponse(
                session_id=req.session_id,
                answer=_guard["response"],
                intent=f"security_{_guard['reason']}",
                confidence=1.0,
                sentiment="NEGATIVE",
                sources=[],
            )

        # ── 1. NLU – Admin intent ──────────────────────────────
        nlu_result = detect_admin_intent(req.message)
        sentiment  = analyze_sentiment(req.message)
        nlu_result.sentiment = sentiment

        # ── 2. Load context (đã load ở guard, dùng lại) ────────
        context = context_pre

        # Load lịch sử
        if req.history:
            history_dicts = [
                {"role": m.role.value, "content": m.content}
                for m in req.history[-6:]
            ]
        else:
            history_dicts = load_history_from_db(req.session_id, max_turns=6)

        # ── 3. Process (Admin Dialog Manager) ─────────────────────────
        _adm_result = await process_admin(
            message=req.message,
            nlu_result=nlu_result,
            user_id=req.user_id,
            context=context,
        )
        # process_admin trả về (answer, sources) hoặc (answer, sources, buttons)
        if len(_adm_result) == 3:
            answer, sources, _nav_buttons = _adm_result
        else:
            answer, sources = _adm_result
            _nav_buttons = []

        # ── 4. Lưu DB ─────────────────────────────────────────
        try:
            context["user_role"] = "admin"
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
            print(f"⚠️ Admin DB save error: {db_err}")

        # ── 5. Build response (admin thấy full debug) ─────────
        latency_ms   = round((time.perf_counter() - t_start) * 1000, 1)
        session_info = get_session_info(req.session_id)
        debug = {
            "intent":     nlu_result.intent,
            "confidence": round(nlu_result.confidence, 3),
            "sentiment":  sentiment,
            "latency_ms": latency_ms,
            "sources":    sources,
            "entities":   nlu_result.entities,
            "session_meta": {
                "turn_count":  session_info.get("turn_count") if session_info else "?",
                "last_active": str(session_info.get("last_active", "")) if session_info else "?",
                "user_id":     req.user_id,
                "role":        "admin",
            },
        }

        from chatbot_app.models import NavigateButton
        nav_btns = [NavigateButton(**b) for b in _nav_buttons]
        return ChatResponse(
            session_id=req.session_id,
            answer=answer,
            intent=nlu_result.intent,
            confidence=nlu_result.confidence,
            sentiment=sentiment,
            sources=sources,
            navigate_buttons=nav_btns,
            debug=debug,
        )

    except Exception as e:
        import traceback
        print(f"❌ ADMIN CHAT ERROR: {e}")
        traceback.print_exc()
        return ChatResponse(
            session_id=req.session_id,
            answer=(
                "😅 Xin lỗi, Admin chatbot gặp sự cố kỹ thuật.\n"
                "Vui lòng thử lại sau."
            ),
            intent="error",
            confidence=0.0,
            sentiment="NEUTRAL",
            sources=[],
        )


@router.get("/history/{session_id}", response_model=SessionHistoryResponse)
@limiter.limit("20/minute")
async def get_admin_chat_history(
    request: Request,
    session_id: str,
    _jwt: dict = Depends(require_admin_role),
):
    """Lịch sử hội thoại của phiên admin chat. Yêu cầu JWT admin."""
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
async def admin_chatbot_health():
    """Admin chatbot health check."""
    return {
        "status":   "ok",
        "service":  "admin-chatbot",
        "version":  "2.0",
        "intents":  21,
        "security": "JWT required (role=admin)",
        "rate_limit": "10/minute",
    }


# ── Admin: Reload Knowledge Base ────────────────────────────────────────────────
@router.post("/reload-kb", summary="Reload Knowledge Base (Admin only)")
@limiter.limit("5/minute")
async def reload_knowledge_base(
    request: Request,
    _jwt: dict = Depends(require_admin_role),
):
    """
    Trigger re-index chatbot_kb từ các file trong knowledge_base/ vào OpenSearch.

    Dùng khi:
      - Cập nhật FAQ mới
      - Sửa chính sách đổi/trả
      - Thêm thông tin khuyến mãi

    Thay vì phải SSH vào server chạy `python scripts/index_kb.py` thủ công.
    Yêu cầu JWT role=admin.
    Rate limit: 5 lần/phút (re-index tốn ~2-5s).
    """
    import asyncio
    import subprocess
    import sys
    from pathlib import Path

    scripts_dir = Path(__file__).resolve().parents[2] / "scripts"
    index_script = scripts_dir / "index_kb.py"

    if not index_script.exists():
        return {
            "status": "error",
            "message": f"Script không tìm thấy: {index_script}",
        }

    try:
        # Chạy index_kb.py trong background thread – không block request
        def _run_index():
            result = subprocess.run(
                [sys.executable, str(index_script)],
                capture_output=True, text=True, timeout=120
            )
            return result.returncode, result.stdout[-500:], result.stderr[-300:]

        returncode, stdout, stderr = await asyncio.to_thread(_run_index)

        if returncode == 0:
            return {
                "status":  "ok",
                "message": "✅ Knowledge Base đã được re-index thành công!",
                "output":  stdout,
            }
        else:
            return {
                "status":  "error",
                "message": "Re-index thất bại",
                "stderr":  stderr,
                "stdout":  stdout,
            }
    except asyncio.TimeoutError:
        return {
            "status":  "timeout",
            "message": "Re-index chạy quá 120s. Kiểm tra kết nối OpenSearch.",
        }
    except Exception as e:
        return {
            "status":  "error",
            "message": str(e),
        }
