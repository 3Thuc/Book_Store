"""
chat_router.py – FastAPI router cho chatbot endpoints.

Endpoint chính: POST /api/chat/message
Pipeline thực thi:
  1. NLU: detect_intent → intent, entities, confidence
  2. Sentiment: analyze_sentiment
  3. Context: load session, resolve co-reference
  4. User Profile: build từ user_actions
  5. Dialog Manager: retrieval + LLM generation
  6. Lưu session + messages vào MySQL
"""
from fastapi import APIRouter
from chatbot_app.models import ChatRequest, ChatResponse
from chatbot_app.nlu.intent_classifier import detect_intent
from chatbot_app.nlu.sentiment_analyzer import analyze_sentiment
from chatbot_app.context.session_manager import (
    load_session, save_session, save_message,
    resolve_coref, update_context_with_entities,
)
from chatbot_app.user_intelligence.profile_builder import build_user_profile
from chatbot_app.generation.dialog_manager import process
from chatbot_app.generation.llm_client import check_ollama_health

router = APIRouter(prefix="/api/chat", tags=["Chatbot"])


@router.post("/message", response_model=ChatResponse)
async def chat_message(req: ChatRequest):
    """
    Endpoint chính của chatbot.
    POST /api/chat/message
    """
    # ── 1. NLU ────────────────────────────────────────────────
    nlu_result = detect_intent(req.message)
    sentiment  = analyze_sentiment(req.message)
    nlu_result.sentiment = sentiment

    # ── 2. Load & resolve context ─────────────────────────────
    context = load_session(req.session_id)
    resolved_message = resolve_coref(req.message, context)

    # ── 3. User Profile ───────────────────────────────────────
    user_profile = build_user_profile(req.user_id)

    # ── 4. Lịch sử hội thoại (format cho LLM) ────────────────
    history_dicts = [
        {"role": m.role.value, "content": m.content}
        for m in req.history[-8:]   # tối đa 8 tin gần nhất
    ]

    # ── 5. Dialog Manager → retrieval + generation ────────────
    answer, sources = await process(
        message=resolved_message,
        nlu_result=nlu_result,
        user_id=req.user_id,
        context=context,
        history=history_dicts,
        user_profile=user_profile,
    )

    # ── 6. Cập nhật context & lưu DB ─────────────────────────
    context = update_context_with_entities(context, nlu_result.entities, nlu_result.intent)
    save_session(req.session_id, context)

    save_message(req.session_id, "user", req.message,
                 intent=nlu_result.intent, confidence=nlu_result.confidence,
                 sentiment=sentiment, entities=nlu_result.entities)
    save_message(req.session_id, "assistant", answer,
                 sources=sources)

    return ChatResponse(
        session_id=req.session_id,
        answer=answer,
        intent=nlu_result.intent,
        confidence=nlu_result.confidence,
        sentiment=sentiment,
        sources=sources,
    )


@router.get("/health")
async def chatbot_health():
    """Kiểm tra chatbot service và Ollama."""
    ollama_ok = await check_ollama_health()
    return {
        "status":      "ok",
        "service":     "chatbot",
        "ollama":      "ready" if ollama_ok else "not_ready",
        "ollama_hint": "Chạy: docker compose exec ollama ollama pull qwen2.5:7b" if not ollama_ok else "",
    }
