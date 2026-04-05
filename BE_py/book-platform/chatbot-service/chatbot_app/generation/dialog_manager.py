"""
dialog_manager.py – Trung tâm điều phối logic chatbot.

Tích hợp đầy đủ Hybrid Intelligent pipeline:
  1. NLU (intent + entities + sentiment)
  2. Context (load session, co-reference)
  3. User Intelligence (profile + cluster → tone)
  4. Smart Retrieval (RAG / MySQL / Recommend)
  5. LLM Generation (Ollama personalized prompt)
"""
from chatbot_app.nlu.intent_classifier import NLUResult
from chatbot_app.nlu.sentiment_analyzer import analyze_sentiment
from chatbot_app.retrieval.rag_retriever import RAGRetriever
from chatbot_app.retrieval.sql_retriever import (
    get_order_info, get_user_orders, get_books_by_genre, get_book_price
)
from chatbot_app.generation.llm_client import generate

# Câu trả lời template (nhanh, không cần LLM)
TEMPLATES = {
    "greeting": "Xin chào! Tôi là trợ lý BookStore 📚. Tôi có thể giúp bạn tìm sách, tra cứu đơn hàng hoặc giải đáp chính sách. Bạn cần hỗ trợ gì?",
    "farewell":  "Cảm ơn bạn đã liên hệ BookStore! Chúc bạn đọc sách vui vẻ 📖",
    "no_auth":   "Vui lòng đăng nhập để sử dụng tính năng này.",
    "no_order":  "Tôi không tìm thấy đơn hàng này. Vui lòng kiểm tra lại mã đơn hoặc liên hệ hotline.",
}


async def process(
    message:    str,
    nlu_result: NLUResult,
    user_id:    int | None,
    context:    dict,
    history:    list[dict],
    user_profile: dict,
) -> tuple[str, list[str]]:
    """
    Xử lý message và trả về (answer, sources).

    Args:
        message:      Câu hỏi (đã qua co-reference resolution)
        nlu_result:   Kết quả NLU (intent, entities, confidence)
        user_id:      ID người dùng (None nếu khách vãng lai)
        context:      Session context
        history:      Lịch sử hội thoại [{role, content}]
        user_profile: Profile người dùng (cluster, tone, favorites)

    Returns:
        (answer_text, list_of_sources_used)
    """
    intent   = nlu_result.intent
    entities = nlu_result.entities
    tone     = user_profile.get("tone", "thân thiện, ngắn gọn")
    rag      = RAGRetriever.get()

    # ── TEMPLATE RESPONSES (không cần LLM, nhanh) ─────────────
    if intent == "greeting":
        return TEMPLATES["greeting"], []
    if intent == "farewell":
        return TEMPLATES["farewell"], []

    # ── ORDER STATUS ──────────────────────────────────────────
    if intent == "order_status":
        order_id = entities.get("order_id") or context.get("last_order_id")
        if order_id:
            order = get_order_info(int(order_id))
            if not order:
                return TEMPLATES["no_order"], []
            ctx = (
                f"Thông tin đơn hàng #{order_id}:\n"
                f"- Trạng thái: {order['status']}\n"
                f"- Tổng tiền: {order['total_price']:,.0f}đ\n"
                f"- Ngày đặt: {order['created_at']}\n"
                f"- Địa chỉ: {order['shipping_address']}"
            )
        elif user_id:
            orders = get_user_orders(user_id, limit=3)
            if not orders:
                ctx = "Bạn chưa có đơn hàng nào."
            else:
                ctx = "Đơn hàng gần đây:\n" + "\n".join(
                    f"- #{o['order_id']}: {o['status']} – {o['total_price']:,.0f}đ ({str(o['created_at'])[:10]})"
                    for o in orders
                )
        else:
            return "Vui lòng cung cấp mã đơn hàng (ví dụ: #12345) hoặc đăng nhập để xem lịch sử.", []
        answer = await generate(message, ctx, history, tone)
        return answer, ["mysql:orders"]

    # ── ORDER HISTORY ─────────────────────────────────────────
    if intent == "order_history":
        if not user_id:
            return TEMPLATES["no_auth"], []
        orders = get_user_orders(user_id, limit=5)
        ctx = f"Lịch sử {len(orders)} đơn hàng:\n" + "\n".join(
            f"- #{o['order_id']}: {o['status']} – {o['total_price']:,.0f}đ – {o['book_count']} cuốn"
            for o in orders
        ) if orders else "Chưa có đơn hàng nào."
        answer = await generate(message, ctx, history, tone)
        return answer, ["mysql:orders"]

    # ── BOOK RECOMMENDATION ───────────────────────────────────
    if intent == "book_recommendation":
        genre = entities.get("genre") or context.get("slots", {}).get("genre", "")
        if genre:
            books = get_books_by_genre(genre, limit=5)
            if books:
                ctx = f"Sách thể loại '{genre}' bán chạy nhất:\n" + "\n".join(
                    f"- {b['title']} | {b['author_name']} | {b['price']:,.0f}đ"
                    for b in books
                )
            else:
                ctx = f"Hiện chưa có sách thể loại '{genre}' trong kho."
        else:
            # Không có genre → RAG tìm gợi ý chung
            hits = rag.retrieve(message, top_k=2)
            ctx  = "\n\n".join(h["text"] for h in hits) or "Hãy cho biết bạn muốn đọc thể loại sách gì?"
        answer = await generate(message, ctx, history, tone)
        return answer, ["mysql:books"]

    # ── BOOK SEARCH / PRICE ───────────────────────────────────
    if intent == "book_search":
        book_title = entities.get("book_title", "")
        if book_title:
            book = get_book_price(book_title)
            ctx = (f"Thông tin sách:\n- {book['title']} | {book['author_name']} | {book['price']:,.0f}đ | Còn {book['stock_quantity']} cuốn"
                   if book else f"Không tìm thấy sách '{book_title}' trong hệ thống.")
        else:
            hits = rag.retrieve(message, top_k=2)
            ctx  = "\n\n".join(h["text"] for h in hits) or "Vui lòng cho biết tên sách hoặc tác giả bạn muốn tìm."
        answer = await generate(message, ctx, history, tone)
        return answer, ["mysql:books"]

    # ── POLICY QUERIES (đổi trả / vận chuyển / thanh toán) ───
    if intent in ("policy_return", "policy_shipping", "policy_payment"):
        hits = rag.retrieve(message, top_k=3)
        ctx  = "\n\n".join(h["text"] for h in hits) if hits else "Liên hệ hotline để được hỗ trợ chi tiết."
        sources = [h["source"] for h in hits]
        answer  = await generate(message, ctx, history, tone)
        return answer, sources

    # ── NEGATIVE SENTIMENT – ưu tiên xin lỗi ─────────────────
    if nlu_result.sentiment == "NEGATIVE":
        hits = rag.retrieve(message, top_k=3)
        ctx  = "\n\n".join(h["text"] for h in hits) if hits else ""
        tone_neg = "đồng cảm, xin lỗi trước rồi mới giải thích, không dùng từ kỹ thuật"
        answer   = await generate(message, ctx, history, tone_neg)
        return answer, [h["source"] for h in hits]

    # ── GENERAL QUERY – RAG + LLM ─────────────────────────────
    hits   = rag.retrieve(message, top_k=3)
    ctx    = "\n\n".join(h["text"] for h in hits) if hits else "Không có thông tin liên quan trong hệ thống."
    answer = await generate(message, ctx, history, tone)
    return answer, [h["source"] for h in hits]
