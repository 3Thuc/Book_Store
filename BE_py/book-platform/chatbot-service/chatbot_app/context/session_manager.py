"""
session_manager.py – Quản lý session và lịch sử hội thoại.

Lưu vào MySQL:
  - chat_sessions: thông tin phiên, context_json (entities đã extract)
  - chat_messages: từng tin nhắn user/assistant

Co-reference Resolution:
  Khi user nói "cuốn đó", "nó", "tác giả đó" → tra context_json
  để resolve về entity thực sự đã nhắc đến trước đó.
"""
import json
from datetime import datetime
from chatbot_app.db import get_connection


def load_session(session_id: str) -> dict:
    """
    Load session context từ MySQL.
    Trả về dict context (last_mentioned_books, slots, last_intent...).
    Nếu session chưa tồn tại → tạo mới và trả về context rỗng.
    """
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT context_json FROM chat_sessions WHERE session_id = %s",
        (session_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return json.loads(row["context_json"] or "{}")
    else:
        _create_session(session_id)
        return {}


def _create_session(session_id: str, user_id: int = None):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT IGNORE INTO chat_sessions
            (session_id, user_id, started_at, last_active, context_json, turn_count)
        VALUES (%s, %s, NOW(), NOW(), '{}', 0)
    """, (session_id, user_id))
    conn.commit()
    cur.close()
    conn.close()


def save_session(session_id: str, context: dict):
    """Cập nhật context_json và last_active."""
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE chat_sessions
        SET context_json = %s, last_active = NOW(), turn_count = turn_count + 1
        WHERE session_id = %s
    """, (json.dumps(context, ensure_ascii=False), session_id))
    conn.commit()
    cur.close()
    conn.close()


def save_message(session_id: str, role: str, content: str,
                 intent: str = None, confidence: float = None,
                 sentiment: str = None, entities: dict = None,
                 sources: list = None):
    """Lưu một tin nhắn vào chat_messages."""
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO chat_messages
            (session_id, role, content, intent, confidence,
             sentiment, entities, retrieval_sources, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    """, (
        session_id, role, content, intent, confidence, sentiment,
        json.dumps(entities or {}, ensure_ascii=False),
        json.dumps(sources  or [], ensure_ascii=False),
    ))
    conn.commit()
    cur.close()
    conn.close()


def resolve_coref(text: str, context: dict) -> str:
    """
    Co-reference Resolution – giải quyết đại từ tham chiếu.
    Ví dụ: "cuốn đó" → "Đắc Nhân Tâm" (từ context)
    """
    COREF_TRIGGERS = ["cuốn đó", "cuốn ấy", "nó", "cái đó",
                      "cuốn này", "tác giả đó", "người đó"]
    text_lower = text.lower()
    for trigger in COREF_TRIGGERS:
        if trigger in text_lower:
            books = context.get("last_mentioned_books", [])
            if books:
                text = text.replace(trigger, books[0])
    return text


def update_context_with_entities(context: dict, entities: dict, intent: str) -> dict:
    """
    Cập nhật context sau mỗi turn:
    - Lưu books/authors được nhắc đến gần đây
    - Cập nhật slots cho intent hiện tại
    - Lưu last_intent để biết context của câu kế
    """
    context["last_intent"] = intent

    if "genre" in entities:
        context.setdefault("slots", {})["genre"] = entities["genre"]
    if "order_id" in entities:
        context["last_order_id"] = entities["order_id"]
    if "book_title" in entities:
        books = context.get("last_mentioned_books", [])
        books.insert(0, entities["book_title"])
        context["last_mentioned_books"] = books[:5]  # giữ tối đa 5

    return context
