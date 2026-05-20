"""
session_manager.py – Quản lý session và lịch sử hội thoại.

=== Phiên bản v2 – Tối ưu DB (E + G) ===

KIẾN TRÚC MỚI:
  ┌─────────────────────────────────────────────────────────┐
  │              L1: IN-MEMORY CACHE (< 1ms)                │  ← Phương án E
  │   _session_cache: dict[session_id → (ctx, expire)]     │
  │   TTL = 1800s (30 phút) | max 500 sessions             │
  └────────────────────┬────────────────────────────────────┘
                       │ MISS (lần đầu / sau restart)
  ┌─────────────────────▼───────────────────────────────────┐
  │              L2: MYSQL chat_sessions (~20ms)            │
  │   context_json bây giờ chứa LUÔN history_window        │  ← Phương án G
  │   → không cần query chat_messages để lấy history       │
  └─────────────────────────────────────────────────────────┘

VÍ DỤ context_json sau cải tiến:
  {
    "last_intent": "book_search",
    "slots": {"genre": "kinh tế"},
    "last_mentioned_books": ["Đắc Nhân Tâm"],
    "history_window": [                          ← MỚI (Phương án G)
      {"role": "user", "content": "Tìm sách hay"},
      {"role": "assistant", "content": "Tôi tìm..."}
    ]
  }

KẾT QUẢ:
  Active session → 0 DB roundtrips blocking (chỉ async write-back)
  First turn     → 1 SELECT thay vì 2 SELECT (load_session thay cho cả 2)
  chat_messages  → chỉ dùng cho analytics/admin, không dùng real-time

FIX giữ lại từ v1: try/except trên mọi DB call – không crash khi pool exhausted.
"""
import json
import time
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from chatbot_app.db import get_connection

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Redis L0 Cache - graceful fallback
try:
    from chatbot_app.cache.redis_client import (
        redis_get_session, redis_set_session, redis_delete_session
    )
    _REDIS_ENABLED = True
except ImportError:
    _REDIS_ENABLED = False
    def redis_get_session(sid): return None
    def redis_set_session(sid, ctx, ttl=7200): return False
    def redis_delete_session(sid): return False


# ═══════════════════════════════════════════════════════════════════════
# PHƯƠNG ÁN E – IN-MEMORY SESSION CACHE
# ═══════════════════════════════════════════════════════════════════════
# Tại sao dùng dict + Lock thay Redis?
#   - Không cần thêm service (Redis) vào docker-compose
#   - Hoàn toàn đủ cho quy mô đồ án (1 worker process)
#   - Dễ giải thích: "LRU-like cache với TTL"
#
# Giới hạn:
#   - Mất cache khi restart container → lần đầu sau restart vẫn query DB (OK)
#   - Không share cache giữa nhiều worker (OK vì hiện tại 1 uvicorn worker)

_session_cache: dict[str, tuple[dict, float]] = {}  # session_id → (context, expire_at)
_cache_lock = threading.Lock()
_SESSION_TTL   = 1800   # 30 phút – đủ cho 1 phiên chat liên tục
_CACHE_MAX     = 500    # tối đa 500 session cùng lúc in-memory (~10MB)
_HISTORY_MAX   = 12     # giữ 12 turns trong context_json (đủ cho LLM window)

# Thread pool để write-back DB không block API response
_db_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="db_write")


def _cache_get(session_id: str) -> dict | None:
    """Lấy context từ L1 cache. Trả None nếu miss hoặc expired."""
    with _cache_lock:
        entry = _session_cache.get(session_id)
        if entry is None:
            return None
        ctx, expire_at = entry
        if time.time() > expire_at:
            del _session_cache[session_id]
            return None
        # Trả bản copy để tránh mutation ảnh hưởng cache
        return dict(ctx)


def _cache_set(session_id: str, context: dict) -> None:
    """Ghi context vào L1 cache. Evict oldest nếu đầy."""
    with _cache_lock:
        # FIFO eviction đơn giản khi đầy
        if len(_session_cache) >= _CACHE_MAX and session_id not in _session_cache:
            oldest = next(iter(_session_cache))
            del _session_cache[oldest]
        _session_cache[session_id] = (dict(context), time.time() + _SESSION_TTL)


def _cache_invalidate(session_id: str) -> None:
    """Xóa session khỏi cache (dùng khi force refresh)."""
    with _cache_lock:
        _session_cache.pop(session_id, None)


# ═══════════════════════════════════════════════════════════════════════
# PHƯƠNG ÁN G – HISTORY NHÚNG TRONG context_json
# ═══════════════════════════════════════════════════════════════════════
# Ý tưởng: thay vì query chat_messages mỗi turn (SELECT... LIMIT 10),
# ta lưu history_window trực tiếp vào context_json của chat_sessions.
#
# Ưu điểm:
#   - load_session() trả về cả context VÀ history trong 1 query
#   - Không cần load_history_from_db() nữa trong hot path
#   - chat_messages vẫn INSERT đầy đủ → dùng cho analytics & admin panel
#
# Nhược điểm có thể chấp nhận:
#   - context_json lớn hơn ~2–3KB mỗi session (OK với JSON column)
#   - Nếu restart giữa chừng → mất history_window → LLM context reset
#     (acceptable: bot sẽ hỏi lại tự nhiên)

def _append_to_history_window(context: dict, role: str, content: str) -> None:
    """
    Thêm tin nhắn mới vào history_window trong context.
    Giữ tối đa _HISTORY_MAX turns, cuộn bỏ cũ nhất khi đầy.

    Ví dụ:
      history_window = [
        {"role": "user",      "content": "Tìm sách kỹ năng"},
        {"role": "assistant", "content": "Đây là top 5 sách..."}
      ]
    """
    window = context.setdefault("history_window", [])
    window.append({"role": role, "content": content[:500]})  # cap 500 chars mỗi message để tránh context_json phình
    if len(window) > _HISTORY_MAX:
        window[:] = window[-_HISTORY_MAX:]  # giữ N cuối


# ═══════════════════════════════════════════════════════════════════════
# PUBLIC API – load_session (thay thế load_session + load_history_from_db)
# ═══════════════════════════════════════════════════════════════════════

def load_session(session_id: str, user_id: int = None, role: str = "customer") -> dict:
    """
    Load session context từ L1 cache hoặc MySQL.

    TRẢ VỀ: dict context bao gồm:
      - Tất cả NLU state (last_intent, slots, last_mentioned_books...)
      - history_window: list[dict] – lịch sử hội thoại (thay thế load_history_from_db)
      - is_guest: bool – dựa trên user_id
      - user_id: int | None

    LUỒNG:
      1. Check L1 cache → HIT (<1ms) → trả về ngay
      2. MISS → SELECT chat_sessions → ghi L1 → trả về
      3. Không tìm thấy session → INSERT mới → trả context rỗng

    GHI CHÚ: Không cần gọi load_history_from_db() sau hàm này nữa.
    Lấy history bằng: context.get("history_window", [])
    """
    # ── L1: RAM cache ──────────────────────────────────────────────
    # L0: Redis
    redis_ctx = redis_get_session(session_id)
    if redis_ctx is not None:
        redis_ctx["is_guest"] = (user_id is None)
        redis_ctx["user_id"]  = user_id
        _cache_set(session_id, redis_ctx)  # warm L1
        return redis_ctx

    cached = _cache_get(session_id)
    if cached is not None:
        cached["is_guest"] = (user_id is None)
        cached["user_id"]  = user_id
        return cached

    # ── L2: MySQL ──────────────────────────────────────────────────
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute(
                "SELECT context_json FROM chat_sessions WHERE session_id = %s",
                (session_id,)
            )
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()

        if row:
            ctx = json.loads(row["context_json"] or "{}")
        else:
            # Session mới → tạo trong DB (fire-and-forget)
            _db_executor.submit(_create_session_sync, session_id, user_id, role)
            ctx = {"role": role, "history_window": []}

        # Inject & cache
        ctx["is_guest"] = (user_id is None)
        ctx["user_id"]  = user_id
        _cache_set(session_id, ctx)
        redis_set_session(session_id, ctx)  # warm L0 Redis
        return ctx

    except Exception as e:
        print(f"⚠️ [load_session] DB error: {e}")
        return {
            "role": role,
            "is_guest": (user_id is None),
            "user_id": user_id,
            "history_window": []
        }


def get_history_from_context(context: dict, max_turns: int = 10) -> list[dict]:
    """
    Lấy lịch sử hội thoại từ context (không query DB).

    Đây là hàm THAY THẾ cho load_history_from_db() trong hot path.
    Trả về list[dict] format: [{"role": "user"|"assistant", "content": "..."}]

    Tại sao không cần DB:
      history_window được lưu trong context_json của chat_sessions,
      đã có sẵn trong load_session() ở bước trước.
    """
    window = context.get("history_window", [])
    # Chỉ trả max_turns cuối để đảm bảo không vượt token limit LLM
    return window[-max_turns:] if len(window) > max_turns else window


def load_history_from_db(session_id: str, max_turns: int = 10) -> list[dict]:
    """
    [LEGACY – giữ để tương thích với admin/staff router và fallback]
    Hot path nên dùng get_history_from_context(context) thay thế.

    Hàm này vẫn giữ để:
    1. Admin panel có thể xem toàn bộ lịch sử từ chat_messages
    2. Fallback khi context bị mất (restart, bug)
    3. Staff/admin router chưa được cập nhật
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT role, content FROM chat_messages
                WHERE session_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (session_id, max_turns))
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()
        rows.reverse()
        return [{"role": r["role"], "content": r["content"]} for r in rows]
    except Exception as e:
        print(f"⚠️ [load_history_from_db] DB error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════
# ASYNC WRITE-BACK – save_session (không block API response)
# ═══════════════════════════════════════════════════════════════════════
# Ý tưởng: update L1 cache NGAY (< 1ms), sau đó ghi DB ở background thread.
# API response không cần chờ MySQL UPDATE (~5–20ms) nữa.

def save_session(session_id: str, context: dict) -> None:
    """
    Cập nhật context session.

    LUỒNG:
      1. Update L1 cache ngay (<1ms) → API response không bị chậm
      2. Submit DB write-back vào thread pool (không block)
      3. MySQL UPDATE chạy trong background (~5–20ms), không ảnh hưởng latency

    Lưu ý: context nên đã được cập nhật với entities trước khi gọi hàm này.
    history_window được tự động cập nhật trong save_message().
    """
    # Bước 1: Update L1 ngay
    _cache_set(session_id, context)
    # Bước 2: Update L0 Redis (background)
    _db_executor.submit(redis_set_session, session_id, context)
    # Bước 3: Write-back DB async (không block)
    _db_executor.submit(_save_session_sync, session_id, context)


def _save_session_sync(session_id: str, context: dict) -> None:
    """
    [Internal] Ghi chat_sessions vào MySQL.
    Chạy trong background thread, không block event loop.

    Trường context được lưu:
      - Bỏ is_guest, user_id (runtime flags, không cần persist)
      - Giữ history_window (12 turns)
      - Giữ tất cả NLU state
    """
    # Loại bỏ runtime-only fields trước khi serialize
    ctx_to_save = {k: v for k, v in context.items() if k not in ("is_guest", "user_id")}
    try:
        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("""
                UPDATE chat_sessions
                SET context_json = %s, last_active = NOW(), turn_count = turn_count + 1
                WHERE session_id = %s
            """, (json.dumps(ctx_to_save, cls=CustomEncoder, ensure_ascii=False), session_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print(f"⚠️ [save_session_sync] DB write-back error: {e}")


def _create_session_sync(session_id: str, user_id: int = None, role: str = "customer") -> None:
    """[Internal] Tạo session mới trong MySQL. Chạy trong background thread."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("""
                INSERT IGNORE INTO chat_sessions
                    (session_id, user_id, started_at, last_active, context_json, turn_count)
                VALUES (%s, %s, NOW(), NOW(), %s, 0)
            """, (session_id, user_id, json.dumps({"role": role, "history_window": []})))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print(f"⚠️ [_create_session_sync] DB error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# save_message – lưu log + cập nhật history_window trong cache
# ═══════════════════════════════════════════════════════════════════════

def save_message(session_id: str, role: str, content: str,
                 intent: str = None, confidence: float = None,
                 sentiment: str = None, entities: dict = None,
                 sources: list = None) -> None:
    """
    Lưu tin nhắn vào chat_messages VÀ cập nhật history_window trong cache.

    Hai nhiệm vụ:
      1. ASYNC: INSERT chat_messages (log đầy đủ cho analytics)
      2. SYNC (fast): Cập nhật history_window trong L1 cache ngay
         → turn tiếp theo của cùng session sẽ có history đúng mà không cần DB

    Lưu ý: chat_messages vẫn được INSERT đầy đủ cho:
      - Admin panel xem lịch sử
      - Analytics intent distribution
      - Debug khi cần trace lại hành vi
    """
    # Bước 1: Cập nhật history_window trong L1 cache NGAY
    cached = _cache_get(session_id)
    if cached is not None:
        _append_to_history_window(cached, role, content)
        _cache_set(session_id, cached)

    # Bước 2: INSERT vào chat_messages async (background)
    _db_executor.submit(
        _save_message_sync,
        session_id, role, content, intent, confidence, sentiment, entities, sources
    )


def _save_message_sync(session_id: str, role: str, content: str,
                       intent: str = None, confidence: float = None,
                       sentiment: str = None, entities: dict = None,
                       sources: list = None) -> None:
    """[Internal] INSERT chat_messages. Chạy trong background thread."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO chat_messages
                    (session_id, role, content, intent, confidence,
                     sentiment, entities, retrieval_sources, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                session_id, role, content, intent, confidence, sentiment,
                json.dumps(entities or {}, cls=CustomEncoder, ensure_ascii=False),
                json.dumps(sources  or [], cls=CustomEncoder, ensure_ascii=False),
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        print(f"⚠️ [save_message_sync] DB error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# READ-ONLY HELPERS – không thay đổi, vẫn query DB trực tiếp
# ═══════════════════════════════════════════════════════════════════════

def get_session_info(session_id: str) -> dict | None:
    """Trả về metadata của phiên: turn_count, last_active, user_id."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT session_id, user_id, started_at, last_active, turn_count
                FROM chat_sessions
                WHERE session_id = %s
            """, (session_id,))
            row = cur.fetchone()
        finally:
            cur.close()
            conn.close()
        return row
    except Exception as e:
        print(f"⚠️ [get_session_info] DB error: {e}")
        return None


def get_all_messages(session_id: str, limit: int = 200) -> list[dict]:
    """
    Trả về tin nhắn của phiên cho UI lịch sử / admin panel.

    FIX v2: Thêm LIMIT (mặc định 200) để tránh scan không giới hạn.
    Admin muốn xem hơn → tăng limit khi gọi.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT id, role, content, intent, confidence, sentiment, created_at
                FROM chat_messages
                WHERE session_id = %s
                ORDER BY created_at ASC
                LIMIT %s
            """, (session_id, limit))
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()
        for r in rows:
            if r.get("created_at"):
                r["created_at"] = str(r["created_at"])
        return rows
    except Exception as e:
        print(f"⚠️ [get_all_messages] DB error: {e}")
        return []


def cache_stats() -> dict:
    """
    Trả về thống kê L1 cache để monitor.
    Dùng trong /api/chat/stats endpoint.
    """
    with _cache_lock:
        now = time.time()
        active = sum(1 for _, (_, exp) in _session_cache.items() if now < exp)
        return {
            "total_entries": len(_session_cache),
            "active_entries": active,
            "max_size": _CACHE_MAX,
            "ttl_seconds": _SESSION_TTL,
        }


# ═══════════════════════════════════════════════════════════════════════
# COREF & CONTEXT UPDATE – không thay đổi logic, giữ nguyên
# ═══════════════════════════════════════════════════════════════════════

def resolve_coref(text: str, context: dict) -> str:
    """
    Co-reference Resolution v2 – giải quyết đại từ tham chiếu mạnh hơn.
    Bổ sung: "cuốn vừa quét", "cuốn trên", "hai cuốn vừa upload", "3 cuốn vừa quét"
    """
    import re as _re

    text_lower = text.lower()

    # ── OCR multi-book reference: "hai/2 cuốn vừa upload/quét"
    if _re.search(r"(hai|2)\s*(cu[oô]n|[aả]nh)\s*(v[uừ]a\s*)?(upload|qu[eé]t|nh[aậ]n d[aạ]ng|scan)", text_lower):
        ocr_hist = context.get("ocr_history", [])
        if len(ocr_hist) >= 2:
            names = " và ".join([b.get("title", "") for b in ocr_hist[-2:]])
            if names:
                text = _re.sub(
                    r"(hai|2)\s*(cu[oô]n|[aả]nh)\s*(v[uừ]a\s*)?(upload|qu[eé]t|nh[aậ]n d[aạ]ng|scan)",
                    names, text, flags=_re.IGNORECASE
                )

    # ── OCR triple-book: "3/ba cuốn vừa quét"
    if _re.search(r"(ba|3)\s*cu[oô]n\s*(v[uừ]a\s*)?(upload|qu[eé]t|nh[aậ]n d[aạ]ng|scan)", text_lower):
        ocr_hist = context.get("ocr_history", [])
        if len(ocr_hist) >= 3:
            names = ", ".join([b.get("title", "") for b in ocr_hist[-3:]])
            if names:
                text = _re.sub(
                    r"(ba|3)\s*cu[oô]n\s*(v[uừ]a\s*)?(upload|qu[eé]t|nh[aậ]n d[aạ]ng|scan)",
                    names, text, flags=_re.IGNORECASE
                )

    # ── Single OCR book reference
    OCR_TRIGGERS = [
        "cuốn vừa quét", "cuốn vừa scan", "cuốn vừa upload", "cuốn vừa nhận dạng",
        "ảnh vừa quét", "cuốn trong ảnh", "cuốn vừa tìm được", "cuốn vừa tìm",
        "cuốn trên",
    ]
    text_lower = text.lower()
    for trigger in OCR_TRIGGERS:
        if trigger in text_lower:
            ref = context.get("last_ocr_title")
            if ref:
                text = text.replace(trigger, ref)
            break

    # ── Generic coreference triggers (sách context)
    COREF_TRIGGERS = [
        "cuốn đó", "cuốn ấy", "nó", "cái đó",
        "cuốn này", "tác giả đó", "người đó", "sách đó",
        "cuốn vừa xem", "cuốn vừa tìm thấy",
    ]
    text_lower = text.lower()
    for trigger in COREF_TRIGGERS:
        if trigger in text_lower:
            ref = (
                context.get("last_ocr_title")
                or context.get("last_found_title")
                or (context.get("last_mentioned_books") or [None])[0]
            )
            if ref:
                text = text.replace(trigger, ref)
            break

    # ── "cuốn đánh giá cao nhất trong list đó" → xem last_books_list
    if _re.search(r"(cu[oô]n|s[aá]ch)\s*(n[aà]o|\s)\s*(d[aá]nh gi[aá]|rating|h[aọ]y)\s*(cao nh[aấ]t|t[oố]t nh[aấ]t)", text_lower):
        books_list = context.get("last_books_list", [])
        if books_list:
            # Tìm sách rating cao nhất trong danh sách
            best = max(books_list, key=lambda b: b.get("rating", 0), default=None)
            if best and best.get("title"):
                context["_hint_best_rated"] = best["title"]

    # ── "cuốn rẻ hơn", "cuốn rẻ nhất" → từ last_books_list
    if _re.search(r"(cu[oô]n|s[aá]ch)\s*(r[eẻ] h[oơ]n|r[eẻ] nh[aấ]t)", text_lower):
        books_list = context.get("last_books_list", [])
        if books_list:
            cheapest = min(books_list, key=lambda b: b.get("price", 999999), default=None)
            if cheapest and cheapest.get("title"):
                context["_hint_cheapest"] = cheapest["title"]

    # ── Order coreference
    ORDER_TRIGGERS = ["đơn đó", "đơn này", "đơn ấy", "đơn hàng đó"]
    text_lower = text.lower()
    for trigger in ORDER_TRIGGERS:
        if trigger in text_lower:
            last_order_id = context.get("last_order_id")
            if last_order_id:
                text = text.replace(trigger, f"đơn {last_order_id}")
            break

    return text


def update_context_with_entities(context: dict, entities: dict, intent: str,
                                  books_result: list = None) -> dict:
    """
    Cập nhật context sau mỗi turn.
    books_result: danh sách sách vừa trả về (list of dict với title/price/rating)
    """
    context["last_intent"] = intent

    if "genre" in entities:
        context.setdefault("slots", {})["genre"] = entities["genre"]
        context["last_category"] = entities["genre"]

    if "order_id" in entities:
        context["last_order_id"] = entities["order_id"]

    if "book_title" in entities:
        books = context.get("last_mentioned_books", [])
        books.insert(0, entities["book_title"])
        context["last_mentioned_books"] = books[:5]
        context["last_found_title"] = entities["book_title"]

    if "search_query" in entities:
        context["last_search_query"] = entities["search_query"]

    if intent in ("recommend_category", "book_search", "recommend_combo") and "genre" in entities:
        context["last_category"] = entities["genre"]

    # Track danh sách sách vừa gợi ý (để resolve "cuốn rẻ nhất", "đánh giá cao nhất")
    if books_result:
        context["last_books_list"] = [
            {
                "title": b.get("title", b.get("name", "")),
                "price": b.get("price", b.get("discounted_price", 0)),
                "rating": b.get("avg_rating", b.get("rating", 0)),
                "book_id": b.get("id", b.get("book_id", None)),
            }
            for b in (books_result[:8] if isinstance(books_result, list) else [])
        ]

    return context


def update_context_after_ocr(context: dict, book_info: dict) -> dict:
    """
    Cập nhật context sau khi OCR nhận dạng được sách.
    Lưu vào last_ocr_title VÀ thêm vào ocr_history để resolve multi-OCR references.
    """
    if not book_info:
        return context

    title = book_info.get("title", book_info.get("name", ""))
    if title:
        context["last_ocr_title"] = title
        context["last_found_title"] = title
        # Thêm vào last_mentioned_books
        books = context.get("last_mentioned_books", [])
        books.insert(0, title)
        context["last_mentioned_books"] = books[:5]

    # Lưu vào ocr_history (giữ tối đa 5 lần OCR gần nhất)
    ocr_hist = context.get("ocr_history", [])
    ocr_hist.append({
        "title": title,
        "price": book_info.get("price", book_info.get("discounted_price", 0)),
        "rating": book_info.get("avg_rating", book_info.get("rating", 0)),
        "book_id": book_info.get("id", book_info.get("book_id", None)),
    })
    context["ocr_history"] = ocr_hist[-5:]

    return context


def get_ocr_history(context: dict) -> list:
    """Trả về danh sách các sách đã nhận dạng qua OCR trong session."""
    return context.get("ocr_history", [])
