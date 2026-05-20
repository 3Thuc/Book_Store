"""
dialog_manager.py – Trung tâm điều phối toàn bộ logic chatbot Customer.

v3 – Nâng cấp Clarify-First System:
  1. NLU (intent + entities + sentiment) – từ intent_classifier + ner_extractor
  2. Context (session, co-reference, slot filling) – từ session_manager
  3. User Intelligence (profile + cluster → tone) – từ profile_builder
  4. Smart Router → 28 intents → đúng agent/nguồn dữ liệu
  5. Response Generation (template / Ollama LLM v2)
  6. [NEW v3] Clarify-First + Confirm-Before-Action:
     - confidence 0.52–0.65 → XÁC NHẬN intent trước khi xử lý
     - thiếu entity quan trọng → HỎI THÊM (slot-filling) thay vì đoán sai
     - sau khi user trả lời → merge context + tiếp tục xử lý bình thường
     - phòng ngừa qwen2.5:3b model nhỏ trả lời sai do thiểu context
"""
import re
import logging
import asyncio as _asyncio
import functools as _functools
from chatbot_app.nlu.customer_intent_classifier import NLUResult
from chatbot_app.nlu.ner_extractor import extract_entities
from chatbot_app.retrieval.opensearch_retriever import search_books_semantic, search_knowledge_base
from chatbot_app.retrieval.sql_retriever import (
    get_order_info, get_user_orders, get_loyalty_points, get_books_by_genre, get_book_price,
    get_discounted_books, get_voucher_info, get_all_vouchers,
)
from chatbot_app.generation.llm_client import generate
from chatbot_app.generation.dialog_utils import resolve_genre_alias, is_garbled_query, is_ocr_message
from chatbot_app.models import NavigateButton
"""
dialog_manager.py – Trung tâm điều phối toàn bộ logic chatbot Customer.

v3 – Nâng cấp Clarify-First System:
  1. NLU (intent + entities + sentiment) – từ intent_classifier + ner_extractor
  2. Context (session, co-reference, slot filling) – từ session_manager
  3. User Intelligence (profile + cluster → tone) – từ profile_builder
  4. Smart Router → 28 intents → đúng agent/nguồn dữ liệu
  5. Response Generation (template / Ollama LLM v2)
  6. [NEW v3] Clarify-First + Confirm-Before-Action:
     - confidence 0.52–0.65 → XÁC NHẬN intent trước khi xử lý
     - thiếu entity quan trọng → HỎI THÊM (slot-filling) thay vì đoán sai
     - sau khi user trả lời → merge context + tiếp tục xử lý bình thường
     - phòng ngừa qwen2.5:3b model nhỏ trả lời sai do thiểu context
"""
import re
import logging
import asyncio as _asyncio
import functools as _functools
from chatbot_app.nlu.customer_intent_classifier import NLUResult
from chatbot_app.nlu.ner_extractor import extract_entities
from chatbot_app.retrieval.opensearch_retriever import search_books_semantic, search_knowledge_base
from chatbot_app.retrieval.sql_retriever import (
    get_order_info, get_user_orders, get_loyalty_points, get_books_by_genre, get_book_price,
    get_discounted_books, get_voucher_info, get_all_vouchers, get_book_realtime,
)
from chatbot_app.generation.llm_client import generate
from chatbot_app.generation.dialog_utils import resolve_genre_alias, is_garbled_query, is_ocr_message
from chatbot_app.models import NavigateButton

_log = logging.getLogger("chatbot.dialog")

# ── OpenSearch Safe Wrappers ──────────────────────────────────────────────────
# Mục đích: Tránh crash toàn bộ request khi OpenSearch down/timeout.
# Chiến lược: try/except → log warning → fallback về MySQL (get_books_by_genre)
#             hoặc danh sách rỗng. User nhận thông báo thân thiện thay vì 500.
_OS_DOWN_MSG = (
    "⚠️ Hệ thống tìm kiếm đang bận, vui lòng thử lại sau ít phút!\n"
    "Bạn cũng có thể tìm trực tiếp tại trang **Tìm kiếm** của BookStore."
)

def _safe_search(query: str, top_k: int = 8, **kwargs) -> list:
    """
    Wrapper an toàn cho search_books_semantic().
    Trả về list rỗng (không raise) nếu OpenSearch down.
    """
    try:
        return search_books_semantic(query, top_k=top_k, **kwargs) or []
    except Exception as _e:
        _log.warning("[OpenSearch DOWN] search_books_semantic failed: %s", _e)
        # Fallback về MySQL genre search nếu có genre trong kwargs
        _genre = kwargs.get("genre")
        if _genre:
            try:
                return get_books_by_genre(_genre, limit=top_k) or []
            except Exception:
                pass
        return []


def _safe_search_kb(query: str, top_k: int = 3) -> list:
    """
    Wrapper an toàn cho search_knowledge_base().
    Trả về list rỗng (không raise) nếu OpenSearch KB index down.
    """
    try:
        return search_knowledge_base(query, top_k=top_k) or []
    except Exception as _e:
        _log.warning("[OpenSearch DOWN] search_knowledge_base failed: %s", _e)
        return []

# ── CSKH escalation message ────────────────────────────────────────────────
# FIX P0: Dùng Unicode đầy đủ - không dùng ASCII không dấu
ESCALATE_MSG = (
    "Vui lòng liên hệ đội CSKH để được hỗ trợ:\n"
    "• **Hotline:** 0353260721 (miễn phí, 8h–22h)\n"
    "• **Email:** cskh@bookstore.vn\n"
    "• Đội ngũ sẽ phản hồi trong vòng 24 giờ làm việc."
)

# ── Template responses ────────────────────────────────────────────────────────
TEMPLATES: dict[str, str] = {
    # Bảo mật / SQL injection / admin action
    "security_block": (
        "⛔ Yêu cầu này không được phép thực hiện qua chatbot.\n\n"
        "Nếu bạn gặp sự cố bảo mật hoặc cần hỗ trợ đặc biệt, vui lòng liên hệ:\n"
        "• **Hotline:** 0353260721 (8h–22h)\n"
        "• **Email:** cskh@bookstore.vn"
    ),
    # Ngoài phạm vi hỗ trợ
    "out_of_scope": (
        "Xin lỗi, yêu cầu này nằm ngoài phạm vi hỗ trợ của tôi. 😊\n\n"
        "Tôi có thể giúp bạn:\n"
        "• 🔍 Tìm sách theo tên, tác giả, thể loại\n"
        "• 📦 Tra cứu đơn hàng và trạng thái giao hàng\n"
        "• 💳 Thông tin thanh toán và khuyến mãi\n"
        "• 🔄 Chính sách đổi trả sách\n\n"
        "Bạn cần hỗ trợ gì khác không?"
    ),
    # Yêu cầu đăng nhập
    "no_auth": (
        "Bạn cần **đăng nhập** để sử dụng tính năng này.\n\n"
        "Vui lòng đăng nhập để xem thông tin cá nhân, lịch sử mua hàng và theo dõi đơn hàng."
    ),
    # Hướng dẫn tài khoản
    "account_help_guide": (
        "**Hướng dẫn quản lý tài khoản BookStore:**\n\n"
        "• **Đổi mật khẩu:** Vào *Thông tin tài khoản → Đổi mật khẩu*\n"
        "• **Cập nhật thông tin:** Vào *Thông tin tài khoản → Chỉnh sửa thông tin*\n"
        "• **Địa chỉ giao hàng:** Vào *Thông tin tài khoản → Địa chỉ*\n"
        "• **Lịch sử đơn hàng:** Vào *Thông tin tài khoản → Đơn hàng*\n\n"
        "Nếu vẫn gặp khó khăn, liên hệ CSKH: **0353260721** (8h–22h)."
    ),
    # Sách không tìm thấy
    "book_not_found": (
        "Rất tiếc, tôi không tìm thấy sách phù hợp với yêu cầu của bạn.\n\n"
        "Bạn có thể thử:\n"
        "• Kiểm tra lại tên sách hoặc tác giả\n"
        "• Tìm theo thể loại (VD: Kỹ năng sống, Văn học, Thiếu nhi...)\n"
        "• Hoặc hỏi tôi gợi ý sách tương tự 📚"
    ),
    # Chào tạm biệt
    "farewell": (
        "Tạm biệt bạn! 👋 Chúc bạn tìm được những cuốn sách ưng ý.\n"
        "BookStore luôn sẵn sàng hỗ trợ bạn bất cứ lúc nào. Hẹn gặp lại! 📖"
    ),
    # Cảm ơn
    "thanks_reply": (
        "Không có gì, rất vui được giúp bạn! 😊\n"
        "Nếu cần thêm hỗ trợ, cứ hỏi tôi nhé!"
    ),
    # Bot identity
    "ai_identity": (
        "Tôi là **Trợ lý BookStore** 🤖 – một chatbot được xây dựng để hỗ trợ bạn mua sắm sách.\n\n"
        "Tôi có thể giúp bạn tìm sách, tra đơn hàng, xem khuyến mãi và giải đáp các thắc mắc. "
        "Tôi **không phải người thật**, nhưng luôn sẵn sàng hỗ trợ bạn tốt nhất! 😊"
    ),
    # Khả năng bot
    "bot_capabilities": (
        "Tôi có thể giúp bạn:\n"
        "• 🔍 **Tìm sách** theo tên, tác giả, thể loại\n"
        "• 📦 **Tra cứu đơn hàng** và trạng thái giao hàng\n"
        "• 💳 **Thông tin thanh toán** và **khuyến mãi**\n"
        "• 🔄 **Chính sách đổi trả** sách\n"
        "• 🖼️ **Nhận diện sách qua ảnh** (OCR)\n"
        "• 📞 Hỗ trợ: **0353260721** (8h–22h)\n\n"
        "Bạn cần giúp gì hôm nay?"
    ),
    # Chào hỏi
    "greeting_guest": (
        "Xin chào! 👋 Chào mừng bạn đến với **BookStore**!\n\n"
        "Tôi có thể giúp bạn tìm sách, xem thông tin sản phẩm hoặc tư vấn mua sắm.\n"
        "Bạn đang tìm loại sách gì hôm nay? 📚"
    ),
    "greeting_member": (
        "Xin chào! 😊 Rất vui được gặp lại bạn tại **BookStore**!\n\n"
        "Hôm nay bạn muốn tìm sách mới, kiểm tra đơn hàng hay có câu hỏi gì không? 📖"
    ),
}


# ── Follow-up suggestions ────────────────────────────────────────────────────
FOLLOW_UP = {
    "book_search":         "\n\nBạn muốn: lọc theo giá, xem đánh giá chi tiết, hay tìm sách tương tự?",
    "book_detail":         "\n\nBạn muốn **thêm vào giỏ hàng** hay xem **sách cùng tác giả**?",
    "recommend_personal":  "\n\nBạn muốn gợi ý thêm hoặc lọc theo thể loại khác?",
    "recommend_gift":      "\n\nBạn muốn xem thêm lựa chọn hoặc điều chỉnh theo ngân sách?",
    "recommend_trending":  "\n\nBạn muốn xem thêm hoặc lọc theo thể loại?",
    "recommend_combo":     "\n\nBạn muốn **gợi ý thêm** hay đổi chủ đề khác?",
    "recommend_category":  "\n\nBạn muốn xem **thể loại khác** hay lọc theo giá?",
    "order_status":        "\n\nBạn muốn xem **chi tiết đơn** hoặc tra cứu **đơn hàng khác**?",
    "order_history":       "\n\nBạn muốn xem chi tiết đơn nào hoặc tìm kiếm theo tháng?",
    "promotion_current":   "",
    "book_availability":   "\n\nBạn muốn **mua ngay** hoặc xem **sách tương tự còn hàng**?",
    "return_policy":       "\n\nBạn muốn thực hiện yêu cầu đổi/trả cụ thể không?",
}

# ── Intents được escalate NGAY (không hỏi thêm) ──────────────────────────────
# complaint_damaged, complaint_wrong: giao sai/hỏng sách → cần CSKH hành động
# payment_issue: mất tiền → khẩn cấp
HARD_ESCALATE_INTENTS = {"complaint_damaged", "complaint_wrong", "payment_issue"}

# ── Intents hỏi thêm thông tin trước khi escalate ─────────────────────────────
SOFT_ESCALATE_INTENTS = {"account_help"}

# ══════════════════════════════════════════════════════════════════════════
# CLARIFY-FIRST SYSTEM (v3)
# Mục tiêu: bù đắp qwen2.5:3b yếu hơn 7b – hỏi làm rõ trước khi xử lý
# ══════════════════════════════════════════════════════════════════════════

# Ngưỡng confidence để xác nhận intent (borderline zone)
CONFIDENCE_CONFIRM_THRESHOLD = 0.65   # >= 0.65 → xử lý luôn
CONFIDENCE_SBERT_MIN         = 0.52   # < 0.52  → out_of_scope (đã xử lý sẵn)
# Vùng 0.52–0.65: hỏi xác nhận

# Intents KHÔNG cần xác nhận dù confidence thấp (vì ritual / các câu ngắn như "có", "ok")
NO_CONFIRM_INTENTS = {
    "chitchat", "confirmation_yes", "confirmation_no",
    "out_of_scope", "escalate",
    # FIX: book_compare luôn có context rõ ràng (đã gợi ý trước) → không cần confirm
    "book_compare",
    # FIX: book_detail/book_review luôn có book title rõ trong message → không cần confirm
    "book_detail", "book_review",
    # FIX: order intents — người dùng hỏi đơn hàng là rõ ràng, không cần confirm
    "order_status", "order_history", "order_cancel",
}

# Mô tả ngắn gọn của mỗi intent → dùng để hỏi xác nhận tự nhiên
INTENT_CONFIRM_DESC: dict[str, str] = {
    "book_search":         "tìm sách",
    "book_detail":         "xem thông tin chi tiết một cuốn sách",
    "book_compare":        "so sánh hai cuốn sách",
    "book_availability":   "kiểm tra tình trạng còn hàng",
    "book_review":         "xem đánh giá / rating sách",
    "recommend_personal":  "gợi ý sách phù hợp với bạn",
    "recommend_trending":  "xem sách đang bán chạy / hot",
    "recommend_gift":      "gợi ý sách làm quà tặng",
    "recommend_combo":     "gợi ý sách đọc kèm / bộ sách",
    "recommend_category":  "gợi ý sách theo thể loại",
    "order_status":        "kiểm tra trạng thái đơn hàng",
    "order_cancel":        "hủy đơn hàng",
    "order_history":       "xem lịch sử mua hàng",
    "cart_help":           "hỗ trợ về giỏ hàng",
    "payment_method":      "hỏi về phương thức thanh toán",
    "payment_issue":       "báo cáo lỗi thanh toán",
    "return_policy":       "hỏi chính sách đổi trả",
    "return_request":      "yêu cầu đổi/trả hàng",
    "complaint_damaged":   "báo sách bị hỏng/rách",
    "complaint_wrong":     "báo giao sai sách",
    "voucher_apply":       "hỏi về mã giảm giá",
    "promotion_current":   "xem khuyến mãi hiện tại",
    "loyalty_points":      "hỏi về điểm tích lũy",
    "account_help":        "hỗ trợ tài khoản",
    "store_info":          "hỏi thông tin cửa hàng",
}

# Câu hỏi slot-filling cho từng intent khi thiếu entity quan trọng
# key = intent, value = (entity_cần_kiểm_tra, câu_hỏi, quick_replies)
SLOT_FILLING_CONFIG: dict[str, tuple] = {
    "book_search": (
        "query",
        "Bạn muốn tìm sách về **chủ đề gì**, hay theo **tên tác giả/tên sách**?",
        ["Kỹ năng sống", "Văn học", "Kinh tế", "Thiếu nhi", "Tâm lý học"],
    ),
    "book_detail": (
        "book_title",
        "Bạn muốn xem thông tin cuốn sách nào? Cho tôi biết **tên sách** nhé!",
        [],
    ),
    "book_availability": (
        "book_title",
        "Bạn muốn kiểm tra tình trạng hàng của cuốn sách nào?",
        [],
    ),
    "book_compare": (
        "book_title",
        "Bạn muốn so sánh **hai cuốn sách** nào với nhau?",
        [],
    ),
    "order_status": (
        "order_id",
        "Bạn có thể cho tôi biết **mã đơn hàng** không? (Ví dụ: #12345)\n"
        "Hoặc nếu bạn đã đăng nhập, tôi có thể xem đơn gần nhất cho bạn.",
        ["Xem đơn gần nhất"],
    ),
    "order_cancel": (
        "order_id",
        "Bạn muốn hủy đơn hàng nào? Vui lòng cho tôi **mã đơn** (VD: #12345).",
        [],
    ),
    "return_request": (
        "order_id",
        "Để xử lý yêu cầu đổi/trả, bạn cho tôi biết **mã đơn hàng** cần đổi trả nhé!",
        [],
    ),
    "recommend_gift": (
        "recipient_type",
        "Bạn muốn tặng sách cho **ai**? Cho tôi biết để gợi ý phù hợp hơn nhé!",
        ["Con nhỏ (0-6 tuổi)", "Trẻ em (7-12 tuổi)", "Thiếu niên", "Bạn gái/Phụ nữ", "Bạn trai/Nam giới", "Người lớn tuổi"],
    ),
    "recommend_category": (
        "genre",
        "Chúng tôi có rất nhiều sách thuộc các lĩnh vực: Văn học, Kinh tế, Thiếu nhi, Tâm lý học, v.v. Bạn muốn khám phá thể loại nào?",
        ["Văn học", "Kinh doanh", "Kỹ năng sống", "Thiếu nhi", "Lịch sử"],
    ),
    "voucher_apply": (
        "voucher_code",
        "Bạn muốn kiểm tra **mã giảm giá** nào? Hãy nhập mã vào đây!",
        [],
    ),
}

# Quick replies hiển thị cho FE sau khi hỏi slot-filling
SLOT_QUICK_REPLIES: dict[str, list[str]] = {
    k: v[2] for k, v in SLOT_FILLING_CONFIG.items() if v[2]
}


# ── Navigate Button Helpers ───────────────────────────────────────────────────
def _make_book_buttons(books: list[dict], max_buttons: int = 4) -> list[NavigateButton]:
    """Tạo navigate buttons cho danh sách sách → link /book/{id} (khớp FE route)"""
    buttons = []
    for book in books[:max_buttons]:
        book_id = book.get("book_id") or book.get("id")
        title = book.get("title") or "Sách"
        if len(title) > 80:
            title = title[:77] + "..."
        price  = book.get("price", 0)
        rating = book.get("avg_rating") or book.get("rating", 0)
        label  = f"{title}"
        if price:
            label += f" – {price:,.0f}đ"
        if rating:
            label += f" | ★ {float(rating):.1f}"
        # Nếu không có book_id, dùng URL search theo tên sách làm fallback
        url = f"/book/{book_id}" if book_id else f"/search?q={title[:40]}"
        buttons.append(NavigateButton(
            label=label,
            url=url,
            type="book",
            metadata={"book_id": book_id, "price": price, "rating": float(rating) if rating else 0},
        ))
    return buttons


def _make_order_buttons(orders: list[dict] | None = None) -> list[NavigateButton]:
    """Tạo navigate buttons cho đơn hàng → tối đa 3 đơn gần nhất + Xem tất cả ở cuối"""
    btns = []
    if orders:
        STATUS_EMOJI = {
            "pending": "⏳", "processing": "⚙️",
            "shipped": "🚚", "delivered": "✅", "cancelled": "❌",
        }
        for o in orders[:3]:
            oid    = o.get("order_id")
            status = o.get("status", "")
            amount = o.get("total_amount", 0)
            emoji  = STATUS_EMOJI.get(status, "📦")
            btns.append(NavigateButton(
                label=f"{emoji} Xem đơn hàng #{oid}",
                url=f"/account?tab=orders&orderId={oid}",
                type="quick_reply",
                metadata={"order_id": oid, "status": status, "amount": amount},
            ))
    # "Xem tất cả" luôn ở cuối
    btns.append(NavigateButton(
        label="📦 Xem tất cả đơn hàng",
        url="/account?tab=orders",
        type="order",
    ))
    return btns




def _run_in_executor(fn, *args):
    """
    Chạy hàm SYNC trong ThreadPoolExecutor để dùng được với await.
    OpenSearch + MySQL là blocking IO → cần wrap để không block event loop.
    """
    loop = _asyncio.get_event_loop()
    return loop.run_in_executor(None, _functools.partial(fn, *args))


async def process_for_stream(
    message:      str,
    nlu_result:   NLUResult,
    user_id:      int | None,
    context:      dict,
    history:      list[dict],
    user_profile: dict,
) -> tuple:
    """
    Retrieval-Only pipeline cho streaming endpoint.

    Chạy toàn bộ NLU routing + slot-filling + data retrieval
    NHƯNG không gọi LLM generate(). Trả về:
        (stream_ctx, tone, intent_str, navigate_buttons, sources, is_template)

    - stream_ctx:    Nếu is_template=True → text hoàn chỉnh trả thẳng về FE.
                     Nếu is_template=False → context string truyền cho generate_stream().
    - is_template:   True  → template/chitchat/early-exit, không cần gen LLM.
                     False → cần streaming LLM với stream_ctx làm context.
    """
    intent    = nlu_result.intent
    entities  = extract_entities(message, intent)
    tone      = user_profile.get("tone", "thân thiện, ngắn gọn")
    sentiment = nlu_result.sentiment
    entities  = {**nlu_result.entities, **entities}
    # [DEBUG] Log entity extraction for order intents
    if intent in ("order_status", "order_cancel", "order_history"):
        print(f"[ORDER_ENTITY] intent={intent} msg='{message[:60]}' entities={entities}")

    # ====== ULTRA-EARLY OCR TOTAL/COMPARE INTERCEPTOR ======
    # Bắt ngay tại đây TRƯỚC mọi thứ khác vì NLU thường classify nhầm
    # "Tổng tiền 3 cuốn trên", "Mua cả 3 cuốn hết bao nhiêu" → out_of_scope / book_detail
    # Cần trả kết quả tính tổng từ last_ocr_books mà không để bị cướp intent
    _ultra_ocr_books = context.get("last_ocr_books", [])
    # FIX: "Cả hai" / "2 cuốn" khi chỉ có 1 OCR book → kết hợp với last_shown_books[0]
    import unicodedata as _ud_uocr
    import re as _re_uocr
    _mn_uocr = "".join(
        c for c in _ud_uocr.normalize("NFD", message.lower().replace("đ", "d"))
        if _ud_uocr.category(c) != "Mn"
    )
    _TWO_BOOK_KW = ["ca hai", "hai cuon", "2 cuon", "ca 2", "mua hai", "tong 2"]
    _TOTAL_KW_EARLY = [
        "tong tien", "tong gia", "tong cong", "tinh tong",
        "het bao nhieu", "bao nhieu tien", "tong chi phi", "tong bao nhieu",
        "tong ngan sach", "bao tien tat ca", "tong bill",
    ]
    _is_two_book_msg = any(kw in _mn_uocr for kw in _TWO_BOOK_KW)
    _is_total_early  = any(kw in _mn_uocr for kw in _TOTAL_KW_EARLY)
    if (len(_ultra_ocr_books) == 1 and _is_two_book_msg and _is_total_early):
        # Ghép OCR book + book được gợi ý gần nhất → tính tổng 2 cuốn khác nhau
        _last_shown = context.get("last_shown_books", [])
        _ocr_title  = _ultra_ocr_books[0].get("title", "")
        _extra_book = next(
            (b for b in _last_shown
             if b.get("title", "").strip().lower() != _ocr_title.strip().lower()),
            None
        )
        if _extra_book:
            _ultra_ocr_books = [_ultra_ocr_books[0], _extra_book]
            # Sync context để subset selector dưới hoạt động đúng
            context["last_ocr_books"] = _ultra_ocr_books

    if len(_ultra_ocr_books) >= 2:
        _TOTAL_KW_ULTRA = [
            "tong tien", "tong gia", "tong cong", "mua ca", "tinh tong",
            "het bao nhieu", "bao nhieu tien", "tong chi phi", "tong bao nhieu",
            "tat ca bao nhieu", "gia bao nhieu", "ca 3 cuon", "ca 2 cuon",
            "ca hai cuon", "ca ba cuon", "bao tien tat ca", "tong bill",
        ]
        _COMPARE_KW_ULTRA = [
            "cuon nao re", "cuon nao dat", "so sanh", "re nhat", "dat nhat",
            "cuon nao phu hop", "cuon nao tot", "cuon nao nen",
            # FIX: "Hai cuốn vừa quét cái nào đắt hơn" → "dat hon"/"cai nao dat"
            "dat hon", "re hon", "mac hon", "cai nao dat", "cai nao re", "cai nao mac",
            "cuon nao dat hon", "cuon nao re hon", "cuon nao mac hon",
            "cuon nao dat nhat", "cuon nao re nhat", "cuon nao mac nhat",
            "dat nhat trong", "re nhat trong", "cuon dat",
        ]
        _total_regex = _re_uocr.compile(
            r'tong\s*tien|tong\s*cong|'
            r'(?:hai|ba|bon|nam|sau|bay|tam|chin|muoi|\d+)\s*cuon.*?(?:het|gia|tong|bao\s*nhieu)'
        )
        _is_total_ultra = (
            any(kw in _mn_uocr for kw in _TOTAL_KW_ULTRA)
            or bool(_total_regex.search(_mn_uocr))
        )
        _is_compare_ultra = any(kw in _mn_uocr for kw in _COMPARE_KW_ULTRA)

        # [FIX] Guard: "so sánh cuốn 1 và cuốn 3" → user so sánh từ last_shown_books (text search)
        # KHÔNG phải so sánh OCR books → suppress _is_compare_ultra, nhường cho handler dưới
        if _is_compare_ultra and context.get("last_shown_books"):
            _OCR_ORD_WORDS = ["cuon 1", "cuon 2", "cuon 3", "cuon 4",
                              "thu 1", "thu 2", "thu 3", "thu 4",
                              "thu nhat", "thu hai", "thu ba", "thu tu"]
            _n_ordinals_in_msg = sum(1 for kw in _OCR_ORD_WORDS if kw in _mn_uocr)
            # Nếu user dùng >= 2 ordinal rõ ràng → chắc chắn hỏi về last_shown_books
            if _n_ordinals_in_msg >= 2:
                _is_compare_ultra = False

        # [FIX] Guard: "Sách nào rẻ/đắt nhất trong các cuốn vừa gợi ý" → last_shown_books
        # Khi user nhắc "vừa gợi ý/đề xuất/xem/tìm" → hỏi về last_shown_books, KHÔNG phải OCR
        _GOI_Y_CONTEXT_KW = [
            "vua goi y", "vua de xuat", "vua hien thi", "vua xem", "vua tim",
            "cuon vua goi", "sach vua goi", "trong cac cuon vua",
            "trong nhung cuon vua", "trong so nhung cuon",
        ]
        if _is_compare_ultra and context.get("last_shown_books") and any(
            kw in _mn_uocr for kw in _GOI_Y_CONTEXT_KW
        ):
            # Nhường cho book_compare handler → sẽ dùng last_shown_books
            _is_compare_ultra = False

        if _is_total_ultra or _is_compare_ultra:
            # ── SUBSET SELECTION (thứ tự ưu tiên) ──────────────────────────
            # 1. Range   : "cuốn 1 đến 4", "từ cuốn 2 đến 5"
            # 2. Ordinals: "cuốn 1, 3, 5" / "cuốn 1 3 5" / "cuốn 1 và 3"
            # 3. First-N : "5 cuốn đầu", "4 cuốn đầu tiên"
            # 4. Last-N  : "2 cuốn gần nhất", "3 cuốn cuối"
            # 5. Fallback: tất cả
            _msg_low = message.lower()
            _N = len(_ultra_ocr_books)
            _working_books = _ultra_ocr_books
            _subset_label  = f"{_N} cuốn"

            # Case 1 – Range (dùng _mn_uocr để 'đến'→'den' được match)
            _range_m = _re_uocr.search(
                r'(?:cu[o\xf4\u1ed1]n\s*)?(\d+)\s*(?:den|->|to)\s*(?:cu[o\xf4\u1ed1]n\s*)?(\d+)',
                _mn_uocr
            )
            if _range_m:
                _rs = max(0, int(_range_m.group(1)) - 1)
                _re_ = min(_N, int(_range_m.group(2)))
                if _rs < _re_:
                    _working_books = _ultra_ocr_books[_rs:_re_]
                    _subset_label  = f"cu\u1ed1n {_rs+1} \u0111\u1ebfn {_re_}"
            else:
                # Case 2 – Ordinal list
                _opfx = _re_uocr.findall(
                    r'cu[o\xf4\u1ed1]n\s*(?:s[o\xf4]\s*|th[u\u01b0]\s*)?(\d+)', _msg_low
                )
                _oaft = []
                if _opfx:
                    _raw = _re_uocr.findall(
                        r'[,]\s*(\d+)(?!\s*cu[o\xf4\u1ed1]n)'      # "1, 3, 5"
                        r'|(?:v[a\xe0]|v[o\u01a1]i|ho[a\u1eb7]c)\s*(\d+)'  # "1 và 3"
                        r'|(?<=\d)\s+(\d{1,2})(?=[\s,]|$)',          # "1 3 5" (space-sep)
                        _msg_low
                    )
                    _oaft = [g for pair in _raw for g in pair if g]
                _oall = _opfx + _oaft
                _oseen, _ouniq = set(), []
                for _ox in _oall:
                    if _ox not in _oseen:
                        _oseen.add(_ox); _ouniq.append(_ox)

                if len(_ouniq) >= 2:
                    _sidx = [int(_ox)-1 for _ox in _ouniq if 0 <= int(_ox)-1 < _N]
                    if _sidx:
                        _working_books = [_ultra_ocr_books[i] for i in _sidx]
                        _subset_label  = "cu\u1ed1n " + ", ".join(str(i+1) for i in _sidx)
                else:
                    # Case 3 – First-N
                    _fn_m = _re_uocr.search(
                        r'(\d+)\s*cu[o\xf4\u1ed1]n\s*d[a\u1ea7]u', _msg_low
                    )
                    # Case 4 – Last-N
                    _ln_m = _re_uocr.search(
                        r'(\d+)\s*cu[o\xf4\u1ed1]n\s*(?:g[a\u1ea7]n\s*nh[a\u1ea5]t|cu[o\xf4]i)',
                        _msg_low
                    )
                    if _fn_m:
                        _nf = min(int(_fn_m.group(1)), _N)
                        _working_books = _ultra_ocr_books[:_nf]
                        _subset_label  = f"{_nf} cu\u1ed1n \u0111\u1ea7u"
                    elif _ln_m:
                        _nl = min(int(_ln_m.group(1)), _N)
                        _working_books = _ultra_ocr_books[-_nl:]
                        _subset_label  = f"{_nl} cu\u1ed1n g\u1ea7n nh\u1ea5t"

            # ── Tính toán trên _working_books ───────────────────────────────
            _books_with_p = [b for b in _working_books if b.get("price") is not None]
            if _books_with_p:
                _sorted_ultra = sorted(_books_with_p, key=lambda b: float(b.get("price", 0)))
                if _is_total_ultra:
                    _total_ultra = sum(float(b.get("price", 0)) for b in _working_books)
                    _lines_u = [f"**T\u1ed5ng chi ph\xed mua {_subset_label}:**"]
                    for _i, _b in enumerate(_working_books, 1):
                        _p = float(_b.get("price", 0))
                        _lines_u.append(f"{_i}. **{_b.get('title','?')}** \u2014 {_p:,.0f}\u0111")
                    _lines_u.append(f"\n\U0001f4b0 **T\u1ed5ng c\u1ed9ng: {_total_ultra:,.0f}\u0111** ({len(_working_books)} cu\u1ed1n)")
                    context.pop("pending_slot_filling", None)
                    context.pop("pending_intent_confirm", None)
                    _ans_u = "\n".join(_lines_u)
                    return _ans_u, user_profile.get("tone", "th\xe2n thi\u1ec7n"), "image_search", [], [], True
                else:
                    _cheapest_u = _sorted_ultra[0]
                    _costliest_u = _sorted_ultra[-1]
                    _is_most_exp_u = any(kw in _mn_uocr for kw in [
                        "dat nhat", "mac nhat", "cao nhat",
                        "dat hon", "mac hon",  # FIX: "cái nào đắt hơn"
                        "cai nao dat", "cai nao mac", "cuon nao dat",
                    ])
                    _lines_u = [f"**So s\xe1nh gi\xe1 {_subset_label}:**"]
                    for _i, _b in enumerate(_working_books, 1):
                        _p = float(_b.get("price", 0))
                        if _is_most_exp_u:
                            _tag = " \u2b50 \u0110\u1eaft nh\u1ea5t" if _b.get("title") == _costliest_u.get("title") else ""
                        else:
                            _tag = " \u2b50 R\u1ebb nh\u1ea5t" if _b.get("title") == _cheapest_u.get("title") else ""
                        _lines_u.append(f"{_i}. **{_b.get('title','?')}** \u2014 {_p:,.0f}\u0111{_tag}")
                    _ref_u = _costliest_u if _is_most_exp_u else _cheapest_u
                    _lbl_u = "\u0111\u1eaft nh\u1ea5t" if _is_most_exp_u else "r\u1ebb nh\u1ea5t"
                    _lines_u.append(f"\n\u27a1\ufe0f Cu\u1ed1n {_lbl_u}: **{_ref_u.get('title','?')}** ({float(_ref_u.get('price',0)):,.0f}\u0111)")
                    context.pop("pending_slot_filling", None)
                    context.pop("pending_intent_confirm", None)
                    _ans_u = "\n".join(_lines_u)
                    return _ans_u, user_profile.get("tone", "th\xe2n thi\u1ec7n"), "image_search", [], [], True

    # ====== ULTRA-EARLY WHITELIST: known quick-reply buttons ======
    # [FIX] Các nút hệ thống như "Người lớn tuổi" bị block vì "lon" là substring của "nguoi lon tuoi"
    # Whitelist ALL known SLOT_FILLING_CONFIG quick replies → skip toàn bộ security/profanity filter
    _all_sf_qr: set = set()
    for _sfv in SLOT_FILLING_CONFIG.values():
        _all_sf_qr.update(_sfv[2])
    _is_system_qr = message.strip() in _all_sf_qr


    # ====== ULTRA-EARLY ORDER BUTTON INTERCEPTOR ======
    # Khi user click nút "⏳ Xem đơn hàng #9257" → FE gửi label này như 1 tin nhắn.
    # NLU thường classify thành order_history hoặc chitchat → không extract được order_id
    # → handler hiển thị lại list đơn thay vì chi tiết đơn cụ thể.
    # FIX: Bắt pattern "#NNNN" hoặc "Xem đơn hàng #NNNN" → force order_status + inject order_id
    import re as _re_ord_btn
    _ord_btn_match = _re_ord_btn.search(
        r'(?:xem\s+)?(?:đơn\s+hàng\s+)?#(\d{4,10})\b',
        message, _re_ord_btn.IGNORECASE
    )
    if _ord_btn_match and user_id:
        _forced_order_id = _ord_btn_match.group(1)
        intent = "order_status"
        entities["order_id"] = _forced_order_id
        nlu_result.intent = "order_status"
        nlu_result.entities["order_id"] = _forced_order_id
        nlu_result.confidence = 1.0

    # ====== ULTRA-EARLY ORDER CANCEL GUARD ======
    # Ngăn SBERT classify nhầm "chi tiết đơn hàng gần nhất" / "xem đơn" → order_cancel
    # → hộp thoại xác nhận "Bạn muốn hủy?" sai xuất hiện trước khi handler guard chạy
    # FIX: Nếu intent = order_cancel mà KHÔNG có từ khóa hủy rõ ràng → force order_status
    if intent == "order_cancel":
        import unicodedata as _ud_early_oc
        _mn_early_oc = "".join(
            c for c in _ud_early_oc.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_early_oc.category(c) != "Mn"
        )
        _CANCEL_KW_EARLY = [
            "huy don", "huy dat hang", "huy lenh", "cancel", "xoa don",
            "bo don", "khong mua nua", "huy mua", "huy thanh toan",
            "muon huy", "can huy", "cho huy", "giup huy",
        ]
        if not any(kw in _mn_early_oc for kw in _CANCEL_KW_EARLY):
            intent = "order_status"
            nlu_result.intent = "order_status"
            nlu_result.confidence = 1.0  # skip confirm dialog

    # ====== ULTRA-EARLY ORDER LOOKUP INTERCEPTOR ======
    # Bắt TẤT CẢ các câu hỏi liên quan đến đơn hàng bị SBERT classify sai intent
    # hoặc classify đúng order_status nhưng confidence borderline → confirm dialog thừa
    # FIX: Boost confidence = 1.0 nếu message khớp keyword; xử lý "gần nhất" đặc biệt
    if user_id:
        import unicodedata as _ud_ord_lu
        _mn_ord_lu = "".join(
            c for c in _ud_ord_lu.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_ord_lu.category(c) != "Mn"
        )
        _ORDER_LOOKUP_KW = [
            "tra cuu don", "don hang khac", "xem don hang", "lich su don",
            "lich su mua", "don hang cua toi", "don hang hien tai",
            "kiem tra don", "trang thai don", "don gan nhat",
            "tat ca don", "cac don hang", "xem tat ca don",
            "chi tiet don", "don moi nhat", "don gan day",
            "trang thai don hang", "xem trang thai", "don hang",
            "don hien tai", "don khac", "don vua", "don cuoi",
        ]
        _RECENT_ORDER_KW = [
            "don gan nhat", "don moi nhat", "don gan day", "don cuoi", "don moi",
            "don hang gan nhat", "don hang moi nhat", "don hang gan day",
            "don hang cuoi", "don hang moi", "don hang vua",
            "gan nhat", "moi nhat",
        ]

        _CANCEL_KW_SKIP = [
            "huy don", "huy dat hang", "huy lenh", "cancel", "xoa don",
            "bo don", "khong mua nua", "huy mua", "huy thanh toan",
            "muon huy", "can huy", "cho huy", "giup huy",
        ]
        _RETURN_KW_SKIP = [
            "muon tra hang", "can tra hang", "yeu cau tra hang",
            "cho toi tra", "giup toi tra", "toi muon tra", "toi can tra",
            "tra hang don", "tra don", "muon tra", "yeu cau tra",
        ]
        _HISTORY_KW = [
            "trong thang", "thang nay", "thang truoc",
            "lich su", "thang 1", "thang 2", "thang 3", "thang 4", "thang 5", "thang 6",
            "thang 7", "thang 8", "thang 9", "thang 10", "thang 11", "thang 12",
            "nam nay", "nam truoc", "nam ngoai", "trong nam", "nam 20",
            "tuan nay", "tuan truoc", "trong tuan",
            "hom nay", "hom qua", "ngay hom nay", "ngay hom qua", "trong ngay",
        ]
        _OCR_PRICE_KW = ["tong tien", "bao nhieu tien"]
        _has_ocr_ctx_ord = bool(context.get("last_ocr_books"))
        _OCR_COMPARE_KW_CHK = ["cuon vua quet", "cuon vua scan", "cuon vua upload",
                                "cuon vua chup", "3 cuon", "hai cuon", "ba cuon", "bon cuon",
                                "cuon vua", "vua quet"]
        _is_ocr_price_msg = (
            any(kw in _mn_ord_lu for kw in _OCR_PRICE_KW)
            and (_has_ocr_ctx_ord or any(kw in _mn_ord_lu for kw in _OCR_COMPARE_KW_CHK))
        )
        _is_cancel_msg = any(kw in _mn_ord_lu for kw in _CANCEL_KW_SKIP)
        _is_return_msg = any(kw in _mn_ord_lu for kw in _RETURN_KW_SKIP)
        _is_history_msg = (
            any(kw in _mn_ord_lu for kw in _HISTORY_KW)
            or (any(kw in _mn_ord_lu for kw in _OCR_PRICE_KW) and not _is_ocr_price_msg)
        )

        if any(kw in _mn_ord_lu for kw in _ORDER_LOOKUP_KW):
            if not _is_cancel_msg and not _is_history_msg and not _is_return_msg:
                # [FIX] Không override nếu intent đã chính xác là return_request/order_cancel
                if intent not in ("return_request", "order_cancel"):
                    intent = "order_status"
                    nlu_result.intent = "order_status"
                    nlu_result.confidence = 1.0
            if any(kw in _mn_ord_lu for kw in _RECENT_ORDER_KW) and not entities.get("order_id"):
                from chatbot_app.retrieval.sql_retriever import get_user_orders as _guo_recent
                _recent = _guo_recent(user_id, limit=1)
                if _recent:
                    entities["order_id"] = str(_recent[0]["order_id"])
                    nlu_result.entities["order_id"] = entities["order_id"]

        # -- [ORDER-ID FALLBACK] Extract 'don so XXXX'/'ma don XXXX' khi NLU miss entity --
        if not entities.get("order_id"):
            import re as _re_oid_ext
            _oid_m = _re_oid_ext.search(
                r"(?:don\s*so|so\s*don|ma\s*don|don\s*hang\s*so|#)\s*(\d{4,})",
                _mn_ord_lu
            )
            if _oid_m:
                entities["order_id"] = _oid_m.group(1)
                nlu_result.entities["order_id"] = _oid_m.group(1)

        if _is_history_msg and not _is_cancel_msg and not _is_ocr_price_msg:
            if any(kw in _mn_ord_lu for kw in ["don", "mua", "tien", "da tieu", "da thanh toan"]):
                # [FIX] Exception: spending queries phải ở lại order_status
                # "tổng tiền trong tháng", "chi bao nhiêu", "tổng chi tiêu" → KHÔNG route sang order_history
                _SPEND_KW_EARLY = [
                    "tong tien", "tong chi", "tong chi tieu", "chi tieu",
                    "chi bao nhieu", "tieu bao nhieu", "mua het bao nhieu",
                    "tong so tien", "tong thanh toan", "gia tri don hang",
                ]
                _is_spending_question = any(kw in _mn_ord_lu for kw in _SPEND_KW_EARLY)
                if not _is_spending_question:
                    intent = "order_history"
                    nlu_result.intent = "order_history"
                    nlu_result.confidence = 1.0


        _STATUS_MAP = {
            # pending
            "dang cho xu ly":   "pending",
            "dang cho":         "pending",
            "chua xu ly":       "pending",
            "cho xu ly":        "pending",
            "cho duyet":        "pending",
            # processing
            "dang xu ly":       "processing",
            "dang chuan bi":    "processing",
            # shipped / delivering
            "dang giao":        "shipped",
            "dang van chuyen":  "shipped",
            "dang ship":        "shipped",
            "dang duoc giao":   "shipped",
            "tren duong giao":  "shipped",
            # delivered
            "da nhan":          "delivered",
            "da giao thanh cong": "delivered",
            "giao thanh cong":  "delivered",
            # failed delivery - đây LÀ trạng thái hợp lệ trong DB
            "giao that bai":    "failed",
            "that bai":         "failed",
            "giao bi that bai": "failed",
            "giao khong thanh cong": "failed",
            "khong giao duoc":  "failed",
            # cancelled
            "da huy":           "cancelled",
            "bi huy":           "cancelled",
            "da bi huy":        "cancelled",
            "don huy":          "cancelled",
            # cancel_requested
            "yeu cau huy":      "cancel_requested",
            "dang cho huy":     "cancel_requested",
            "xin huy":          "cancel_requested",
            # return_requested / returned
            "yeu cau tra hang": "return_requested",
            "dang cho tra hang": "return_requested",
            "da tra hang":      "returned",
            "da hoan tra":      "returned",
            "tra hang thanh cong": "returned",
        }
        _detected_status = next(
            (v for k, v in _STATUS_MAP.items() if k in _mn_ord_lu),
            None
        )
        if _detected_status and not _is_cancel_msg:
            entities["status_filter"] = _detected_status
            nlu_result.entities["status_filter"] = _detected_status
            intent = "order_history"
            nlu_result.intent = "order_history"
            nlu_result.confidence = 1.0

        # ── [TRẢ HÀNG QUERY] Phân biệt hỏi về đơn trả vs yêu cầu trả hàng ──────
        # "Có đơn nào trả hàng không" → query, KHÔNG phải return_request
        _TRA_HANG_QUERY_KW = ["co don nao tra hang", "don tra hang", "hang da tra",
                              "tra lai hang", "co don tra", "don bi tra", "don tra",
                              "co don nao tra", "da tra hang"]
        _TRA_HANG_REQUEST_KW = ["muon tra hang", "can tra hang", "yeu cau tra",
                                "cho toi tra", "giup toi tra", "toi muon tra", "toi can tra"]
        _is_tra_hang_query   = any(kw in _mn_ord_lu for kw in _TRA_HANG_QUERY_KW)
        _is_tra_hang_request = any(kw in _mn_ord_lu for kw in _TRA_HANG_REQUEST_KW)
        if _is_tra_hang_query and not _is_tra_hang_request and not _detected_status:
            # Query: hiển thị đơn return_requested VÀ returned từ DB
            entities["status_filter"] = "return_requested|returned"
            nlu_result.entities["status_filter"] = "return_requested|returned"
            intent = "order_history"
            nlu_result.intent = "order_history"
            nlu_result.confidence = 1.0

        # Detect status keyword → inject status_filter entity + route order_history

        # -- [GIAO THAT BAI] -> Route DB voi status=failed
        # DB CO trang thai 'failed' hop le -> query don, khong tra cung
        _GIAO_THAT_BAI_KW = ["giao that bai", "giao khong thanh cong", "khong nhan duoc",
                             "giao bi loi", "giao bi that bai", "giao khong duoc",
                             "don that bai", "that bai giao"]
        if any(kw in _mn_ord_lu for kw in _GIAO_THAT_BAI_KW) and not _detected_status:
            entities["status_filter"] = "failed"
            nlu_result.entities["status_filter"] = "failed"
            intent = "order_history"
            nlu_result.intent = "order_history"
            nlu_result.confidence = 1.0

        # -- [TRA HANG QUERY] -> Route DB voi status=return_requested|returned
        # DB CO trang thai return_requested va returned hop le
        _TRA_HANG_QUERY_KW = ["co don nao tra hang", "don tra hang", "da tra hang",
                              "tra lai hang", "hang da tra", "da tra lai",
                              "don bi tra", "don tra", "co don nao tra"]
        _TRA_HANG_REQUEST_KW = ["muon tra hang", "can tra hang", "yeu cau tra",
                                "cho toi tra", "giup toi tra", "toi muon tra", "toi can tra"]
        _is_tra_hang_query   = any(kw in _mn_ord_lu for kw in _TRA_HANG_QUERY_KW)
        _is_tra_hang_request = any(kw in _mn_ord_lu for kw in _TRA_HANG_REQUEST_KW)
        _already_set_tra = entities.get("status_filter") in ("return_requested|returned", "return_requested", "returned")
        if _is_tra_hang_query and not _is_tra_hang_request and not _detected_status and not _already_set_tra:
            entities["status_filter"] = "return_requested|returned"
            nlu_result.entities["status_filter"] = "return_requested|returned"
            intent = "order_history"
            nlu_result.intent = "order_history"
            nlu_result.confidence = 1.0

        # ── [ĐƠN NÀO CÓ THỂ TRẢ] Query delivered orders + kiểm tra 7 ngày ────
        # Dùng 2-keyword AND check vì "tôi còn" có thể nằm giữa "đơn nào" và "có thể trả"
        _is_eligible_query = (
            ("don nao" in _mn_ord_lu and any(kw in _mn_ord_lu for kw in [
                "co the tra", "tra duoc", "hoan tra", "doi tra", "du dieu kien"
            ]))
            or any(kw in _mn_ord_lu for kw in [
                "don nao co the tra", "don nao tra duoc", "du dieu kien tra",
                "kiem tra dieu kien tra", "co don nao du dieu kien",
            ])
        )
        if _is_eligible_query:
            if user_id:
                from chatbot_app.retrieval.sql_retriever import get_user_orders_by_status as _guobs_ret
                import datetime as _dt_elig
                _delivered = _guobs_ret(user_id, status="delivered", limit=20)
                _eligible = []
                for _o in _delivered:
                    _ca = _o.get("created_at")
                    if _ca:
                        try:
                            _d = _ca if not isinstance(_ca, str) else _dt_elig.datetime.fromisoformat(_ca)
                            if (_dt_elig.datetime.now() - _d).days <= 7:
                                _eligible.append(_o)
                        except Exception:
                            _eligible.append(_o)  # Không parse được → giả định còn hạn
                if _eligible:
                    _lines_el = ["✅ Các đơn **đủ điều kiện đổi/trả** (giao trong 7 ngày qua):"]
                    for _o in _eligible:
                        _lines_el.append(f"  • Đơn **#{_o['order_id']}** — {_o.get('total_amount', 0):,.0f}đ (giao {str(_o.get('created_at',''))[:10]})")
                    _lines_el.append("\n📞 Liên hệ **0353260721** để yêu cầu trả hàng.")
                    # [FIX] Lưu context để coref "trả đơn đó" resolve được
                    if len(_eligible) == 1:
                        context["last_order_id"] = str(_eligible[0]["order_id"])
                    return "\n".join(_lines_el), tone, "return_request", [], ["mysql:orders"], True
                else:
                    return (
                        "ℹ️ Bạn không có đơn hàng nào đủ điều kiện đổi/trả hiện tại.\n"
                        "(Chỉ đơn đã giao trong **7 ngày** mới được đổi/trả.)",
                        tone, "return_request", [], [], True
                    )
            # Guest: hướng dẫn đăng nhập
            return "🔐 Vui lòng **đăng nhập** để kiểm tra đơn hàng đủ điều kiện đổi/trả.", tone, "return_request", [], [], True

        # ── [TRẢ ĐƠN ĐÓ - COREF] "trả đơn đó" → resolve last_order_id ────────
        _TRA_DON_DO_KW = ["tra don do", "tra don nay", "tra don vua roi", "tra don gan nhat",
                          "yeu cau tra don do", "muon tra don do", "tra hang don do"]
        if any(kw in _mn_ord_lu for kw in _TRA_DON_DO_KW):
            _coref_oid = context.get("last_order_id")
            if _coref_oid:
                entities["order_id"] = str(_coref_oid)
                nlu_result.entities["order_id"] = str(_coref_oid)
                intent = "return_request"
                nlu_result.intent = "return_request"
                nlu_result.confidence = 1.0
            # Nếu không có last_order_id → slot-fill sẽ hỏi

        # ── ["BAO GIỜ" COREF] "Bao giờ đơn đó được xử lý" → order_status ────
        _BAO_GIO_KW = [
            "bao gio", "khi nao", "bao lau nua", "may ngay nua",
            "duoc xu ly khi nao", "khi nao xu ly", "khi nao giao",
            "may khi", "can bao lau",
        ]
        _is_bao_gio = any(kw in _mn_ord_lu for kw in _BAO_GIO_KW)
        _last_ord_id = context.get("last_order_id") or entities.get("order_id")
        if _is_bao_gio and _last_ord_id and intent not in ("order_cancel", "order_history"):
            intent = "order_status"
            nlu_result.intent = "order_status"
            nlu_result.confidence = 1.0
            entities["order_id"] = str(_last_ord_id)
            nlu_result.entities["order_id"] = str(_last_ord_id)

    import re as _re_i, unicodedata as _uc_i
    def _ai(s):
        return _uc_i.normalize("NFD", s.lower().replace("đ","d").replace("Đ","d")).encode("ascii","ignore").decode()
    _mn_early = _ai(message)

    # ══ [FIX ABSOLUTE] ORDINAL PAIR → book_compare: chạy SỚM NHẤT, trước mọi interceptor ══
    # "So sánh cuốn 1 và cuốn 3" bị NLU classify sai (book_availability/book_detail)
    # → phải force book_compare TRƯỚC KHI bất kỳ handler nào chạy
    _OP_KW_LIST = ["cuon 1","thu nhat","thu 1","cuon 2","thu hai","thu 2",
                   "cuon 3","thu ba","thu 3","cuon 4","thu tu","thu 4"]
    _op_found = [o for o in _OP_KW_LIST if o in _mn_early]
    _op_nums  = {o.split()[1] if len(o.split()) > 1 else o for o in _op_found}
    if "so sanh" in _mn_early and len(_op_nums) >= 2:
        intent = "book_compare"
        nlu_result.intent = "book_compare"
        nlu_result.confidence = 0.99

    # ── [FIX] "Bìa cũ/bìa mới cái nào đang bán" → hard template với DB lookup ─
    # NLU nhầm "bìa" = hư hỏng → return_request. Thực ra là hỏi phiên bản/edition
    _BIA_EDITION_KW = ["bia cu", "bia moi", "bia cung", "bia mem", "ban cu", "ban moi",
                       "phien ban cu", "phien ban moi", "tai ban", "in lan"]
    _BIA_AVAIL_KW   = ["dang ban", "con ban", "cai nao", "loai nao", "ban nao",
                       "dang co", "con hang", "hien tai", "hien dang"]
    _has_bia_ed  = any(kw in _mn_early for kw in _BIA_EDITION_KW)
    _has_bia_av  = any(kw in _mn_early for kw in _BIA_AVAIL_KW)
    if _has_bia_ed and _has_bia_av:
        _ctx_book_bia = (
            context.get("last_best_book_title")
            or context.get("last_found_title")
            or context.get("last_recommend_title")
        )
        if _ctx_book_bia:
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection as _gc_bia
                _conn_bia = _gc_bia()
                _cur_bia = _conn_bia.cursor(dictionary=True)
                # Lấy phiên bản đang bán: ưu tiên tái bản mới nhất (title có năm cao nhất)
                _cur_bia.execute(
                    "SELECT book_id, title, price, stock_quantity, avg_rating "
                    "FROM books WHERE title LIKE %s AND status = 'active' "
                    "ORDER BY COALESCE(stock_quantity, 0) DESC, created_at DESC LIMIT 5",
                    (f"%{_ctx_book_bia[:40]}%",)
                )
                _bia_books = _cur_bia.fetchall()
                _cur_bia.close(); _conn_bia.close()
                if _bia_books:
                    _bia_in_stock  = [b for b in _bia_books if int(b.get("stock_quantity") or 0) > 0]
                    _bia_displayed = _bia_in_stock[0] if _bia_in_stock else _bia_books[0]
                    _bia_title = _bia_displayed["title"]
                    _bia_price = float(_bia_displayed["price"] or 0)
                    _bia_stock = int(_bia_displayed["stock_quantity"] or 0)
                    _bia_bid   = _bia_displayed.get("book_id", "")
                    _bia_avail = "✅ Còn hàng" if _bia_stock > 0 else "❌ Tạm hết hàng"
                    _bia_text = (
                        f"📖 Phiên bản hiện đang bán:\n\n"
                        f"**{_bia_title}** — {_bia_price:,.0f}đ | {_bia_avail}\n\n"
                        f"Nếu bạn cần phiên bản khác (bìa cũ/tái bản cũ hơn), "
                        f"vui lòng liên hệ hotline **0353260721** (8h–22h) để được kiểm tra thêm."
                    )
                    _bia_btns = [NavigateButton(
                        label=f"{_bia_title[:55]}{'...' if len(_bia_title)>55 else ''} – {_bia_price:,.0f}đ",
                        url=f"/book/{_bia_bid}" if _bia_bid else "",
                        type="book",
                        metadata={"book_id": _bia_bid, "price": _bia_price}
                    )]
                    return _bia_text, tone, "book_availability", _bia_btns, ["mysql:books"], True
            except Exception as _e_bia:
                import logging as _lg_bia
                _lg_bia.getLogger(__name__).warning("bia_edition_interceptor: %s", _e_bia)
        # Fallback: không có context book → redirect sang book_availability thường
        intent = "book_availability"
        nlu_result.intent = "book_availability"
        nlu_result.confidence = 1.0

    # ── [FIX] "X là thể loại gì / X thuộc thể loại nào" → book_detail trực tiếp ─
    # NLU classify book_detail confidence thấp → Clarify-First dialog sai
    import re as _re_genre_q
    _GENRE_Q_KW = ["la the loai gi", "thuoc the loai", "the loai gi", "kieu sach gi",
                   "dang sach gi", "loai sach gi", "noi ve gi", "viet ve gi", "noi dung gi"]
    _is_genre_question = any(kw in _mn_early for kw in _GENRE_Q_KW)
    if _is_genre_question:
        # [FIX] Tìm ranh giới "là/thuộc" trong _mn_early → cắt từ message gốc theo index token
        _gq_tokens_norm = _mn_early.split()
        _gq_tokens_orig = message.strip().split()
        # Tìm index token đầu tiên là "la" hoặc "thuoc" → tên sách là các token trước đó
        _gq_split_idx = None
        for _gi, _gw in enumerate(_gq_tokens_norm):
            if _gw in ("la", "thuoc") and _gi > 0:
                _gq_split_idx = _gi
                break
        if _gq_split_idx is not None and _gq_split_idx <= len(_gq_tokens_orig):
            _gq_parts = _gq_tokens_orig[:_gq_split_idx]
            # Strip prefix "Cuốn/Cuon/Sách/Quyển/Quyen/Cuon" không phân biệt hoa thường
            if _gq_parts and _gq_parts[0].lower() in (
                "cu\u1ed1n", "cu\u00f4n", "s\u00e1ch", "sach", "quy\u1ec3n", "quyen", "cuon"
            ):
                _gq_parts = _gq_parts[1:]
            _gq_title = " ".join(_gq_parts).strip()
            if len(_gq_title) >= 3:
                intent = "book_detail"
                nlu_result.intent = "book_detail"
                nlu_result.confidence = 1.0
                nlu_result.entities["book_title"] = _gq_title
                entities["book_title"] = _gq_title

    # Khi user click button "Xem sách của Nguyễn Nhật Ánh", NLU classify out_of_scope
    # → force book_search để author search interceptor xử lý đúng
    import re as _re_btn_author
    _match_btn_author = _re_btn_author.match(
        r'(?:xem|tim|xem them)\s+s[aá]ch\s+c[uủ]a\s+(.+)',
        message.strip().lower()
    )
    if _match_btn_author:
        intent = "book_search"
        nlu_result.intent = "book_search"
        nlu_result.confidence = 1.0
        nlu_result.entities["query"] = _match_btn_author.group(1).strip()

    # ── [FIX] BUTTON QUICK-REPLY: "Xem chi tiết: X" → book_detail ───────────────────
    # Khi user click button "Xem chi tiết: Tên Sách" → NLU classify out_of_scope
    import re as _re_btn_detail
    _match_btn_detail = _re_btn_detail.match(
        r'xem\s+chi\s+tiết[:\s]+(.+)',
        message.strip(),
        _re_btn_detail.IGNORECASE
    )
    if _match_btn_detail:
        _btn_title = _match_btn_detail.group(1).strip().rstrip('.')
        # Nếu tiêu đề bị cắt (kết thúc ...) → dùng last_found_title từ context
        if _btn_title.endswith('..') or _btn_title.endswith('...'):
            _btn_title = (
                context.get("last_best_book_title")
                or context.get("last_found_title")
                or _btn_title.rstrip('.')
            )
        intent = "book_detail"
        nlu_result.intent = "book_detail"
        nlu_result.confidence = 1.0
        nlu_result.entities["book_title"] = _btn_title

    # ── [FIX] TRENDING BESTSELLER: "Sách bán chạy nhất tháng này/hiện nay" ─────
    # OpenSearch text-match sai với "Sách bán chạy nhất tháng này là gì"
    # → phải dùng MySQL ORDER BY rating/stock làm proxy bestseller
    _TRENDING_KW   = ["ban chay nhat", "noi tieng nhat", "pho bien nhat", "nhieu nguoi mua nhat",
                      "duoc mua nhieu", "sach hay nhat", "top sach", "xep hang"]
    _TRENDING_SCOPE = ["thang nay", "thang vua", "thang truoc", "tuan nay", "tuan truoc",
                       "hom nay", "hien nay", "hien tai", "gan day", "dang hot",
                       "dang ban chay", "dang pho bien", "hien hanh"]
    _is_trending_q  = any(kw in _mn_early for kw in _TRENDING_KW)
    _is_time_scope  = any(kw in _mn_early for kw in _TRENDING_SCOPE)
    # Không fire khi đã có author pronoun (đó là câu hỏi về tác giả cụ thể)
    _has_any_author_ref = any(kw in _mn_early for kw in
                              ["cua ong", "cua ba", "tac gia nay", "cua ho",
                               "cua nguoi", "tac pham cua"])
    if _is_trending_q and _is_time_scope and not _has_any_author_ref:
        try:
            from chatbot_app.retrieval.sql_retriever import get_connection as _gc_tr
            _conn_tr = _gc_tr()
            _cur_tr = _conn_tr.cursor(dictionary=True)
            _cur_tr.execute(
                "SELECT b.book_id, b.title, b.price, b.avg_rating, b.stock_quantity, "
                "       a.author_name AS author "
                "FROM books b LEFT JOIN authors a ON b.author_id = a.author_id "
                "WHERE b.status = 'active' AND b.stock_quantity > 0 "
                "ORDER BY b.avg_rating DESC, b.stock_quantity DESC LIMIT 4"
            )
            _tr_books = _cur_tr.fetchall()
            _cur_tr.close(); _conn_tr.close()
            if _tr_books:
                _tr_lines = "\n".join(
                    f"{i+1}. **{b['title']}** – {float(b['price'] or 0):,.0f}đ | ★ {float(b['avg_rating'] or 0):.1f}"
                    for i, b in enumerate(_tr_books)
                )
                _tr_text = (
                    f"🔥 Top sách được yêu thích nhất hiện nay tại BookStore:\n\n"
                    f"{_tr_lines}\n\n"
                    f"Bạn muốn xem chi tiết, lọc theo thể loại, hay tìm sách tương tự?"
                )
                _tr_btns = _make_book_buttons(_tr_books)
                return _tr_text, tone, "recommend_trending", _tr_btns, ["mysql:books"], True
        except Exception as _e_tr:
            import logging as _lg_tr
            _lg_tr.getLogger(__name__).warning("trending_interceptor: %s", _e_tr)

    # ── [FIX] CATEGORY COUNT: "Shop có bao nhiêu thể loại / danh mục" ────────────
    # NLU/OpenSearch match sách có "bao nhiêu" trong tên → book_compare sai
    _CAT_COUNT_KW = ["bao nhieu the loai", "co bao nhieu the loai", "tong cong bao nhieu the loai",
                     "bao nhieu danh muc", "co bao nhieu danh muc", "bao nhieu loai sach",
                     "bao nhieu chu de", "the loai nao co", "co nhung the loai gi",
                     "nhung the loai gi", "cac the loai gi", "loai sach gi co",
                     "kho co the loai", "shop co the loai", "cua hang co the loai"]
    _is_cat_count = any(kw in _mn_early for kw in _CAT_COUNT_KW)
    if _is_cat_count:
        try:
            from chatbot_app.retrieval.sql_retriever import get_connection as _gc_cat
            _conn_cat = _gc_cat()
            _cur_cat = _conn_cat.cursor(dictionary=True)
            _cur_cat.execute(
                "SELECT c.category_name, COUNT(bc.book_id) AS book_count "
                "FROM categories c "
                "LEFT JOIN book_categories bc ON bc.category_id = c.category_id "
                "LEFT JOIN books b ON bc.book_id = b.book_id AND b.status = 'active' "
                "GROUP BY c.category_id, c.category_name "
                "HAVING book_count > 0 "
                "ORDER BY book_count DESC"
            )
            _cat_rows = _cur_cat.fetchall()
            _cur_cat.close(); _conn_cat.close()
            if _cat_rows:
                _cat_total = len(_cat_rows)
                _cat_top = _cat_rows[:8]
                _cat_lines = "\n".join(
                    f"• **{r['category_name']}** ({r['book_count']} đầu sách)"
                    for r in _cat_top
                )
                _more = f"\n*...và {_cat_total - 8} thể loại khác*" if _cat_total > 8 else ""
                _cat_text = (
                    f"📚 BookStore hiện có **{_cat_total} thể loại** sách:\n\n"
                    f"{_cat_lines}{_more}\n\n"
                    f"Bạn muốn xem sách theo thể loại nào?"
                )
                _cat_btns = [
                    NavigateButton(
                        label=r['category_name'],
                        url=f"/search/{r['category_name']}",
                        type="page"
                    )
                    for r in _cat_top[:4]
                ]
                return _cat_text, tone, "recommend_category", _cat_btns, ["mysql:categories"], True
        except Exception as _e_cat:
            import logging as _lg_cat
            _lg_cat.getLogger(__name__).warning("category_count_interceptor: %s", _e_cat)

    # ── [FIX] "Xem thêm sách kỹ năng sống / Cho tôi xem sách [thể loại]" → recommend_category ──
    # Coref hay inject book_title từ context vào → book_detail handler bắt nhầm
    _BROWSE_GENRE_KW = ["xem them sach", "cho toi xem sach", "them sach", "xem sach the loai",
                        "tim them sach", "co sach nao khac", "sach the loai khac",
                        "gioi thieu them", "muon xem them", "xem tiep", "xem them cuon"]
    _is_browse_genre = any(kw in _mn_early for kw in _BROWSE_GENRE_KW)
    if _is_browse_genre:
        # Try extract genre from message
        _browse_genre = resolve_genre_alias(message)
        if not _browse_genre:
            # Fallback: try extracting any known genre keyword from normalized message
            _KNOWN_GENRES = {
                "ky nang song": "Kỹ năng sống", "phat trien ban than": "Kỹ năng sống",
                "van hoc": "Văn học", "tieu thuyet": "Văn học - Tiểu thuyết",
                "thieu nhi": "Sách thiếu nhi", "kinh te": "Kinh tế - Quản lý",
                "tam ly": "Tâm lý học", "khoa hoc": "Khoa học",
                "lich su": "Lịch sử", "giao khoa": "Giáo khoa - Tham khảo",
                "nuoi day con": "Nuôi dạy con", "truyen tranh": "Truyện tranh",
                "hoi ky": "Hồi ký - Tự truyện", "ton giao": "Tôn giáo - Tâm linh",
            }
            _browse_genre = next((v for k, v in _KNOWN_GENRES.items() if k in _mn_early), None)
        if _browse_genre:
            intent = "recommend_category"
            nlu_result.intent = "recommend_category"
            nlu_result.confidence = 1.0
            entities["genre"] = _browse_genre
            entities.pop("book_title", None)  # Clear injected book_title
            nlu_result.entities.pop("book_title", None)

    # Resolve "ông/bà/tác giả này" từ context → query DB lấy sách đánh giá cao nhất
    _BEST_BOOK_KW = ["noi tieng nhat", "best seller", "ban chay nhat", "duoc yeu thich nhat",
                     "danh gia cao nhat", "hay nhat", "noi bat nhat", "dinh nhat", "kinh dien nhat",
                     "tac pham noi tieng", "sach noi tieng", "cuon noi tieng"]
    _AUTHOR_REF_KW = [
        "ong", "ba", "chi", "anh", "co", "thay", "chu", "bac",
        "tac gia nay", "tac gia do", "tac gia tren",
        "ho", "nguoi nay", "nguoi do",
        "cua ong", "cua ba", "cua chi", "cua anh", "cua co", "cua thay",
        "cua chu", "cua bac", "cua nguoi", "cua ho",
    ]
    _is_best_book = any(kw in _mn_early for kw in _BEST_BOOK_KW)
    _has_author_ref = any(kw in _mn_early for kw in _AUTHOR_REF_KW)
    # [FIX] Guard: không fire khi query là bestseller toàn hệ thống (không phải của tác giả cụ thể)
    _GLOBAL_SCOPE_KW = ["thang nay", "thang vua", "tuan nay", "hom nay", "hien nay",
                        "toan he thong", "tren ca nuoc", "noi chung", "nhin chung",
                        "tat ca", "trong nuoc", "the gioi", "amazon", "goodreads"]
    _is_global_scope = any(kw in _mn_early for kw in _GLOBAL_SCOPE_KW)
    # [FIX] Guard: "anh"/"chi"/"co" là substring rất phổ biến → chỉ chấp nhận khi
    # (a) dùng dạng sở hữu "cua chi/cua anh/cua co" HOẶC
    # (b) standalone "ong"/"ba"/"thay"/"chu"/"bac" (ít ambiguous hơn)
    _POSSESSIVE_PRONOUNS = ["cua chi", "cua anh", "cua co", "cua thay", "cua chu", "cua bac"]
    _SAFE_PRONOUNS       = ["ong", "ba", "thay", "chu", "bac", "tac gia nay", "tac gia do",
                            "tac gia tren", "ho", "nguoi nay", "nguoi do", "cua ong", "cua ba",
                            "cua nguoi", "cua ho"]
    _has_author_ref = (
        any(kw in _mn_early for kw in _POSSESSIVE_PRONOUNS)  # "của chị/anh/cô" rõ ràng
        or any(kw in _mn_early for kw in _SAFE_PRONOUNS)     # pronoun ít ambiguous
    )
    # Chỉ fire khi: có pronoun tác giả RÕ RÀNG VÀ không phải query global scope VÀ có author trong ctx
    if _is_best_book and _has_author_ref and not _is_global_scope:
        _ctx_best_author = (
            context.get("last_author_name")
            or (context.get("last_shown_books", [{}])[0].get("author_name")
                or context.get("last_shown_books", [{}])[0].get("author")
                if context.get("last_shown_books") else None)
        )
        if _ctx_best_author:
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection as _gc_best
                _conn_best = _gc_best()
                _cur_best = _conn_best.cursor(dictionary=True)
                _cur_best.execute(
                    "SELECT b.book_id, b.title, b.price, b.avg_rating, b.stock_quantity "
                    "FROM books b JOIN authors a ON b.author_id = a.author_id "
                    "WHERE a.author_name LIKE %s AND b.status = 'active' "
                    "ORDER BY b.avg_rating DESC, b.stock_quantity DESC LIMIT 1",
                    (f"%{_ctx_best_author}%",)
                )
                _best_book = _cur_best.fetchone()
                _cur_best.close(); _conn_best.close()
                if _best_book:
                    _bt = _best_book["title"]
                    _bid = _best_book.get("book_id", "")
                    _bp = float(_best_book["price"] or 0)
                    _br = float(_best_book["avg_rating"] or 0)
                    _star = f" | ★ {_br:.1f}" if _br else ""
                    _best_text = (
                        f"⭐ Cuốn sách được đánh giá cao nhất của **{_ctx_best_author}** trong kho:\n\n"
                        f"**{_bt}** – {_bp:,.0f}đ{_star}\n\n"
                        f"Bạn muốn xem chi tiết, thêm vào giỏ hàng, hay tìm sách cùng thể loại?"
                    )
                    # [FIX] Dùng type="book" với URL thực → FE navigate thẳng đến trang sản phẩm
                    _bt_label = _bt if len(_bt) <= 60 else _bt[:57] + "..."
                    _best_btns = [
                        NavigateButton(
                            label=f"{_bt_label} – {_bp:,.0f}đ | ★ {_br:.1f}",
                            url=f"/book/{_bid}" if _bid else "",
                            type="book",
                            metadata={"book_id": _bid, "title": _bt, "price": _bp}
                        )
                    ]
                    context["last_best_book_title"] = _bt
                    context["last_found_title"] = _bt
                    return _best_text, tone, "book_detail", _best_btns, ["mysql:books"], True

            except Exception as _e_best:
                import logging as _lg_best
                _lg_best.getLogger(__name__).warning("author_best_book_interceptor: %s", _e_best)

    # ── [FIX] AUTHOR COUNT INTERCEPTOR: trả về số sách thực từ DB, không để LLM bịa ──
    # "Tác giả này viết bao nhiêu cuốn rồi" → NLU thường classify nhầm book_detail/book_search
    _AUTHOR_COUNT_KW_EARLY = ["viet bao nhieu", "bao nhieu cuon", "bao nhieu sach",
                               "co bao nhieu", "da viet bao nhieu", "tac pham bao nhieu",
                               "bao nhieu tac pham", "tong cong bao nhieu"]
    _is_author_count_early = any(kw in _mn_early for kw in _AUTHOR_COUNT_KW_EARLY)
    if _is_author_count_early:
        # [FIX] last_shown_books dùng cả "author" lẫn "author_name" tùy SQL query
        _lsb_early = context.get("last_shown_books", [])
        _lsb0_early = _lsb_early[0] if _lsb_early else {}
        _ctx_author_early = (
            context.get("last_author_name")
            or _lsb0_early.get("author_name")
            or _lsb0_early.get("author")
        )
        if _ctx_author_early:
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection as _gc_early
                _conn_early_ac = _gc_early()
                _cur_early_ac = _conn_early_ac.cursor(dictionary=True)
                _cur_early_ac.execute(
                    "SELECT COUNT(*) AS cnt FROM books b "
                    "JOIN authors a ON b.author_id = a.author_id "
                    "WHERE a.author_name LIKE %s AND b.status = 'active'",
                    (f"%{_ctx_author_early}%",)
                )
                _cnt_early = _cur_early_ac.fetchone()
                _cur_early_ac.close(); _conn_early_ac.close()
                _book_cnt = _cnt_early["cnt"] if _cnt_early else 0
                _ac_text = (
                    f"📖 Tác giả **{_ctx_author_early}** hiện có **{_book_cnt} đầu sách** trong kho của BookStore.\n\n"
                    f"Bạn muốn xem danh sách, lọc theo giá, hay tìm thể loại cụ thể?"
                )
                _ac_btns = [NavigateButton(label=f"Xem sách của {_ctx_author_early}", url="", type="quick_reply")]
                # process_for_stream phải return 6-tuple: (stream_ctx, tone, intent_str, navigate_buttons, sources, is_template)
                return _ac_text, tone, "book_search", _ac_btns, ["mysql:books"], True
            except Exception:
                pass

    # ── [FIX] GUEST PURCHASE QUERY INTERCEPTOR ──────────────────────────────────

    # "Tôi chưa đăng nhập thì có mua được không?" → NLU thường classify nhầm thành
    # promotion_current / checkout. Cần trả về câu trả lời trực tiếp về khả năng mua hàng khách.
    _GUEST_NOT_LOGIN_KW = ["chua dang nhap", "khong dang nhap", "chua co tai khoan",
                           "khong co tai khoan", "chua login", "khong login"]
    _GUEST_CAN_BUY_KW  = ["mua duoc khong", "co mua duoc", "mua hang duoc khong",
                           "dat hang duoc khong", "co dat duoc", "co the mua",
                           "co the dat", "mua dc khong", "dat dc khong",
                           "mua hang khong", "dat hang khong"]
    _has_not_login = any(kw in _mn_early for kw in _GUEST_NOT_LOGIN_KW)
    _has_buy_query = any(kw in _mn_early for kw in _GUEST_CAN_BUY_KW)

    if _has_not_login and _has_buy_query:
        return (
            "**Mua hàng khi chưa đăng nhập:**\n\n"
            "• ✅ **Có thể:** Xem sách, tìm kiếm\n"
            "• ❌ **Cần đăng nhập:** Đặt hàng, thanh toán, theo dõi đơn hàng, áp mã khuyến mãi\n\n"
            "💡 **Gợi ý:** Đăng ký tài khoản miễn phí để đặt hàng nhanh hơn, "
            "theo dõi đơn dễ dàng!\n\n"
            "Bạn muốn **đăng nhập** hoặc **đăng ký** ngay không?",
            tone, "account_help",
            [NavigateButton(label="🔑 Đăng nhập", url="/login", type="page"),
             NavigateButton(label="📝 Đăng ký tài khoản", url="/login?tab=register", type="page")],
            [], True
        )

    # ── [FIX] PAYMENT METHOD INTERCEPTOR (COD, chuyển khoản, thẻ) ─────────────────────
    # NLU thường classify nhầm "thanh toán COD" thành out_of_scope
    _PAYMENT_KW = ["cod", "tien mat", "chuyen khoan", "the tin dung", "the ngan hang",
                   "vi dien tu", "momo", "vnpay", "zalopay", "banking",
                   "thanh toan online", "tra tien mat", "payment"]
    _has_payment_kw = any(kw in _mn_early for kw in _PAYMENT_KW)
    if _has_payment_kw and intent in ("out_of_scope", "chitchat", "book_search", "recommend_gift"):
        intent = "payment_method"
        nlu_result.intent = "payment_method"
        nlu_result.confidence = 0.95

    if intent in ("chitchat", "greet", "out_of_scope"):

        import re as _re_greet_strip
        _GREET_PREFIX_RE = _re_greet_strip.compile(
            r'^(xin\s+ch[àa]o|ch[àa]o\s*(b[ạa]n|c[ảa]c\s+b[ạa]n|anh|ch[ịi]|em)?'
            r'|hi|hello|hey|good\s+(morning|afternoon|evening))'
            r'[\s,!.]*'
            # Tiếp tục strip filler words sau lời chào: "hãy", "vui lòng", "làm ơn", "có thể", "giúp tôi"
            r'(h[ãa]y\s+|vui\s+l[òo]ng\s+|l[àa]m\s+[ơo]n\s+|c[óo]\s+th[ểe]\s+|gi[úu]p\s+t[ôo]i\s+)*',
            _re_greet_strip.IGNORECASE
        )
        _stripped_msg = _re_greet_strip.sub(_GREET_PREFIX_RE, '', message).strip()
        if _stripped_msg and len(_stripped_msg) > 5:
            _ACTION_KW_STRIP = [
                "don hang", "dat hang", "kiem tra", "trang thai", "xem don",
                "huy don", "tra hang", "hoan tien",
                "tim sach", "tim kiem", "goi y", "tuong tu",
                "gia", "bao nhieu", "con hang", "het hang",
                "voucher", "ma giam gia", "khuyen mai",
                "diem", "tich luy", "tai khoan",
                "sach", "mua", "gio hang", "thanh toan",
            ]
            _mn_stripped = _ai(_stripped_msg)
            if any(kw in _mn_stripped for kw in _ACTION_KW_STRIP):
                from chatbot_app.nlu.customer_intent_classifier import detect_intent as _redetect
                _redetected = _redetect(_stripped_msg)
                if _redetected.intent not in ("chitchat", "greet", "out_of_scope"):
                    intent = _redetected.intent
                    nlu_result.intent = _redetected.intent
                    nlu_result.confidence = _redetected.confidence
                    _reentities = extract_entities(_stripped_msg, intent)
                    entities = {**entities, **_reentities, **_redetected.entities}
                    nlu_result.entities = {**nlu_result.entities, **_redetected.entities}

    # ====== ULTRA-EARLY "XEM THÊM SÁCH" INTERCEPTOR ======
    # Khi user gõ "Các sách khác", "Xem thêm", "Sách khác cùng thể loại"...
    # → kiểm tra context có last_genre/last_category không
    # → nếu có, inject genre vào entities và force recommend_category
    _SEE_MORE_KW = [
        "sach khac", "cac sach khac", "xem them", "them cuon",
        "xem tiep", "cuon khac", "cung the loai",
        "sach khac cung", "go y them", "goi y them", "tim them",
        "the loai nay", "the loai do", "cung loai", "sach cung the loai",
        # Thêm các pattern tiếng Việt tự nhiên
        "con cuon nao khac", "con sach nao khac", "co cuon nao khac", "co sach nao khac",
        "cuon nao nua", "sach nao nua", "cuon nao khac", "sach nao khac",
        "co them cuon", "co them sach", "them sach nao", "them cuon nao",
        "cuon khac khong", "sach khac khong", "cuon nao them", "sach nao them",
        "con cuon nao", "con sach nao",
        # FIX: "Còn thể loại nào khác không" → recommend_category
        "con the loai nao", "the loai nao khac", "the loai nao nua",
        "con loai nao khac", "co the loai nao khac", "the loai khac khong",
    ]
    # Keywords cho "sách tương tự" → dùng semantic search theo title
    _SIMILAR_KW = [
        "tuong tu", "sach tuong tu", "giong vay", "giong nhu", "giong the",
        "cung chu de", "sach tuong tu nhu vay", "nhu the nay",
        "tim sach tuong tu", "goi y sach tuong tu",
    ]
    _last_genre_ctx = context.get("last_genre") or context.get("last_category", "")
    _last_title_ctx = context.get("last_found_title", "")

    # Interceptor 1: Sách tương tự → semantic search theo title (hybrid)
    if any(kw in _mn_early for kw in _SIMILAR_KW) and (_last_title_ctx or _last_genre_ctx):
        intent = "recommend_category"
        nlu_result.intent = "recommend_category"
        nlu_result.confidence = 1.0
        # [FIX] Ưu tiên genre khi có trong context — semantic title search chỉ là fallback
        # VD: sau khi xem "Xuyên Không..." (genre="Xuyên không - Trọng sinh")
        #     → "Có sách nào tương tự không" → nên trả sách cùng genre, không match "Làm Giàu"
        if _last_genre_ctx:
            # Ưu tiên 1: genre-based — chính xác nhất
            entities["genre"] = _last_genre_ctx
            nlu_result.entities["genre"] = _last_genre_ctx
            context.pop("_find_similar_to", None)
        elif _last_title_ctx:
            # Fallback: không có genre → semantic search theo title
            context["_find_similar_to"] = _last_title_ctx
            entities.pop("genre", None)
            nlu_result.entities.pop("genre", None)


    # Interceptor 2: Xem thêm cùng thể loại → recommend_category theo genre
    elif _last_genre_ctx and any(kw in _mn_early for kw in _SEE_MORE_KW):
        intent = "recommend_category"
        nlu_result.intent = "recommend_category"
        nlu_result.confidence = 1.0
        entities["genre"] = _last_genre_ctx
        nlu_result.entities["genre"] = _last_genre_ctx

    # Interceptor 3: "Đổi thể loại khác" → hỏi user muốn thể loại gì (show genre picker)
    _CHANGE_GENRE_KW = [
        "doi the loai", "the loai khac", "the loai khac nha", "muon xem the loai",
        "tim the loai khac", "doi sang the loai", "that ra muon xem",
        "chon the loai khac", "doi thanh the loai khac",
        # FIX: "Còn thể loại nào khác không" không có last_genre_ctx → show genre picker
        "con the loai nao khac", "co the loai nao khac", "the loai nao nua",
    ]
    if any(kw in _mn_early for kw in _CHANGE_GENRE_KW):
        # Route sang recommend_category với _change_genre flag
        # để handler dòng ~1833 hiển thị genre picker (thay vì recommend_personal)
        intent = "recommend_category"
        nlu_result.intent = "recommend_category"
        nlu_result.confidence = 1.0
        # Đánh dấu đây là "Đổi thể loại" để handler biết cần clear + show picker
        context["_force_genre_picker"] = True
        # Xóa genre cũ để không reuse
        entities.pop("genre", None)
        nlu_result.entities.pop("genre", None)

    # Interceptor 4: "Ngân sách khoảng X" / "Giá dưới X" sau khi xem gợi ý
    # → book_search với price_max filter theo genre hiện tại
    import re as _re_budget
    _BUDGET_KW = [
        "ngan sach", "tam gia", "gia duoi", "duoi", "khoang", "trong khoang",
        "chi phi", "muc gia", "phi", "bao nhieu", "trong tam", "voi muc",
    ]
    _budget_has_kw = any(kw in _mn_early for kw in _BUDGET_KW)
    _budget_has_num = bool(_re_budget.search(r'\d', _mn_early))
    _budget_match = _re_budget.search(
        r'(\d[\d.,]*)\s*(ngan|k\b|000|trieu|tr\b)',
        _mn_early, _re_budget.IGNORECASE
    )
    if _budget_has_kw and _budget_has_num and (_last_genre_ctx or context.get("last_shown_books")):
        _price_max = None
        if _budget_match:
            _raw_num = float(_budget_match.group(1).replace(",", "").replace(".", ""))
            _unit = _budget_match.group(2).lower()
            if _unit in ("ngan", "k"):
                _price_max = _raw_num * 1000
            elif _unit in ("000",):
                _price_max = _raw_num * 1000
            elif _unit in ("trieu", "tr"):
                _price_max = _raw_num * 1_000_000
            else:
                _price_max = _raw_num
        else:
            # Thử match số thuần (VD: "200000")
            _plain_match = _re_budget.search(r'(\d{4,})', _mn_early)
            if _plain_match:
                _price_max = float(_plain_match.group(1))
        if _price_max and _price_max > 0:
            intent = "book_search"
            nlu_result.intent = "book_search"
            nlu_result.confidence = 1.0
            entities["price_max"] = int(_price_max)
            nlu_result.entities["price_max"] = int(_price_max)
            _budget_query = _last_genre_ctx or context.get("last_search_query", "sách hay")
            entities["query"] = _budget_query
            nlu_result.entities["query"] = _budget_query

    # Interceptor 5: "Ship về X mất mấy ngày" / "Giao hàng đến X bao lâu" → shipping_info
    # Phải đặt TRƯỚC security block vì có thể bị mis-classify là recommend_gift / out_of_scope
    _SHIP_DETECT_KW = [
        "ship", "giao hang", "mat may ngay", "bao lau giao", "van chuyen",
        "giao den", "nhan hang", "thoi gian giao", "giao bao lau", "bao nhieu ngay giao",
        "giao hang toi", "ship toi", "giao ve", "ship ve",
    ]
    _CITY_KW_SHIP = [
        "ha noi", "ho chi minh", "hcm", "tp hcm", "sai gon", "da nang", "hue",
        "can tho", "hai phong", "nha trang", "vung tau", "bien hoa",
        "quy nhon", "dong nai", "binh duong", "long an", "ba ria",
    ]
    _has_ship_kw = any(kw in _mn_early for kw in _SHIP_DETECT_KW)
    _has_city_kw = any(kw in _mn_early for kw in _CITY_KW_SHIP)
    if _has_ship_kw or _has_city_kw:
        if _has_ship_kw:  # ship keyword là đủ để route shipping_info
            intent = "shipping_info"
            nlu_result.intent = "shipping_info"
            nlu_result.confidence = 1.0

    # Interceptor 6: "So sánh cuốn trong ảnh với cuốn thứ N bạn gợi ý"
    # Resolve book1 = OCR/last_found_title, book2 = last_shown_books[ordinal]
    # Bypass Clarify-First → force confidence=1.0
    _CMP_KW = ["so sanh", "so voi", "khac gi", "cuon nao tot hon", "chon cuon nao"]
    _CMP_OCR_KW = ["cuon trong anh", "cuon vua quet", "cuon vua scan", "anh vua gui", "cuon trong hinh"]
    _CMP_PREV_KW = [
        "cuon dau tien", "thu nhat", "cuon thu nhat", "cuon ban goi y",
        "cuon dau", "cuon 1", "thu 1", "cuon thu 2", "thu hai", "cuon 2",
    ]
    _has_cmp_kw    = any(kw in _mn_early for kw in _CMP_KW)
    _has_ocr_ref   = any(kw in _mn_early for kw in _CMP_OCR_KW)
    _has_prev_ref  = any(kw in _mn_early for kw in _CMP_PREV_KW)
    if _has_cmp_kw and (_has_ocr_ref or _has_prev_ref):
        # book1: cuốn trong ảnh → last_found_title hoặc last_ocr_books[-1]
        _cmp_ocr_list = context.get("last_ocr_books", [])
        _cmp_b1 = (
            context.get("last_found_title")
            or (_cmp_ocr_list[-1].get("title") if _cmp_ocr_list else None)
        )
        # book2: ordinal từ last_shown_books
        _cmp_shown = context.get("last_shown_books", [])
        _cmp_ord_idx = 0
        for _ord_kw, _ord_i in {"thu hai": 1, "cuon 2": 1, "cuon thu 2": 1, "thu 2": 1}.items():
            if _ord_kw in _mn_early:
                _cmp_ord_idx = _ord_i
                break
        _cmp_b2 = _cmp_shown[_cmp_ord_idx].get("title") if _cmp_ord_idx < len(_cmp_shown) else None

        if _has_ocr_ref and _has_prev_ref and _cmp_b1 and _cmp_b2:
            intent = "book_compare"
            nlu_result.intent = "book_compare"
            nlu_result.confidence = 1.0
            entities["book_title"]              = _cmp_b1
            entities["book_title_2"]            = _cmp_b2
            nlu_result.entities["book_title"]   = _cmp_b1
            nlu_result.entities["book_title_2"] = _cmp_b2
        elif _has_cmp_kw and not _has_ocr_ref and len(_cmp_shown) >= 2:
            # "So sánh cuốn 1 và cuốn 3" → resolve cả 2 ordinal từ last_shown_books
            _ORD_NORM_MAP = {
                "cuon 1": 0, "thu nhat": 0, "thu 1": 0, "cuon thu nhat": 0, "cuon dau": 0, "dau tien": 0,
                "cuon 2": 1, "thu hai": 1, "thu 2": 1, "cuon thu hai": 1,
                "cuon 3": 2, "thu ba": 2, "thu 3": 2, "cuon thu ba": 2,
                "cuon 4": 3, "thu tu": 3, "thu 4": 3, "cuon thu tu": 3,
            }
            _cmp_idxs = []
            for _ok, _oi in _ORD_NORM_MAP.items():
                if _ok in _mn_early and _oi not in _cmp_idxs:
                    _cmp_idxs.append(_oi)
            # Sort để đảm bảo thứ tự tự nhiên (cuốn nhỏ hơn trước)
            _cmp_idxs = sorted(set(_cmp_idxs))
            if len(_cmp_idxs) >= 2:
                _b1_idx, _b2_idx = _cmp_idxs[0], _cmp_idxs[1]
                _cmp_t1 = _cmp_shown[_b1_idx].get("title") if _b1_idx < len(_cmp_shown) else None
                _cmp_t2 = _cmp_shown[_b2_idx].get("title") if _b2_idx < len(_cmp_shown) else None
                if _cmp_t1 and _cmp_t2:
                    intent = "book_compare"
                    nlu_result.intent = "book_compare"
                    nlu_result.confidence = 1.0
                    entities["book_title"]              = _cmp_t1
                    entities["book_title_2"]            = _cmp_t2
                    nlu_result.entities["book_title"]   = _cmp_t1
                    nlu_result.entities["book_title_2"] = _cmp_t2
                else:
                    intent = "book_compare"
                    nlu_result.intent = "book_compare"
                    nlu_result.confidence = 1.0
            else:
                intent = "book_compare"
                nlu_result.intent = "book_compare"
                nlu_result.confidence = 1.0

    # —— Interceptor 7: Ordinal + Review → DIRECT RETURN (bypass toàn bộ NLU pipeline) ——
    # "Cuốn thứ nhất/thứ 2... bạn gợi ý có đánh giá mấy sao / rating mấy sao"
    # Phải đặt TRƯỚC security block để không bị pending/clarify-first chặn
    _I7_REV_KW  = ["danh gia", "may sao", "bao nhieu sao", "rating", "review", "nhan xet", "tot khong"]
    _I7_ORD_MAP = {
        "dau tien": 0, "thu nhat": 0, "thu 1": 0, "cai 1": 0, "cuon 1": 0,
        "thu hai":  1, "thu 2":  1, "cai 2": 1, "cuon 2": 1,
        "thu ba":   2, "thu 3":  2, "cai 3": 2, "cuon 3": 2,
        "thu tu":   3, "thu 4":  3, "cai 4": 3, "cuon 4": 3,
    }
    _i7_has_rev = any(kw in _mn_early for kw in _I7_REV_KW)
    _i7_ord_idx = next((idx for kw, idx in _I7_ORD_MAP.items() if kw in _mn_early), None)
    _i7_last    = context.get("last_shown_books", [])
    if _i7_has_rev and _i7_ord_idx is not None and _i7_ord_idx < len(_i7_last):
        _i7_book = _i7_last[_i7_ord_idx]
        _i7_bid  = _i7_book.get("book_id") or _i7_book.get("id")
        _i7_title = _i7_book.get("title", "")
        if _i7_bid:
            try:
                _i7_live = get_book_realtime(int(_i7_bid))
                if _i7_live:
                    _i7_rt    = float(_i7_live.get("avg_rating") or 0)
                    _i7_rc    = int(_i7_live.get("review_count") or 0)
                    _i7_au    = _i7_live.get("author") or "Đang cập nhật"
                    _i7_pr    = float(_i7_live.get("price") or 0)
                    _i7_avail = int(_i7_live.get("available_quantity") or 0)
                    _i7_st    = "Còn hàng" if _i7_avail > 0 else "Hết hàng"
                    _i7_ti    = _i7_live.get("title") or _i7_title
                    if _i7_rt:
                        _rc_s = f" ({_i7_rc:,} đánh giá)" if _i7_rc else ""
                        _i7_ans = (
                            f"📖 **{_i7_ti}**\n\n"
                            f"• **Tác giả**: {_i7_au}\n"
                            f"• **Đánh giá**: {_i7_rt:.1f}/5{_rc_s}\n"
                            f"• **Giá**: {_i7_pr:,.0f}đ | {_i7_st}\n\n"
                            "Bạn muốn thêm vào giỏ hàng hay tìm sách tương tự?"
                        )
                    else:
                        _i7_ans = (
                            f"📖 **{_i7_ti}**\n\n"
                            f"• **Tác giả**: {_i7_au}\n"
                            f"• **Đánh giá**: Chưa có đánh giá trên hệ thống\n"
                            f"• **Giá**: {_i7_pr:,.0f}đ | {_i7_st}\n\n"
                            "Bạn muốn thêm vào giỏ hàng hay tìm sách tương tự?"
                        )
                    context["last_found_title"] = _i7_ti
                    _i7_bid_s = str(_i7_bid)
                    _i7_btn = next(
                        (b for b in _i7_last if str(b.get("book_id") or b.get("id") or "") == _i7_bid_s),
                        None
                    )
                    _i7_btns = _make_book_buttons([_i7_btn]) if _i7_btn else []
                    return _i7_ans, tone, "book_review", _i7_btns, ["mysql:books"], True
            except Exception as _i7_err:
                _log.warning(f"[interceptor7] err: {_i7_err}")
        # bid = None → không có book_id, fall through to normal flow

    # —— Bảo mật: SQL injection + admin destruct actions → security_block template ——
    # FIX P0: Check cả message gốc (cho SQL tiếng Anh) VÀ _mn_early (cho tiếng Việt normalized)
    _SECURITY_RE = _re_i.compile(
        r'drop\s+(table|database|schema|index)|delete\s+from|truncate\s+(table|schema)|'
        r'insert\s+into|update\s+\w+\s+set|union\s+(all\s+)?select|exec\s*\(|xp_\w+|'
        r'benchmark\s*\(|sleep\s*\(\s*\d|load_file\s*\(|into\s+outfile|'
        r'<script[\s>]|javascript\s*:|onerror\s*=|onload\s*=|alert\s*\(|'
        r'1\s*=\s*1|or\s+1\s*=\s*1|and\s+1\s*=\s*1|--\s*$|;\s*select',
        _re_i.IGNORECASE
    )
    _ADMIN_DESTRUCT_RE = _re_i.compile(
        # FIX P0: Match "xoa tai khoan admin" (không cần khoảng cách giữa các từ)
        r'xoa\s*tai\s*khoan|'
        r'xoa\s*(admin|user|he\s*thong|co\s*so\s*du\s*lieu|root|database)|'
        r'delete\s+(account|admin|user|where)|'
        r'h[a4]c?k(er|ing)?|bypass\s*(auth|login|security)|exploit|brute\s*force|'
        r'lay\s*(quyen|token|cookie|session)\s*(admin|root)|'
        r'sql\s*inject|payload|reverse\s*shell',
        _re_i.IGNORECASE
    )
    # FIX P0: Check RAW message trước (SQL tiếng Anh) + _mn_early (tiếng Việt)
    if _SECURITY_RE.search(message) or _SECURITY_RE.search(_mn_early) or _ADMIN_DESTRUCT_RE.search(_mn_early):
        return TEMPLATES["security_block"], tone, "security_block", [], [], True

    # ── [V6 FIX] ULTRA-EARLY Profanity / Toxic Speech Filter ────────────────
    # Chặn: chửi tục, ngôn từ xúc phạm, nội dung 18+, kỳ thị, kích động.
    # Dùng ASCII không dấu (_mn_early) để bắt cả có dấu lẫn không dấu.
    # LƯU Ý: Bỏ từ ngắn 1 syllable ("cac", "cu", "lon", "may", "tao", "dm")
    # vì sau normalize, "các"→"cac", "cụ"→"cu", "lớn"→"lon", "mày"→"may" → false positive
    _PROFANITY_KW = [
        # ── Chửi thề / tục tĩu phổ biến (ASCII, dạng GHÉP hoặc đủ ngữ cảnh) ──
        "dit", "buoi", "cho chet", "vl", "dcm",
        "con bo", "thang cho", "con cho", "may dien", "may khoc", "thang dien",
        "do ngu", "do cho", "thang ngu", "con ngu", "do dien",
        "chet di", "chet me", "di chet", "cho mat", "con di",
        "du ma", "du me", "choi me",
        "vo hoc", "liem", "troll", "troll bot",
        # ── Nội dung 18+ / khiêu dâm ──
        "khieu dam", "phim sex", "sex", "dam duc", "khoa than",
        "hiep dam", "cuong buc", "bao luc tinh duc",
        # ── Kỳ thị / phân biệt ──
        "ki thi", "phan biet", "racist", "chong pha", "pha hoai",
        # ── Kích động / đe dọa ──
        "giet nguoi", "danh nguoi", "de doa", "tan cong",
        "tu sat", "tu tu", "tieu diet",
        # ── Spam / nonsense ──
        "aaaa", "asdf", "qwer", "1234567", "zzzz",
    ]
    import re as _re_prof
    # FIX: Dùng word boundary (\b) để tránh false positive bắt nhầm từ con (VD: "con nguoi" chứa "con ngu")
    _prof_pattern = r'\b(?:' + '|'.join(_re_prof.escape(kw) for kw in _PROFANITY_KW) + r')\b'
    _matched_prof_match = _re_prof.search(_prof_pattern, _mn_early)
    
    if not _is_system_qr and _matched_prof_match:
        _matched_profanity = _matched_prof_match.group(0)
        print(f"🛑 PROFANITY TRIGGERED! Message: {message!r} | Normalized: {_mn_early!r} | Matched keyword: {_matched_profanity!r}", flush=True)
        return (
            "Xin lỗi, tôi không thể hỗ trợ nội dung này. "
            "BookStore là không gian văn hóa, tôi chỉ có thể giúp bạn "
            "**tìm sách**, **kiểm tra đơn hàng** hoặc **giải đáp chính sách**. "
            "Bạn cần hỗ trợ gì về sách không? 📚",
            tone, "profanity_block", [], [], True
        )


    # —— Escalation keywords: khiếu nại, tố cáo → escalate handler ——
    # FIX P1: Bổ sung thêm các từ khoá khiếu nại phổ biến
    _ESCALATE_KW = [
        "khieu nai", "to cao", "noi chuyen voi nguoi that", "gap quan ly",
        "noi voi nhan vien", "yeu cau gap", "keu ca", "bao cao", "phan nan",
        "roi khoi", "khong chap nhan", "se kien", "kien tung",
        # FIX P1: Thêm pattern phổ biến còn thiếu
        "toi se kien", "toi kien", "nop don kieu nai", "gui don kieu nai",
        "lap luat su", "bao nguoi tieu dung", "bao gia ca", "bao cong an",
        "van chua duoc giai quyet", "van chua giai quyet", "chua duoc xu ly",
        "yeu cau boi thuong", "doi boi thuong", "khong chap nhan cach giai quyet",
        # ISSUE-03: Mạng xã hội threat → escalate ngay với hotline
        "dang len mang xa hoi", "dang len mang", "post len mang",
        "se dang bai", "se dang review", "review xau",
        "len facebook", "len tiktok", "len youtube",
        "pham an", "truyen thong", "bao chi",
        "se phan anh", "gui len bao", "phan anh len",
    ]
    if any(kw in _mn_early for kw in _ESCALATE_KW):
        intent = "escalate"


    # —— Admin inventory block (các lệnh chỉ admin mới làm) ——
    _INV_I = _re_i.compile(
        r"cap nhat|them vao kho|them hang|nhap kho|xuat kho"
        r"|xa kho|kiem kho|kiem ke kho"
        r"|danh sach khach|khach hang cho giao|log lai|bao cao kho"
        r"|xoa cuon|xoa sach|rebuild|xuat file|quan tri"
        r"|cap nhat ton kho|so luong ton"
        r"|cap nhat quyen|doi quyen admin|sql inject",
        _re_i.IGNORECASE
    )
    if _INV_I.search(message) or _INV_I.search(_mn_early):
        return TEMPLATES["out_of_scope"], tone, "out_of_scope", [], [], True

    # —— ULTRA-EARLY ACKNOWLEDGEMENT (Tránh SBERT nhận nhầm thành book_search vì trùng tên sách) ——
    _ACK_KW_EARLY = ["thoi duoc", "ok roi", "hieu roi", "ra roi", "biet roi", "duoc roi", "oke", "got it", "noted", "clear"]
    if len(message.split()) <= 8 and any(kw in _mn_early for kw in _ACK_KW_EARLY):
        # Chặn nếu user cố tình thêm các từ khóa tìm sách
        if not any(kw in _mn_early for kw in ["sach", "tim", "mua", "gia", "cuon"]):
            return (
                "Vui lòng liên hệ nếu bạn cần thêm hỗ trợ! 😊\n"
                "📞 Hotline: **0353260721** (8h–22h)",
                tone, "chitchat", [], [], True
            )

    # ── [V6 FIX] ULTRA-EARLY Negative Sentiment Check ────────────────────────
    # Phải đặt ở đây – TRUỚC Clarify-First và slot-filling.
    # intent complaint_damaged có confidence thấp → bị confidence-gate bắt truớc khi
    # đến _SENTIMENT_NEG_KW ở line ~750 → bot hiện Clarify-First thay vì xin lỗi.
    # _mn_early đã là ASCII không dấu → dùng trực tiếp cho keyword matching.
    _SENT_NEG_ULTRA = [
        "tuc thiet",      # tức thiệt
        "ghet",           # ghét
        "chua giao",      # chưa giao
        "te qua",         # tệ quá
        "that vong",      # thất vọng
        "buc minh",       # bực mình
        "giao cham",      # giao chậm
        "giao tre",       # giao trễ
        "giao muon",      # giao muộn
        "chan qua",       # chán quá
        "tuc qua",        # tức quá
        "kho chiu",       # khó chịu
        "vo ly",          # vô lý
        "kem chat luong", # kém chất lượng
    ]
    # Guard: không trigger complaint nếu message là câu tra cứu đơn hàng
    _ORDER_QUERY_KW_SENT = [
        "co don", "don hang", "lich su", "trang thai don", "don nao",
        "giao that bai", "that bai giao", "don that bai",
        "tra hang", "hoan tra", "don huy", "da huy",
    ]
    _is_order_query_sent = any(kw in _mn_early for kw in _ORDER_QUERY_KW_SENT)
    _has_order_entity_sent = bool(entities.get("order_id") or context.get("last_order_id"))
    if not _has_order_entity_sent and not _is_order_query_sent and any(kw in _mn_early for kw in _SENT_NEG_ULTRA):
        return (
            "BookStore xin lỗi vì bạn có trải nghiệm không vui. "
            "Chúng tôi chân thành xin lỗi vì sự bất tiện này! "
            "Bạn có thể cung cấp mã đơn hàng để tôi kiểm tra ngay, "
            "hoặc liên hệ CSKH: **0353260721** (8h–22h) để được hỗ trợ nhanh nhất.",
            tone, "sentiment_negative", [], [], True
        )

    # ── [NEW] EXACT MATCH of a recent book title ────────────────────────────
    # Nếu tin nhắn trùng chính xác (hoặc gần chính xác) tên sách vừa xem, force book_detail
    # [FIX] Bỏ qua nếu intent đã là policy/info intent (tránh "chính sách" bị nhầm)
    _POLICY_INTENTS = {"return_policy", "shipping_info", "store_info", "payment_method",
                       "promotion_current", "return_request", "escalate"}
    _last_books_exact = context.get("last_shown_books", [])
    if _last_books_exact and intent not in _POLICY_INTENTS:
        _msg_lower = message.strip().lower()
        for _lb in _last_books_exact:
            _lb_title = _lb.get("title", "").strip().lower()
            if _msg_lower and _lb_title and (
                _msg_lower == _lb_title 
                or (_lb_title.startswith(_msg_lower) and len(_msg_lower) >= 15)
                or (_msg_lower.startswith(_lb_title) and len(_lb_title) >= 15)
                or (len(_lb_title) >= 5 and _lb_title in _msg_lower)
            ):
                intent = "book_detail"
                entities["book_title"] = _lb.get("title")
                nlu_result.entities["book_title"] = _lb.get("title")
                nlu_result.intent = "book_detail"
                break


    # ── [v8] Coreference Resolution (ASCII-normalized) ────────────────

    import unicodedata as _ud7
    _mn7 = "".join(c for c in _ud7.normalize("NFD", message.lower().replace("đ","d")) if _ud7.category(c)!="Mn")
    _ck7 = [
        "tim hieu them","ve no","cuon do","cuon nay","chi tiet hon",
        "xem them","biet them","gia cuon","bao nhieu tien",
        "sahcs do","sach do","cuon sach do","no la",
        # Follow-up sau OCR hoặc search
        "cuon nay gia","sach nay gia","gia sach nay","cuon do gia",
        "gia cuon do","bao nhieu vay","sach vua roi","cuon vua xem",
        "cuon vua tim","sach vua tim","no gia","cuon tren gia",
        "gia cuon tren","cuon do bao nhieu","sach do bao nhieu",
        # FIX P1: thêm các pattern phổ biến sau khi xem danh sách
        "cuon nay bao nhieu","gia cuon nay","cuon nay co","sach nay co",
        "cuon do co","no bao nhieu","gia no la","cuon do gia la",
        "cai nay gia","cai do gia","cai tren gia",
        # FIX COREF T2: catch"Cuốn đó giá bao nhiêu", "Giá bao nhiêu" khi context có sách
        "gia bao nhieu","bao nhieu tien","gia la bao","gia the nao",
        "nen mua cuon nao","dat hay re",
        # Tác giả follow-ups
        "tac gia la ai", "ai viet no", "ai viet cuon do", "ai viet cuon nay",
        "ai viet sach nay", "tac gia cua no", "tac gia cua sach", "tac gia cua cuon",
        "tac gia la", "ai viet", "ai viet cuon sach tren"
    ]
    # ── [FRESH OCR GATE] ──────────────────────────────────────────────────────
    # Dùng KEY EXISTENCE (không phải truthiness) để phân biệt:
    #   - "_ocr_data" in context → ảnh mới vừa upload (dù OCR thất bại, dict rỗng)
    #   - "_ocr_data" NOT in context → tin nhắn text thuần túy (follow-up)
    # Điều này ngăn chặn mọi coreference override khi user upload ảnh mới
    _is_fresh_ocr = "_ocr_data" in context

    # ── [AUTHOR SEARCH OVERRIDE] ──────────────────────────────────────────────
    # "Xem sách cùng tác giả với..." → force book_search trước khi Case 1a chạy
    # Tránh SBERT classify nhầm → recommend_category → Peppa Pig / hallucinated results
    # ⚠️ GUARD: Không áp dụng khi đây là ảnh mới upload (intent vẫn phải là image_search)
    _AUTHOR_SEARCH_KW = [
        "cung tac gia", "sach cung tac gia", "cung mot tac gia",
        "tac gia cung", "sach khac cua tac gia", "xem tac gia",
        "tac gia do", "tac gia cuon do", "tac gia cuon nay",
    ]
    if not _is_fresh_ocr and any(kw in _mn7 for kw in _AUTHOR_SEARCH_KW):
        intent = "book_search"
        nlu_result.intent = "book_search"
        # Nếu không có book_title entity, thử extract từ message hoặc dùng context
        if not entities.get("book_title"):
            _last_books7_auth = context.get("last_shown_books", [])
            _rf7_auth = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_last_books7_auth[0].get("title") if _last_books7_auth else None)
            )
            if _rf7_auth:
                entities["book_title"] = _rf7_auth
                nlu_result.entities["book_title"] = _rf7_auth

    # ── Ordinal reference map (defined here, used in Case 3 + resolver below) ─
    _ORDINAL_MAP = {
        "dau tien": 0, "thu nhat": 0, "cai 1": 0, "cuon 1": 0, "cai dau": 0, "thu 1": 0,
        "thu hai":  1, "cai 2":   1, "cuon 2": 1, "thu 2": 1,
        "thu ba":   2, "cai 3":   2, "cuon 3": 2, "thu 3": 2,
        "thu tu":   3, "cai 4":   3, "cuon 4": 3, "thu 4": 3,
    }

    # Case 1a: intent không phải book_detail → check _ck7 HOẶC standalone price query với context có sách
    # ⚠️ QUAN TRỌNG: KHÔNG áp dụng coref khi intent = image_search (ảnh mới upload)
    # vì "Còn cuốn này thì sao" chứa "cuon nay" → sẽ bị redirect về sách cũ thay vì ảnh mới
    if intent not in ("book_detail", "book_availability", "book_review", "image_search",
                       # [FIX] Các intent policy/info KHÔNG được redirect sang book_detail
                       # dù message chứa keyword trùng tên sách (VD: "chính sách" chứa "sách")
                       "return_policy", "shipping_info", "store_info", "payment_method",
                       "promotion_current", "return_request"):

        _has_ctx_book = bool(
            context.get("last_found_title") or context.get("last_search_query")
            or context.get("last_shown_books")
        )
        if any(t in _mn7 for t in _ck7) and _has_ctx_book:
            # FIX P1: ưu tiên last_found_title (OCR) → last_search_query → last_shown_books[0]
            _last_books7 = context.get("last_shown_books", [])
            _rf7 = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_last_books7[0].get("title") if _last_books7 else None)
            )
            if _rf7:
                intent = "book_detail"
                entities["book_title"] = _rf7
                nlu_result.entities["book_title"] = _rf7
    # Case 2: intent đã là book_detail nhưng THIẾU book_title → dùng context
    elif intent == "book_detail" and not entities.get("book_title"):
        # Mở rộng trigger: standalone "gia" / "bao nhieu" / "tac gia" cũng kích hoạt coref nếu có context
        _PRICE_WORDS = ["gia", "bao nhieu", "cuon", "sach", "no", "cai", "cuon do", "gia la", "tac gia", "ai viet"]
        if any(t in _mn7 for t in _ck7) or any(kw in _mn7 for kw in _PRICE_WORDS):
            _last_books7b = context.get("last_shown_books", [])
            _rf7 = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_last_books7b[0].get("title") if _last_books7b else None)
            )
            if _rf7:
                entities["book_title"] = _rf7
                nlu_result.entities["book_title"] = _rf7

    # [FIX] ORDINAL PAIR → book_compare phải chạy TRƯỚC Case 3 book_availability coref
    # VD: "So sánh cuốn 1 và cuốn 3 xem sao" → NLU=book_availability → Case 3 inject sách sai
    _ORD_PAIR_DETECT_EARLY = ["cuon 1","thu nhat","thu 1","cuon 2","thu hai","thu 2",
                              "cuon 3","thu ba","thu 3","cuon 4","thu tu","thu 4"]
    _found_ord_early = [o for o in _ORD_PAIR_DETECT_EARLY if o in _mn7]
    _ord_nums_early  = {o.split()[1] if len(o.split()) > 1 else o for o in _found_ord_early}
    if "so sanh" in _mn7 and len(_ord_nums_early) >= 2:
        intent = "book_compare"
        nlu_result.intent = "book_compare"
        nlu_result.confidence = 0.99

    # Case 3: intent là book_availability / book_review nhưng THIẾU book_title → resolve từ context
    # FIX: "Còn hàng không", "Xem đánh giá chi tiết" không được inject book_title từ NLU
    # [FIX] Guard: không chạy nếu đã detect ordinal pair compare
    elif intent in ("book_availability", "book_review") and not entities.get("book_title"):
        _coref_avail = [
            "con hang khong", "co hang khong", "con khong", "hang con khong",
            "dat duoc khong", "cuon vua xem con", "con stock", "het stock",
            "xem danh gia", "danh gia chi tiet", "review chi tiet",
            "nha xuat ban", "tac gia la", "tac gia nao", "mo ta sach",
            "sach tren", "cuon tren", "sach vua", "cuon vua",
        ]
        _COREF_WORDS3 = [
            "con hang", "co hang", "hang con", "het hang",
            "con", "sach", "cuon", "no", "danh gia",
        ]
        _has_coref3 = any(kw in _mn7 for kw in _coref_avail) or any(kw in _mn7 for kw in _COREF_WORDS3)
        if _has_coref3:
            _last_books7c = context.get("last_shown_books", [])
            _rf7c = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_last_books7c[0].get("title") if _last_books7c else None)
            )
            if _rf7c:
                # [FIX] Không dùng coref chung nếu câu có chứa số thứ tự (để dành cho ordinal resolver phía dưới)
                _is_ordinal_msg = any(kw in _mn7 for kw in _ORDINAL_MAP.keys())
                if not _is_ordinal_msg:
                    entities["book_title"] = _rf7c
                    nlu_result.entities["book_title"] = _rf7c

    # ── Ordinal reference: "cuốn đầu tiên", "cuốn thứ 2"… ────────────
    _ORDINAL_MAP = {
        "dau tien": 0, "thu nhat": 0, "cai 1": 0, "cuon 1": 0, "cai dau": 0, "thu 1": 0,
        "thu hai":  1, "cai 2":   1, "cuon 2": 1, "thu 2": 1,
        "thu ba":   2, "cai 3":   2, "cuon 3": 2, "thu 3": 2,
        "thu tu":   3, "cai 4":   3, "cuon 4": 3, "thu 4": 3,
    }
    # ── Ordinal reference resolver ─────────────────────────────────────────────
    # FIX: Phân biệt OCR ordinal vs search ordinal + phát hiện similar/review intent
    _ord_is_ocr_ctx = any(kw in _mn7 for kw in ["quet", "scan", "upload", "chup", "vua gui"])
    _ord_is_similar = any(kw in _mn7 for kw in ["tuong tu", "giong", "goi y sach", "lien quan"])
    _ord_is_review  = any(kw in _mn7 for kw in ["danh gia", "bao nhieu sao", "may sao", "rating", "review", "nhan xet", "tot khong"])
    _ord_ocr_books  = context.get("last_ocr_books", []) if _ord_is_ocr_ctx else []
    _last_books     = _ord_ocr_books if _ord_ocr_books else context.get("last_shown_books", [])
    # [FIX] EARLY DETECT: "So sánh cuốn 1 và cuốn 3" → force book_compare TRƯỚC ordinal resolver
    # Nếu message có "so sanh" + 2 ordinal khác nhau → NLU có thể classify sai thành book_detail
    _ORD_PAIR_DETECT = ["cuon 1","thu nhat","thu 1","cuon 2","thu hai","thu 2",
                        "cuon 3","thu ba","thu 3","cuon 4","thu tu","thu 4"]
    _found_ord_pair = [o for o in _ORD_PAIR_DETECT if o in _mn7]
    _ord_nums = {o.split()[1] if len(o.split()) > 1 else o for o in _found_ord_pair}
    if "so sanh" in _mn7 and len(_ord_nums) >= 2:
        intent = "book_compare"
        nlu_result.intent = "book_compare"
        nlu_result.confidence = 0.99

    if _last_books:

        for _ord_key, _idx in _ORDINAL_MAP.items():
            if _ord_key in _mn7 and _idx < len(_last_books):
                _ord_title = _last_books[_idx].get("title", "")
                _ord_bid   = _last_books[_idx].get("book_id") or _last_books[_idx].get("id")
                # FIX: Interceptor 6 đã set book_compare với cả 2 book → không override intent
                if intent == "book_compare":
                    break
                entities["book_title"] = _ord_title
                nlu_result.entities["book_title"] = _ord_title
                # ── Inject book_id trực tiếp → book_review/detail fast path không cần tìm lại
                if _ord_bid:
                    entities["book_id"] = _ord_bid
                    nlu_result.entities["book_id"] = _ord_bid
                if _ord_is_similar:
                    intent = "book_search"
                    nlu_result.intent = "book_search"
                    entities["query"] = _ord_title
                    nlu_result.entities["query"] = _ord_title
                elif _ord_is_review:
                    intent = "book_review"
                    nlu_result.intent = "book_review"
                else:
                    intent = "book_detail"
                break

    # ── Slot-filling pending ──────────────────────────────────

    if context.get("pending_slot_filling") or context.get("pending_intent_confirm"):
        # [FIX P0] Nếu intent mới là OCR hoặc chủ đề hoàn toàn khác → xóa pending và xử lý bình thường
        _FORCE_CLEAR_INTENTS = {
            "image_search", "recommend_trending", "chitchat", "store_info",
            "promotion_list", "book_search", "recommend_personal",
            # FIX P0: Thêm loyalty_points, account_help, escalate → phải clear slot cũ ngay
            "loyalty_points", "account_help", "escalate", "payment_method",
            "return_policy", "bot_capabilities", "farewell",
            # FIX RECURSIVE: book_review/detail/compare/availability gây đệ quy vô hạn
            "book_review", "book_detail", "book_compare", "book_availability",
            "recommend_category", "recommend_gift",
            # FIX: order intents — user đổi chủ đề sang đơn hàng phải clear pending cũ
            "order_history", "order_status", "order_cancel",
        }
        _pending_sf = context.get("pending_slot_filling", {})
        _pending_intent_target = _pending_sf.get("target_intent", "")
        _is_ocr = (intent == "image_search") or ("OCR" in message or "\U0001f50d" in message)

        # [FIX] Nếu message là quick-reply button hợp lệ cho pending intent → KHÔNG force-clear
        # VD: "Con nhỏ (0-6 tuổi)" bị NLU classify nhầm thành chitchat → không xóa pending
        _expected_qr = SLOT_FILLING_CONFIG.get(_pending_intent_target, ("", "", []))[2] if _pending_intent_target else []
        _is_valid_quick_reply = message.strip() in _expected_qr

        if not _is_valid_quick_reply and (_is_ocr or (intent in _FORCE_CLEAR_INTENTS and intent != _pending_intent_target)):
            context.pop("pending_slot_filling",  None)
            context.pop("pending_intent_confirm", None)
        else:
            # [FIX] Intercept TẤT CẢ pending slot-filling (recommend_category, recommend_gift, v.v.)
            # Tránh NLU classify nhầm button clicks → out_of_scope/order_history → lỗi
            if _pending_intent_target == "recommend_category":
                import unicodedata as _ud_pending
                _mn_pending = "".join(
                    c for c in _ud_pending.normalize("NFD", message.lower().replace("đ","d"))
                    if _ud_pending.category(c) != "Mn"
                )
                _GENRE_PICKER_AGAIN_KW = [
                    "kham pha them", "the loai khac", "doi the loai", "chu de khac",
                    "xem the loai khac", "kham pha the loai khac",
                ]
                _is_picker_again = any(kw in _mn_pending for kw in _GENRE_PICKER_AGAIN_KW)

                if _is_picker_again:
                    # User nhấn "Khám phá thêm thể loại khác" → hiện picker mới
                    context.pop("pending_slot_filling", None)
                    context.pop("pending_intent_confirm", None)
                    intent = "recommend_category"
                    nlu_result.confidence = 1.0
                    # fall-through → _is_change_genre sẽ bắt và hiện picker mới
                else:
                    _genre_from_pending = resolve_genre_alias(message)
                    context.pop("pending_slot_filling", None)
                    context.pop("pending_intent_confirm", None)
                    intent = "recommend_category"
                    entities["genre"] = _genre_from_pending or message.strip()
                    nlu_result.confidence = 1.0
                    # fall-through → recommend_category handler bên dưới

            elif _pending_intent_target:
                # [FIX GENERAL] Các intent khác (recommend_gift, recommend_personal, v.v.)
                # Inject message thẳng vào required slot, override intent, rồi delegate
                _req_slot = _pending_sf.get("required_slot", "")
                if _req_slot and not entities.get(_req_slot):
                    entities[_req_slot] = message.strip()
                    nlu_result.entities[_req_slot] = message.strip()
                intent = _pending_intent_target
                nlu_result.intent = _pending_intent_target
                nlu_result.confidence = 1.0
                context.pop("pending_slot_filling",  None)
                context.pop("pending_intent_confirm", None)
                # Delegate với intent đã được fix
                answer, sources, btns = await process(
                    message=message, nlu_result=nlu_result, user_id=user_id,
                    context=context, history=history, user_profile=user_profile,
                )
                return answer, tone, intent, btns, sources, True

            else:
                # Không có target intent → delegate như cũ
                answer, sources, btns = await process(
                    message=message, nlu_result=nlu_result, user_id=user_id,
                    context=context, history=history, user_profile=user_profile,
                )
                return answer, tone, intent, btns, sources, True

    # ── 0a. CONFIDENCE CHECK ──────────────────────────────────
    is_sbert_result = nlu_result.confidence < 0.95
    in_borderline   = CONFIDENCE_SBERT_MIN <= nlu_result.confidence < CONFIDENCE_CONFIRM_THRESHOLD
    # FIX E: Mở rộng skip-confirm conditions
    _has_ocr_ctx_fs    = bool(context.get("last_ocr_books") or context.get("last_found_title"))
    _has_any_ctx       = bool(context.get("last_search_query") or context.get("last_category")
                              or context.get("last_intent") in {
                                  "book_search", "book_detail", "recommend_category",
                                  "recommend_personal", "image_search", "order_history",
                                  "book_availability", "recommend_trending"})
    _followup_kws   = [
        # OCR-specific
        "cuon vua", "sach vua", "quet", "upload", "anh vua",
        # Price/author/detail follow-ups
        "gia bao nhieu", "bao nhieu tien", "gia", "tien",
        "con hang", "het hang", "ton kho",
        "tac gia", "ai viet",
        "cuon do", "sach do", "cuon nay", "sach nay", "no", "cuon kia",
        "danh gia", "rating", "review", "nhan xet",
        "the nao", "nhu the nao", "tot khong", "chat luong",
        # Compare/filter follow-ups
        "re hon", "dat hon", "re nhat", "dat nhat",
        "cuon nao re", "cuon nao dat", "cai nao", "cuon nao",
        "tuong tu", "giong the", "cung chu de", "cung tac gia",
        "150k", "200k", "100k", "300k",  # tầm giá cụ thể
        # Action follow-ups
        "mua cuon", "them vao gio", "dat mua", "xem them",
        "tiep tuc", "con gi nua", "gui toi",
    ]
    _mn_followup = _mn7
    if (_has_ocr_ctx_fs or _has_any_ctx) and any(kw in _mn_followup for kw in _followup_kws):
        in_borderline = False  # Không hỏi xác nhận khi rõ ràng là follow-up
    # Nếu intent là các intents cụ thể có entity → skip confirm
    if entities and intent in ("book_search", "book_detail", "book_availability", "recommend_category",
                               "order_status", "voucher_apply") and in_borderline:
        in_borderline = False  # Có entity → đủ confident để xử lý

    # [FIX] Skip confirm dialog khi message là known quick-reply button từ SLOT_FILLING_CONFIG
    # VD: "Trẻ em (7-12 tuổi)", "Bạn gái/Phụ nữ", "Kỹ năng sống" click lần 2 không có pending
    _all_known_qr: set = set()
    for _sf_v in SLOT_FILLING_CONFIG.values():
        _all_known_qr.update(_sf_v[2])
    if message.strip() in _all_known_qr:
        in_borderline = False  # Known button → skip confirm, handle directly

    # [FIX] Recipient-label rescue: "Trẻ em (7-12 tuổi)" click lần 2 không có pending
    # → override sang recommend_gift thay vì recommend_category/out_of_scope
    _RECIPIENT_QR = set(SLOT_FILLING_CONFIG.get("recommend_gift", ("", "", []))[2])
    if message.strip() in _RECIPIENT_QR and intent != "recommend_gift":
        intent = "recommend_gift"
        entities["recipient_type"] = message.strip()
        nlu_result.intent = "recommend_gift"
        nlu_result.entities["recipient_type"] = message.strip()
        nlu_result.confidence = 1.0
        in_borderline = False

    if is_sbert_result and in_borderline and intent not in NO_CONFIRM_INTENTS:
        desc = INTENT_CONFIRM_DESC.get(intent, intent)
        btns = [
            NavigateButton(label="✅ Đúng rồi!",  url="", type="confirm_yes"),
            NavigateButton(label="❌ Không phải", url="", type="confirm_no"),
        ]
        context["pending_intent_confirm"] = {
            "guessed_intent": intent, "original_message": message,
            "confidence": nlu_result.confidence,
        }
        return (
            f"Bạn đang muốn **{desc}** phải không?\n"
            "(Trả lời **Có** để tiếp tục, hoặc nói rõ hơn điều bạn cần)",
            tone, intent, btns, [], True
        )

    # ── 0b. SLOT-FILLING CHECK ────────────────────────────────────
    if intent in SLOT_FILLING_CONFIG and intent not in NO_CONFIRM_INTENTS:
        required_slot, question, quick_replies = SLOT_FILLING_CONFIG[intent]
        if _is_slot_missing(intent, required_slot, entities, user_id, context, message):
            btns = []
            if intent == "recommend_category":
                import random
                ALL_GENRES = ["Văn học", "Kinh doanh", "Kỹ năng sống", "Thiếu nhi", "Lịch sử", "Tâm lý học", "Tiểu thuyết", "Khoa học", "Nấu ăn", "Y học", "Truyện tranh", "Kinh dị", "Lập trình", "Triết học", "Đầu tư", "Marketing"]
                last_g = context.get("last_genre", "").lower()
                pool = [g for g in ALL_GENRES if g.lower() not in last_g]
                
                shuffled = random.sample(pool, 4) if len(pool) >= 4 else pool
                
                is_all = any(kw in message.lower() for kw in ["tất cả", "toàn bộ", "liet ke", "liệt kê", "các thể loại"])
                if is_all:
                    question = f"Cửa hàng chúng tôi cung cấp rất nhiều danh mục sách phong phú, tiêu biểu gồm: **{', '.join(ALL_GENRES)}** và vô số lĩnh vực khác.\n\nBạn có thể nhấn vào các nhóm nội bật dưới đây hoặc gõ tên thể loại mà bạn đặc biệt muốn tìm nhé!"
                else:
                    question = f"Bạn muốn khám phá sách về mảng nào? Chúng tôi có nhiều đầu sách hay thuộc các lĩnh vực như **{', '.join(shuffled)}**..."
                
                for qr in shuffled:
                    btns.append(NavigateButton(label=qr, url="", type="quick_reply"))
                btns.append(NavigateButton(label="Khám phá thêm thể loại khác", url="", type="quick_reply"))
            else:
                btns = [NavigateButton(label=qr, url="", type="quick_reply") for qr in quick_replies]
                
            # [FIX] Set pending_slot_filling cho TẤT CẢ intent để intercept đúng button click
            # Tránh NLU classify nhầm button clicks → out_of_scope/wrong_intent
            context["pending_slot_filling"] = {
                "target_intent":    intent,
                "original_message": message,
                "filled_entities":  {},
                "required_slot":    required_slot,
            }
            return question, tone, intent, btns, [], True

    # ── [STREAM-GUARD] HAL-02 + H-04: phải đặt TRƯỚC chitchat handler ────────────
    import re as _re_sg, unicodedata as _ud_sg
    _mn_sg = "".join(
        c for c in _ud_sg.normalize("NFD", message.lower().replace("đ","d"))
        if _ud_sg.category(c) != "Mn"
    )
    # HAL-02: "Thêm vào giỏ hàng" / "Mua cuốn X đó" → NavigateButton, không thể tự thêm
    _CART_KW_SG = ["them vao gio hang", "them vao gio", "them vao cart",
                   "mua ngay giup", "them gio giup", "dat mua giup",
                   "mua cuon nay giup", "mua sach giup", "them cuon nay vao gio",
                   # FIX BUG T6: "Tôi muốn mua cuốn rẻ nhất đó"
                   "muon mua cuon", "mua cuon re nhat", "mua cuon do",
                   "mua cuon nay", "mua cuon kia", "toi muon mua",
                   "cho toi mua", "dat mua cuon", "mua cuon vua",
                   "mua cuon dau", "mua cuon thu",
                   # Ordinal + cart: "thêm cuốn thứ 2 vào giỏ"
                   "them cuon", "vao gio giup", "them vao gio cuon",
                   # "Mua cả hai", "mua 2 cuốn"
                   "mua ca hai", "mua ca 2", "mua het", "mua ca 2 cuon"]
    # [FIX] Exclusion: skip cart-add block khi message có ngữ cảnh tặng quà / gợi ý
    # VD: "Tôi muốn mua sách tặng cho em gái" → recommend_gift, không phải cart-add
    _CART_GIFT_EXCL_SG = ["tang cho", "tang qua", "lam qua", "goi y cho", "mua tang",
                          "em gai", "anh trai", "chi gai", "em trai", "ban be", "nguoi than",
                          "ba me", "bo me", "ong ba", "chong", "vo ", "con trai", "con gai"]
    _has_gift_ctx_sg = any(kw in _mn_sg for kw in _CART_GIFT_EXCL_SG)
    if not _has_gift_ctx_sg and any(kw in _mn_sg for kw in _CART_KW_SG):
        # Thử resolve ordinal reference từ last_shown_books ("cuốn thứ 2", "cuốn 2"...)
        _ORDINAL_CART_SG = {
            "dau tien": 0, "thu nhat": 0, "cuon 1": 0, "thu 1": 0, "cai 1": 0,
            "thu hai":  1, "cuon 2":   1, "thu 2": 1,  "cai 2": 1,
            "thu ba":   2, "cuon 3":   2, "thu 3": 2,  "cai 3": 2,
            "thu tu":   3, "cuon 4":   3, "thu 4": 3,  "cai 4": 3,
        }
        _last_shown_sg = context.get("last_shown_books", [])
        # [FIX-STALE] Kiểm tra last_shown_books có phải từ session hiện tại không
        # bằng cách verify title đã xuất hiện trong recent history (assistant messages)
        _recent_history_text = " ".join(
            (m.get("content") or "") for m in (history or [])[-8:]
            if m.get("role") == "assistant"
        ).lower()
        _last_shown_sg_fresh = [
            b for b in _last_shown_sg
            if b.get("title", "").lower()[:20] in _recent_history_text
        ] if _last_shown_sg and _recent_history_text else []
        # Chỉ dùng last_shown_books nếu đã verify là fresh (trong history session này)
        _last_shown_sg = _last_shown_sg_fresh

        _ordinal_book_sg = None
        for _ok, _oi in _ORDINAL_CART_SG.items():
            if _ok in _mn_sg and _oi < len(_last_shown_sg):
                _ordinal_book_sg = _last_shown_sg[_oi]
                break

        _ocr_sg   = context.get("last_ocr_books", [])
        # Phát hiện user có dùng ordinal rõ ràng không ("cuốn thứ 2", "cái 1", ...)
        _has_ordinal_sg = any(_ok in _mn_sg for _ok in _ORDINAL_CART_SG)
        # [A] "Mua cả hai / mua hết / mua cả 2" → show tất cả fresh books làm buttons
        _MUA_CA_KW = ["mua ca hai", "mua ca 2", "mua het", "mua ca 2 cuon", "mua tat ca", "them ca hai", "them ca 2"]
        _is_mua_ca = any(kw in _mn_sg for kw in _MUA_CA_KW)
        if _is_mua_ca and _last_shown_sg:
            _ship_note = ""
            _SHIP_KW = ["ship", "van chuyen", "giao hang", "may ngay", "bao lau", "da nang", "ha noi", "ho chi minh", "tphcm"]
            if any(kw in _mn_sg for kw in _SHIP_KW):
                _ship_note = "\n\n📦 Thời gian giao hàng thường **2–5 ngày làm việc** (nội thành), **3–7 ngày** (tỉnh thành khác). Miễn ship cho đơn từ 150,000đ!"
            _all_btns_sg = [
                NavigateButton(label=f"📖 Xem & Mua: {b['title'][:45]}", url=f"/book/{b['book_id']}", type="book")
                for b in _last_shown_sg if b.get("book_id")
            ]
            _title_list = "\n".join(f"• **{b['title']}**" for b in _last_shown_sg)
            return (
                f"🛒 Chatbot **không thể tự thêm** sách vào giỏ hàng.\n"
                f"Nhấn từng nút bên dưới để vào trang sản phẩm → nhấn **\"Thêm vào giỏ hàng\"**:\n\n"
                f"{_title_list}{_ship_note}",
                tone, intent, _all_btns_sg, [], True
            )
        elif _is_mua_ca and not _last_shown_sg:
            # Không có fresh books → hỏi sách nào
            return (
                "📚 Bạn muốn mua những cuốn sách nào? Hãy cho tôi biết tên sách để tôi tìm link nhanh nhé!",
                tone, intent, [], [], True
            )

        _title_sg, _bid_sg = None, None
        if _ordinal_book_sg:
            _title_sg = _ordinal_book_sg.get("title", "")
            _bid_sg   = _ordinal_book_sg.get("book_id")
            # [FIX] book_id=None → lookup DB theo title
            if _title_sg and not _bid_sg:
                try:
                    from chatbot_app.retrieval.sql_retriever import get_connection as _gc_cart_sg
                    _conn_cart_sg = _gc_cart_sg()
                    _cur_cart_sg  = _conn_cart_sg.cursor(dictionary=True)
                    _cur_cart_sg.execute(
                        "SELECT book_id FROM books WHERE title = %s LIMIT 1",
                        (_title_sg,)
                    )
                    _row_cart_sg = _cur_cart_sg.fetchone()
                    _cur_cart_sg.close(); _conn_cart_sg.close()
                    if _row_cart_sg:
                        _bid_sg = _row_cart_sg["book_id"]
                except Exception:
                    pass
        elif _has_ordinal_sg:
            # [FIX] User chỉ định ordinal nhưng không resolve được (last_shown_books rỗng)
            # → KHÔNG fallback stale context, hỏi lại user
            return (
                "📚 Bạn muốn thêm sách nào vào giỏ? \n"
                "Vui lòng cho tôi biết **tên sách** hoặc tìm sách trước rồi thêm nhé!",
                tone, intent, [], [], True
            )
        elif _ocr_sg:
            # Không có ordinal, dùng OCR context
            _title_sg = _ocr_sg[0].get("title", "")
            _bid_sg   = _ocr_sg[0].get("book_id")
        elif _last_shown_sg:
            # Không có ordinal, dùng sách đầu tiên trong list
            _title_sg = _last_shown_sg[0].get("title", "")
            _bid_sg   = _last_shown_sg[0].get("book_id")
        else:
            _title_sg = context.get("last_found_title") or context.get("last_book_name")
            _bid_sg   = None
            # Nếu có title nhưng không có book_id, lookup DB
            if _title_sg:
                try:
                    from chatbot_app.retrieval.sql_retriever import get_connection as _gc_cart_sg2
                    _conn_csg2 = _gc_cart_sg2()
                    _cur_csg2  = _conn_csg2.cursor(dictionary=True)
                    _cur_csg2.execute("SELECT book_id FROM books WHERE title = %s LIMIT 1", (_title_sg,))
                    _r2 = _cur_csg2.fetchone()
                    _cur_csg2.close(); _conn_csg2.close()
                    if _r2: _bid_sg = _r2["book_id"]
                except Exception:
                    pass

        if _title_sg and _bid_sg:
            _btn_sg = NavigateButton(
                label=f"📖 Xem & Mua: {_title_sg[:45]}", url=f"/book/{_bid_sg}", type="book"
            )
            return (
                f"🛒 Chatbot **không thể tự thêm** sách vào giỏ hàng của bạn.\n"
                f"Để thêm **\"{_title_sg}\"** vào giỏ, vui lòng nhấn nút bên dưới → nhấn **\"Thêm vào giỏ hàng\"**:",
                tone, intent, [_btn_sg], [], True
            )
        return (
            "🛒 Chatbot **chưa hỗ trợ** thêm sách vào giỏ hàng trực tiếp.\n"
            "Vui lòng vào trang sản phẩm và nhấn **\"Thêm vào giỏ hàng\"** nhé!\n\n"
            "_(Bạn có thể hỏi tên sách để tôi tìm link nhanh hơn)_",
            tone, intent, [], [], True
        )

    # H-04: "Thêm [sách] vào đơn" → không thể sửa đơn qua chatbot
    _ORDER_MOD_RE_SG = _re_sg.compile(
        r'them.{0,30}vao\s*don|bo\s*sung.{0,25}(vao\s*)?don|'
        r'sua\s*don\s*hang|chinh\s*sua\s*don|'
        r'them\s*san\s*pham\s*vao\s*don|them\s*vao\s*don\s*do'
    )
    if _ORDER_MOD_RE_SG.search(_mn_sg):
        return (
            "❌ **Không thể thêm/sửa đơn hàng đã tạo** qua chatbot.\n\n"
            "Sau khi đặt hàng, bạn **không thể bổ sung hoặc bớt sản phẩm** trong đơn.\n\n"
            "✅ **Để mua thêm sách:** Tạo đơn hàng mới tại trang sản phẩm.\n"
            "📞 Hotline: **0353260721** (8h-22h).",
            tone, intent, [], [], True
        )
    # ── end STREAM-GUARD ──────────────────────────────────────────────────────────

    # ── [OCR-DETERMINISTIC] Tính tổng giá + so sánh từ session OCR log ──────────
    # Phải TRƯỚC slot-filling để không bị intercept bởi book_compare slot check
    import re as _re_ocr_det
    _ocr_det_log = context.get("last_ocr_books", [])
    if _ocr_det_log:
        import unicodedata as _ud_od
        _mn_od = "".join(
            c for c in _ud_od.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_od.category(c) != "Mn"
        )
        # BUG-3 FIX: "N cuốn đã quét tổng tiền bao nhiêu?"
        _total_re = _re_ocr_det.compile(
            r'tong (tien|gia|chi phi)|'
            r'het bao nhieu tien|'
            r'(bao nhieu tien|gia bao nhieu).{0,20}N? cuon|'
            r'tat ca.{0,20}(cuon|sach).{0,20}(tien|gia)'
        )
        _n_cuon_re = _re_ocr_det.search(r'(\d+)\s*cuon.{0,20}(quet|scan|upload|hom nay)', _mn_od)
        _is_session_total = bool(_total_re.search(_mn_od)) and (
            "da quet" in _mn_od or "vua quet" in _mn_od or "hom nay" in _mn_od or
            bool(_re_ocr_det.search(r'(\d+)\s*cuon', _mn_od))
        )
        if _is_session_total and len(_ocr_det_log) >= 1:
            # Xác định N cuốn từ message
            _n_match_od = _re_ocr_det.search(r'(\d+)\s*cuon', _mn_od)
            _n_books = int(_n_match_od.group(1)) if _n_match_od else len(_ocr_det_log)
            _n_books = min(_n_books, len(_ocr_det_log))
            _books_to_sum = _ocr_det_log[:_n_books]
            _books_w_price = [b for b in _books_to_sum if b.get("price")]
            if _books_w_price:
                _total_od = sum(float(b["price"]) for b in _books_w_price)
                _lines_od = [f"**Tổng chi phí {len(_books_w_price)} cuốn sách đã quét:**"]
                for _i, _b in enumerate(_books_w_price, 1):
                    _lines_od.append(f"{_i}. **{_b.get('title','?')}** — {float(_b['price']):,.0f}đ")
                _lines_od.append(f"\n💰 **Tổng cộng: {_total_od:,.0f}đ**")
                return "\n".join(_lines_od), tone, intent, [], [], True

        # BUG-4 FIX: "Hai cuốn / N cuốn vừa quét cái nào rẻ/đắt hơn?"
        _cmp_re = _re_ocr_det.compile(
            r'(hai|2|ba|3)\s*cuon.{0,30}(re hon|dat hon|chat hon|nhieu hon|dat nhat|re nhat)|'
            r'(re hon|dat hon).{0,30}(hai|2|ba|3)\s*cuon|'
            r'cuon nao (re|dat|chat|nhieu|phu hop).{0,15}(hon|nhat).{0,20}(vua|tren|do)|'
            r'quay lai.{0,20}cuon.{0,20}(re|dat|so sanh)'
        )
        if _cmp_re.search(_mn_od) and len(_ocr_det_log) >= 2:
            # xác định rẻ hay đắt
            _want_cheapest = any(kw in _mn_od for kw in ["re hon", "re nhat", "re nhat", "it tien"])
            _want_dearest  = any(kw in _mn_od for kw in ["dat hon", "dat nhat", "nhieu tien", "mac nhat"])
            _sorted_od = sorted(_ocr_det_log, key=lambda b: float(b.get("price") or 0))
            _cheapest_od = _sorted_od[0]
            _dearest_od  = _sorted_od[-1]
            _lines_cmp = ["**So sánh giá các sách đã quét:**"]
            for _i, _b in enumerate(_ocr_det_log, 1):
                _p = float(_b.get("price") or 0)
                _tag = ""
                if _b["title"] == _dearest_od["title"]: _tag = " ⭐ Đắt nhất"
                elif _b["title"] == _cheapest_od["title"]: _tag = " ✅ Rẻ nhất"
                _lines_cmp.append(f"{_i}. **{_b.get('title','?')}** — {_p:,.0f}đ{_tag}")
            if _want_dearest:
                _lines_cmp.append(f"\n➡️ Cuốn đắt nhất: **{_dearest_od.get('title','?')}** ({float(_dearest_od.get('price',0)):,.0f}đ)")
            else:
                _lines_cmp.append(f"\n➡️ Cuốn rẻ nhất: **{_cheapest_od.get('title','?')}** ({float(_cheapest_od.get('price',0)):,.0f}đ)")
            return "\n".join(_lines_cmp), tone, intent, [], [], True

        # BUG-6 FIX: "Cuốn đầu tiên tôi quét, gợi ý tương tự"
        _first_ocr_re = _re_ocr_det.compile(r'cuon dau tien.{0,20}(quet|scan|upload|chup)')
        if _first_ocr_re.search(_mn_od) and _ocr_det_log:
            _first_book = _ocr_det_log[0]
            context["last_search_query"] = _first_book.get("title", "")
            context["last_found_title"]  = _first_book.get("title", "")
            # Không return – để process_for_stream tìm sách tương tự
    # ── end OCR-DETERMINISTIC ────────────────────────────────────────────────


    # ── EARLY EXITS (template, no LLM needed) ────────────────
    # ── [V6 FIX] EARLY EXIT for Negative Sentiment ────────────────
    # QUAN TRỌNG: _ai() đã normalize về ASCII không dấu → phải dùng chuỗi ASCII ở đây
    _SENTIMENT_NEG_KW = [
        "tuc thiet",  # tức thiệt
        "ghet",       # ghét
        "chua giao",  # chưa giao
        "te qua",     # tệ quá
        "that vong",  # thất vọng
        "buc minh",   # bực mình
        "giao cham",  # giao chậm
        "chan qua",   # chán quá
        "tuc qua",    # tức quá
        "kho chiu",   # khó chịu
        # NOTE: "that bai" đã xóa - không phải complaint signal (query đơn 'giao thất bại' sẽ bị false-positive)
        "vo ly",      # vô lý
        "kem chat luong", # kém chất lượng
        "giao tre",   # giao trễ
        "giao muon",  # giao muộn
    ]
    _mn_neg = _ai(message)  # normalize về ASCII không dấu để so keyword
    # Guard: không trigger complaint nếu message là câu tra cứu đơn hàng
    _ORDER_QUERY_KW_NEG = [
        "co don", "don hang", "lich su", "trang thai don", "don nao",
        "giao that bai", "that bai giao", "don that bai",
        "tra hang", "hoan tra", "don huy", "da huy",
    ]
    _is_order_query_neg = any(kw in _mn_neg for kw in _ORDER_QUERY_KW_NEG)
    if not _is_order_query_neg and any(kw in _mn_neg for kw in _SENTIMENT_NEG_KW):
        return (
            "BookStore xin lỗi vì bạn có trải nghiệm không vui. "
            "Chúng tôi chân thành xin lỗi vì sự bất tiện này! "
            "Bạn có thể cung cấp mã đơn hàng để tôi kiểm tra ngay, hoặc liên hệ CSKH: **0353260721** (8h–22h) để được hỗ trợ nhanh nhất.",
            tone, "sentiment_negative", [], [], True
        )

    # ── [FIX] BOOK DETAIL HANDLER trong process_for_stream ───────────────────────
    # book_detail với book_title đã biết → query DB trực tiếp, không để LLM hallucinate
    # [FIX] Guard: ONLY skip book_detail khi user đang BROWSE (xem thêm sách loại X)
    # KHÔNG skip khi hỏi thông tin chi tiết sách (thể loại gì, giá bao nhiêu, còn hàng không)
    # Lỗi cũ: "the loai" trong "la the loai gi" → false positive → book_detail bị skip
    _BD_BROWSE_KW = ["xem them sach", "tim them sach", "them sach", "gioi thieu them",
                     "muon xem them", "sach khac", "cuon khac", "goi y them",
                     "tim sach tuong tu", "sach tuong tu"]
    _is_browse_signal = any(kw in _mn_early for kw in _BD_BROWSE_KW)
    if intent == "book_detail" and entities.get("book_title") and not _is_browse_signal:
        _bd_title = entities["book_title"]

        # [FIX] Không dùng get_book_price() vì strategy-2 match từng từ đơn → sai
        # Dùng query riêng: title PHẢI chứa TẤT CẢ các từ >= 4 ký tự trong _bd_title
        def _strict_book_lookup(title_q: str) -> dict | None:
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection as _gc_strict
                _conn_s = _gc_strict()
                _cur_s = _conn_s.cursor(dictionary=True)
                # Bước 1: LIKE chứa toàn bộ chuỗi (exact-ish)
                _cur_s.execute(
                    "SELECT b.book_id, b.title, b.price, b.stock_quantity, b.avg_rating, "
                    "       a.author_name AS author, c.category_name AS genre "
                    "FROM books b "
                    "LEFT JOIN authors a ON b.author_id = a.author_id "
                    "LEFT JOIN book_categories bc ON bc.book_id = b.book_id "
                    "LEFT JOIN categories c ON c.category_id = bc.category_id "
                    "WHERE b.title LIKE %s AND b.status='active' "
                    "ORDER BY CASE WHEN b.title = %s THEN 0 ELSE 1 END, LENGTH(b.title) ASC LIMIT 1",
                    (f"%{title_q}%", title_q)
                )
                _row_s = _cur_s.fetchone()
                if _row_s:
                    _cur_s.close(); _conn_s.close()
                    return _row_s
                # Bước 2: Tất cả các từ >= 4 ký tự phải có trong title
                _words_s = [w for w in title_q.split() if len(w) >= 4]
                if len(_words_s) >= 2:
                    _cond_s = " AND ".join(["b.title LIKE %s"] * len(_words_s))
                    _params_s = [f"%{w}%" for w in _words_s]
                    _cur_s.execute(
                        f"SELECT b.book_id, b.title, b.price, b.stock_quantity, b.avg_rating, "
                        f"       a.author_name AS author, c.category_name AS genre "
                        f"FROM books b "
                        f"LEFT JOIN authors a ON b.author_id = a.author_id "
                        f"LEFT JOIN book_categories bc ON bc.book_id = b.book_id "
                        f"LEFT JOIN categories c ON c.category_id = bc.category_id "
                        f"WHERE ({_cond_s}) AND b.status='active' "
                        f"ORDER BY b.avg_rating DESC LIMIT 1",
                        _params_s
                    )
                    _row_s2 = _cur_s.fetchone()
                    if _row_s2:
                        _cur_s.close(); _conn_s.close()
                        return _row_s2
                _cur_s.close(); _conn_s.close()
                return None
            except Exception:
                return None

        _bd_book = _strict_book_lookup(_bd_title)
        if _bd_book:
            _bd_price = float(_bd_book.get("price") or 0)
            _bd_author = _bd_book.get("author") or ""
            _bd_genre = _bd_book.get("genre") or ""
            _bd_rating = float(_bd_book.get("avg_rating") or 0)
            _bd_stock = int(_bd_book.get("stock_quantity") or 0)
            _bd_title_disp = _bd_book.get("title", _bd_title)
            _bd_lines = [f"📖 **{_bd_title_disp}**"]
            if _bd_author:  _bd_lines.append(f"• **Tác giả**: {_bd_author}")
            if _bd_genre:   _bd_lines.append(f"• **Thể loại**: {_bd_genre}")
            _bd_lines.append(f"• **Giá**: {_bd_price:,.0f}đ")
            if _bd_rating:  _bd_lines.append(f"• **Đánh giá**: ★ {_bd_rating:.1f}/5")
            _bd_avail = "✅ Còn hàng" if _bd_stock > 0 else "❌ Tạm hết hàng"
            _bd_lines.append(f"• **Tình trạng**: {_bd_avail}")
            _bd_lines.append("\nBạn muốn mua ngay, xem đánh giá, hay tìm sách tương tự?")
            context["last_found_title"] = _bd_title_disp
            if _bd_genre:
                # [FIX] Lưu genre vào context → "sách tương tự" sau đó dùng genre-based search
                context["last_genre"]    = _bd_genre
                context["last_category"] = _bd_genre
            if _bd_author:
                context["last_author_name"] = _bd_author
            return "\n".join(_bd_lines), tone, "book_detail", _make_book_buttons([_bd_book]), ["mysql:books"], True
        else:
            # Không tìm thấy → MySQL LIKE keyword đầu (>= 4 ký tự)
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection as _gc_bd
                _conn_bd = _gc_bd()
                _cur_bd = _conn_bd.cursor(dictionary=True)
                _kw_bd = next((w for w in _bd_title.split() if len(w) >= 4), "")
                if _kw_bd:
                    _cur_bd.execute(
                        "SELECT book_id, title, price, avg_rating, stock_quantity FROM books "
                        "WHERE title LIKE %s AND status='active' ORDER BY avg_rating DESC LIMIT 3",
                        (f"%{_kw_bd}%",)
                    )
                    _bd_similar = _cur_bd.fetchall()
                else:
                    _bd_similar = []
                _cur_bd.close(); _conn_bd.close()
            except Exception:
                _bd_similar = []
            if _bd_similar:
                _sim_lines = "\n".join(
                    f"{i+1}. **{b['title']}** – {float(b['price'] or 0):,.0f}đ"
                    + (f" | ★ {float(b['avg_rating']):.1f}" if b.get("avg_rating") else "")
                    for i, b in enumerate(_bd_similar)
                )
                return (
                    f"Không tìm thấy **\"{_bd_title}\"** chính xác trong hệ thống.\n\n"
                    f"Một số sách có tên tương tự:\n{_sim_lines}",
                    tone, "book_detail", _make_book_buttons(_bd_similar), ["mysql:books"], True
                )
            return (
                f"Xin lỗi, không tìm thấy **\"{_bd_title}\"** trong hệ thống.\n\n"
                f"Thử tìm với tên khác, hoặc hotline **0353260721** (8h–22h).",
                tone, "book_detail", [], ["mysql:books"], True
            )


    if intent in HARD_ESCALATE_INTENTS:
        prefixes = {
            "complaint_damaged": "Rất tiếc khi nghe sách bạn nhận bị hư hỏng! ",
            "complaint_wrong":   "Xin lỗi vì đã giao sai sách cho bạn! ",
            "payment_issue":     "Tôi rất tiếc vì bạn gặp sự cố thanh toán! ",
        }
        return prefixes.get(intent, "") + ESCALATE_MSG, tone, intent, [], ["escalate:cskh"], True

    if intent in SOFT_ESCALATE_INTENTS or intent == "account_help":
        # FIX P1: Phân biệt guest muốn mua không cần đăng nhập vs hỗ trợ tài khoản
        _mn_ah = _ai(message)
        _GUEST_BUY_KW = [
            "chua dang nhap", "chua co tai khoan", "chua dang ky",
            "khong dang nhap", "chua login", "guest",
            "co the mua", "mua duoc khong", "co mua duoc",
        ]
        if any(kw in _mn_ah for kw in _GUEST_BUY_KW) and any(
            kw in _mn_ah for kw in ["mua", "dat hang", "thanh toan", "order"]
        ):
            _reg_btn = NavigateButton(label="📝 Đăng ký tài khoản", url="/login?tab=register", type="page")
            _login_btn = NavigateButton(label="🔑 Đăng nhập", url="/login", type="page")
            return (
                "✅ **Bạn hoàn toàn có thể mua sách mà không cần đăng nhập** (mua với tư cách khách).\n\n"
                "Tuy nhiên, khi **đăng ký tài khoản** bạn sẽ được:\n"
                "• 📦 Theo dõi đơn hàng dễ dàng\n"
                "• 🎁 Tích lũy điểm & nhận khuyến mãi sớm\n"
                "• 📋 Lưu lịch sử & địa chỉ giao hàng\n\n"
                "👉 Bạn muốn đăng ký hay mua ngay không?",
                tone, intent, [_reg_btn, _login_btn], [], True
            )
        btn = NavigateButton(label="👤 Đi đến Trang tài khoản", url="/account", type="page")
        return TEMPLATES["account_help_guide"], tone, intent, [btn], [], True

    if intent == "chitchat":
        # [FIX] Genre rescue: nếu message là tên thể loại bị NLU classify nhầm thành chitchat
        # (VD: "Lịch sử", "Truyện tranh", "Tiểu thuyết" click từ genre picker)
        _genre_rescue_ch = resolve_genre_alias(message)
        if _genre_rescue_ch:
            intent = "recommend_category"
            entities["genre"] = _genre_rescue_ch
            nlu_result.confidence = 1.0
            context.pop("pending_slot_filling",  None)
            context.pop("pending_intent_confirm", None)
            # Không return → fall-through xuống recommend_category handler bên dưới
        else:
            # FIX P0: Wrap _handle_chitchat trong try/except để tránh exception crash toàn bộ
            try:
                _mn_ch = _ai(message)
                # Bot identity questions → static answer, không gọi LLM
                _BOT_ID_KW = ["do ai tao", "ai lam ra", "ai tao", "ai lap trinh", "ai thiet ke",
                              "created by", "who made", "who built"]
                if any(kw in _mn_ch for kw in _BOT_ID_KW):
                    return (
                        "Tôi là **Trợ lý BookStore** 🤖, được phát triển để hỗ trợ bạn mua sắm sách tiện lợi hơn.\n\n"
                        "Tôi có thể giúp bạn:\n"
                        "• 🔍 Tìm sách theo tên, tác giả, thể loại\n"
                        "• 📦 Tra cứu đơn hàng và trạng thái giao hàng\n"
                        "• 💳 Thông tin thanh toán và khuyến mãi\n"
                        "• 🔄 Chính sách đổi trả\n\n"
                        "Bạn cần giúp gì hôm nay?",
                        tone, intent, [], [], True
                    )
                # Acknowledgement ("thôi được rồi", "ok rồi", "hiểu rồi") → short response
                _ACK_KW = ["thoi duoc", "ok roi", "hieu roi", "ra roi", "biet roi",
                           "duoc roi", "oke", "got it", "noted", "clear"]
                if any(kw in _mn_ch for kw in _ACK_KW):
                    return (
                        "Vui lòng liên hệ nếu bạn cần thêm hỗ trợ! 😊\n"
                        "📞 Hotline: **0353260721** (8h–22h)",
                        tone, intent, [], [], True
                    )
                return _handle_chitchat(message, user_id), tone, intent, [], [], True
            except Exception as _e_ch:
                _log.error("chitchat handler error: %s", _e_ch, exc_info=True)
                return (
                    "Xin chào! Tôi là trợ lý BookStore. Bạn cần tìm sách, tra đơn hàng hay hỗ trợ gì không?",
                    tone, intent, [], [], True
                )

    if intent == "out_of_scope":
        # [FIX] Trước khi reject → thử resolve genre (VD: user gõ "Nấu ăn", "Lịch sử"...)
        _genre_rescue = resolve_genre_alias(message)
        if _genre_rescue:
            # Override intent → recommend_category và fall-through xuống handler bên dưới
            intent = "recommend_category"
            entities["genre"] = _genre_rescue
            nlu_result.confidence = 1.0
            context.pop("pending_slot_filling",  None)
            context.pop("pending_intent_confirm", None)
            # không return → fall-through xuống recommend_category handler
        else:
            # FAIL-7 FIX: Luôn trả template, không để LLM bịa thông tin ngoài phạm vi
            return TEMPLATES["out_of_scope"], tone, intent, [], [], True

    if intent in ("store_info", "shipping_info"):  # FIX: interceptor routes shipping_info but template was only in store_info
        import unicodedata as _ud_si
        _mls_si = "".join(
            c for c in _ud_si.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_si.category(c) != "Mn"
        )
        # FIX-H03: ship về tỉnh thành → trả thẳng thông tin vận chuyển
        _SHIP_KW = ["ship", "van chuyen", "giao hang", "phi ship", "phi giao",
                    "may ngay", "bao lau giao", "bao nhieu ngay",
                    "mat may ngay", "mat bao lau", "bao lau", "het bao lau",
                    "khi nao nhan", "bao nhieu ngay giao", "toc do giao"]
        if any(kw in _mls_si for kw in _SHIP_KW):
            # FIX P1: City-specific shipping time mapping
            _CITY_TIME_MAP = {
                "ha noi": "1–2", "noi thanh": "1–2",
                "tp hcm": "1–2", "tp.hcm": "1–2", "ho chi minh": "1–2", "hcm": "1–2",
                "binh duong": "1–2", "dong nai": "1–2",
                "da nang": "2–3", "hue": "2–3", "hai phong": "2–3",
                "can tho": "2–3", "vung tau": "2–3", "vinh": "3–4",
                "nha trang": "3–4", "buon ma thuot": "3–4",
            }
            _detected_city = ""
            _ship_days = "3–5"
            for _ck, _cd in _CITY_TIME_MAP.items():
                if _ck in _mls_si:
                    _detected_city = _ck.title()
                    _ship_days = _cd
                    break
            _city_label = f"về **{_detected_city}**" if _detected_city else "nội địa"
            _ship_btns = []
            if any(kw in _mls_si for kw in ["mua", "gio hang", "dat hang"]):
                _lsb_ship = context.get("last_shown_books", [])
                for _sb in _lsb_ship[:2]:  # Limit to max 2 buttons to avoid clutter
                    if _sb.get("book_id"):
                        _t_short = _sb.get("title", "")[:35] + ("..." if len(_sb.get("title", "")) > 35 else "")
                        _ship_btns.append(NavigateButton(label=f"🛒 Xem & Mua: {_t_short}", url=f"/book/{_sb.get('book_id')}", type="book"))
            return (
                f"**Thời gian giao hàng BookStore {_city_label}:**\n\n"
                f"⏰ Ước tính: **{_ship_days} ngày làm việc** kể từ khi đơn xác nhận\n\n"
                f"💰 **Phí vận chuyển:**\n"
                f"  • Miễn phí với đơn từ **150,000đ**\n"
                f"  • Dưới 150,000đ: **15,000đ–30,000đ** tùy khu vực\n\n"
                f"🚚 Đơn vị: **GHTK, GHN, ViettelPost**\n"
                f"📞 Hỗ trợ: **0353260721** (8h–22h)",
                tone, intent, _ship_btns, [], True
            )
        _aw_si = ["o dau", "o dau", "dia chi", "dia chi",
                  "nam o", "nam o", "vi tri", "vi tri", "cho nao", "chi nhanh"]
        if any(w in _mls_si for w in _aw_si):
            return (
                "**Thông tin BookStore:**\n"
                "• Hotline: 0353260721 (miễn phí, 8h-22h)\n"
                "• Email: cskh@bookstore.vn\n"
                "• Website: www.bookstore.vn\n"
                "• Giờ hỗ trợ: Thứ 2-Chủ nhật, 8h-22h\n"
                "BookStore là cửa hàng trực tuyến!"
            ), tone, intent, [], [], True
        # Tier1 - ship / store info KB
        return (
            "**Thông tin liên hệ BookStore:**\n"
            "• Hotline: 0353260721 (miễn phí, 8h-22h)\n"
            "• Email: cskh@bookstore.vn\n"
            "• Website: www.bookstore.vn\n"
            "• Giờ hỗ trợ: Thứ 2-Chủ nhật, 8h-22h",
            tone, intent, [], [], True
        )

    if intent == "recommend_trending":
        books = await _run_in_executor(_safe_search, "sách bán chạy được yêu thích nhất", 8)
        if not books:
            return _OS_DOWN_MSG, tone, intent, [], [], True
        display_books = _filter_and_track_books(books, context, max_items=4)
        if display_books:
            _tr_lines = "\n".join(
                f"{i+1}. **{b.get('title', 'Sách')}** – {float(b.get('price') or 0):,.0f}đ"
                + (f" | ★ {float(b.get('rating') or b.get('avg_rating') or 0):.1f}"
                   if (b.get('rating') or b.get('avg_rating')) else "")
                for i, b in enumerate(display_books)
            )
            _tr_answer = (
                f"🔥 **Top {len(display_books)} sách đang hot tại BookStore:**\n\n"
                f"{_tr_lines}\n\n"
                "Bạn muốn: xem chi tiết, lọc theo thể loại, hay tìm sách tương tự?"
            )
        else:
            _tr_answer = "Hiện tôi chưa tìm được sách hot. Bạn thử tìm theo thể loại nhé!"
        btns = _make_book_buttons(display_books)
        return _tr_answer, tone, intent, btns, ["opensearch:books"], True

    if intent == "recommend_personal":
        if not user_id:
            books = await _run_in_executor(_safe_search, "sách bán chạy này này được đọc nhiều", 8)
            if not books:
                return _OS_DOWN_MSG, tone, intent, [], [], True
            display_books = _filter_and_track_books(books, context, max_items=4)
            answer = "📚 Dưới đây là một số sách bạn có thể thích — xem chi tiết ngay bên dưới nhé!"
            return answer, tone, intent, _make_book_buttons(display_books), ["opensearch:books"], True

        genres = user_profile.get("favorite_genres", [])
        genre  = genres[0] if genres else ""
        fetch = _run_in_executor(get_books_by_genre, genre, 8) if genre else _run_in_executor(_safe_search, "sách bán chạy hay đọc nhiều", 8)
        books = await fetch
        if not books:
            return _OS_DOWN_MSG, tone, intent, [], [], True
        display_books = _filter_and_track_books(books, context, max_items=4)
        genre_hint = f" thể loại **{genre}**" if genre else ""
        answer = f"📖 Dựa trên sở thích của bạn, đây là những cuốn sách{genre_hint} được gợi ý:"
        # Lưu genre vào context để interceptor "xem thêm" biết thể loại hiện tại
        if genre:
            context["last_genre"] = genre
            context["last_category"] = genre
        return answer, tone, intent, _make_book_buttons(display_books), ["mysql:books"], True

    if intent in ("recommend_combo", "recommend_category"):
        # ── [FIX] Phát hiện "Đổi thể loại khác" → clear genre context + show genre picker ─
        import unicodedata as _ud_gc2
        _mn_gc2 = "".join(
            c for c in _ud_gc2.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_gc2.category(c) != "Mn"
        )
        _CHANGE_GENRE_KW = [
            "doi the loai khac", "the loai khac", "thay the loai",
            "doi sang the loai", "doi chu de", "chu de khac",
            "muon xem the loai khac", "xem the loai khac",
            "kham pha the loai khac",
        ]
        _is_change_genre = (
            intent == "recommend_category"
            and (
                context.pop("_force_genre_picker", False)  # Từ Interceptor 3 (nút "Đổi thể loại khác")
                or any(kw in _mn_gc2 for kw in _CHANGE_GENRE_KW)  # User tự gõ
            )
        )
        if _is_change_genre:
            # Xóa genre context cũ để tránh reuse
            _old_genre = context.pop("last_genre", "") or context.pop("last_category", "")
            import random
            _ALL_GENRES_CG = [
                "Văn học", "Kinh doanh", "Kỹ năng sống", "Thiếu nhi", "Lịch sử",
                "Tâm lý học", "Tiểu thuyết", "Khoa học", "Nấu ăn", "Y học",
                "Truyện tranh", "Kinh dị", "Lập trình", "Triết học", "Đầu tư", "Marketing"
            ]
            # Loại bỏ thể loại vừa xem — guard khi _old_genre="" để tránh filter hết
            _pool_cg = [g for g in _ALL_GENRES_CG if not _old_genre or _old_genre.lower() not in g.lower()]
            _shuffled_cg = random.sample(_pool_cg, 4) if len(_pool_cg) >= 4 else _pool_cg
            _picker_btns = [NavigateButton(label=g, url="", type="quick_reply") for g in _shuffled_cg]
            _picker_btns.append(NavigateButton(label="Khám phá thêm thể loại khác", url="", type="quick_reply"))
            _picker_q = (
                f"Bạn muốn khám phá sách về mảng nào?\n"
                f"Chúng tôi có nhiều đầu sách hay thuộc các lĩnh vực như "
                f"**{', '.join(_shuffled_cg)}**... Hoặc gõ tên thể loại bạn muốn tìm!"
            )
            # [FIX] Set pending_slot_filling để message tiếp theo (VD: "Nấu ăn") được xử lý đúng
            context["pending_slot_filling"] = {
                "target_intent":    "recommend_category",
                "original_message": message,
                "filled_entities":  {},
                "required_slot":    "genre",
            }
            return _picker_q, tone, intent, _picker_btns, [], True

        genre = entities.get("genre")
        # Không reuse last_category khi user gửi message mới về recommend_category
        # (chỉ reuse nếu message là follow-up ngắn, không phải yêu cầu mới)
        if not genre and context.get("last_category") and len(message.split()) <= 4:
            genre = context["last_category"]

        # ── [SIMILAR BOOKS] Semantic search theo title khi flag _find_similar_to được set ──
        _similar_to = context.pop("_find_similar_to", None)
        if _similar_to:
            _sim_results = await _run_in_executor(_safe_search, _similar_to, 16)
            # Loại bỏ chính cuốn sách đó khỏi kết quả
            _sim_results = [b for b in _sim_results if b.get("title","").lower() != _similar_to.lower()]
            # [FIX] Lọc sách có cùng thể loại nếu có context thể loại (tránh sách không liên quan)
            _ref_genre = context.get("last_category") or context.get("last_genre")
            if not _ref_genre:
                # Thử lấy genre từ last_shown_books[0]
                _lsb0 = (context.get("last_shown_books") or [{}])[0]
                _ref_genre = _lsb0.get("category") or _lsb0.get("category_name")
            if _ref_genre:
                _genre_filtered = [b for b in _sim_results
                                   if _ref_genre.lower() in (b.get("category") or b.get("category_name") or "").lower()]
                # Chỉ dùng genre filter nếu có ≥ 3 kết quả, tránh mất quá nhiều
                if len(_genre_filtered) >= 3:
                    _sim_results = _genre_filtered
            _sim_display = _filter_and_track_books(_sim_results, context, max_items=4)
            if _sim_display:
                _sim_list = "\n".join(
                    f"• **{b.get('title','')}** – {float(b['price']):,.0f}đ"
                    + (f" | ★ {b['rating']}" if b.get("rating") else "")
                    for b in _sim_display
                )
                _loop = context.pop("_loop_notice", False)
                if _loop:
                    _sim_header = f"📖 Đây là các sách tương tự với **{_similar_to}** (đã quay lại từ đầu danh sách):\n\n"
                else:
                    _sim_header = f"📖 Các sách tương tự với **{_similar_to}**:\n\n"
                _sim_ans = _sim_header + _sim_list + "\n\nBạn muốn xem chi tiết cuốn nào?"
                _sim_btns = _make_book_buttons(_sim_display)
                return _sim_ans, tone, intent, _sim_btns, ["opensearch:books"], True
            elif genre or context.get("last_genre"):
                # Không tìm được sách tương tự → fallback về genre
                pass  # tiếp tục xuống logic genre bên dưới
            else:
                return (
                    f"Rất tiếc, tôi chưa tìm được sách tương tự với **{_similar_to}** trong kho. "
                    "Bạn muốn tìm theo thể loại khác không?",
                    tone, intent, [], [], True
                )

        if genre:
            books = await _run_in_executor(get_books_by_genre, genre, 10)
            context["last_category"] = genre
        else:
            books = await _run_in_executor(_safe_search, message, 10)

        if not books:
            return _OS_DOWN_MSG, tone, intent, [], [], True
        display_books = _filter_and_track_books(books, context, max_items=4)

        # [FIX] Dùng template thay LLM để tránh hallucinate và câu bị cắt
        genre_label = genre if genre else "phù hợp"
        if context.pop("_loop_notice", False):
            _stream_answer = (
                f"Tất cả **{len(books)} sách {genre_label}** trong kho đều đã được hiển thị.\n\n"
                "Dưới đây là các sách phổ biến nhất. Bạn muốn xem chi tiết hay tìm thể loại khác?"
            )
        else:
            _stream_answer = (
                f"📚 Dưới đây là **{len(display_books)} sách {genre_label}** được nhiều người yêu thích:\n\n"
                "Bạn muốn xem chi tiết, lọc theo giá hay tìm thể loại khác?"
            )
        _stream_btns = _make_book_buttons(display_books)
        _stream_btns.append(NavigateButton(label="Đổi thể loại khác", url="", type="quick_reply"))
        return _stream_answer, tone, intent, _stream_btns, ["opensearch:books", "mysql:books"], True

    if intent == "recommend_gift":
        recipient = entities.get("recipient_type", "adult")
        price_max = entities.get("price_max") or entities.get("budget", 300_000)
        # [SYNC với _process_inner] Search terms tinh chỉnh, multi-query, template response
        _gift_genre_map = {
            "child_0_6":    ["sách tranh thiếu nhi", "ehon trẻ em"],
            "child_7_12":   ["thiếu nhi 7 tuổi", "truyện thiếu nhi"],
            "teenager":     ["kỹ năng sống thiếu niên", "sách teen"],
            "adult_female": ["tâm lý phụ nữ", "văn học lãng mạn"],
            "adult_male":   ["kinh doanh phát triển bản thân", "kỹ năng lãnh đạo"],
            "adult":        ["kỹ năng sống", "phát triển bản thân"],
            "elderly":      ["hồi ký", "sức khỏe người cao tuổi"],
            "Con nhỏ (0-6 tuổi)":   ["sách tranh thiếu nhi", "ehon trẻ em 0-6 tuổi"],
            "Trẻ em (7-12 tuổi)":   ["thiếu nhi", "truyện tranh thiếu nhi kỹ năng"],
            "Thiếu niên":           ["kỹ năng sống thiếu niên", "sách teen phát triển"],
            "Bạn gái/Phụ nữ":       ["tâm lý phụ nữ", "văn học phụ nữ"],
            "Bạn trai/Nam giới":    ["phát triển bản thân", "kinh doanh khởi nghiệp"],
            "Người lớn tuổi":       ["hồi ký", "sức khỏe tuổi trung niên"],
        }
        _gift_queries = _gift_genre_map.get(recipient, ["kỹ năng sống"])
        if isinstance(_gift_queries, str):
            _gift_queries = [_gift_queries]

        context.pop("last_shown_book_ids", None)
        _gift_books: list = []
        for _gq in _gift_queries:
            _ghits = await _run_in_executor(lambda q=_gq: _safe_search(q, top_k=10, price_max=price_max))
            for _gh in _ghits:
                if _gh not in _gift_books:
                    _gift_books.append(_gh)
            if len(_gift_books) >= 6:
                break
        _gift_display = _filter_and_track_books(_gift_books, context, max_items=4)

        _recipient_label = recipient if len(recipient) < 30 else _gift_queries[0]
        if _gift_display:
            _book_list = "\n".join(
                f"• **{b.get('title', '')}** – {b.get('price', 0):,.0f}đ"
                for b in _gift_display
            )
            _gift_answer = (
                f"🎁 Gợi ý sách tặng cho **{_recipient_label}**:\n\n"
                f"{_book_list}\n\n"
                "Bạn muốn xem chi tiết hoặc điều chỉnh theo ngân sách?"
            )
        else:
            _gift_answer = (
                f"Rất tiếc, tôi chưa tìm được sách phù hợp để tặng cho **{_recipient_label}**. "
                "Bạn có thể thử thể loại khác hoặc đặt ngân sách rộng hơn nhé!"
            )
        return _gift_answer, tone, intent, _make_book_buttons(_gift_display), ["opensearch:books"], True

    # ── Mọi intent còn lại → delegate về process() thông thường ─
    # Đồng bộ lại intent đã bị override trước khi gọi _process_inner
    nlu_result.intent = intent
    answer, sources, btns = await process(
        message=message, nlu_result=nlu_result, user_id=user_id,
        context=context, history=history, user_profile=user_profile,
    )
    return answer, tone, intent, btns, sources, True




async def process(
    message:      str,
    nlu_result:   NLUResult,
    user_id:      int | None,
    context:      dict,
    history:      list[dict],
    user_profile: dict,
) -> tuple[str, list[str], list[NavigateButton]]:
    """
    Xử lý message và trả về (answer, sources, navigate_buttons).
    """
    # FIX P0: Global safety net — bắt mọi exception không được handle ở handler con
    try:
        return await _process_inner(message, nlu_result, user_id, context, history, user_profile)
    except Exception as _e_global:
        _log.error("process() uncaught exception: %s", _e_global, exc_info=True)
        return (
            "Xin lỗi, tôi chưa thể xử lý yêu cầu này lúc này. "
            "Vui lòng thử lại, hoặc liên hệ **0353260721** để được hỗ trợ ngay.",
            [], []
        )


async def _process_inner(
    message:      str,
    nlu_result:   NLUResult,
    user_id:      int | None,
    context:      dict,
    history:      list[dict],
    user_profile: dict,
) -> tuple[str, list[str], list[NavigateButton]]:
    """
    Xử lý message và trả về (answer, sources, navigate_buttons).
    """
    intent    = nlu_result.intent
    entities  = nlu_result.entities
    sentiment = nlu_result.sentiment or "NEUTRAL"
    tone      = user_profile.get("tone", "thân thiện, ngắn gọn")

    # ══ [FIX ABSOLUTE] ORDINAL PAIR → force book_compare ngay khi vào _process_inner ══
    import unicodedata as _ud_op_pi
    _mn_op_pi = "".join(
        c for c in _ud_op_pi.normalize("NFD", message.lower().replace("đ","d"))
        if _ud_op_pi.category(c) != "Mn"
    )
    _OP_PI_KW = ["cuon 1","thu nhat","thu 1","cuon 2","thu hai","thu 2",
                 "cuon 3","thu ba","thu 3","cuon 4","thu tu","thu 4"]
    _op_pi_found = [o for o in _OP_PI_KW if o in _mn_op_pi]
    _op_pi_nums  = {o.split()[1] if len(o.split()) > 1 else o for o in _op_pi_found}
    if "so sanh" in _mn_op_pi and len(_op_pi_nums) >= 2:
        intent = "book_compare"
        nlu_result.intent = "book_compare"
        nlu_result.confidence = 0.99

    # ── [FRESH OCR GATE] ──────────────────────────────────────────────────────

    # Key existence check (không phải truthiness) để phân biệt ảnh mới vs follow-up text
    # "_ocr_data" in context → ảnh vừa upload (dù OCR fail trả {} vẫn tính là fresh)
    # "_ocr_data" NOT in context → tin nhắn text thuần túy
    _is_fresh_ocr: bool = "_ocr_data" in context

    # ── [FIX-H04] Early reject: "thêm vào đơn / sửa đơn hàng" ─────────────────
    import re as _re_h04, unicodedata as _ud_h04
    _mn_h04 = "".join(
        c for c in _ud_h04.normalize("NFD", message.lower().replace("đ","d"))
        if _ud_h04.category(c) != "Mn"
    )
    # FIX-HAL02: "Thêm vào giỏ giúp tôi" / "Mua cuốn X đó" → NavigateButton (không thể tự thêm giỏ)
    _CART_ADD_KW = ["them vao gio hang", "them vao gio", "them vao cart", "mua ngay giup", "them gio giup",
                   # FIX BUG T6 — SYNC với _CART_KW_SG trong process_for_stream
                   "muon mua cuon", "mua cuon re nhat", "mua cuon do",
                   "mua cuon nay", "mua cuon kia", "toi muon mua",
                   "cho toi mua", "dat mua cuon", "mua cuon vua",
                   "mua cuon dau", "mua cuon thu",
                   # Ordinal + cart patterns: "thêm cuốn thứ 2 vào giỏ"
                   "them cuon", "vao gio giup", "dat mua cuon thu",
                   "them vao gio cuon", "mua cuon thu"]
    # [FIX] Exclusion: skip cart-add block khi message có ngữ cảnh tặng quà / gợi ý
    # VD: "Tôi muốn mua sách tặng cho em gái" → recommend_gift, không phải cart-add
    _CART_GIFT_EXCL = ["tang cho", "tang qua", "lam qua", "goi y cho", "mua tang",
                       "em gai", "anh trai", "chi gai", "em trai", "ban be", "nguoi than",
                       "ba me", "bo me", "ong ba", "con", "chong", "vo"]
    _has_gift_ctx = any(kw in _mn_h04 for kw in _CART_GIFT_EXCL)
    if not _has_gift_ctx and any(kw in _mn_h04 for kw in _CART_ADD_KW):
        # Thử resolve ordinal reference từ last_shown_books ("cuốn thứ 2", "cuốn 2"...)
        _ORDINAL_CART = {
            "dau tien": 0, "thu nhat": 0, "cuon 1": 0, "thu 1": 0, "cai 1": 0,
            "thu hai":  1, "cuon 2":   1, "thu 2": 1,  "cai 2": 1,
            "thu ba":   2, "cuon 3":   2, "thu 3": 2,  "cai 3": 2,
            "thu tu":   3, "cuon 4":   3, "thu 4": 3,  "cai 4": 3,
        }
        _last_shown = context.get("last_shown_books", [])
        _ordinal_book = None
        for _ok, _oi in _ORDINAL_CART.items():
            if _ok in _mn_h04 and _oi < len(_last_shown):
                _ordinal_book = _last_shown[_oi]
                break

        _ocr_list = context.get("last_ocr_books", [])
        if _ordinal_book:
            _title_ref = _ordinal_book.get("title", "")
            _book_id   = _ordinal_book.get("book_id")
            # [FIX] book_id=None → lookup DB theo title
            if _title_ref and not _book_id:
                try:
                    from chatbot_app.retrieval.sql_retriever import get_connection as _gc_crt
                    _conn_crt = _gc_crt()
                    _cur_crt  = _conn_crt.cursor(dictionary=True)
                    _cur_crt.execute(
                        "SELECT book_id FROM books WHERE title = %s LIMIT 1", (_title_ref,)
                    )
                    _row_crt = _cur_crt.fetchone()
                    _cur_crt.close(); _conn_crt.close()
                    if _row_crt:
                        _book_id = _row_crt["book_id"]
                except Exception:
                    pass
        elif _ocr_list:
            _title_ref = _ocr_list[0].get("title")
            _book_id   = _ocr_list[0].get("book_id")
        elif _last_shown:
            _title_ref = _last_shown[0].get("title")
            _book_id   = _last_shown[0].get("book_id")
        else:
            _title_ref = None
            _book_id   = None

        if _title_ref and _book_id:
            return (
                f"🛒 Chatbot **không thể tự thêm** sách vào giỏ hàng của bạn.\n\n"
                f"Để thêm **\"{_title_ref}\"** vào giỏ, vui lòng nhấn nút bên dưới → vào trang sách → nhấn **\"Thêm vào giỏ hàng\"**:",
                [], [NavigateButton(label=f"📖 Xem & Mua: {_title_ref[:45]}", url=f"/book/{_book_id}", type="book")]
            )
        return (
            "🛒 Chatbot **chưa hỗ trợ** thêm sách vào giỏ hàng trực tiếp.\n"
            "Vui lòng vào trang sản phẩm và nhấn **\"Thêm vào giỏ hàng\"** nhé!\n\n"
            "_(Bạn có thể hỏi tên sách để tôi tìm link nhanh hơn)_",
            [], []
        )

    # FIX-H04: Regex match "thêm [X] vào đơn", "sửa đơn", không phụ thuộc context
    _MODIFY_ORDER_RE = _re_h04.compile(
        r'them.{0,30}vao\s*don|bo\s*sung.{0,25}(vao\s*)?don|'
        r'sua\s*don|chinh\s*sua\s*don|them\s*san\s*pham\s*vao\s*don|'
        r'them\s*vao\s*gio\s*don|cap\s*nhat\s*don\s*hang'
    )
    if _MODIFY_ORDER_RE.search(_mn_h04):
        return (
            "❌ **Không thể thêm/sửa đơn hàng đã tạo.**\n\n"
            "Sau khi đặt hàng, bạn **không thể thêm hoặc bớt sản phẩm** trong đơn đã xử lý.\n\n"
            "✅ **Để mua thêm sách:** Vui lòng tạo **đơn hàng mới** tại trang sản phẩm.\n"
            "📞 Hoặc liên hệ CSKH: **0353260721** (8h-22h) để được hỗ trợ."
        ), [], []

    # ── [v8] Coreference Resolution + Ordinal Resolver (process) ────────────────
    import unicodedata as _ud8p
    _mn7 = "".join(c for c in _ud8p.normalize("NFD", message.lower().replace("đ","d")) if _ud8p.category(c)!="Mn")
    _ck7 = [
        "tim hieu them","ve no","cuon do","cuon nay","chi tiet hon",
        "xem them","biet them","gia cuon","bao nhieu tien",
        "sahcs do","sach do","cuon sach do","no la",
        "cuon nay gia","sach nay gia","gia sach nay","cuon do gia",
        "gia cuon do","bao nhieu vay","sach vua roi","cuon vua xem",
        "cuon vua tim","sach vua tim","no gia","cuon tren gia",
        "gia cuon tren","cuon do bao nhieu","sach do bao nhieu",
        "cuon nay bao nhieu","gia cuon nay","cuon nay co","sach nay co",
        "cuon do co","no bao nhieu","gia no la","cai nay gia","cai do gia",
        # SYNC với process_for_stream — bắt các pattern giá rử trừng
        "gia bao nhieu","bao nhieu tien","gia la bao","gia the nao",
        "nen mua cuon nao","dat hay re",
    ]

    # ── Định nghĩa _ORDINAL_MAP_P SớM (trước mọi block dùng nó) ─────────────────────
    _ORDINAL_MAP_P = {
        "dau tien": 0, "thu nhat": 0, "cai 1": 0, "cuon 1": 0, "cai dau": 0, "thu 1": 0,
        "thu hai":  1, "cai 2":   1, "cuon 2": 1, "thu 2": 1,
        "thu ba":   2, "cai 3":   2, "cuon 3": 2, "thu 3": 2,
        "thu tu":   3, "cai 4":   3, "cuon 4": 3, "thu 4": 3,
    }

    # ── EARLY HANDLER: "gợi ý sách tương tự cuốn N tôi quét" ─────────────────────
    # Phải chạy TRƯỚC mọi coref block để ngăn "last_found_title" (đánh cắp) inject sách sai
    _ocr_books_early  = context.get("last_ocr_books", [])
    _is_ocr_ref_early = any(kw in _mn7 for kw in ["quet", "scan", "upload", "chup", "vua gui"])
    _is_sim_early     = any(kw in _mn7 for kw in [
        "tuong tu", "giong voi", "goi y sach", "sach lien quan",
        "sach kieu nhu", "goi y cuon sach", "de xuat sach",
    ])
    if _is_ocr_ref_early and _is_sim_early and _ocr_books_early:
        import re as _re_early_ord
        for _eord_key, _eord_idx in _ORDINAL_MAP_P.items():
            _eord_pat = r'\b' + _eord_key.replace(' ', r'\s+') + r'(?!\s*(ngay|thang|nam|phut|giay|gio))\b'
            if _re_early_ord.search(_eord_pat, _mn7) and _eord_idx < len(_ocr_books_early):
                entities["book_title"] = _ocr_books_early[_eord_idx].get("title", "")
                entities["query"]      = entities["book_title"]
                intent = "book_search"
                nlu_result.intent = "book_search"
                nlu_result.confidence = 0.99
                break

    # Guard: nếu message chứa ordinal keyword rõ ràng → bỏ qua coref Case-1a (ordinal block sẽ xử lý sau)
    _has_ordinal_in_msg = any(kw in _mn7 for kw in _ORDINAL_MAP_P.keys())
    if intent not in ("book_detail","book_availability","book_review","image_search",
                       # [FIX] Policy intents không được redirect sang book_detail
                       "return_policy","shipping_info","store_info","payment_method",
                       "promotion_current","return_request",
                       # [FIX] book_compare đã được set bởi ordinal pair detection → không override
                       "book_compare"):
        _has_ctx_book_p = bool(
            context.get("last_found_title") or context.get("last_search_query")
            or context.get("last_shown_books")
        )
        if any(t in _mn7 for t in _ck7) and _has_ctx_book_p and not _has_ordinal_in_msg:
            _lb8 = context.get("last_shown_books", [])
            _rf8 = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_lb8[0].get("title") if _lb8 else None)
            )
            if _rf8:
                intent = "book_detail"
                entities["book_title"] = _rf8
    elif intent in ("book_detail", "book_availability", "book_review") and not entities.get("book_title"):
        # [FIX] book_availability coref: "cuốn vừa quét còn hàng không?" → dùng last_found_title
        _coref_kw8 = [
            "cuon vua", "sach vua", "cuon nay", "sach nay", "cuon do", "sach do", "no con", "no bao nhieu",
            # BUG-02: short availability queries
            "con hang khong", "co hang khong", "con khong", "hang con khong",
            "sach do con", "cuon do con", "dat duoc khong", "cuon vua xem con",
            # FOLLOW-UP: publisher / author / detail queries (không có tên sách trong câu)
            "nha xuat ban", "nxb", "xuat ban boi", "ai xuat ban",
            "tac gia la", "tac gia nao", "ai viet", "nguoi viet",
            "mo ta sach", "noi dung sach", "tom tat", "giai thieu sach",
            "the loai nao", "thuoc the loai", "loai sach nao",
            "bao nhieu trang", "so trang", "day bao nhieu",
            "sach tren", "cuon tren", "sach vua xem", "cuon vua xem",
            "sach nay cua", "cuon nay cua", "cua no la",
        ]
        _has_coref8 = any(t in _mn7 for t in _ck7) or any(kw in _mn7 for kw in _coref_kw8)
        # Mở rộng: standalone price words cũng trigger coref nếu có context
        _PRICE_WORDS_P = [
            "gia", "bao nhieu", "cuon", "sach", "no", "cai do", "gia la", "con hang", "het hang",
            # BUG-02: thêm availability-specific
            "con hang", "co hang", "con stock", "het stock", "con trong kho",
        ]
        if _has_coref8 or any(kw in _mn7 for kw in _PRICE_WORDS_P):
            _lb8b = context.get("last_shown_books", [])
            _rf8 = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_lb8b[0].get("title") if _lb8b else None)
            )
            if _rf8:
                # [FIX] Không dùng coref chung nếu câu có chứa số thứ tự
                _is_ordinal_msg = any(kw in _mn7 for kw in _ORDINAL_MAP_P.keys())
                if not _is_ordinal_msg:
                    entities["book_title"] = _rf8
                # Nếu có context đủ → xóa pending slot-fill để không hỏi lại
                if context.get("pending_slot_filling", {}).get("target_intent") in ("book_detail","book_availability","book_compare"):
                    context.pop("pending_slot_filling",  None)
                    context.pop("pending_intent_confirm", None)

    _ORDINAL_MAP_P = {
        "dau tien": 0, "thu nhat": 0, "cai 1": 0, "cuon 1": 0, "cai dau": 0, "thu 1": 0,
        "thu hai":  1, "cai 2":   1, "cuon 2": 1, "thu 2": 1,
        "thu ba":   2, "cai 3":   2, "cuon 3": 2, "thu 3": 2,
        "thu tu":   3, "cai 4":   3, "cuon 4": 3, "thu 4": 3,
    }  # ← reassignment an toàn (nước đôi khi một số path bỏ qua EARLY HANDLER trên)
    
    # [FIX] Phân biệt "cuốn đầu tiên tôi quét" (OCR) và "cuốn đầu tiên" (Search/Recommend)
    _is_ocr_ordinal = any(kw in _mn7 for kw in ["quet", "scan", "upload", "chup", "anh", "vua gui"])
    if _is_ocr_ordinal and context.get("last_ocr_books"):
        _last_books_p = context.get("last_ocr_books")
    else:
        _last_books_p = context.get("last_shown_books", [])

    _bt_val = entities.get("book_title", "").lower()
    import unicodedata as _ud_bt
    _bt_mn = "".join(c for c in _ud_bt.normalize("NFD", _bt_val.replace("đ","d")) if _ud_bt.category(c) != "Mn")
    _is_bt_ordinal = any(kw in _bt_mn for kw in _ORDINAL_MAP_P.keys())

    # [FIX] Không dùng single-book ordinal resolver khi intent đã là book_compare
    # (ordinal pair resolver trong book_compare handler sẽ xử lý cả 2 cuốn)
    if _last_books_p and (not entities.get("book_title") or _is_bt_ordinal) \
            and intent not in ("image_search", "book_compare"):

        import re as _re_ord
        for _ord_key_p, _idx_p in _ORDINAL_MAP_P.items():
            _pattern = r'\b' + _ord_key_p.replace(' ', r'\s+') + r'(?!\s*(ngay|thang|nam|phut|giay|gio))\b'
            if _re_ord.search(_pattern, _mn7) and _idx_p < len(_last_books_p):
                # OCR books typically have "title"
                entities["book_title"] = _last_books_p[_idx_p].get("title", "")
                
                # [FIX] Nếu câu có chứa "tương tự", "gợi ý", "thể loại" → đổi intent sang recommend
                _is_similar_req = any(kw in _mn7 for kw in ["tuong tu", "giong", "goi y", "lien quan", "the loai"])
                if _is_similar_req:
                    intent = "book_search"
                    entities["query"] = entities["book_title"]  # đảm bảo book_search dùng đúng sách
                else:
                    intent = "book_detail"
                nlu_result.confidence = 0.95
                break

    # ── OCR COMPARE: "hai cuốn trên/cuốn nào rẻ nhất/trong 3 cuốn/ mua cả N cuốn" ─────────
    # Phải trước pending checks để không bị intercept
    import re as _re_cmp
    _COMPARE_KW = [
        "hai cuon", "cuon nao re", "cuon nao dat", "so sanh", "cai nao re",
        "re nhat trong", "dat nhat trong", "cuon nao phu hop", "cai nao phu",
        "trong 3 cuon", "trong hai cuon", "trong 2 cuon",
        "ba cuon vua", "3 cuon vua", "cuon nao nen", "cuon nao tot hon",
        "cuon nao phu hop", "cai nao nen", "cai nao tot",
        # ACTION-2+3: tổng giá và đắt nhất
        "tong gia", "het bao nhieu tien", "dat nhat",
        "trong 3 cuon vua upload", "trong 2 cuon vua", "ba cuon tren",
        # FIX-B: Thêm keyword bắt "vừa quét", "cuốn trước"
        "cuon vua quet", "cuon vua scan", "cuon vua upload", "cuon vua chup",
        "hai cuon tren", "2 cuon tren", "ba cuon tren", "3 cuon do",
        "cuon nay va cuon truoc", "cuon truoc va cuon nay", "cuon truoc",
        "cuon vua chup va", "cuon toi vua", "hai cuon vua xem", "2 cuon vua xem",
        "phu hop hon", "cuon nao phu hop hon",
        # FIX-C: "tổng N cuốn" / "tổng cuốn" không có "tiền" vẫn phải vào compare block
        "tong cuon", "tong cac cuon", "tong tat ca cuon", "tong so cuon",
        "tat ca cuon", "tat ca sach", "cuon quet", "cuon scan",
        "tong tien cuon", "tong cac sach",
    ]
    # FIX-A: Phan biet cuon vua goi y (recommend) vs cuon vua quet (OCR)

    _RECOMMEND_REF_KW = [

        "vua goi y", "ban vua goi", "vua de xuat", "goi y vua roi",
        "trong cac cuon vua goi", "gia re nhat trong cac cuon vua goi",

    ]

    _is_about_recommend = any(kw in _mn7 for kw in _RECOMMEND_REF_KW)

    if _is_about_recommend:

        _ocr_books = context.get("last_shown_recommend") or context.get("last_shown_books", [])

    else:

        _ocr_books = context.get("last_ocr_books") or context.get("last_shown_books", [])

    # Check bang regex de phat hien pattern so linh hoat hon

    # FIX: Regex chạy trên _mn7 (ASCII-normalized) để tránh lỗi Unicode
    # "cuốn" (ố=U+1ED1) ≠ "ô" (U+00F4) khi search raw message — phải dùng ASCII "cuon"
    _compare_regex = _re_cmp.compile(
        r'(hai|2|ba|3|4|5)\s*cuon|'
        r'cuon\s*nao\s*(re|dat|mac|phu\s*hop|nen|tot)|'
        r'so\s*sanh|re\s*nhat|dat\s*nhat|mac\s*nhat|tong\s*gia|'
        r'vua\s*(quet|scan|upload|chup|xem)|'
        r'tong\s*(?:\d+\s*)?cuon'   # "tổng 3 cuốn", "tổng cuốn"
    )

    _is_compare = (any(kw in _mn7 for kw in _COMPARE_KW) or bool(_compare_regex.search(_mn7)))
    # [FIX] GUARD: "So sánh cuốn 1 và cuốn 2" → là book_compare intent (so sánh chi tiết 2 cuốn cụ thể)
    # KHÔNG phải price summary của toàn bộ list. Detect: có từ "so sanh" + 2 ordinal khác nhau
    _ORD_DETECT = ["cuon 1", "thu nhat", "thu 1", "cuon 2", "thu hai", "thu 2",
                   "cuon 3", "thu ba", "thu 3", "cuon 4", "thu tu", "thu 4"]
    _found_ords_in_msg = [o for o in _ORD_DETECT if o in _mn7]
    _is_ordinal_pair_compare = (
        "so sanh" in _mn7 and
        len({o.split()[1] if len(o.split()) > 1 else o for o in _found_ords_in_msg}) >= 2
    )
    # GUARD: nếu là OCR compare/total query → skip resolve_genre_alias ngay bên dưới
    _skip_genre_alias_for_ocr = _is_compare and not _is_ordinal_pair_compare and len(_ocr_books) >= 2
    if _is_compare and not _is_ordinal_pair_compare and len(_ocr_books) >= 2:

        # ── PRE-FILTER A: "3 cuốn mới nhất" → slice _ocr_books trước _sorted_p ──────
        # Chạy SỚM để ảnh hưởng CÙNG LÚC cả nhánh tổng tiền và so sánh rẻ/đắt nhất
        # FIX: Dùng _mn7 (ASCII) thay vì message.lower() để tránh lỗi Unicode dấu
        _latest_n_match_cmp = re.search(
            r'(\d+)\s*cuon'
            r'(?:\s*(?:vua\s*)?(?:quet|scan|upload|chup)\s*)?'
            r'(?:moi\s*nhat|cuoi|gan\s*nhat|gan\s*day)',
            _mn7,
        )
        _desc_prefix = "c\u00e1c s\u00e1ch \u0111\u00e3 qu\u00e9t"
        if _latest_n_match_cmp:
            _n_latest_cmp = int(_latest_n_match_cmp.group(1))
            if 1 <= _n_latest_cmp <= len(_ocr_books):
                _ocr_books = _ocr_books[-_n_latest_cmp:]
                _desc_prefix = f"{_n_latest_cmp} cu\u1ed1n g\u1ea7n nh\u1ea5t"

        _books_with_price = [b for b in _ocr_books if b.get("price") is not None]
        if _books_with_price:
            _sorted_p = sorted(_books_with_price, key=lambda b: float(b.get("price", 0)))
            _cheapest = _sorted_p[0]
            _costliest = _sorted_p[-1]

            # ACTION-2+3: Xử lý "mua cả N cuốn hết bao nhiêu" → tính TỔNG, không phải N × giá đơn
            # Bắt các biến thể thứ tự từ tự nhiên trong tiếng Việt:
            # "tổng tiền hết bao nhiêu", "3 cuốn hết bao nhiêu", "tổng cộng là bao nhiêu"...
            _is_total_q = (
                any(kw in _mn7 for kw in ["tong gia", "het bao nhieu tien", "tong tien",
                                          "tong chi phi", "bao nhieu tien ca", "tinh tong",
                                          "tong cong", "tat ca bao nhieu",
                                          "het bao nhieu", "bao tien tat ca",
                                          # FIX-C: "tổng N cuốn" / "tổng cuốn" không có "tiền"
                                          "tong cuon", "tong cac cuon", "tong tat ca",
                                          "tong tien cuon", "tat ca cuon"])
                or bool(_re_cmp.search(r'tong\s*(?:tien|gia|cong|cac|tat\s*ca)?\s*(?:\d+\s*)?cuon', _mn7))
            )
            if _is_total_q:
                # ── SUB-CASE A: "3 cuốn mới nhất tổng..." → slice last N books ────────
                # FIX: Dùng _mn7 (ASCII) — tránh lỗi Unicode
                _latest_n_match = re.search(
                    r'(\d+)\s*cuon\s*(?:(?:vua\s*)?(?:quet|scan|upload|chup)\s*)?'
                    r'(?:moi\s*nhat|cuoi|gan\s*nhat|gan\s*day)',
                    _mn7
                )
                if _latest_n_match:
                    _n_latest = int(_latest_n_match.group(1))
                    if 1 <= _n_latest <= len(_ocr_books):
                        _ocr_books = _ocr_books[-_n_latest:]  # Chỉ tính N cuốn mới nhất

                # ── SUB-CASE B: "cuốn 1 2 5 tổng bao nhiêu" → pick by ordinal ─────────
                _ordinal_pick_raw = re.findall(
                    r'(?:cu[o\xf4]n|s[a\xe1]ch|c[a\xe1]i|s[o\xf4])\s*(?:s[o\xf4]\s*)?(\d+)',
                    message.lower()
                )
                if len(_ordinal_pick_raw) < 2:
                    # Backup: "cuon 1 2 5" or "cuon 1, 2, 5" — digits after cuon keyword
                    _before_num = re.search(r'cu[o\xf4]n\s*(?:s[o\xf4]\s*)?(\d[,\s\d]+)', message.lower())
                    if _before_num:
                        _ordinal_pick_raw = re.findall(r'\d+', _before_num.group(0))
                if _ordinal_pick_raw:
                    _pick_indices = sorted(set(
                        int(s) - 1 for s in _ordinal_pick_raw
                        if s.isdigit() and 0 <= int(s) - 1 < len(_ocr_books)
                    ))
                    if len(_pick_indices) >= 2:
                        _picked = [_ocr_books[i] for i in _pick_indices]
                        _total = sum(float(b.get("price", 0)) for b in _picked)
                        _num_desc = "cuốn " + ", ".join(str(i + 1) for i in _pick_indices)
                        _lines2 = [f"**Tổng chi phí mua {_num_desc}:**"]
                        for _disp_i, _b in zip(_pick_indices, _picked):
                            _p = float(_b.get("price", 0))
                            _lines2.append(f"{_disp_i + 1}. **{_b.get('title', '?')}** \u2014 {_p:,.0f}\u0111")
                        _lines2.append(f"\n\U0001f4b0 **T\u1ed5ng c\u1ed9ng: {_total:,.0f}\u0111** ({len(_picked)} cu\u1ed1n)")
                        context.pop("pending_slot_filling", None)
                        context.pop("pending_intent_confirm", None)
                        return "\n".join(_lines2), [], []

                _total = sum(float(b.get("price", 0)) for b in _ocr_books)
                _lines = [f"**Tổng chi phí mua {_desc_prefix}:**"]
                for _i, _b in enumerate(_ocr_books, 1):
                    _p = float(_b.get("price", 0))
                    _lines.append(f"{_i}. **{_b.get('title','?')}** — {_p:,.0f}đ")
                _lines.append(f"\n💰 **Tổng cộng: {_total:,.0f}đ** ({len(_ocr_books)} cuốn)")
                context.pop("pending_slot_filling",  None)
                context.pop("pending_intent_confirm", None)
                return "\n".join(_lines), [], []

            # So sánh rẻ/đắt nhất — dùng _ocr_books đã được slice bởi PRE-FILTER A
            _is_most_exp = any(kw in _mn7 for kw in ["dat nhat", "mac nhat", "cao nhat"])
            _lines = [f"**So sánh giá {_desc_prefix}:**"]
            for _i, _b in enumerate(_ocr_books, 1):
                _p = float(_b.get("price", 0))
                if _is_most_exp:
                    _tag = " ⭐ Đắt nhất" if _b.get("title") == _costliest.get("title") else ""
                else:
                    _tag = " ⭐ Rẻ nhất" if _b.get("title") == _cheapest.get("title") else ""
                _lines.append(f"{_i}. **{_b.get('title','?')}** — {_p:,.0f}đ{_tag}")
            if _is_most_exp:
                _lines.append(
                    f"\n➡️ Cuốn đắt nhất: **{_costliest.get('title','?')}** ({float(_costliest.get('price',0)):,.0f}đ)"
                )
            else:
                _lines.append(
                    f"\n➡️ Cuốn rẻ nhất: **{_cheapest.get('title','?')}** ({float(_cheapest.get('price',0)):,.0f}đ)"
                )
            if any(kw in _mn7 for kw in ["phu hop","sinh vien","hoc","nen mua","nen chon"]):
                _lines.append("\n💡 Dành cho sinh viên, nên ưu tiên nội dung phù hợp nhu cầu học tập.")
            elif any(kw in _mn7 for kw in ["phu hop","tot hon","nen"]):
                _lines.append("\n💡 Tùy nhu cầu: chọn cuốn phù hợp mục tiêu học của bạn.")
            context.pop("pending_slot_filling",  None)
            context.pop("pending_intent_confirm", None)
            return "\n".join(_lines), [], []

    # ── GENRE QUICK-SELECT — ĐẶT TRƯỚC pending checks ──────────────────────
    # "giảm stress", "tiếng Anh" phải resolve genre ngay, KHÔNG hỏi lại
    # ⚠️ GUARD 1: KHÔNG áp dụng khi intent là image_search → tránh book title
    # chứa genre keyword (VD: "Tiếng Anh Như Gió") làm override intent
    # ⚠️ GUARD 2: KHÔNG áp dụng khi _skip_genre_alias_for_ocr=True tức là
    # đây là OCR compare/total query với đủ sách trong context
    _resolved_genre = resolve_genre_alias(message)
    if _resolved_genre and intent != "image_search" and not _skip_genre_alias_for_ocr:
        intent = "recommend_category"
        entities["genre"] = _resolved_genre
        nlu_result.confidence = 1.0
        context.pop("pending_slot_filling",  None)
        context.pop("pending_intent_confirm", None)

    # ══ CLARIFY-FIRST: Xử lý pending state (sau khi genre alias đã xử lý) ══
    if context.get("pending_slot_filling"):
        # FIX P0: Mở rộng danh sách intent phải clear pending slot-fill
        _FORCE_CLEAR_P = {
            "image_search", "recommend_trending", "chitchat", "store_info",
            "promotion_list", "loyalty_points", "account_help", "escalate",
            "payment_method", "return_policy", "bot_capabilities", "farewell",
            # FIX: order intents phải clear pending slot-fill củ (VD: return_request)
            "order_history", "order_status", "order_cancel",
        }
        _sf_tgt = context["pending_slot_filling"].get("target_intent", "")
        if intent in _FORCE_CLEAR_P and intent != _sf_tgt:
            context.pop("pending_slot_filling",  None)
            context.pop("pending_intent_confirm", None)
        else:
            answer, sources, btns = await _handle_slot_filling_response(
                message, entities, user_id, context, history, user_profile
            )
            return answer, sources, btns

    if context.get("pending_intent_confirm"):
        answer, sources, btns = await _handle_intent_confirm_response(
            message, intent, nlu_result, user_id, context, history, user_profile
        )
        return answer, sources, btns


    # ══════════════════════════════════════════════════════════════════════
    # [v3] CONFIDENCE CHECK: Với confidence borderline → hỏi xác nhận
    # Chỉ áp dụng cho SBERT (confidence < 0.95, vì regex luôn trả 0.95)
    # ══════════════════════════════════════════════════════════════════════
    is_sbert_result = nlu_result.confidence < 0.95
    in_borderline   = CONFIDENCE_SBERT_MIN <= nlu_result.confidence < CONFIDENCE_CONFIRM_THRESHOLD

    # ── Skip confirm nếu câu hỏi rõ ràng là follow-up với context ──
    _HAS_CTX = bool(
        context.get("last_found_title")
        or context.get("last_search_query")
        or context.get("last_category")
        or context.get("last_shown_books")
        or context.get("last_ocr_books")
        or context.get("last_intent")
    )
    # Từ khóa giá/tác giả/lọc rõ ràng — không cần xác nhận
    _PRICE_FILTER_KW = [
        "duoi", "tren", "tu", "den", "trong tam", "tam gia", "gia",
        "bao nhieu", "re nhat", "dat nhat", "re hon", "dat hon",
        "tac gia", "cua tac gia", "cung tac gia",
        "phu hop cho", "tuoi", "cho tre", "cho nguoi",
        "tuong tu", "cung chu de", "cung the loai",
    ]
    _skip_confirm = (
        _HAS_CTX and any(kw in _mn7 for kw in _PRICE_FILTER_KW)
    ) or (
        # follow-up đặt hàng/mua khi là guest → escalate login gate
        intent in ("order_place", "cart_add") and not user_id
    ) or (
        # message cực ngắn là follow-up (vd: "Cuốn đó bao nhiêu?", "Còn hàng không?")
        len(message.split()) <= 7 and _HAS_CTX and intent in (
            "book_detail", "book_availability", "book_review",
            "book_search", "recommend_category",
        )
    )

    if is_sbert_result and in_borderline and intent not in NO_CONFIRM_INTENTS and not _skip_confirm:
        context["pending_intent_confirm"] = {
            "guessed_intent":   intent,
            "original_message": message,
            "confidence":       nlu_result.confidence,
        }
        desc = INTENT_CONFIRM_DESC.get(intent, intent)
        return (
            f"Bạn đang muốn **{desc}** phải không?\n"
            "(Trả lời **Có** để tiếp tục, hoặc nói rõ hơn điều bạn cần)",
            [],
            [
                NavigateButton(label="✅ Đúng rồi!",  url="", type="confirm_yes"),
                NavigateButton(label="❌ Không phải", url="", type="confirm_no"),
            ],
        )


    # [v3] SLOT-FILLING CHECK: Thiếu entity quan trọng → hỏi trước khi xử lý
    # Chỉ áp dụng khi confidence cao (regex hit hoặc SBERT chắc chắn)
    # SKIP nếu genre/entity đã được resolve trực tiếp từ resolve_genre_alias
    # ══════════════════════════════════════════════════════════════════════
    # [FIX] Phát hiện "Đổi thể loại khác" → KHÔNG reuse last_genre/last_category
    import unicodedata as _ud_gc3
    _mn_gc3 = "".join(
        c for c in _ud_gc3.normalize("NFD", message.lower().replace("đ","d"))
        if _ud_gc3.category(c) != "Mn"
    )
    _CHANGE_GENRE_KW2 = [
        "doi the loai khac", "the loai khac", "thay the loai",
        "doi sang the loai", "doi chu de", "chu de khac",
        "muon xem the loai khac", "xem the loai khac",
        "kham pha the loai khac",
    ]
    _is_change_genre2 = (
        intent == "recommend_category"
        and any(kw in _mn_gc3 for kw in _CHANGE_GENRE_KW2)
    )
    if _is_change_genre2:
        # Xóa genre context cũ + bắt slot-filling
        context.pop("last_genre", None)
        context.pop("last_category", None)

    _slot_genre_resolved = bool(
        entities.get("genre")
        or (context.get("last_genre") and not _is_change_genre2)
        or (context.get("last_category") and not _is_change_genre2)
    )
    if (
        intent in SLOT_FILLING_CONFIG
        and intent not in NO_CONFIRM_INTENTS
        # BUG-B/C FIX: không hỏi slot nếu genre/entity đã có
        and not (intent == "recommend_category" and _slot_genre_resolved)
    ):
        required_slot, question, quick_replies = SLOT_FILLING_CONFIG[intent]
        missing = _is_slot_missing(intent, required_slot, entities, user_id, context, message)
        if missing:
            context["pending_slot_filling"] = {
                "target_intent":    intent,
                "original_message": message,
                "filled_entities":  entities,
                "required_slot":    required_slot,
            }
            
            btns = []
            if intent == "recommend_category":
                import random
                ALL_GENRES = ["Văn học", "Kinh doanh", "Kỹ năng sống", "Thiếu nhi", "Lịch sử", "Tâm lý học", "Tiểu thuyết", "Khoa học", "Nấu ăn", "Y học", "Truyện tranh", "Kinh dị", "Lập trình", "Triết học", "Đầu tư", "Marketing"]
                last_g = context.get("last_genre", "").lower()
                pool = [g for g in ALL_GENRES if g.lower() not in last_g]
                shuffled = random.sample(pool, 4) if len(pool) >= 4 else pool
                
                is_all = any(kw in message.lower() for kw in ["tất cả", "toàn bộ", "liet ke", "liệt kê", "các thể loại"])
                if is_all:
                    question = f"Cửa hàng chúng tôi cung cấp rất nhiều danh mục sách phong phú, tiêu biểu gồm: **{', '.join(ALL_GENRES)}** và vô số lĩnh vực khác.\n\nBạn có thể nhấn vào các nhóm nội bật dưới đây hoặc gõ tên thể loại mà bạn đặc biệt muốn tìm nhé!"
                else:
                    question = f"Bạn muốn khám phá sách về mảng nào? Chúng tôi có nhiều đầu sách hay thuộc các lĩnh vực như **{', '.join(shuffled)}**..."
                
                for qr in shuffled:
                    btns.append(NavigateButton(label=qr, url="", type="quick_reply"))
                # Thêm nút điều hướng trang cho user bao quát toàn bộ thể loại
                btns.append(NavigateButton(label="Khám phá thêm thể loại khác", url="", type="quick_reply"))
            else:
                btns = [
                    NavigateButton(label=qr, url="", type="quick_reply")
                    for qr in quick_replies
                ]
            
            return question, [], btns

    # ── 0a. HARD ESCALATE ngay với complaint/payment intent ──────────────────
    if intent in HARD_ESCALATE_INTENTS:
        # Đính kèm context cho CSKH
        escalate_prefix = {
            "complaint_damaged":  "Rất tiếc khi nghe sách bạn nhận bị hư hỏng! ",
            "complaint_wrong":    "Xin lỗi vì đã giao sai sách cho bạn! ",
            "payment_issue":      "Tôi rất tiếc vì bạn gặp sự cố thanh toán! ",
        }
        prefix = escalate_prefix.get(intent, "")
        return prefix + ESCALATE_MSG, ["escalate:cskh"], []

    # ── 0b. SOFT ESCALATE: account_help → hướng dẫn tự giải quyết trước ─────
    if intent in SOFT_ESCALATE_INTENTS:
        account_btn = NavigateButton(
            label="👤 Đi đến Trang tài khoản",
            url="/account",
            type="page",
        )
        return TEMPLATES["account_help_guide"], [], [account_btn]

    # ── 0c. Sentiment NEGATIVE → đồng cảm trước, không escalate ngay ─────────
    neg_count = context.get("negative_count", 0)
    if sentiment == "NEGATIVE":
        # FAIL-6 FIX: Kiểm tra ESCALATE_KW ngay trong process() để không bỏ lọt
        import unicodedata as _ud_esc_p
        _mn_esc_p = "".join(
            c for c in _ud_esc_p.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_esc_p.category(c) != "Mn"
        )
        _SOCIAL_THREAT_KW = [
            "dang len mang xa hoi", "dang len mang", "post len mang",
            "se dang bai", "se dang review", "review xau",
            "len facebook", "len tiktok", "len youtube",
            "pham an", "truyen thong", "bao chi",
            "se phan anh", "gui len bao", "phan anh len",
            "khieu nai", "se kien", "kien tung", "bao cong an",
            "bao nguoi tieu dung", "khong chap nhan",
        ]
        if any(kw in _mn_esc_p for kw in _SOCIAL_THREAT_KW):
            return ESCALATE_MSG, ["escalate:cskh"], []

        context["negative_count"] = neg_count + 1
        if neg_count + 1 >= 3:
            return ESCALATE_MSG, ["escalate:cskh"], []
        if neg_count + 1 == 1:
            # Lần 1: xin lỗi và hỏi thêm
            tone = "đồng cảm sâu sắc, xin lỗi trước, sau đó hỏi rõ vấn đề cụ thể"
        elif neg_count + 1 == 2:
            # Lần 2: đề nghị giải pháp mạnh hơn
            tone = "đồng cảm, đề xuất giải pháp cụ thể, nhắc hotline"
    else:
        context["negative_count"] = 0

    # ── 0d. Confirmation pending state (order cancel, etc.) ──────────────
    if context.get("pending_confirmation"):
        # Nếu NLU classify rõ ràng là yes/no → xử lý confirm ngay
        if intent in ("confirmation_yes", "confirmation_no"):
            return _handle_confirmation(intent, context)

        # Nếu message rất ngắn (1-3 từ) → có thể là bare "Có"/"Không"
        # mà NLU không classify đúng → vẫn route vào confirm handler
        _msg_word_count = len(message.strip().split())
        _YES_BARE = ["co", "duoc", "ok", "oke", "xac nhan", "dung", "chac chan",
                     "dong y", "yes", "yep", "sure", "tiep tuc"]
        _NO_BARE  = ["khong", "thoi", "huy", "no", "cancel", "dung lai",
                     "bo", "tat", "dung huy", "khong can"]
        import unicodedata as _ud_cnf
        _mn_cnf = "".join(
            c for c in _ud_cnf.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_cnf.category(c) != "Mn"
        )
        _is_bare_yes = any(kw == _mn_cnf.strip() or _mn_cnf.strip().startswith(kw + " ")
                           for kw in _YES_BARE)
        _is_bare_no  = any(kw == _mn_cnf.strip() or _mn_cnf.strip().startswith(kw + " ")
                           for kw in _NO_BARE)
        if _msg_word_count <= 3 and (_is_bare_yes or _is_bare_no):
            _resolved_intent = "confirmation_yes" if _is_bare_yes else "confirmation_no"
            return _handle_confirmation(_resolved_intent, context)

        # User gửi câu hỏi mới hoàn toàn → clear pending và xử lý câu mới bình thường
        context.pop("pending_confirmation", None)
        # (tiếp tục xử lý intent mới bên dưới)


    # ══ NHÓM A – TÌM KIẾM SÁCH ═════════════════════════════════════════════
    if intent == "book_search":
        raw_query = entities.get("query") or message
        query     = _clean_book_query(raw_query)

        # ── SIMILAR BOOKS INTERCEPTION ────────────────────────────────────────
        # Nếu user yêu cầu "sách tương tự" và title đã được map (qua SBERT hoặc ordinal coref)
        import unicodedata as _udc_sim
        _mn_sim = "".join(c for c in _udc_sim.normalize("NFD", message.lower().replace("đ","d")) if _udc_sim.category(c) != "Mn")
        _SIMILAR_KW = ["tuong tu", "giong", "goi y", "lien quan"]
        _is_similar_req_bs = any(kw in _mn_sim for kw in _SIMILAR_KW)
        if _is_similar_req_bs and entities.get("book_title"):
            raw_query = entities.get("book_title")
            query = _clean_book_query(raw_query)

        # ── HARD TEMPLATE: similar-book từ OCR ordinal → tránh LLM cắt giữa chừng ──────────
        # Khi ordinal resolver đã biết chính xác cuốn sách cần tìm tương tự,
        # trả template cứng thay vì LLM để tránh truncation + hallucination tên sách
        _is_ocr_sim_query = (
            _is_similar_req_bs
            and entities.get("book_title")
            and entities.get("query")
            and any(kw in _mn_sim for kw in ["quet", "scan", "upload", "chup", "vua gui"])
        )
        if _is_ocr_sim_query:
            _ref_title_sim = entities["book_title"]
            _sim_books_raw = _safe_search(_ref_title_sim, top_k=8)
            # Loại bỏ chính cuốn sách đó khỏi kết quả
            _sim_books_raw = [b for b in _sim_books_raw
                              if b.get("title", "").strip().lower() != _ref_title_sim.strip().lower()]
            _sim_display = _filter_and_track_books(_sim_books_raw, context, max_items=4)
            if _sim_display:
                _sim_lines = "\n".join(
                    f"• **{b.get('title', '')}** – {float(b.get('price', 0)):,.0f}đ"
                    + (f" | ★ {b['avg_rating']:.1f}" if b.get("avg_rating") else "")
                    for b in _sim_display
                )
                _sim_answer = (
                    f"📖 Các sách tương tự với **{_ref_title_sim}**:\n\n"
                    f"{_sim_lines}\n\n"
                    "Bạn muốn xem chi tiết cuốn nào?"
                )
                btns = _make_book_buttons(_sim_display)
                context["last_search_query"]    = _ref_title_sim
                context["last_recommend_title"] = _sim_display[0].get("title", "")
                return _sim_answer, ["opensearch:books"], btns
            else:
                return (
                    f"Xin lỗi, hiện tôi chưa tìm được sách tương tự với **{_ref_title_sim}**.\n"
                    "Bạn thử thể loại khác hoặc tìm theo tên tác giả nhé!",
                    [], []
                )

        # ── AUTHOR COUNT INTERCEPTOR ──────────────────────────────────────────
        # "Tác giả này viết bao nhiêu cuốn rồi" → query DB count, trả template cứng
        import unicodedata as _udc_ac
        _mn_ac = "".join(c for c in _udc_ac.normalize("NFD", message.lower().replace("đ","d"))
                         if _udc_ac.category(c) != "Mn")
        _AUTHOR_COUNT_KW = ["viet bao nhieu", "bao nhieu cuon", "bao nhieu sach",
                            "co bao nhieu", "tong cong bao nhieu", "viet duoc bao nhieu",
                            "da viet bao nhieu", "tac pham bao nhieu"]
        _is_author_count = any(kw in _mn_ac for kw in _AUTHOR_COUNT_KW)
        if _is_author_count:
            _ctx_author = (
                context.get("last_author_name")
                or (context.get("last_shown_books", [{}])[0].get("author_name")
                    if context.get("last_shown_books") else None)
            )
            if _ctx_author:
                try:
                    from chatbot_app.retrieval.sql_retriever import get_connection as _gc_ac
                    _conn_ac = _gc_ac()
                    _cur_ac = _conn_ac.cursor(dictionary=True)
                    _cur_ac.execute(
                        "SELECT COUNT(*) AS cnt FROM books b "
                        "JOIN authors a ON b.author_id = a.author_id "
                        "WHERE a.author_name LIKE %s AND b.status = 'active'",
                        (f"%{_ctx_author}%",)
                    )
                    _cnt_row = _cur_ac.fetchone()
                    _cur_ac.close(); _conn_ac.close()
                    _book_count = _cnt_row["cnt"] if _cnt_row else 0
                    return (
                        f"📖 Tác giả **{_ctx_author}** hiện có **{_book_count} đầu sách** trong kho của BookStore.\n\n"
                        f"Bạn muốn xem danh sách sách, lọc theo giá, hay tìm thể loại cụ thể?",
                        ["mysql:books"], [
                            NavigateButton(label=f"Xem sách của {_ctx_author}", url="", type="quick_reply"),
                        ]
                    )
                except Exception:
                    pass

        # ── AUTHOR SEARCH INTERCEPTION ────────────────────────────────────────

        # Khi user hỏi "sách cùng tác giả", resolve tác giả từ context → query thực
        import unicodedata as _udc_auth
        def _ai_author(s):
            return "".join(c for c in _udc_auth.normalize("NFD", s.lower().replace("đ","d"))
                           if _udc_auth.category(c) != "Mn")
        _mn_auth = _ai_author(message)
        _AUTHOR_KW = ["cung tac gia", "tac gia do", "tac gia cuon do",
                      "tac gia cuon nay", "sach khac cua", "xem them cua", "cua cung tac gia",
                      "tac gia vua", "same author", "sach cung tac gia",
                      "cua tac gia nay", "cua tac gia tren"]
        _is_author_req = any(kw in _mn_auth for kw in _AUTHOR_KW)
        if _is_author_req:
            # Lấy tên sách đang được hỏi từ context
            _ref_title = (
                entities.get("book_title")
                or context.get("last_found_title")
                or context.get("last_search_query")
            )
            if _ref_title:
                # Tra DB lấy tác giả thực (author_name + author_id)
                _ref_book = get_book_price(_ref_title)
                _author_name = _ref_book.get("author_name") if _ref_book else None
                _author_id   = _ref_book.get("author_id")   if _ref_book else None
                if _author_name:
                    # ── Query MySQL theo author_id (CHÍNH XÁC) ──
                    # Tránh OpenSearch match tên tác giả trong TIÊU ĐỀ sách
                    # (VD: "Cùng Dale Carnegie Tiến Tới" ≠ sách CỦA Dale Carnegie)
                    from chatbot_app.retrieval.sql_retriever import get_books_by_author_id as _get_by_author
                    _author_books = []
                    if _author_id:
                        _author_books = _get_by_author(
                            author_id=_author_id,
                            exclude_title=_ref_title,
                            limit=8
                        )
                    # Fallback: nếu không có author_id hoặc SQL trả rỗng → dùng author_name
                    if not _author_books:
                        _author_books = _safe_search(_author_name, top_k=8)
                        # Chỉ giữ sách có author_name khớp CHÍNH XÁC (không phải title)
                        _author_books = [
                            b for b in _author_books
                            if _author_name.lower() in (b.get("author_name") or b.get("author","")).lower()
                            and b.get("title","").lower() != (_ref_title or "").lower()
                        ]

                    display_books = _filter_and_track_books(_author_books, context, max_items=4)
                    if display_books:
                        # [FIX HALLUCINATION] Không dùng LLM, trả template cứng để tránh bịa sách
                        _book_list_str = "\n".join(
                            f"• **{b.get('title', '')}** – {float(b['price']):,.0f}đ"
                            + (f" | ★ {b['rating']}" if b.get("rating") else "")
                            for b in display_books
                        )
                        _author_template = (
                            f"📚 Tác giả **{_author_name}** còn có {len(display_books)} cuốn sách khác trong kho:\n\n"
                            f"{_book_list_str}\n\n"
                            "Bạn muốn xem chi tiết cuốn nào?"
                        )
                        btns = _make_book_buttons(display_books)
                        return _author_template + FOLLOW_UP.get(intent, ""), ["mysql:books"], btns
                    else:
                        # Lưu pending để khi user trả lời "có" → recommend_category (deterministic, không qua LLM)
                        # Ưu tiên: genre từ context, fallback: tên sách làm search query
                        _similar_genre = (
                            context.get("last_genre")
                            or context.get("last_category")
                            or _ref_title
                        )
                        context["pending_intent_confirm"] = {
                            "guessed_intent": "recommend_category",
                            "original_message": _similar_genre,
                        }
                        # Đặt sẵn last_genre để recommend_category handler dùng được
                        context["last_genre"] = _similar_genre
                        return (
                            f"Hiện tại cửa hàng chưa có sách khác của tác giả **{_author_name}** "
                            f"ngoài cuốn \"{_ref_title}\". Bạn có muốn tìm sách theo chủ đề tương tự không?",
                            ["mysql:books"], [
                                NavigateButton(label="Có, tìm sách tương tự", url="", type="quick_reply"),
                                NavigateButton(label="Không, cảm ơn", url="", type="quick_reply"),
                            ]
                        )
                else:
                    return (
                        f"Tôi chưa xác định được tác giả của cuốn \"{_ref_title}\". "
                        "Bạn có thể cho tôi biết tên tác giả không?",
                        [], []
                    )



        # ── PRICE SORT INTERCEPTION ──────────────────────────────────────────
        # Khi user click "Lọc theo giá" → sắp xếp last_shown_books theo giá tăng dần
        import unicodedata as _udc_ps
        def _ai_ps(s):
            return "".join(c for c in _udc_ps.normalize("NFD", s.lower().replace("đ","d"))
                           if _udc_ps.category(c) != "Mn")
        _mn_ps = _ai_ps(message)
        _PRICE_SORT_KW = ["loc theo gia", "loc theo muc gia", "sap xep theo gia",
                          "xem theo gia", "re nhat truoc", "sap theo gia", "gia tang dan"]
        _is_price_sort = any(kw in _mn_ps for kw in _PRICE_SORT_KW)
        if _is_price_sort:
            _last_bks = context.get("last_shown_books", [])
            if _last_bks:
                # Sắp xếp theo giá tăng dần
                _sorted_bks = sorted(_last_bks, key=lambda b: float(b.get("price", 0)))
                def _ps_line(b):
                    t = b.get("title", "Sách"); p = float(b.get("price", 0))
                    r = b.get("rating") or b.get("avg_rating") or 0
                    star = f" ⭐ {r:.1f}" if r else ""
                    return f"• **{t}** – {int(p):,}đ{star}"
                _lines = "\n".join(_ps_line(b) for b in _sorted_bks)
                answer = (
                    f"📋 Danh sách sắp xếp từ rẻ đến đắt:\n\n{_lines}\n\n"
                    "Bạn muốn xem chi tiết cuốn nào không?"
                )
                btns = _make_book_buttons(_sorted_bks, max_buttons=5)
                return answer, [], btns

        # Truy vấn hệ thống Hybrid OpenSearch (BM25 + Semantic Vector)
        price_max = entities.get("price_max")
        price_min = entities.get("price_min")
        genre = entities.get("genre")

        # ── EXPLICIT AUTHOR QUERY INTERCEPTION (phải đặt TRƯỚC _safe_search) ──────────
        # OpenSearch hay match tên tác giả với từ trong tiêu đề (VD: "Ánh" → "Tôi Ươm Ánh Mặt Trời")
        # → Dùng MySQL author_id lookup để đảm bảo kết quả CHÍNH XÁC
        import re as _re_auth_direct
        _mn_auth_direct = "".join(
            c for c in __import__("unicodedata").normalize("NFD", query.lower().replace("đ","d"))
            if __import__("unicodedata").category(c) != "Mn"
        )
        _author_q = None
        # [FIX] "sach cua tac gia X" phải match đúng X, không lẫn "tac gia" vào tên
        _match_auth_direct = _re_auth_direct.search(
            r'(?:sach cua\s+(?:tac gia\s+)?|tac gia\s+|by\s+)(.+)', _mn_auth_direct
        )
        if _match_auth_direct:
            _author_q = _match_auth_direct.group(1).strip()
            # Bóc thêm "tac gia" prefix nếu vẫn còn dính vào
            _author_q = _re_auth_direct.sub(r'^tac gia\s+', '', _author_q).strip()
        # Fallback: nếu query ngắn (≤5 từ) và không có keyword tìm kiếm sách thông thường
        _NON_AUTHOR_KW = ["marketing","triet","lich su","kinh te","tam ly","khoa hoc",
                          "van hoc","tieu thuyet","ky nang"]
        if not _author_q and len(query.split()) <= 5 and not any(kw in _mn_auth_direct for kw in _NON_AUTHOR_KW):
            _author_q = query.strip()


        books = None
        _found_author_name = None
        if _author_q:
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection, get_books_by_author_id
                _conn_auth = get_connection()
                _cur_auth = _conn_auth.cursor(dictionary=True)
                # [FIX] Dùng original query (có dấu) để extract tên tác giả cho DB LIKE
                import re as _re_orig_auth
                _match_orig = _re_orig_auth.search(
                    r'(?:s[aá]ch c[uủ]a\s+(?:t[aá]c gi[aả]\s+)?|t[aá]c gi[aả]\s+|by\s+)(.+)',
                    query.lower()
                )
                _author_q_orig = _match_orig.group(1).strip() if _match_orig else _author_q
                # Thử LIKE với tên gốc (có dấu) trước, fallback tên không dấu
                _cur_auth.execute(
                    "SELECT author_id, author_name FROM authors WHERE author_name LIKE %s LIMIT 1",
                    (f"%{_author_q_orig}%",)
                )
                _found_author = _cur_auth.fetchone()
                if not _found_author:
                    # Fallback 1: không dấu (dùng MySQL COLLATION nếu set utf8_general_ci)
                    _cur_auth.execute(
                        "SELECT author_id, author_name FROM authors WHERE author_name LIKE %s LIMIT 1",
                        (f"%{_author_q}%",)
                    )
                    _found_author = _cur_auth.fetchone()
                # [FIX] Fallback 2: Strip typo ở cuối tên (VD: "Nguyễn Nhật Ánhd" → "Nguyễn Nhật Ánh")
                # Thử cắt bớt 1-3 ký tự cuối nếu vẫn không tìm thấy
                if not _found_author and len(_author_q_orig) > 4:
                    for _trim in range(1, 4):
                        _trimmed = _author_q_orig[:-_trim].strip()
                        if len(_trimmed) < 4:
                            break
                        _cur_auth.execute(
                            "SELECT author_id, author_name FROM authors WHERE author_name LIKE %s LIMIT 1",
                            (f"%{_trimmed}%",)
                        )
                        _found_author = _cur_auth.fetchone()
                        if _found_author:
                            break
                # [FIX] Fallback 3: Tìm theo từng từ trong tên (bắt tên viết sai/thiếu dấu ở từ giữa)
                if not _found_author:
                    _words = [w for w in _author_q_orig.split() if len(w) >= 3]
                    if len(_words) >= 2:
                        _word_like = "%".join(_words)
                        _cur_auth.execute(
                            "SELECT author_id, author_name FROM authors WHERE author_name LIKE %s LIMIT 1",
                            (f"%{_word_like}%",)
                        )
                        _found_author = _cur_auth.fetchone()
                _cur_auth.close(); _conn_auth.close()
                _found_author = _found_author  # noqa

                if _found_author:
                    _found_author_name = _found_author["author_name"]
                    _raw_author_books = get_books_by_author_id(
                        author_id=_found_author["author_id"], limit=20
                    )
                    # [FIX] Dedup theo title chuẩn hóa – giữ cuốn đánh giá cao nhất trong mỗi nhóm title
                    _seen_titles: dict = {}
                    for _ab in (_raw_author_books or []):
                        import unicodedata as _ud_dedup
                        _nt = "".join(
                            c for c in _ud_dedup.normalize("NFD", _ab.get("title","").lower().replace("đ","d"))
                            if _ud_dedup.category(c) != "Mn"
                        )[:40]  # Lấy 40 ký tự đầu để gom các biến thể cùng tiêu đề
                        _ar = float(_ab.get("avg_rating") or 0)
                        if _nt not in _seen_titles or _ar > float(_seen_titles[_nt].get("avg_rating") or 0):
                            _seen_titles[_nt] = _ab
                    books = sorted(_seen_titles.values(), key=lambda x: float(x.get("avg_rating") or 0), reverse=True)
            except Exception:
                pass

        # Fallback: OpenSearch nếu không tìm thấy tác giả hoặc kết quả rỗng
        if not books:
            books = _safe_search(
                query, top_k=8,
                price_max=price_max, price_min=price_min, genre=genre,
                min_score=CONFIDENCE_SBERT_MIN
            )


        # ISSUE-05: Guard chống hallucinate sách "miễn phí"
        # "Sách miễn phí" → check nếu không có price_max=0 từ entities thì chặn sớm
        import unicodedata as _udc_free
        _query_norm_free = "".join(
            c for c in _udc_free.normalize("NFD", query.lower().replace("đ","d"))
            if _udc_free.category(c) != "Mn"
        )
        _FREE_KW = ["mien phi", "khong mat phi", "giam 100", "gia 0", "0d", "0 dong", "mien phi hoan toan"]
        if any(kw in _query_norm_free for kw in _FREE_KW) and price_max is None:
            return (
                "Hiện tại **BookStore chưa có sách miễn phí**.\n\n"
                "Bạn có thể tìm sách giá rẻ dưới **50,000đ** không? "
                "Chúng tôi có nhiều lựa chọn hấp dẫn!",
                tone, intent, [], [], True
            )


        # Retry không có score threshold nếu kết quả rỗng (query dài/phức tạp)
        if not books:
            books = _safe_search(
                query, top_k=8,
                price_max=price_max, price_min=price_min, genre=genre,
            )
        if not books:
            books = get_books_by_genre(query, limit=8)
        if not books and genre:
            books = get_books_by_genre(genre, limit=8)

        # [FIX BUG-G] SQL keyword fallback cho tên công nghệ cụ thể (Python, Java, C++, v.v.)
        # Vector search có thể không match được vì sách tiếng Việt không có từ "Python" trong vector
        _TECH_KEYWORDS = [
            "python", "java", "javascript", "c++", "c#", "rust", "golang", "kotlin",
            "machine learning", "deep learning", "sql", "data science", "flutter", "react",
            "lap trinh", "coding", "algorithm",
        ]
        _query_lower = query.lower()
        import unicodedata as _udc_g
        _query_norm = "".join(c for c in _udc_g.normalize("NFD", _query_lower) if _udc_g.category(c) != "Mn")
        _matched_tech = [kw for kw in _TECH_KEYWORDS if kw in _query_lower or kw in _query_norm]
        if _matched_tech and (not books or not any(
            kw in b.get("title", "").lower() for kw in _matched_tech for b in books
        )):
            try:
                from chatbot_app.retrieval.sql_retriever import get_connection as _gc2
                _conn2 = _gc2()
                _cur2  = _conn2.cursor(dictionary=True)
                _tech_kw = _matched_tech[0]
                # Tìm trong title + description
                _cur2.execute("""
                    SELECT b.book_id, b.title, b.price, b.stock_quantity, b.avg_rating,
                           a.author_name AS author, c.category_name AS category
                    FROM books b
                    LEFT JOIN authors a ON b.author_id = a.author_id
                    LEFT JOIN book_categories bc ON b.book_id = bc.book_id
                    LEFT JOIN categories c ON bc.category_id = c.category_id
                    WHERE b.status = 'active' AND (
                        b.title LIKE %s OR b.description LIKE %s
                    )
                    ORDER BY b.avg_rating DESC
                    LIMIT 8
                """, (f"%{_tech_kw}%", f"%{_tech_kw}%"))
                _tech_books = _cur2.fetchall()
                # Nếu vẫn trống → fallback qua category "Lập trình" / "Công nghệ"
                if not _tech_books:
                    _cur2.execute("""
                        SELECT b.book_id, b.title, b.price, b.stock_quantity, b.avg_rating,
                               a.author_name AS author, c.category_name AS category
                        FROM books b
                        LEFT JOIN authors a ON b.author_id = a.author_id
                        LEFT JOIN book_categories bc ON b.book_id = bc.book_id
                        LEFT JOIN categories c ON bc.category_id = c.category_id
                        WHERE b.status = 'active' AND c.category_name LIKE '%Lập trình%'
                        ORDER BY b.avg_rating DESC
                        LIMIT 8
                    """)
                    _tech_books = _cur2.fetchall()
                _cur2.close(); _conn2.close()
                if _tech_books:
                    books = _tech_books  # ưu tiên SQL kết quả có keyword cụ thể
            except Exception:
                pass  # fallback to existing books


        if not books:
            return TEMPLATES["book_not_found"], [], []
            
        context["last_search_query"] = query

        # Kiểm tra mức độ khớp từ khóa – loại stop words ra khỏi danh sách
        _VN_STOP = {"sach", "cuon", "tim", "hay", "de", "cho", "cua", "la", "co",
                     "va", "nhung", "mot", "cac", "nao", "nha", "di", "vao", "ra",
                     "tren", "duoi", "trong", "ngoai", "ve", "tu", "den", "voi",
                     "book", "the", "and", "or", "of", "a", "an", "in", "on"}
        query_keywords = [
            w for w in query.lower().split()
            if len(w) >= 3 and w not in _VN_STOP
        ]
        matched_books = []
        if _found_author_name:
            # [FIX] Đã fetch chính xác từ DB theo author_id → không lọc lại theo title
            matched_books = list(books)
        elif query_keywords:
            for b in books:
                if any(kw in b.get("title", "").lower() for kw in query_keywords):
                    matched_books.append(b)

        if matched_books:
            # Tìm thấy chính xác → số cuốn hiển thị tùy vào loại query
            # Nếu query ngắn (1 keyword, có vẻ tên sách cụ thể) → chỉ 1 cuốn
            # Nếu query dài/chủ đề → tối đa 4 cuốn
            _is_topic_query = len(query_keywords) >= 2 or len(matched_books) >= 3
            _max_display = 4 if _is_topic_query else 1
            display_books = _filter_and_track_books(matched_books, context, max_items=_max_display)
            book_titles_str = ", ".join([f'"{b.get("title", "")}"' for b in display_books])
            if len(display_books) == 1:
                ctx = f"""[HỆ THỐNG]: Khách hỏi sách: "{raw_query}".
Hệ thống TÌM THẤY CHÍNH XÁC sách trong kho: {book_titles_str}.
NHIỆM VỤ: Thông báo vui vẻ, dứt khoát rằng cửa hàng "Có cuốn sách đó", và mời khách tham khảo nút bên dưới. KHÔNG NÓI là "tìm thấy các sách chủ đề tương tự".
LUẬT: Chỉ nhắc đúng tên sách trong danh sách: {book_titles_str}. Không tự bịa thêm tên sách khác."""
            else:
                ctx = f"""[HỆ THỐNG]: Khách tìm sách chủ đề "{raw_query}".
Hệ thống tìm thấy {len(display_books)} sách phù hợp trong kho: {book_titles_str}.
NHIỆM VỤ: Giới thiệu ngắn gọn rằng cửa hàng có nhiều lựa chọn, liệt kê tên từng cuốn.
LUẬT: Chỉ dùng tên sách trong danh sách: {book_titles_str}. Không tự bịa thêm tên sách khác."""
        else:
            # Không khớp keyword → dùng shared is_garbled_query() từ dialog_utils
            if is_garbled_query(raw_query) or is_garbled_query(query) or not books:
                return TEMPLATES["book_not_found"], [], []
            # Không tìm thấy → fallback gợi ý sách liên quan
            display_books = _filter_and_track_books(books, context, max_items=4)
            book_titles_str = ", ".join([f'"{b.get("title", "")}"' for b in display_books])
            ctx = f"""[HỆ THỐNG]: Khách tìm sách: "{raw_query}".
Kho hàng KHÔNG CÓ sách khớp chính xác, hệ thống gợi ý các sách liên quan: {book_titles_str}.
NHIỆM VỤ: Thú nhận khéo léo rằng chưa có "{raw_query}", sau đó giới thiệu các sách trên như là phương án thay thế đáng tham khảo.
LUẬT: Chỉ dùng tên sách trong danh sách: {book_titles_str}. Không tự bịa tên sách khác."""


            
        if display_books:
            context["last_category"]   = display_books[0].get("category", "")
            # ACTION-2: Lưu recommend title riêng, không ghi đè last_found_title (OCR context)
            context["last_recommend_title"] = display_books[0].get("title", "")
            context["last_search_query"]    = query
            # Lưu tên tác giả nếu vừa tìm theo tác giả
            if _found_author_name:
                context["last_author_name"] = _found_author_name
            # Chỉ cập nhật last_found_title khi không có OCR session đang active
            if not context.get("last_ocr_books"):
                context["last_found_title"] = display_books[0].get("title", "")

        # [BUG-H GUARDRAIL] Instruction cứng: cấm LLM bịa tên sách, tác giả ngoài danh sách
        instruction = (
            f"{tone}. "
            "CHI DUOC nhac den ten sach trong danh sach [HE THONG] duoi day. "
            "TUYET DOI KHONG tu biet them ten sach, tac gia, hay cuon sach nao khac. "
            "Neu khong co sach nao phu hop, noi 'Xin loi chua tim thay' thay vi biet them."
        )

        # ── HARD TEMPLATE: FORCE cho MỌI kết quả tìm kiếm (kể cả author search) ──
        # Lý do: LLM có thể liệt kê 3/4 sách hoặc đếm sai → không nhất quán với buttons
        _use_hard_tpl = True
        if _use_hard_tpl and display_books:
            _lines_ht = []
            for _i, _b in enumerate(display_books, 1):
                _t = _b.get("title", "")
                _p = float(_b.get("price", 0))
                _r = _b.get("avg_rating") or _b.get("rating") or 0
                _star = f" | ★ {float(_r):.1f}" if _r else ""
                _lines_ht.append(f"{_i}. **{_t}** – {_p:,.0f}đ{_star}")

            # Header thông minh theo loại query
            if _found_author_name:
                _header = f"📚 Dưới đây là {len(display_books)} sách của **{_found_author_name}** được yêu thích:"
            elif price_max:
                _genre_note = genre or context.get("last_genre", "")
                _g_text = f" **{_genre_note}**" if _genre_note else ""
                _header = f"📚 Dưới đây là {len(display_books)} sách{_g_text} trong ngân sách **{int(price_max):,}đ**:"
            elif genre:
                _header = f"📚 Dưới đây là {len(display_books)} sách **{genre}** phù hợp với yêu cầu của bạn:"
            else:
                _header = f"📚 Dưới đây là {len(display_books)} sách phù hợp với yêu cầu của bạn:"
            answer = _header + "\n\n" + "\n".join(_lines_ht)


        else:
            answer = await generate(message, ctx, history, instruction, intent="book_search")
            # [BUG-H GUARDRAIL] Nếu LLM trả rỗng → dùng template an toàn (không để LLM retry để tránh hallucination)
            if not answer or not answer.strip():
                if matched_books:
                    answer = f"📚 Cửa hàng có **{len(display_books)} sách** phù hợp với yêu cầu của bạn!"
                else:
                    answer = f"Xin lỗi, tôi không tìm thấy sách nào cho yêu cầu **\"{raw_query}\"**. Bạn thử tìm thể loại khác nhé!"

        btns   = _make_book_buttons(display_books)
        # Fallback: nếu dedup filter đã loại hết display_books → dùng books gốc để tạo buttons
        if not btns and books:
            btns = _make_book_buttons(books[:4])
        return answer + FOLLOW_UP.get(intent, ""), ["opensearch:books", "mysql:books"], btns


    if intent == "book_detail":
        # Ưu tiên: coref v7 đã inject book_title, nếu không thì extract từ message
        book_title = entities.get("book_title") or _extract_title_from_message(message)
        
        _detail_btns = []
        _found_book = None

        # [FIX BUG-B] Version-only query + context merge
        # Nếu user hỏi "bản tái bản 2023" hoặc "bìa cứng có giá bao nhiêu" trong context có sách
        import re as _re_ver
        _VERSION_ONLY_PAT = _re_ver.compile(
            r'^(t[\u00e1a]i\s*b[\u1ea3a]n|b[\u00ecnh]a\s+c[\u1ee9u]ng|kh[\u1ed5o]\s+nh[\u1ecco]|\d{4})\b',
            _re_ver.IGNORECASE
        )
        _is_version_only = bool(_VERSION_ONLY_PAT.search(book_title or "")) or (
            book_title and _re_ver.search(r'\d{4}', book_title)
            and len((book_title or "").split()) <= 4
        )
        if _is_version_only and context.get("last_found_title"):
            # Merge: "last_found_title + version_qualifier"
            book_title = f"{context['last_found_title']} {book_title}"
        # Fallback cuối: dùng last_found_title từ context nếu vẫn chưa có
        if not book_title:
            book_title = context.get("last_found_title") or context.get("last_search_query")
        # [FIX] Discard garbage extracted from follow-up questions
        # Nếu book_title trông giống câu hỏi ("nào", "không", ngắn < 4 chữ) và có context thì dùng context
        import unicodedata as _udc_bt
        def _norm_bt(s):
            return "".join(c for c in _udc_bt.normalize("NFD", (s or "").lower().replace("\u0111","d"))
                           if _udc_bt.category(c) != "Mn")
        _bt_norm = _norm_bt(book_title or "")
        _QUESTION_FRAGMENTS = ["nao", "khong", "gi", "la ai", "cua ai", "nhu the nao", "bao nhieu",
                                "nha xuat ban", "nxb", "tac gia", "mo ta", "tom tat"]
        _is_garbage_title = (
            book_title
            and len(book_title.split()) <= 4
            and any(frag in _bt_norm for frag in _QUESTION_FRAGMENTS)
        )
        if _is_garbage_title and (context.get("last_found_title") or context.get("last_search_query")):
            book_title = context.get("last_found_title") or context.get("last_search_query")
        if book_title:
            book = get_book_price(book_title)
            if book:
                ctx = _format_book_detail(book)
                _found_book = book  # sentinel: có sách thực từ DB
                _detail_btns = _make_book_buttons([book])
            else:
                # [FIX] Không dùng _safe_search (OpenSearch match sai "là gì")
                # Thay bằng MySQL LIKE với từ khóa đầu của tên sách
                try:
                    from chatbot_app.retrieval.sql_retriever import get_connection as _gc_nf
                    _conn_nf = _gc_nf()
                    _cur_nf = _conn_nf.cursor(dictionary=True)
                    _kw_nf = (book_title or "").split()[0] if book_title else ""
                    if _kw_nf and len(_kw_nf) >= 3:
                        _cur_nf.execute(
                            "SELECT book_id, title, price, avg_rating, stock_quantity "
                            "FROM books WHERE title LIKE %s AND status='active' "
                            "ORDER BY avg_rating DESC LIMIT 3",
                            (f"%{_kw_nf}%",)
                        )
                        _nf_books = _cur_nf.fetchall()
                    else:
                        _nf_books = []
                    _cur_nf.close(); _conn_nf.close()
                except Exception:
                    _nf_books = []

                if _nf_books:
                    _nf_lines = "\n".join(
                        f"{i+1}. **{b['title']}** – {float(b['price'] or 0):,.0f}đ"
                        + (f" | ★ {float(b['avg_rating']):.1f}" if b.get("avg_rating") else "")
                        for i, b in enumerate(_nf_books)
                    )
                    ctx = (
                        f"Không tìm thấy **\"{book_title}\"** chính xác trong hệ thống.\n\n"
                        f"Một số sách có tên tương tự:\n{_nf_lines}"
                    )
                    _detail_btns = _make_book_buttons(_nf_books)
                    # Trả hard template — không để LLM tự thêm sách giả
                    return ctx, [f"mysql:books"], _detail_btns
                else:
                    # Không tìm được gì → hard template không LLM
                    return (
                        f"Xin lỗi, không tìm thấy thông tin sách **\"{book_title}\"** trong hệ thống.\n\n"
                        f"Bạn có thể:\n"
                        f"• Thử tìm kiếm với tên gần đúng hơn\n"
                        f"• Gọi hotline **0353260721** (8h–22h) để được tra cứu thủ công",
                        ["mysql:books"], []
                    )

        else:
            # Không có book_title gì hết → semantic search bằng toàn bộ message
            books = _safe_search(message, top_k=1)
            ctx   = _format_book_detail(books[0]) if books else "Vui lòng cho tôi biết tên sách bạn muốn xem."
            if books:
                _found_book = books[0]
                _detail_btns = _make_book_buttons([books[0]])

        # [FIX G-02 T6] Khi có dữ liệu sách thực từ DB, cấm LLM bịa thêm tên sách khác
        if _found_book:
            import unicodedata as _udc_det
            _mn_det = "".join(c for c in _udc_det.normalize("NFD", message.lower().replace("đ","d"))
                              if _udc_det.category(c) != "Mn")
            # ── Cập nhật context để follow-up queries (còn hàng? cùng tác giả?) dùng được ──
            context["last_found_title"] = _found_book.get("title", book_title or "")

            # [FIX ORDINAL] Nếu câu hỏi dùng số thứ tự → KHÔNG dùng LLM, trả template ngay
            _is_ordinal_msg = any(kw in _mn_det for kw in _ORDINAL_MAP_P.keys())
            if _is_ordinal_msg:
                _b = _found_book
                _avail = "còn hàng" if _b.get("stock", 1) > 0 else "hết hàng"
                _price_str = f"{float(_b['price']):,.0f}đ" if _b.get("price") else "Liên hệ"
                _author_str = f" của tác giả **{_b['author']}**" if _b.get("author") else ""
                _rating_str = f" | ★ {_b['rating']}" if _b.get("rating") else ""
                _template_ans = (
                    f"Cuốn **{_b.get('title', '')}**{_author_str} có giá **{_price_str}**{_rating_str}, "
                    f"tình trạng **{_avail}**."
                )
                return _template_ans + FOLLOW_UP.get(intent, ""), ["mysql:books"], _detail_btns

            # Detect câu hỏi về NXB (DB không có) → hướng dẫn LLM nói thật
            _asking_publisher = any(kw in _mn_det for kw in [
                "nha xuat ban", "nxb", "xuat ban", "publisher"
            ])
            if _asking_publisher:
                _detail_instruction = (
                    f"{tone}. "
                    f"Thông tin sách trong [HE THONG]: {ctx}. "
                    "Hệ thống CHƯA LƯU thông tin nhà xuất bản. "
                    "Thông báo lịch sự rằng hệ thống chưa có thông tin NXB và gợi ý khách xem trên bìa sách. "
                    "TUYỆT ĐỐI KHÔNG nhắc, bịa, hoặc gợi ý tên bất kỳ cuốn sách nào khác."
                )
            else:
                _detail_instruction = (
                    f"{tone}. "
                    "Chỉ trả lời trực tiếp thông tin về cuốn sách trong [HE THONG]. "
                    "TUYỆT ĐỐI KHÔNG đề xuất, bịa, hoặc nhắc tên sách nào khác."
                )
            _llm_message = message
        else:
            _detail_instruction = tone
            _llm_message = message

        answer = await generate(_llm_message, ctx, history, _detail_instruction, intent="book_detail")
        return answer + FOLLOW_UP.get(intent, ""), ["mysql:books"], _detail_btns



    if intent == "book_compare":
        # Coreference: "so sánh 2 cuốn trên", "so sánh hai cuốn này"
        import unicodedata as _ud_cmp, re as _re_cmp
        _mn_cmp = "".join(c for c in _ud_cmp.normalize("NFD", message.lower().replace("đ","d")) if _ud_cmp.category(c)!="Mn")
        _COMPARE_COREF = ["2 cuon tren", "hai cuon tren", "2 sach tren", "hai sach tren",
                           "2 cuon nay", "hai cuon nay", "ca hai", "ca 2",
                           # ISSUE-07: thêm ordinal comparison keywords
                           "cuon dat hon", "cuon re hon", "cuon nao dat", "cuon nao re",
                           "cuon nao re hon", "cuon nao dat hon",
                           "2 cuon vua quet", "hai cuon vua quet", "ca 2 cuon sau",
                           "2 cuon do", "hai cuon do",
                        ]
        _cmp_coref = any(p in _mn_cmp for p in _COMPARE_COREF)
        _last_books = context.get("last_shown_books", [])

        # ISSUE-07: Ưu tiên ocr_history cho compare nếu có OCR books
        _ocr_hist = context.get("ocr_history", [])
        if _cmp_coref and len(_ocr_hist) >= 2:
            # Chỉ dùng matched OCR books, không dùng similar books
            book1 = _ocr_hist[-2]["title"]
            book2 = _ocr_hist[-1]["title"]
        elif _cmp_coref and len(_last_books) >= 2:
            book1 = _last_books[-2]["title"]
            book2 = _last_books[-1]["title"]
        elif _cmp_coref and len(_last_books) == 1:
            book1 = _last_books[0]["title"]
            book2 = context.get("last_found_title", "")
        else:
            book1 = entities.get("book_title", "")
            book2 = entities.get("book_title_2", "")
            # [FIX] "So sánh cuốn 1 và cuốn 2" → resolve ordinal pair từ last_shown_books
            if not (book1 and book2) and _last_books:
                _ORD_PAIR_MAP = {
                    "cuon 1": 0, "thu nhat": 0, "thu 1": 0, "cai 1": 0, "dau tien": 0,
                    "cuon 2": 1, "thu hai":  1, "thu 2": 1, "cai 2": 1,
                    "cuon 3": 2, "thu ba":   2, "thu 3": 2, "cai 3": 2,
                    "cuon 4": 3, "thu tu":   3, "thu 4": 3, "cai 4": 3,
                }
                _found_ord = sorted(
                    [(idx, kw) for kw, idx in _ORD_PAIR_MAP.items() if kw in _mn_cmp],
                    key=lambda x: x[0]
                )
                _found_ord = list({v[0]: v for v in _found_ord}.values())  # dedup by index
                if len(_found_ord) >= 2:
                    _i1, _i2 = _found_ord[0][0], _found_ord[1][0]
                    if _i1 < len(_last_books) and _i2 < len(_last_books):
                        book1 = _last_books[_i1].get("title", "")
                        book2 = _last_books[_i2].get("title", "")
                elif len(_found_ord) == 1:
                    # "So sánh cuốn 1 với cuốn trên" → book1 = ordinal, book2 = last_found_title
                    _i1 = _found_ord[0][0]
                    if _i1 < len(_last_books):
                        book1 = _last_books[_i1].get("title", "")
                        book2 = context.get("last_found_title", "")

        if book1 and book2:
            hits = [get_book_price(book1), get_book_price(book2)]
            hits = [h for h in hits if h]
            if not hits:
                hits = _safe_search(f"{book1} {book2}", top_k=2)
        else:
            hits = _safe_search(message, top_k=2)

        # ISSUE-07: Nếu query hỏi "cuốn rẻ/đắt hơn" → tính toán thay vì để LLM đoán
        _CHEAP_KW = ["re hon", "re nhat", "re nhat trong", "gia re hon", "giam gia hon"]
        _EXPEN_KW = ["dat hon", "dat nhat", "dat nhat trong", "gia cao hon"]
        # [FIX] "Cuốn nào được đánh giá cao hơn" → so sánh rating từ last_shown_books
        _RATING_KW = ["danh gia cao", "danh gia tot hon", "nhieu sao hon", "sao cao hon",
                      "duoc danh gia cao", "danh gia cao hon", "rating cao", "rating tot hon",
                      "tot hon", "chat luong hon"]
        if any(kw in _mn_cmp for kw in _RATING_KW):
            _rated_books = context.get("last_shown_books", [])
            if len(_rated_books) >= 2:
                _rb1, _rb2 = _rated_books[0], _rated_books[1]
                # [FIX] last_shown_books thường không có avg_rating → fetch từ DB
                def _get_rating(rb: dict) -> float:
                    r = float(rb.get("avg_rating") or rb.get("rating") or 0)
                    if r > 0:
                        return r
                    # Thử fetch realtime qua book_id
                    _bid = rb.get("book_id") or rb.get("id")
                    if _bid:
                        _live = get_book_realtime(_bid)
                        if _live:
                            return float(_live.get("avg_rating") or _live.get("rating") or 0)
                    # Fallback: tìm qua title
                    _found = get_book_price(rb.get("title", ""))
                    if _found:
                        return float(_found.get("avg_rating") or _found.get("rating") or 0)
                    return 0.0
                _r1 = _get_rating(_rb1)
                _r2 = _get_rating(_rb2)
                _winner = _rb1 if _r1 >= _r2 else _rb2
                return (
                    f"**So sánh đánh giá:**\n\n"
                    f"1. **{_rb1.get('title','')}** — ★ {_r1:.1f}/5\n"
                    f"2. **{_rb2.get('title','')}** — ★ {_r2:.1f}/5\n\n"
                    f"✅ **{_winner.get('title','')}** được đánh giá cao hơn với ★ {max(_r1,_r2):.1f}/5"
                ), ["mysql:books"], []

        if hits and len(hits) >= 2:

            _prices = [(h.get("price", 0) or h.get("discounted_price", 0), h.get("title","")) for h in hits]
            _prices_sorted = sorted(_prices, key=lambda x: x[0])
            if any(kw in _mn_cmp for kw in _CHEAP_KW):
                _cheap = _prices_sorted[0]
                ctx = (
                    f"Trong 2 cuốn sách đã quét:\n"
                    f"1. **{_prices[0][1]}** — {_prices[0][0]:,}đ\n"
                    f"2. **{_prices[1][1]}** — {_prices[1][0]:,}đ\n\n"
                    f"✅ **{_cheap[1]}** là cuốn **rẻ hơn** với giá **{_cheap[0]:,}đ**"
                )
                return ctx, ["opensearch:books"], []
            elif any(kw in _mn_cmp for kw in _EXPEN_KW):
                _exp = _prices_sorted[-1]
                ctx = (
                    f"Trong 2 cuốn sách đã quét:\n"
                    f"1. **{_prices[0][1]}** — {_prices[0][0]:,}đ\n"
                    f"2. **{_prices[1][1]}** — {_prices[1][0]:,}đ\n\n"
                    f"✅ **{_exp[1]}** là cuốn **đắt hơn** với giá **{_exp[0]:,}đ**"
                )
                return ctx, ["opensearch:books"], []

        # ── HARD TEMPLATE so sánh 2 cuốn (thay LLM để tránh hallucinate) ──
        if hits and len(hits) >= 2:
            _h1, _h2 = hits[0], hits[1]
            _bid1 = _h1.get("book_id") or _h1.get("id")
            _bid2 = _h2.get("book_id") or _h2.get("id")
            _live1 = get_book_realtime(_bid1) if _bid1 else None
            _live2 = get_book_realtime(_bid2) if _bid2 else None
            # Ưu tiên MySQL realtime, fallback sang OpenSearch
            _d1 = _live1 if _live1 else _h1
            _d2 = _live2 if _live2 else _h2
            def _fmt_cmp(d, h):
                ti = h.get("title", "?")
                au = d.get("author") or d.get("author_name") or h.get("author","?")
                pr = float(d.get("price") or h.get("price") or 0)
                rt = float(d.get("avg_rating") or d.get("rating") or h.get("avg_rating") or 0)
                rc = int(d.get("review_count") or d.get("rating_count") or 0)
                av = int(d.get("available_quantity") or d.get("stock_quantity") or h.get("stock","0") or 0)
                st = "Còn hàng" if av > 0 else "Hết hàng"
                rt_str = f"{rt:.1f}/5 ({rc:,} đánh giá)" if rt else "Chưa có đánh giá"
                return ti, au, pr, rt_str, st
            _t1, _a1, _p1, _r1, _s1 = _fmt_cmp(_d1, _h1)
            _t2, _a2, _p2, _r2, _s2 = _fmt_cmp(_d2, _h2)
            _price_note = ""
            if _p1 and _p2:
                if _p1 < _p2:   _price_note = f"\n💡 **{_t1}** rẻ hơn {_p2-_p1:,.0f}đ"
                elif _p2 < _p1: _price_note = f"\n💡 **{_t2}** rẻ hơn {_p1-_p2:,.0f}đ"
                else:            _price_note = "\n💡 Hai cuốn có giá bằng nhau"
            cmp_ans = (
                f"📊 **So sánh 2 cuốn sách**\n\n"
                f"**① {_t1}**\n"
                f"• Tác giả: {_a1}\n"
                f"• Giá: {_p1:,.0f}đ | {_s1}\n"
                f"• Đánh giá: {_r1}\n\n"
                f"**② {_t2}**\n"
                f"• Tác giả: {_a2}\n"
                f"• Giá: {_p2:,.0f}đ | {_s2}\n"
                f"• Đánh giá: {_r2}"
                f"{_price_note}"
            )
            return cmp_ans, ["mysql:books", "opensearch:books"], _make_book_buttons(hits[:2])
        elif hits and len(hits) == 1:
            _h = hits[0]
            _bid = _h.get("book_id") or _h.get("id")
            _live = get_book_realtime(_bid) if _bid else None
            _d = _live if _live else _h
            _ti = _h.get("title","?")
            _pr = float(_d.get("price") or 0)
            return (
                f"Tìm được 1 cuốn: **{_ti}** — {_pr:,.0f}đ\n"
                "Bạn muốn so sánh với cuốn nào khác?",
                ["opensearch:books"], _make_book_buttons([_h])
            )
        else:
            return (
                "Không tìm thấy đủ thông tin để so sánh. Bạn có thể nói rõ tên 2 cuốn sách muốn so sánh?",
                [], []
            )




    if intent == "book_availability":
        # 1. Ưu tiên entity đã extract được từ NLU
        book_title = entities.get("book_title")

        # 2. Nếu chưa có, resolve từ context (last_found_title hoặc sách cuối cùng hiển thị)
        if not book_title:
            _lb_avail = context.get("last_shown_books", [])
            book_title = (
                context.get("last_found_title")
                or context.get("last_search_query")
                or (_lb_avail[0].get("title") if _lb_avail else None)
            )

        # 3. Nếu vẫn chưa có → hỏi ngược user (không dùng message thô làm tên sách)
        if not book_title:
            return (
                "Bạn muốn kiểm tra tình trạng hàng của cuốn sách nào? "
                "Vui lòng cho tôi biết tên sách nhé!",
                [], []
            )

        book = get_book_price(book_title)
        if book:
            avail = int(book.get("stock_quantity", 0) or 0)
            status = "Còn hàng" if avail > 0 else "Hết hàng"
            status_icon = "✅" if avail > 0 else "❌"
            price_str = f"{float(book['price']):,.0f}đ" if book.get("price") else "Đang cập nhật"
            avail_str = f" ({avail} cuốn)" if avail > 0 else ""
            title_display = book.get("title", book_title)
            author_display = book.get("author", "")
            author_part = f"\n• **Tác giả**: {author_display}" if author_display else ""
            answer = (
                f"📖 **{title_display}**{author_part}\n"
                f"• **Tình trạng**: {status_icon} {status}{avail_str}\n"
                f"• **Giá**: {price_str}\n\n"
                f"Bạn muốn mua ngay hoặc xem sách tương tự còn hàng?"
            )
        else:
            answer = (
                f"Xin lỗi, không tìm thấy thông tin sách **\"{book_title}\"** trong hệ thống.\n"
                f"Bạn có thể thử tìm với tên khác hoặc kiểm tra lại tên sách nhé!"
            )
        return answer, ["mysql:books"], []


    if intent == "book_review":
        # Ưu tiên 1: Dùng context (last_found_title) nếu message là follow-up ngắn
        _review_title = entities.get("book_title") or context.get("last_found_title")
        _last_bks_rev = context.get("last_shown_books", [])

        if _review_title:
            # ── FAST PATH: dùng book_id từ ordinal resolver (đã inject vào entities) ──
            # Ưu tiên: entities["book_id"] → last_shown_books fold-match → get_book_price
            import unicodedata as _ud_rev
            def _fold_rev(s):
                s = (s or "").lower().replace("đ", "d")
                return "".join(c for c in _ud_rev.normalize("NFD", s)
                               if _ud_rev.category(c) != "Mn")
            _target_fold = _fold_rev(_review_title)
            _direct_bid = None
            try:
                # Ưu tiên 0: book_id đã inject từ ordinal resolver
                _direct_bid = entities.get("book_id")
                # Ưu tiên 1: fold-match trong last_shown_books
                if not _direct_bid:
                    for _ctx_b in _last_bks_rev:
                        if _fold_rev(_ctx_b.get("title", "")) == _target_fold:
                            _direct_bid = _ctx_b.get("book_id") or _ctx_b.get("id")
                            break
                # Ưu tiên 2: MySQL LIKE search fallback
                if not _direct_bid:
                    _gp = get_book_price(_review_title)
                    if _gp:
                        _direct_bid = _gp.get("book_id")
            except Exception as _fp_err:
                _log.warning(f"[book_review fast-path] err: {_fp_err}")
                _direct_bid = None

            if _direct_bid:
                try:
                    _live = get_book_realtime(int(_direct_bid))
                    if _live:
                        _rt  = float(_live.get("avg_rating") or 0)
                        _rc  = int(_live.get("review_count") or 0)
                        _au  = _live.get("author") or "Đang cập nhật"
                        _pr  = float(_live.get("price") or 0)
                        _avail = int(_live.get("available_quantity") or 0)
                        _st  = "Còn hàng" if _avail > 0 else "Hết hàng"
                        _ti  = _live.get("title") or _review_title
                        if _rt:
                            _rc_str = f" ({_rc:,} đánh giá)" if _rc else ""
                            _rev_ans = (
                                f"📖 **{_ti}**\n\n"
                                f"• **Tác giả**: {_au}\n"
                                f"• **Đánh giá**: {_rt:.1f}/5{_rc_str}\n"
                                f"• **Giá**: {_pr:,.0f}đ | {_st}\n\n"
                                "Bạn muốn thêm vào giỏ hàng hay tìm sách tương tự?"
                            )
                        else:
                            _rev_ans = (
                                f"📖 **{_ti}**\n\n"
                                f"• **Tác giả**: {_au}\n"
                                f"• **Đánh giá**: Chưa có đánh giá trên hệ thống\n"
                                f"• **Giá**: {_pr:,.0f}đ | {_st}\n\n"
                                "Bạn muốn thêm vào giỏ hàng hay tìm sách tương tự?"
                            )
                        context["last_found_title"] = _ti
                        _bid_str = str(_direct_bid)
                        _btn_b = next(
                            (b for b in _last_bks_rev
                             if str(b.get("book_id") or b.get("id") or "") == _bid_str),
                            None
                        )
                        btns = _make_book_buttons([_btn_b]) if _btn_b else []
                        return _rev_ans, ["mysql:books"], btns
                except Exception as _rt_err:
                    _log.warning(f"[book_review realtime] err: {_rt_err}")

            # Không tìm được qua MySQL → OpenSearch fallback
            books = _safe_search(_review_title, top_k=5)
        elif _last_bks_rev and len(message.split()) <= 6:
            # Message ngắn (click nút) + có context → dùng sách vừa hiển thị
            books = _last_bks_rev
        else:
            # Fallback: search theo message
            books = _safe_search(message, top_k=5)

        # ── HARD TEMPLATE: tìm exact match trước (fallback khi không có book_id) ──
        # _fold_rev và _target_fold đã được định nghĩa trong fast path ở trên
        # (chỉ chạy đến đây khi fast path không tìm được book_id)
        if not hasattr(locals(), "_target_fold") or not _target_fold:
            import unicodedata as _ud_rev
            def _fold_rev(s):
                s = (s or "").lower().replace("đ", "d")
                return "".join(c for c in _ud_rev.normalize("NFD", s) if _ud_rev.category(c) != "Mn")
            _target_fold = _fold_rev(_review_title or "")
            _exact_book  = None
        if _target_fold and books:
            # Tìm book có title gần nhất với _review_title (exact > contains)
            for _b in books:
                _b_fold = _fold_rev(_b.get("title", ""))
                if _b_fold == _target_fold:
                    _exact_book = _b
                    break
            if not _exact_book:
                for _b in books:
                    _b_fold = _fold_rev(_b.get("title", ""))
                    # Chỉ dùng contains nếu chuỗi ngắn hơn đã match ít nhất 60 chars
                    if len(_target_fold) > 20 and len(_b_fold) > 20:
                        if _b_fold == _target_fold[:len(_b_fold)]:
                            _exact_book = _b
                            break

        if _exact_book:
            # Hard template – không dùng LLM để tránh hallucination
            # Ưu tiên dữ liệu REALTIME từ MySQL (tránh OpenSearch stale index)
            _bid = _exact_book.get("book_id") or _exact_book.get("id")
            _rt_live = get_book_realtime(_bid) if _bid else None
            _src = _rt_live if _rt_live else _exact_book  # fallback sang OpenSearch nếu MySQL lỗi

            _rt = float(_src.get("avg_rating") or _src.get("rating") or 0)
            _rc = int(_src.get("review_count") or _src.get("rating_count")
                      or _src.get("num_reviews") or _src.get("total_reviews") or 0)
            _au = _src.get("author") or _src.get("author_name") or "Đang cập nhật"
            _pr = float(_src.get("price") or 0)
            _avail = _src.get("available_quantity") or _src.get("stock_quantity") or 0
            _st = "Còn hàng" if int(_avail) > 0 else "Hết hàng"
            _ti = _exact_book.get("title", _review_title)

            if _rt:
                _rc_str = f" ({_rc:,} đánh giá)" if _rc else ""
                _rev_ans = (
                    f"📖 **{_ti}**\n\n"
                    f"• **Tác giả**: {_au}\n"
                    f"• **Đánh giá**: {_rt:.1f}/5{_rc_str}\n"
                    f"• **Giá**: {_pr:,.0f}đ | {_st}\n\n"
                    "Bạn muốn thêm vào giỏ hàng hay tìm sách tương tự?"
                )
            else:
                _rev_ans = (
                    f"📖 **{_ti}**\n\n"
                    f"• **Tác giả**: {_au}\n"
                    f"• **Đánh giá**: Chưa có đánh giá trên hệ thống\n"
                    f"• **Giá**: {_pr:,.0f}đ | {_st}\n\n"
                    "Bạn muốn thêm vào giỏ hàng hay tìm sách tương tự?"
                )
            context["last_found_title"] = _ti
            btns = _make_book_buttons([_exact_book])
            return _rev_ans, ["opensearch:books"], btns

        # ── Fallback: dùng LLM khi không tìm được exact match ──
        # KHÔNG gọi _filter_and_track_books → tránh dirty last_shown_books với sách "tương tự"
        _review_display = books[:2] if books else []
        ctx   = _format_books(_review_display) if _review_display else "Không tìm thấy sách này."
        answer = await generate(
            message, ctx, history,
            "Trình bày đánh giá khách quan, nêu ngắn gọn ưu điểm (rating, nội dung). "
            "KHÔNG liệt kê thành danh sách dài. CHỈ nhắc tên sách từ [CONTEXT].",
            intent="book_review"
        )
        btns = _make_book_buttons(_review_display) if _review_display else []
        return answer, ["opensearch:books"], btns


    # ══ NHÓM B – GỢI Ý ═════════════════════════════════════════════════════
    if intent == "recommend_personal":
        if not user_id:
            query  = _clean_book_query(message)
            books  = _safe_search(query, top_k=8)
            display_books = _filter_and_track_books(books, context, max_items=4)
            if display_books:
                answer = "📚 Dưới đây là một số sách bạn có thể thích — xem chi tiết ngay bên dưới nhé!"
            else:
                answer = "Hiện tôi chưa tìm thấy gợi ý phù hợp. Bạn hãy đăng nhập để nhận gợi ý cá nhân hoá tốt hơn!"
            btns = _make_book_buttons(display_books)
            return answer, ["opensearch:books"], btns

        genres = user_profile.get("favorite_genres", [])
        genre  = genres[0] if genres else ""
        books  = (get_books_by_genre(genre, limit=8) if genre else _safe_search("sách bán chạy hay đọc nhiều", top_k=8))
        display_books = _filter_and_track_books(books, context, max_items=4)
        if display_books:
            genre_hint = f" thể loại **{genre}**" if genre else ""
            answer = f"📖 Dựa trên sở thích của bạn, đây là những cuốn sách{genre_hint} được gợi ý:"
        else:
            answer = "Hiện chưa có đủ dữ liệu để gợi ý cá nhân hoá. Bạn thử tìm theo thể loại yêu thích nhé!"
        btns = _make_book_buttons(display_books)
        return answer, ["mysql:books"], btns

    if intent == "recommend_trending":
        books = _safe_search("sách bán chạy được yêu thích nhất", top_k=8)
        display_books = _filter_and_track_books(books, context, max_items=5)
        if display_books:
            def _line(b):
                t = b.get("title","Sách"); a = b.get("author","") or "Đang cập nhật"
                p = b.get("price",0) or 0; dp = b.get("discount_price") or 0
                r = b.get("rating") or b.get("avg_rating") or 0
                star = f" ⭐ {r:.1f}" if r else ""
                if dp and 0 < dp < p:
                    return f"• **{t}** – {a} – ~~{int(p):,}đ~~ → **{int(dp):,}đ**{star}"
                return f"• **{t}** – {a} – **{int(p):,}đ**{star}"
            book_list = "\n".join(_line(b) for b in display_books)
            answer = (
                f"🔥 **Top {len(display_books)} sách đang hot nhất:**\n\n"
                f"{book_list}\n\n"
                "Bạn muốn xem chi tiết, lọc theo thể loại hay tìm sách tương tự?"
            )
        else:
            answer = "Hiện tôi chưa tìm được sách hot. Bạn thử tìm theo thể loại nhé!"
        btns = _make_book_buttons(display_books, max_buttons=5)
        return answer, ["opensearch:books"], btns

    if intent == "recommend_gift":
        recipient = entities.get("recipient_type", "adult")
        price_max = entities.get("price_max") or entities.get("budget", 300_000)
        # Search terms tinh chỉnh theo thực tế DB (dựa trên kết quả thực nghiệm)
        # Ưu tiên thuật ngữ ngắn, chính xác → semantic search hit tốt hơn
        genre_map = {
            # Internal NLU codes
            "child_0_6":    ["sách tranh thiếu nhi", "ehon trẻ em"],
            "child_7_12":   ["thiếu nhi 7 tuổi", "truyện thiếu nhi"],
            "teenager":     ["kỹ năng sống thiếu niên", "sách teen"],
            "adult_female": ["tâm lý phụ nữ", "văn học lãng mạn"],
            "adult_male":   ["kinh doanh phát triển bản thân", "kỹ năng lãnh đạo"],
            "adult":        ["kỹ năng sống", "phát triển bản thân"],
            "elderly":      ["hồi ký", "sức khỏe người cao tuổi"],
            # Button text labels (từ SLOT_FILLING_CONFIG quick replies)
            "Con nhỏ (0-6 tuổi)":   ["sách tranh thiếu nhi", "ehon trẻ em 0-6 tuổi"],
            "Trẻ em (7-12 tuổi)":   ["thiếu nhi", "truyện tranh thiếu nhi kỹ năng"],
            "Thiếu niên":           ["kỹ năng sống thiếu niên", "sách teen phát triển"],
            "Bạn gái/Phụ nữ":       ["tâm lý phụ nữ", "văn học phụ nữ"],
            "Bạn trai/Nam giới":    ["phát triển bản thân", "kinh doanh khởi nghiệp"],
            "Người lớn tuổi":       ["hồi ký", "sức khỏe tuổi trung niên"],
        }
        queries = genre_map.get(recipient, ["kỹ năng sống"])
        if isinstance(queries, str):
            queries = [queries]

        # [FIX] Reset book dedup tracking để không cross-contaminate giữa các recipient type
        context.pop("last_shown_book_ids", None)
        # Multi-query search: thử từng query cho đến khi có kết quả đủ
        books: list = []
        for _q in queries:
            _hits = _safe_search(_q, top_k=10, price_max=price_max)
            for _h in _hits:
                if _h not in books:
                    books.append(_h)
            if len(books) >= 6:
                break
        display_books = _filter_and_track_books(books, context, max_items=4)

        # [FIX] Template response 100% - KHÔNG dùng LLM để tránh hallucinate tên sách
        _recipient_label = recipient if len(recipient) < 30 else queries[0]
        if display_books:
            _book_list = "\n".join(
                f"• **{b.get('title', '')}** – {b.get('price', 0):,.0f}đ"
                for b in display_books
            )
            answer = (
                f"🎁 Gợi ý sách tặng cho **{_recipient_label}**:\n\n"
                f"{_book_list}\n\n"
                "Bạn muốn xem chi tiết hoặc điều chỉnh theo ngân sách?"
            )
        else:
            answer = (
                f"Rất tiếc, tôi chưa tìm được sách phù hợp để tặng cho **{_recipient_label}**. "
                "Bạn có thể thử thể loại khác hoặc đặt ngân sách rộng hơn nhé!"
            )
        btns = _make_book_buttons(display_books)
        return answer, ["opensearch:books"], btns

    if intent in ("recommend_combo", "recommend_category"):
        genre = entities.get("genre")
        books_raw = _safe_search(message, top_k=10, genre=genre)
        
        # Fallback to DB query if semantic search yields empty but we do have a genre
        if not books_raw and genre:
            books_raw = get_books_by_genre(genre, limit=10)
            
        display_books = _filter_and_track_books(books_raw, context, max_items=4)
        if genre:
            context["last_genre"] = genre
            
        if not display_books: # Prevent LLM Hallucination when no context exists
            return f"Rất tiếc, tôi tạm thời chưa có sách phù hợp cho thể loại '{genre}' mà bạn yêu cầu.", [], [NavigateButton(label="Lịch sử", url="", type="quick_reply")]
            
        # Template thay Ollama – tránh hallucinate và câu bị cắt
        genre_label = genre if genre else "phù hợp"
        answer = (
            f"📚 Dưới đây là **{len(display_books)} sách {genre_label}** được nhiều người yêu thích:\n\n"
            "Bạn muốn xem chi tiết, lọc theo giá hay tìm thể loại khác?"
        )
        btns = _make_book_buttons(display_books)
        btns.append(NavigateButton(label="Đổi thể loại khác", url="", type="quick_reply"))
        return answer, ["opensearch:books", "mysql:books"], btns

    # ══ NHÓM C – ĐƠN HÀNG ══════════════════════════════════════════════════
    if intent == "order_status":
        # [SECURITY] Chỉ member đăng nhập mới được xem đơn hàng
        # Guest không được phép tra dù có mã đơn cụ thể (data leakage risk)
        if not user_id:
            return (
                "🔒 Bạn cần **đăng nhập** để kiểm tra đơn hàng.\n"
                "Vui lòng [đăng nhập](/login) rồi thử lại!",
                [], []
            )

        # FIX-BUG20: PRESERVE OCR context trước khi xử lý order_status
        _preserved_ocr_books   = context.get("last_ocr_books")
        _preserved_found_title = context.get("last_found_title")

        # [FIX] Chỉ dùng context.get("last_order_id") khi message nhắc đến đơn cụ thể/đơn vừa xem
        # Các cụm từ chung ("tra cứu khác", "xem chi tiết") → hiển list, không dùng context
        import unicodedata as _ud_os, re as _re_os
        _mn_os = "".join(
            c for c in _ud_os.normalize("NFD", message.lower().replace("đ","d"))

            if _ud_os.category(c) != "Mn"
        )
        _SPECIFIC_ORDER_REF_KW = [
            "don do", "cai do", "don nay", "don vua", "vua xem", "don tren",
            "kiem tra lai", "don nay ra sao", "don vua roi", "don cuoi",
        ]
        _has_specific_ref = bool(
            entities.get("order_id")                                          # có mã đơn trong message
            or any(kw in _mn_os for kw in _SPECIFIC_ORDER_REF_KW)             # tham chiếu rõ đến đơn cũ
            or _re_os.search(r'#\d{4,}', message)                             # có pattern #NNNN
        )
        # [FIX] Detect câu hỏi "Tổng đơn hàng / tổng chi tiêu / tổng tiền"
        _COUNT_ORDER_KW = [
            # đếm đơn
            "tong don", "bao nhieu don", "tat ca don", "xem tat ca", "tong so don",
            "tong don hang", "co bao nhieu", "bao nhieu cai", "tat ca cac don",
            "toi co may don", "may don hang", "bao nhieu don hang",
            # tổng chi tiêu / tổng tiền
            "tong tien", "chi tieu", "tieu het", "da chi", "da mua het",
            "tieu bao nhieu", "mua bao nhieu", "tong thanh toan", "tong mua",
            "tong so tien", "bao nhieu tien", "chi bao nhieu", "mua het bao nhieu",
            "trong thang", "thang nay", "tong tien mua", "tong gia tri",
        ]
        _is_count_query = any(kw in _mn_os for kw in _COUNT_ORDER_KW)
        # Detect nếu là spending-focused query (ưu tiên hiện tiền trước)
        _SPEND_KW = [
            "tong tien", "chi tieu", "tieu het", "da chi", "tieu bao nhieu",
            "mua bao nhieu", "tong thanh toan", "tong mua", "bao nhieu tien",
            "chi bao nhieu", "trong thang", "thang nay", "tong tien mua", "tong gia tri",
        ]
        _is_spend_query = any(kw in _mn_os for kw in _SPEND_KW)

        # [FIX] Detect câu hỏi theo trạng thái cụ thể: "đơn thất bại", "đơn đã hủy", "đơn hoàn thành"...
        _STATUS_VI = {
            "pending":          "Chờ xử lý",
            "processing":       "Đang xử lý",
            "shipped":          "Đang giao",
            "delivered":        "Hoàn thành",
            "cancelled":        "Đã hủy",
            "cancel_requested": "Yêu cầu hủy",
            "failed":           "Thất bại",
            "return_requested": "Yêu cầu trả",
            "returned":         "Đã trả hàng",
        }
        _STATUS_KW_MAP = {
            "that bai":        "failed",
            "bi that bai":     "failed",
            "don that bai":    "failed",
            "hoan thanh":      "delivered",
            "da giao":         "delivered",
            "giao thanh cong": "delivered",
            "dang xu ly":      "processing",
            "cho xu ly":       "pending",
            "cho duyet":       "pending",
            "dang giao":       "shipped",
            "dang ship":       "shipped",
            "da huy":          "cancelled",
            "bi huy":          "cancelled",
            "yeu cau huy":     "cancel_requested",
            "can huy":         "cancel_requested",
            "tra hang":        "returned",
            "da tra":          "returned",
            "yeu cau tra":     "return_requested",
            "muon tra":        "return_requested",
        }
        _status_filter: str | None = next(
            (db_st for kw, db_st in _STATUS_KW_MAP.items() if kw in _mn_os), None
        )

        order_id = entities.get("order_id") or (context.get("last_order_id") if _has_specific_ref else None)

        # [FIX] Coref: "Bao giờ đơn đó được xử lý" → dùng last_order_id từ context
        _ETA_KW = ["bao gio", "khi nao", "bao lau", "duoc xu ly", "duoc giao", "bao nhieu ngay"]
        _is_eta_question = any(kw in _mn_os for kw in _ETA_KW) and not entities.get("order_id")
        if _is_eta_question and not order_id:
            _last_oid = context.get("last_order_id")
            if _last_oid:
                order_id = _last_oid

        if order_id:
            context["last_order_id"] = order_id  # lưu context
            order = get_order_info(int(order_id), user_id=user_id or None)  # security: verify owner
            ctx   = (_format_order(order) if order
                     else f"Không tìm thấy đơn hàng #{order_id}. Vui lòng kiểm tra lại mã đơn.")
        elif _is_count_query:
            # [FIX] Tổng đơn hàng + breakdown theo status + tổng chi tiêu
            from chatbot_app.retrieval.sql_retriever import get_user_order_summary as _get_summary
            from chatbot_app.retrieval.sql_retriever import get_loyalty_points as _get_lp
            _summary = _get_summary(user_id)
            _lp = _get_lp(user_id)  # tính tiền đơn delivered (thực chi)
            _STATUS_VI = {
                "pending":          "Chờ xử lý",
                "processing":       "Đang xử lý",
                "shipped":          "Đang giao",
                "delivered":        "Hoàn thành",
                "cancelled":        "Đã hủy",
                "cancel_requested": "Yêu cầu hủy",
                "failed":           "Thất bại",
                "return_requested": "Yêu cầu trả",
                "returned":         "Đã trả hàng",
            }
            _total = _summary["total"]
            _total_spent_all  = float(_summary.get("total_spent", 0))   # tất cả đơn
            _total_spent_done = float(_lp.get("total_spent", 0))        # chỉ delivered
            if _total == 0:
                ctx = "Bạn chưa có đơn hàng nào trong hệ thống."
            else:
                if _is_spend_query:
                    # Spending-focused: hiện tiền trước
                    _lines = [
                        "💰 **Tổng chi tiêu của bạn:**\n",
                        f"• Thực chi (giao thành công): **{_total_spent_done:,.0f}đ**",
                        f"• Tổng giá trị tất cả đơn: **{_total_spent_all:,.0f}đ**",
                        f"\n📦 Tổng cộng: **{_total} đơn** ({_lp.get('delivered_count', 0)} đơn hoàn thành)\n",
                    ]
                else:
                    # Count-focused: hiện đơn trước
                    _lines = [f"📊 **Tổng đơn hàng của bạn: {_total} đơn**\n"]
                for _st, _cnt in sorted(_summary["by_status"].items(), key=lambda x: -x[1]):
                    _vi = _STATUS_VI.get(_st, _st)
                    _lines.append(f"• {_vi}: **{_cnt}** đơn")
                if not _is_spend_query:
                    _lines.append(f"\n💵 Thực chi (hoàn thành): **{_total_spent_done:,.0f}đ**")
                _lines.append("\nHỏi tôi về trạng thái cụ thể (VD: \"Đơn thất bại\") để xem chi tiết.")
                ctx = "\n".join(_lines)
        elif _status_filter and not order_id:
            # [FIX] Hỏi theo trạng thái cụ thể → list 3 đơn gần nhất của trạng thái đó
            from chatbot_app.retrieval.sql_retriever import get_user_orders_by_status as _get_by_st
            _status_orders = _get_by_st(user_id, _status_filter, limit=3)
            _vi_status = _STATUS_VI.get(_status_filter, _status_filter)
            if not _status_orders:
                ctx = f"Bạn không có đơn hàng nào ở trạng thái **{_vi_status}**."
            else:
                ctx = (f"📦 **Đơn hàng {_vi_status} gần nhất:**\n" + "\n".join(
                    f"• **#{o['order_id']}** – {float(o['total_amount'] or 0):,.0f}đ | {str(o.get('created_at', ''))[:10]}"
                    for o in _status_orders
                ))
        else:
            orders = get_user_orders(user_id, limit=3)
            ctx = ("Đơn hàng gần đây của bạn:\n" + "\n".join(
                f"• **#{o['order_id']}**: {o['status']} – {o['total_amount']:,.0f}đ"
                for o in orders
            ) if orders else "Bạn chưa có đơn hàng nào.")
        # FIX-BUG8: trả thẳng từ DB, không qua LLM để tránh hallucinate
        answer = ctx  # dùng ctx (đã format từ _format_order) thay vì generate()

        # FIX-BUG20: Restore OCR context sau khi order_status xử lý xong
        if _preserved_ocr_books:
            context["last_ocr_books"] = _preserved_ocr_books
        if _preserved_found_title:
            context["last_found_title"] = _preserved_found_title

        # [FIX] Buttons tùy theo loại query
        _only_see_all_btn = [NavigateButton(
            label="📦 Xem tất cả đơn hàng",
            url="/account?tab=orders",
            type="order",
        )]
        if order_id:
            # Xem 1 đơn cụ thể → không cần buttons
            return answer, ["mysql:orders"], []
        elif _is_count_query:
            # Tổng quan → chỉ 1 button "Xem tất cả"
            return answer + FOLLOW_UP.get(intent, ""), ["mysql:orders"], _only_see_all_btn
        elif _status_filter:
            # List đơn theo status → buttons xem từng đơn + Xem tất cả
            from chatbot_app.retrieval.sql_retriever import get_user_orders_by_status as _get_by_status2
            _st_orders_btn = _get_by_status2(user_id, _status_filter, limit=3)
            btns = _make_order_buttons(_st_orders_btn)
            return answer + FOLLOW_UP.get(intent, ""), ["mysql:orders"], btns
        else:
            # Danh sách gần nhất → buttons đơn + Xem tất cả
            orders_for_btn = get_user_orders(user_id, limit=3)
            btns = _make_order_buttons(orders_for_btn)
            return answer + FOLLOW_UP.get(intent, ""), ["mysql:orders"], btns

    if intent == "order_cancel":
        # [SECURITY] Chỉ member mới được hủy đơn
        if not user_id:
            return (
                "🔒 Bạn cần **đăng nhập** để thực hiện hủy đơn hàng.\n"
                "Vui lòng [đăng nhập](/login) rồi thử lại!",
                [], []
            )
        # [GUARD] Chỉ xử lý hủy đơn khi message có từ khóa hủy rõ ràng
        # Tránh NLU classify nhầm "cho tôi xem đơn hàng" → order_cancel → auto-pick đơn gần nhất
        import unicodedata as _ud_oc
        _mn_oc = "".join(
            c for c in _ud_oc.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_oc.category(c) != "Mn"
        )
        _CANCEL_KW_REQUIRED = [
            "huy don", "huy dat hang", "huy lenh", "cancel", "xoa don",
            "bo don", "khong mua nua", "huy mua", "huy thanh toan",
            "muon huy", "can huy", "cho huy", "giup huy",
        ]
        _has_cancel_kw = any(kw in _mn_oc for kw in _CANCEL_KW_REQUIRED)
        # Nếu không có từ khóa hủy → redirect sang order_status để xem list đơn
        if not _has_cancel_kw:
            # Thực thi ngay order_status logic, không fall-through (Python không nhảy ngược được)
            _orders_redirect = get_user_orders(user_id, limit=3)
            _ctx_redirect = ("Đơn hàng gần đây của bạn:\n" + "\n".join(
                f"• **#{o['order_id']}**: {o['status']} – {o['total_amount']:,.0f}đ"
                for o in _orders_redirect
            ) if _orders_redirect else "Bạn chưa có đơn hàng nào.")
            _btns_redirect = _make_order_buttons(_orders_redirect)
            return _ctx_redirect + FOLLOW_UP.get("order_status", ""), ["mysql:orders"], _btns_redirect

    if intent == "order_cancel":
        # Thực thi luồng hủy đơn (chỉ vào đây khi đã qua guard ở trên)
        order_id = entities.get("order_id") or context.get("last_order_id")
        if not order_id and user_id:
            orders   = get_user_orders(user_id, limit=1)
            order_id = orders[0]["order_id"] if orders else None
        if order_id:
            order = get_order_info(int(order_id), user_id=user_id)  # security: verify owner
            if order:
                if order["status"] in ("pending", "processing"):
                    context["pending_confirmation"] = {
                        "action":        "order_cancel",
                        "order_id":      order_id,
                        "order_summary": f"#{order_id} – {order['total_amount']:,.0f}đ",
                    }
                    return (
                        f"Bạn muốn hủy đơn hàng **#{order_id}** ({order['total_amount']:,.0f}đ)?\n"
                        "⚠️ Một khi hủy sẽ không thể khôi phục.\n\n"
                        "Vui lòng đến trang đơn hàng để thực hiện hủy:"
                    ), ["mysql:orders"], [
                        NavigateButton(
                            label=f"📦 Quản lý đơn #{order_id}",
                            url=f"/account?tab=orders&orderId={order_id}",
                            type="order",
                        )
                    ]
                else:
                    _status_label = {
                        "delivered":  "đã giao thành công",
                        "shipped":    "đang vận chuyển",
                        "cancelled":  "đã hủy trước đó",
                        "returned":   "đã hoàn trả",
                    }.get(order["status"], order["status"])
                    return (
                        f"Đơn hàng **#{order_id}** đã được **xác nhận** {_status_label} → không thể hủy.\n"
                        "Nếu cần hỗ trợ thêm, vui lòng liên hệ hotline **0353260721** (8h–22h).",
                        ["mysql:orders"], []
                    )
            else:
                return f"Không tìm thấy thông tin đơn hàng #{order_id} trong hệ thống.", [], []
        return "Vui lòng cung cấp **mã đơn hàng** để hủy. Ví dụ: `hủy đơn #12345`", [], []

    if intent == "order_history":
        # FIX-BUG20: PRESERVE OCR context trước khi xử lý order_history
        _preserved_ocr_books_h   = context.get("last_ocr_books")
        _preserved_found_title_h = context.get("last_found_title")

        if not user_id:
            return (
                "🔒 Bạn cần **đăng nhập** để xem lịch sử đơn hàng.\n"
                "Vui lòng [đăng nhập](/login) rồi thử lại!",
                [], []
            )

        # [FIX] Status filter: "đơn đang chờ", "đơn giao thất bại", "đơn đã hủy"...
        from chatbot_app.retrieval.sql_retriever import get_user_orders_by_status as _guobs
        import unicodedata as _ud_h
        _mn_h = "".join(
            c for c in _ud_h.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_h.category(c) != "Mn"
        )
        _status_filter = entities.get("status_filter")
        # Keyword fallback nếu NLU entity không extract được status
        if not _status_filter:
            _HIST_STATUS_KW_MAP = {
                "hoan thanh":      "delivered",
                "da giao":         "delivered",
                "giao thanh cong": "delivered",
                "that bai":        "failed",
                "bi that bai":     "failed",
                "giao that bai":   "failed",
                "dang xu ly":      "processing",
                "cho xu ly":       "pending",
                "dang cho":        "pending",
                "dang giao":       "shipped",
                "dang ship":       "shipped",
                "da huy":          "cancelled",
                "bi huy":          "cancelled",
                "yeu cau huy":     "cancel_requested",
                "yeu cau tra":     "return_requested",
                "muon tra":        "return_requested",
                "tra hang":        "returned",
                "da tra":          "returned",
                "hoan tra":        "returned",
            }
            _status_filter = next(
                (db_st for kw, db_st in _HIST_STATUS_KW_MAP.items() if kw in _mn_h), None
            )

        _STATUS_VI = {
            "pending":                   "đang chờ xử lý",
            "processing":                "đang xử lý",
            "shipped":                   "đang giao hàng",
            "delivered":                 "đã giao thành công",
            "cancelled":                 "đã hủy",
            "cancel_requested":          "đang yêu cầu hủy",
            "return_requested":          "đang yêu cầu trả hàng",
            "returned":                  "đã hoàn trả",
            "failed":                    "giao thất bại",
            "return_requested|returned": "trả hàng (yêu cầu + đã hoàn trả)",
        }
        orders = _guobs(user_id, status=_status_filter, limit=10)

        if not orders:
            if _status_filter:
                _sv = _STATUS_VI.get(_status_filter, _status_filter)
                return (
                    f"Bạn không có đơn hàng nào **{_sv}**.",
                    [], []
                )
            return "Bạn chưa có đơn hàng nào.", [], []

        total  = len(orders)
        shown  = orders[:5]

        # Tiêu đề contextual theo status
        if _status_filter:
            _sv = _STATUS_VI.get(_status_filter, _status_filter)
            _header = f"Đơn hàng **{_sv}** của bạn:"
        else:
            _header = f"Lịch sử mua hàng ({total} đơn gần nhất):"

        _STATUS_ICON = {
            "pending":          "⏳",
            "processing":       "🔄",
            "shipped":          "🚚",
            "delivered":        "✅",
            "cancelled":        "❌",
            "cancel_requested": "🚫",
            "return_requested": "🔄",
            "returned":         "📦",
            "failed":           "⚠️",
        }
        answer = _header + "\n" + "\n".join(
            f"• {_STATUS_ICON.get(o['status'], '📦')} **#{o['order_id']}**: "
            f"{_STATUS_VI.get(o['status'], o['status'])} – {float(o['total_amount'] or 0):,.0f}đ"
            for o in shown
        )
        if total > 5:
            answer += f"\n\n📋 Hiển thị 5/{total} đơn. Hỏi để xem thêm."
        answer += "\n\nBạn muốn xem chi tiết đơn hoặc tra cứu đơn hàng khác?"

        btns = _make_order_buttons(orders)

        # FIX-BUG20: Restore OCR context
        if _preserved_ocr_books_h:
            context["last_ocr_books"] = _preserved_ocr_books_h
        if _preserved_found_title_h:
            context["last_found_title"] = _preserved_found_title_h

        # Khi chi co 1 don -> luu vao context de 'bao gio don do' coref hoat dong
        if len(shown) == 1:
            context["last_order_id"] = str(shown[0]["order_id"])

        return answer, ["mysql:orders"], btns

    if intent == "cart_help":
        # Trích tên sách từ message nếu có
        book_title = entities.get("book_title") or _extract_title_from_message(message)
        if not book_title:
            # thử lấy từ context nếu đang ở flow tìm sách
            book_title = context.get("last_found_title") or context.get("last_search_query")
        if book_title:
            book = get_book_price(book_title)
            if book:
                ctx = (
                    f"[HỆ THỐNG]: Khách muốn thêm sách \"{book_title}\" vào giỏ hàng.\n"
                    f"Thông tin sách: {book.get('title','?')} – {float(book.get('price',0)):,.0f}đ\n"
                    f"Tình trạng: {'Còn hàng' if (book.get('stock_quantity') or 0) > 0 else 'Hết hàng'}\n"
                    f"NHIỆM VỤ: Xác nhận rằng sách có sẵn và hướng dẫn khách nhấn nút 'Thêm vào giỏ' "
                    f"trên trang sách, hoặc nhấn nút 'Xem giỏ hàng' bên dưới. KHÔNG nói 'ngoài phạm vi'."
                )
                btns = [
                    NavigateButton(label=f"📖 Xem {book.get('title','sách')[:40]}", url=f"/book/{book.get('book_id')}", type="book"),
                    NavigateButton(label="🛒 Xem giỏ hàng", url="/cart", type="page"),
                ]
            else:
                ctx = (
                    f"[HỆ THỐNG]: Khách muốn thêm \"{book_title}\" vào giỏ. "
                    f"Không tìm thấy sách chính xác trong kho. "
                    f"NHIỆM VỤ: Hướng dẫn khách tìm sách trên trang web và nhấn 'Thêm vào giỏ'. "
                    f"Gợi ý tìm kiếm bằng thanh tìm kiếm. Giọng thân thiện."
                )
                btns = [NavigateButton(label="🛒 Xem giỏ hàng", url="/cart", type="page")]
        else:
            kb  = _safe_search_kb("giỏ hàng mua sách", top_k=1)
            ctx = kb[0]["text"] if kb else (
                "[HỆ THỐNG]: Khách hỏi về giỏ hàng.\n"
                "NHIỆM VỤ: Hướng dẫn: Chọn sách yêu thích → nhấn 'Thêm vào giỏ' → vào Giỏ hàng → Thanh toán. "
                "Gợi ý nhấn nút bên dưới để xem giỏ hàng."
            )
            btns = [NavigateButton(label="🛒 Xem giỏ hàng", url="/cart", type="page")]
        answer = await generate(message, ctx, history, tone, intent="cart_help")
        return answer, ["mysql:books"], btns


    # ══ NHÓM D – THANH TOÁN ═════════════════════════════════════════════════
    if intent == "payment_method":
        # FIX P2: Hardcode template chính xác, KHÔNG dùng LLM để tránh hallucinate phương thức thanh toán
        return (
            "**BookStore hỗ trợ các phương thức thanh toán:**\n\n"
            "• 💵 **Tiền mặt (COD):** Thanh toán khi nhận hàng\n"
            "• 📱 **MoMo:** Chuyển khoản qua ví MoMo\n"
            "• 📱 **ZaloPay:** Thanh toán qua ví ZaloPay\n"
            "• 🏦 **Chuyển khoản ngân hàng:** Theo thông tin trên hóa đơn\n"
            "• 💳 **VNPay:** Hỗ trợ thẻ ATM, Visa, Mastercard\n\n"
            "Bạn cần hỗ trợ về phương thức nào? Hotline **0353260721** (8h–22h).",
            ["hardcoded:payment"], []
        )

    if intent == "payment_issue":
        kb  = _safe_search_kb(message, top_k=2)
        ctx = "\n\n".join(k["text"] for k in kb) if kb else "Liên hệ hotline để hỗ trợ thanh toán."
        ctx += "\n\n**Nếu vấn đề vẫn còn:** Hotline **0353260721** (8h–22h)"
        answer = await generate(message, ctx, history, tone, intent="payment_issue")
        return answer, [k["id"] for k in kb], []

    # ══ NHÓM E – ĐỔI TRẢ ════════════════════════════════════════════════════
    if intent == "return_policy":
        # [FIX] Dùng template cố định thay vì KB (KB chunks lộn xộn, thiếu thông tin)
        answer = (
            "**Chính sách đổi trả BookStore:**\n\n"
            "• ⏰ **Thời hạn:** Đổi/trả trong vòng **7 ngày** kể từ ngày nhận hàng\n"
            "• 📦 **Điều kiện:** Sách còn nguyên vẹn, chưa tháo bọc (trừ trường hợp sách bị lỗi từ phía chúng tôi)\n"
            "• 📋 **Quy trình:**\n"
            "   1. Liên hệ CSKH kèm mã đơn hàng và lý do đổi/trả\n"
            "   2. Đóng gói sách cẩn thận và gửi về địa chỉ kho\n"
            "   3. Nhận sách mới hoặc hoàn tiền trong **3–5 ngày làm việc**\n\n"
            "• 📞 **Hotline:** 0353260721 | ✉️ **Email:** cskh@bookstore.vn (8h–22h)\n\n"
            "Bạn muốn thực hiện yêu cầu đổi/trả cụ thể không?"
        )
        return answer, [], []


    if intent == "return_request":
        order_id = entities.get("order_id")

        # [FIX] ORDER-ID FALLBACK: extract "don so XXXX" / "ma don XXXX" / "#XXXX" từ message
        # Khi NLU entity extractor miss vì không có # prefix
        if not order_id:
            import re as _re_oid_ret
            import unicodedata as _ud_ret
            _mn_ret = "".join(
                c for c in _ud_ret.normalize("NFD", message.lower().replace("đ", "d"))
                if _ud_ret.category(c) != "Mn"
            )
            _oid_m_ret = _re_oid_ret.search(
                r"(?:don\s*so|so\s*don|ma\s*don|don\s*hang\s*so|don\s*hang\s*#?|#)\s*(\d{4,})",
                _mn_ret
            )
            if _oid_m_ret:
                order_id = _oid_m_ret.group(1)
                entities["order_id"] = order_id

        # Coref: "trả đơn đó" → last_order_id trong context
        if not order_id:
            order_id = context.get("last_order_id")

        if not order_id:
            return (
                "Bạn muốn trả đơn hàng nào? Vui lòng cho tôi biết **mã đơn hàng** (VD: #1234).",
                [], []
            )

        order = get_order_info(int(order_id), user_id=user_id)  # security: verify owner
        if not order:
            return f"❌ Không tìm thấy đơn **#{order_id}**. Vui lòng kiểm tra lại mã đơn.", [], []

        raw_status = order.get("status", "")

        # Kiểm tra eligibility theo status
        if raw_status in ("return_requested", "returned"):
            _sv = "đã yêu cầu trả" if raw_status == "return_requested" else "đã hoàn trả"
            return (
                f"ℹ️ Đơn **#{order_id}** đã ở trạng thái **{_sv}**. \n"
                "Không cần yêu cầu lại. Liên hệ CSKH: **0353260721** nếu cần thêm thông tin.",
                ["mysql:orders"], []
            )

        if raw_status in ("cancelled", "failed"):
            _sv = "đã hủy" if raw_status == "cancelled" else "giao thất bại"
            return (
                f"❌ Đơn **#{order_id}** đã ở trạng thái **{_sv}**, không đủ điều kiện đổi/trả.\n"
                "Nếu cần hỗ trợ, vui lòng liên hệ: **0353260721** (8h–22h).",
                ["mysql:orders"], []
            )

        if raw_status in ("pending", "processing", "shipped"):
            _sv_map = {"pending": "đang chờ xử lý", "processing": "đang xử lý", "shipped": "đang vận chuyển"}
            return (
                f"⏳ Đơn **#{order_id}** đang ở trạng thái **{_sv_map.get(raw_status, raw_status)}**.\n"
                "Bạn chưa nhận hàng nên chưa thể yêu cầu trả. Hãy đợi đến khi đơn được giao xong nhé!",
                ["mysql:orders"], []
            )

        if raw_status == "delivered":
            # Kiểm tra trong 7 ngày
            import datetime as _dt
            created_at = order.get("created_at")
            if created_at:
                try:
                    if isinstance(created_at, str):
                        _order_date = _dt.datetime.fromisoformat(created_at)
                    else:
                        _order_date = created_at
                    _days_since = (_dt.datetime.now() - _order_date).days
                    if _days_since > 7:
                        return (
                            f"❌ Đơn **#{order_id}** đã giao ngày {str(created_at)[:10]}, "
                            f"quá **7 ngày** đổi/trả ({_days_since} ngày trước).\n"
                            "Thời hạn đổi/trả đã hết. Liên hệ CSKH: **0353260721** để được hỗ trợ thêm.",
                            ["mysql:orders"], []
                        )
                except Exception:
                    pass  # Nếu không parse được ngày thì tiếp tục

            # Đủ điều kiện → hướng dẫn quy trình trả
            total = order.get("total_amount", 0)
            return (
                f"✅ Đơn **#{order_id}** (đã giao, tổng {total:,.0f}đ) đủ điều kiện đổi/trả!\n\n"
                "**Quy trình yêu cầu trả hàng:**\n"
                "1️⃣ Liên hệ CSKH kèm **mã đơn #{}** và lý do trả\n".format(order_id) +
                "2️⃣ Đóng gói sách cẩn thận, gửi về địa chỉ kho\n"
                "3️⃣ Nhận sách mới hoặc hoàn tiền trong **3–5 ngày làm việc**\n\n"
                "📞 **Hotline:** 0353260721 | ✉️ **Email:** cskh@bookstore.vn (8h–22h)",
                ["mysql:orders"], []
            )

        # Status không xác định
        return (
            f"ℹ️ Đơn **#{order_id}** hiện ở trạng thái `{raw_status}`.\n"
            "Vui lòng liên hệ CSKH: **0353260721** để được tư vấn cụ thể.",
            ["mysql:orders"], []
        )

    # ══ NHÓM F – KHUYẾN MÃI ════════════════════════════════════════════════
    if intent == "voucher_apply":
        voucher = entities.get("voucher_code", "")
        # [FIX] Auto-extract mã voucher từ message nếu NLU chưa bóc tách được
        if not voucher:
            import re as _re_v, unicodedata as _ud_v
            # Fix: replace cả đ và Đ trước khi upper để NFD strip hoạt động đúng
            _mn_v = "".join(
                c for c in _ud_v.normalize("NFD",
                    message.replace("đ","d").replace("Đ","D").upper())
                if _ud_v.category(c) != "Mn"
            )
            # Tìm chuỗi ký tự in hoa + số liên tiếp (4-20 ký tự) sau từ khóa mã/code
            _code_match = _re_v.search(
                r'(?:NHAP|DUNG|SU DUNG|AP|KIEM TRA|CHECK|MA|CODE|VOUCHER|COUPON)\s+([A-Z][A-Z0-9]{3,19})\b|'
                r'\b([A-Z][A-Z0-9]{3,19})\b(?=\s*(?:DUOC GIAM|GIAM|DUNG DUOC|AP DUNG|HIEU LUC|LA MA|LA CODE))',
                _mn_v
            )
            if _code_match:
                voucher = (_code_match.group(1) or _code_match.group(2) or "").strip()

        if voucher:
            # Tra cứu trong DB
            _vr = None
            try:
                _vr = get_voucher_info(voucher)
            except Exception:
                pass

            if _vr:
                _disc = _vr.get("discount_value") or _vr.get("discount_percent")
                _disc_type = _vr.get("type", "percent")
                _min_order = _vr.get("min_order_value") or _vr.get("min_order")
                _exp = _vr.get("expiry_date") or _vr.get("end_date")
                _disc_str = (
                    f"**{float(_disc):.0f}%**" if _disc_type == "percent"
                    else f"**{float(_disc):,.0f}đ**"
                ) if _disc else "theo điều kiện riêng"
                _min_str = f" (đơn từ **{float(_min_order):,.0f}đ**)" if _min_order else ""
                _exp_str = f" – hết hạn **{_exp}**" if _exp else ""
                _voucher_ans = (
                    f"🏷️ Mã **{voucher}** giảm {_disc_str}{_min_str}{_exp_str}.\n\n"
                    "Nhập mã tại trang Thanh toán → **Mã giảm giá** để áp dụng nhé!"
                )
                return _voucher_ans, ["mysql:vouchers"], []
            else:
                # Không tìm thấy mã trong DB
                _voucher_ans = (
                    f"❌ Không tìm thấy mã **{voucher}** trong hệ thống.\n\n"
                    "Mã có thể đã hết hạn hoặc chưa được kích hoạt. "
                    "Bạn có thể xem mã hiện có bằng cách hỏi **\"Có mã giảm giá không?\"**"
                )
                return _voucher_ans, ["mysql:vouchers"], []
        else:
            # Không extract được mã → hỏi lại
            return "Bạn muốn kiểm tra mã giảm giá nào? Hãy nhập mã vào đây!", [], []

    if intent in ("promotion_current", "promotion_info"):
        # Luôn lấy mã voucher từ DB trước để hiển thị chính xác
        _active_vouchers = get_all_vouchers(active_only=True)
        _voucher_section = ""
        if _active_vouchers:
            _v_lines = []
            for v in _active_vouchers:
                _v_exp = f" – hết hạn {v['end_date']}" if v.get("end_date") else ""
                _v_lines.append(f"• Mã **{v['code']}**: Giảm **{v['discount_percent']:.0f}%**{_v_exp}")
            _voucher_section = "🏷️ Các mã giảm giá hiện có:\n" + "\n".join(_v_lines) + "\n\nNhập mã khi thanh toán để được hưởng ưu đãi nhé!"

        # Lấy sách nổi bật/bán chạy từ MySQL
        sale_books = get_discounted_books(limit=20)
        if sale_books:
            display_books = _filter_and_track_books(sale_books, context, max_items=5)
            lines = []
            for b in display_books:
                dprice = b.get("discount_price") or 0
                oprice = b.get("price") or 0
                author = b.get("author_name") or ""
                title  = b.get("title") or ""
                pct    = b.get("discount_pct") or 0
                if dprice and int(dprice) > 0 and int(dprice) < int(oprice):
                    saved = int(oprice) - int(dprice)
                    lines.append(
                        f"• **{title}** ({author}) – Giá gốc: {int(oprice):,}đ → "
                        f"Giá KM: **{int(dprice):,}đ** (giảm {int(pct)}%, tiết kiệm {saved:,}đ)"
                    )
                else:
                    rating_str = f" ⭐ {float(b['avg_rating']):.1f}" if b.get("avg_rating") else ""
                    lines.append(f"• **{title}** ({author}) – **{int(oprice):,}đ**{rating_str}")

            _has_real_promo = any(
                (b.get("discount_price") or 0) > 0 and (b.get("discount_price") or 0) < (b.get("price") or 99999)
                for b in display_books
            )
            book_list = "\n".join(lines)
            _book_section = ""
            if _has_real_promo:
                _book_section = f"\n\n📚 Sách đang khuyến mãi:\n{book_list}"

            if _voucher_section or _book_section:
                answer = (_voucher_section + _book_section).strip()
                if not answer:
                    answer = "Hiện tại BookStore chưa có chương trình khuyến mãi nào đang diễn ra."
            else:
                answer = "Hiện tại BookStore chưa có chương trình khuyến mãi nào đang diễn ra.\nBạn có thể theo dõi website để cập nhật ưu đãi mới nhất nhé!"
            _promo_btns = _make_book_buttons(display_books, max_buttons=5) if _has_real_promo else []
        else:
            if _voucher_section:
                answer = _voucher_section
            else:
                answer = "Hiện tại BookStore chưa có chương trình khuyến mãi nào đang diễn ra.\nBạn có thể theo dõi website để cập nhật ưu đãi mới nhất nhé!"
            _promo_btns = []
        return answer, ["mysql:promotions"], _promo_btns

    if intent == "loyalty_points":
        # BookStore KHÔNG có chương trình tích điểm/thẻ thành viên
        # Kiểm tra xem có mã khuyến mãi đang chạy không để gợi ý thêm
        try:
            from chatbot_app.retrieval.admin_agents import get_active_promotions
            active_promos = get_active_promotions()
        except Exception:
            active_promos = []

        base_msg = (
            "ℹ️ **BookStore hiện chưa có chương trình tích điểm hay thẻ thành viên.**\n\n"
        )

        if active_promos:
            promo_lines = [
                f"• Mã **{p['code']}**: Giảm {p['discount_percent']}%"
                for p in active_promos
            ]
            promo_msg = (
                "Tuy nhiên, BookStore đang có các **mã giảm giá** sau để bạn áp dụng khi thanh toán:\n"
                + "\n".join(promo_lines)
                + "\n\n💡 Nhập mã tại bước thanh toán để được giảm giá trực tiếp vào đơn hàng."
            )
        else:
            promo_msg = (
                "💡 BookStore tập trung vào việc cung cấp **giá sách tốt nhất** thay vì chương trình tích điểm.\n"
                "Bạn có thể theo dõi các chương trình khuyến mãi định kỳ trên website nhé!"
            )

        final_msg = base_msg + promo_msg + "\n\nBạn cần hỗ trợ gì thêm không?"
        return final_msg, [], []



    # ══ NHÓM G – HỖ TRỢ ════════════════════════════════════════════════════
    if intent in ("store_info", "shipping_info"):  # FIX: shipping_info must not fall through to LLM
        kb = _safe_search_kb("thông tin cửa hàng liên hệ hotline", top_k=2)
        if kb:
            # KB có dữ liệu → LLM tóm gọn lại ngắn (vẫn cần vì text KB thường thô)
            ctx = "\n\n".join(k["text"] for k in kb)
            answer = await generate(
                message, ctx, history,
                "ngắn gọn 2-3 câu, trình bày các thông tin liên hệ rõ ràng",
                intent="store_info"
            )
        else:
            # Không có KB → trả template ngay, KHÔNG gọi LLM → nhanh hơn ~2 giây
            answer = (
                "**Thông tin liên hệ BookStore:**\n"
                "• **Hotline:** 0353260721 (miễn phí, 8h–22h)\n"
                "• **Email:** cskh@bookstore.vn\n"
                "• **Website:** www.bookstore.vn\n"
                "• **Giờ hỗ trợ:** Thứ 2–Chủ nhật, 8h–22h"
            )
        return answer, ["opensearch:kb"], []


    # ── Chitchat – phân biệt các loại ──────────────────────────────────────
    # == image_search: OCR result -> template co cau truc (khong dung LLM) ==
    if intent == "image_search":
        _ocr_data = context.get("_ocr_data") or {}
        _ocr_books = _ocr_data.get("search_results", [])
        _ocr_title = _ocr_data.get("book_title")

        if not _ocr_title:
            _ocr_m = re.search(r'\u201c(.+?)\u201d|"(.+?)"', message)
            if not _ocr_m:
                _ocr_m = re.search(r'[:\u2019]\s*(.+?)$', message)
            if _ocr_m:
                _ocr_title = (_ocr_m.group(1) or _ocr_m.group(2) or '').strip()
        # Fallback về context cũ CHỈ KHI đây là follow-up text (không có ảnh mới)
        # Dùng _is_fresh_ocr (được tính từ KEY EXISTENCE) thay vì truthiness của _ocr_data
        # → ngay cả khi OCR service fail trả về {}, vẫn KHÔNG fallback về sách cũ
        if not _ocr_title and not _is_fresh_ocr:
            _ocr_title = context.get("last_found_title") or context.get("last_search_query")


        def _fmt_ocr_book(b: dict) -> str:
            title  = b.get("title", "Sách")
            # Xử lý các format khác nhau của author từ OCR / OpenSearch
            _auth_raw = b.get("author") or b.get("author_name") or b.get("authors")
            if isinstance(_auth_raw, list):
                author = ", ".join(_auth_raw) if _auth_raw else "Đang cập nhật"
            else:
                author = str(_auth_raw) if _auth_raw else "Đang cập nhật"
                
            price  = b.get("price", 0) or 0
            dprice = b.get("discount_price") or b.get("discounted_price", 0)
            if dprice and 0 < dprice < price:
                return f"• **{title}** – {author} – ~~{int(price):,}đ~~ → **{int(dprice):,}đ**"
            return f"• **{title}** – {author} – **{int(price):,}đ**"

        if _ocr_title and len(_ocr_title) > 3:
            if not _ocr_books:
                _ocr_books = _safe_search(_ocr_title, top_k=4)
            if not _ocr_books:
                # FIX P0 ROOT: KHÔNG dùng `message` làm fallback query
                # vì message có thể chứa filename (AI/thong minh/intellig...)
                # → bị OpenSearch map sang thể loại "Trí tuệ nhân tạo"
                # Thay bằng: thử từng từ TÊN SÁCh (bỏ số/dấu gạch ngang từ filename)
                import re as _re_ocr
                _clean_title = _re_ocr.sub(r'[^a-zA-ZÀ-ỹ\s]', ' ', _ocr_title).strip()
                _clean_words = [w for w in _clean_title.split() if len(w) >= 4]
                if _clean_words:
                    _search_q = " ".join(_clean_words[:5])
                    _ocr_books = _safe_search(_search_q, top_k=4)
            _ocr_display = _filter_and_track_books(_ocr_books, context, max_items=4) if _ocr_books else []
            if _ocr_display:
                context["last_found_title"] = _ocr_display[0].get("title", "")
                context["last_ocr_title_hint"] = _ocr_title  # FIX-D: luu OCR title hint

                context["last_category"]    = _ocr_display[0].get("category", "")
                # Tích lũy OCR books qua nhiều OCR turn (không overwrite)
                _prev_ocr = context.get("last_ocr_books", [])
                _new_ocr_entry = {
                    "title": _ocr_display[0].get("title", ""),
                    "price": _ocr_display[0].get("price", 0),
                    "book_id": _ocr_display[0].get("book_id") or _ocr_display[0].get("id"),
                }
                _already_titles = {b.get("title") for b in _prev_ocr}
                if _new_ocr_entry["title"] not in _already_titles:
                    _prev_ocr.append(_new_ocr_entry)
                context["last_ocr_books"] = _prev_ocr[-50:]  # giới hạn 50 cuốn/phiên
                _book_list = "\n".join(_fmt_ocr_book(b) for b in _ocr_display)

                # ── Kiểm tra title có garbled/không tin cậy không ────────────
                # Dùng NỘI DUNG title (không chỉ confidence) để phát hiện garbled
                # compound_conf < 0.60 quá rộng: sách không có tác giả/NXB sẽ bị
                # falsely flagged dù title đọc đúng (VD: "Hãy Nhớ Tên Anh Ấy")
                _ocr_conf = _ocr_data.get("confidence", 1.0)
                import re as _re_ocr_title
                def _is_garbled_ocr_title(t: str) -> bool:
                    """Title thực sự garbled nếu bản thân text bị hỏng (không chỉ conf thấp)."""
                    if not t or len(t.strip()) < 2:
                        return True
                    alpha = sum(1 for c in t if c.isalpha())
                    if alpha == 0:
                        return True
                    # Noise pattern: EasyOCR đọc sai → trộn ký tự ASCII + dấu tiếng Việt lạ
                    # VD: "IOỒ", "trùll" (ll sau dấu Việt), "PôLỆ", chữ in hoa + dấu nguyên âm
                    _noise = bool(_re_ocr_title.search(
                        r'[a-z]{2,}[ùúủũụ]|[ùúủũụ][a-z]{2,}|[A-Z]{2,}[ỒỢỐỔỖỘỚỜỞỠỢ]'
                        r'|[ỒỢỐổỖỘỚỜỞỠợ][A-Z]{2,}', t
                    ))
                    if _noise:
                        return True
                    # Pure ASCII (không có chữ Việt) + confidence rất thấp → có thể garbled
                    _has_vn = bool(_re_ocr_title.search(
                        r'[àáảãạăắằẳẵặâấầẩẫậèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợ'
                        r'ùúủũụưứừửữựỳýỷỹỵđÀÁẢÃẠĂẮẰẲẴẶÂẤẦẨẪẬÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊ'
                        r'ÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴĐĐ]', t
                    ))
                    if not _has_vn and _ocr_conf < 0.35:
                        return True
                    return False

                _title_garbled = _is_garbled_ocr_title(_ocr_title or "")

                # ── Kiểm tra có sách nào trong kết quả KHỚP với OCR title không ──
                # Nếu không khớp → chatbot nhận ra sách nhưng chưa có trong hệ thống
                import unicodedata as _ud_ocr2
                def _fold_title(s: str) -> str:
                    s = (s or "").lower().replace("đ", "d")
                    s = _ud_ocr2.normalize("NFD", s)
                    return "".join(c for c in s if _ud_ocr2.category(c) != "Mn")

                _title_fold = _fold_title(_ocr_title or "")
                _exact_in_db = _title_fold and any(
                    (_title_fold in _fold_title(b.get("title", ""))
                     or _fold_title(b.get("title", "")) in _title_fold)
                    for b in _ocr_display
                    if len(b.get("title", "")) >= 5
                )

                if len(_ocr_display) == 1:
                    # 1 kết quả: dùng tên sách từ DB (luôn đúng), không dùng OCR title
                    _db_title = _ocr_display[0].get("title", _ocr_title)
                    _scanned_count = len(context.get("last_ocr_books", []))
                    _count_hint = (
                        f"\n\n📋 *Phiên này bạn đã quét **{_scanned_count} cuốn**. "
                        "Hỏi 'Tổng tiền' bất cứ lúc nào để tính tổng!*"
                    ) if _scanned_count >= 2 else ""
                    _ans = (
                        f"📚 Tôi đã tìm thấy cuốn sách **\"{_db_title}\"** từ ảnh bìa của bạn:\n\n"
                        f"{_book_list}\n\n"
                        "Bạn muốn **xem đánh giá chi tiết**, **thêm vào giỏ hàng**, hay **tìm sách cùng tác giả**?"
                        f"{_count_hint}"
                    )
                elif _title_garbled:
                    # Nhiều kết quả + title garbled → KHÔNG hiện title xấu, dùng message generic
                    _ans = (
                        f"🔍 Tôi nhận dạng bìa sách từ ảnh của bạn.\n"
                        f"Dưới đây là {len(_ocr_display)} sách tương tự:\n\n"
                        f"{_book_list}\n\n"
                        "Bạn muốn xem **chi tiết**, lọc theo **giá** hay tìm **sách tương tự**?"
                    )
                elif _ocr_title and not _exact_in_db:
                    # Title nhận dạng được nhưng KHÔNG có trong hệ thống
                    _ans = (
                        f"📖 Tôi nhận dạng đây là sách **\"{_ocr_title}\"**, "
                        f"nhưng hiện tại BookStore chưa có cuốn này trong kho.\n\n"
                        f"Dưới đây là {len(_ocr_display)} sách tương tự bạn có thể quan tâm:\n\n"
                        f"{_book_list}\n\n"
                        "Bạn muốn xem **chi tiết** cuốn nào, hay tìm **sách cùng thể loại**?"
                    )
                else:
                    # Nhiều kết quả + title khớp DB → hiện title bình thường
                    _ans = (
                        f"📚 Tôi tìm thấy **{len(_ocr_display)}** cuốn sách liên quan đến **\"{_ocr_title}\"**:\n\n"
                        f"{_book_list}\n\n"
                        "Bạn quan tâm đến cuốn nào nhất? Bạn có thể yêu cầu **xem chi tiết** hoặc **thêm vào giỏ hàng**."
                    )

                # ── POST-OCR similar-book hook ─────────────────────────────────
                # Nếu user gửi ảnh kèm yêu cầu "gợi ý sách đọc kèm / tương tự"
                # → tìm similar books ngay thay vì trả response mặc định
                import unicodedata as _ud_ocr_sim
                _mn_ocr_msg = "".join(
                    c for c in _ud_ocr_sim.normalize("NFD", message.lower().replace("đ","d"))
                    if _ud_ocr_sim.category(c) != "Mn"
                )
                _OCR_SIM_KW = [
                    "goi y sach", "tuong tu", "doc kem", "doc cung", "sach lien quan",
                    "sach nhu the", "sach cung the loai", "sach giong", "sach hay lien quan",
                    "kieu tuong tu", "goi y them", "co sach nao tuong tu",
                ]
                _ocr_wants_similar = any(kw in _mn_ocr_msg for kw in _OCR_SIM_KW)
                if _ocr_wants_similar:
                    _sim_ref = _ocr_display[0].get("title", "")
                    _sim_raw = _safe_search(_sim_ref, top_k=8)
                    _sim_raw = [b for b in _sim_raw
                                if b.get("title","").strip().lower() != _sim_ref.strip().lower()]
                    _sim_disp = _filter_and_track_books(_sim_raw, context, max_items=4)
                    if _sim_disp:
                        _sim_lines = "\n".join(
                            f"• **{b.get('title','')}** – {float(b.get('price',0)):,.0f}đ"
                            + (f" | ★ {b['avg_rating']:.1f}" if b.get("avg_rating") else "")
                            for b in _sim_disp
                        )
                        _sim_ans = (
                            f"📖 Dựa trên cuốn **\"{_sim_ref}\"** từ ảnh của bạn, "
                            f"đây là {len(_sim_disp)} cuốn sách phù hợp để đọc kèm:\n\n"
                            f"{_sim_lines}\n\n"
                            "Bạn muốn xem chi tiết cuốn nào?"
                        )
                        context["last_search_query"] = _sim_ref
                        context["last_recommend_title"] = _sim_disp[0].get("title","")
                        return _sim_ans, ["opensearch:books"], _make_book_buttons(_sim_disp)
                # ── end post-OCR hook ──
                return _ans, ["opensearch:books"], _make_book_buttons(_ocr_display)


        # FIX P0: Fallback khi không tìm được sách từ OCR
        # KHAI KHÔNG DÙNG raw message vì có thể chứa AI/thông minh → bị map sang "Trí tuệ nhân tạo"
        # Thay vào đó: search sach bán chạy hoặc sách mớn nhất
        _safe_fallback_query = "sách kỹ năng bán chạy"
        _ocr_fallback = _safe_search(_safe_fallback_query, top_k=4)
        _ocr_fb_disp  = _filter_and_track_books(_ocr_fallback, context, max_items=4) if _ocr_fallback else []
        if _ocr_fb_disp:

            # FIX-D: neu co OCR title hint, set last_found_title

            _hint_title = context.get("last_ocr_title_hint", "")

            if _hint_title and not context.get("last_found_title"):

                context["last_found_title"] = _hint_title

            _book_list = "\n".join(_fmt_ocr_book(b) for b in _ocr_fb_disp)
            _fb_ans = (
                "\U0001f4da T\u00f4i ch\u01b0a nh\u1eadn d\u1ea1ng r\u00f5 t\u00ean s\u00e1ch t\u1eeb \u1ea3nh, "
                "nh\u01b0ng \u0111\u00e2y l\u00e0 m\u1ed9t s\u1ed1 s\u00e1ch n\u1ed5i b\u1eadt b\u1ea1n c\u00f3 th\u1ec3 tham kh\u1ea3o:\n\n"
                f"{_book_list}\n\n"
                "B\u1ea1n c\u00f3 th\u1ec3 cho t\u00f4i bi\u1ebft t\u00ean s\u00e1ch c\u1ee5 th\u1ec3 kh\u00f4ng? \u0110\u1ec3 t\u00f4i t\u00ecm ch\u00ednh x\u00e1c h\u01a1n."
            )
            return _fb_ans, ["opensearch:books"], _make_book_buttons(_ocr_fb_disp)

        return "\U0001f50d R\u1ea5t ti\u1ebfc, t\u00f4i ch\u01b0a nh\u1eadn d\u1ea1ng \u0111\u01b0\u1ee3c s\u00e1ch t\u1eeb h\u00ecnh \u1ea3nh n\u00e0y. B\u1ea1n c\u00f3 th\u1ec3 cho t\u00f4i bi\u1ebft t\u00ean s\u00e1ch kh\u00f4ng?", [], []



    if intent == "chitchat":
        try:
            return _handle_chitchat(message, user_id), [], []
        except Exception as _ech2:
            _log.error("process chitchat error: %s", _ech2, exc_info=True)
            return "Xin chào! Bạn cần tìm sách hay hỗ trợ gì không? Tôi luôn sẵn sàng giúp bạn 😊", [], []

    if intent == "out_of_scope":
        # === EARLY EXIT: closing/farewell phrases ===
        _msg_oos = message.lower().strip()
        _farewell_kw = [
            "xong roi", "xong r", "the thoi", "the la xong", "ok xong", "duoc roi",
            "cam on", "cam on nhe", "cam on ban", "camon", "thanks", "thank you",
            "ok bye", "bye", "tam biet", "hen gap lai", "thoat", "logout",
            "ok minh hieu roi", "ok ok", "da hieu", "ok r", "ok.", "ok!",
            "em cam on", "anh cam on", "toi cam on", "minh cam on",
            "the nhe", "nhe", "ok nhe", "thoi nhe", "thoi", "ck", "done",
            "quen nhe", "ok thoi", "vay thoi", "anh cap nhat",
        ]
        # FIX P0: Normalize _msg_oos to ASCII for farewell matching (tránh lỗi Unicode so sánh)
        import unicodedata as _ud_fw
        _msg_oos_n = "".join(
            c for c in _ud_fw.normalize("NFD", _msg_oos.replace("đ","d"))
            if _ud_fw.category(c) != "Mn"
        ).strip()
        _BYE_DIRECT = ["tam biet", "bye bye", "goodbye", "chao nhe", "hen gap lai", "bye"]
        _is_farewell_kw = any(
            _msg_oos_n == kw or _msg_oos_n.startswith(kw + " ") or _msg_oos_n.endswith(" " + kw)
            for kw in _farewell_kw
        ) or any(kw in _msg_oos_n for kw in _BYE_DIRECT)
        if _is_farewell_kw:
            if any(w in _msg_oos_n for w in ("cam on", "thanks", "thank")):
                return (
                    TEMPLATES.get("thanks_reply") or
                    "Không có gì! Rất vui được giúp bạn. Hẹn gặp lại! 😊"
                ), [], []
            if any(w in _msg_oos_n for w in ("bye", "tam biet", "thoat", "logout", "hen gap")):
                return (
                    TEMPLATES.get("farewell") or
                    "Cảm ơn bạn đã ghé thăm BookStore! 📚 Chúc bạn đọc sách vui vẻ. Hẹn gặp lại!"
                ), [], []
            return "Không có gì! Nếu cần hỗ trợ thêm, tôi luôn ở đây 😊", [], []
        # Confidence cao từ regex → đúng OOS, trả luồng thẳng
        if nlu_result.confidence >= 0.9:
            return TEMPLATES["out_of_scope"], [], []
        # Message quá ngắn (< 8 ký tự) → hỏi lại thay vì xử lý LLM, tiết kiệm ~2 giây
        if len(message.strip()) < 8:
            return (
                "Bạn muốn tìm gì hôm nay? Tôi có thể giúp bạn **tìm sách**, "
                "**kiểm tra đơn hàng** hoặc **tư vấn chính sách** nhé!",
                [], [],
            )
        # Confidence thấp → thử smart RAG fallback

    # ══ SMART FALLBACK – Tìm sách + KB + LLM (song song) ═══════════════════
    # Pha 3: dùng _asyncio.gather() thay vì gọi tuần tự → tiết kiệm ~200-300ms
    books_fb, kb_fb = await _asyncio.gather(
        _run_in_executor(_safe_search, message, 3),
        _run_in_executor(_safe_search_kb, message, 2),
    )


    ctx_parts: list[str] = []
    if books_fb:
        # Ở Fallback, tuyệt đối không chèn nút UI. Chỉ đưa thông tin dạng text siêu tối giản
        # để LLM tự quyết định có nhắc đến hay không.
        ctx_parts.append(
            "Sách có thể liên quan:\n" + 
            "\n".join([f"- {b['title']} (Giá: {b.get('price', 0):,.0f}đ)" for b in books_fb])
        )
    if kb_fb:
        ctx_parts.append("\n".join(k["text"] for k in kb_fb))

    if ctx_parts:
        ctx    = "\n\n".join(ctx_parts)
        answer = await generate(
            message, ctx, history,
            f"{tone}, dựa vào Context để trả lời thân thiện. NẾU không chắc chắn hoặc không liên quan, hãy hỏi khách muốn tìm hiểu gì. TUYỆT ĐỐI không tự bịa thông tin."
        )
        sources: list[str] = []
        if books_fb:
            sources.append("opensearch:books")
        sources.extend(k["id"] for k in kb_fb)
        
        # KHÔNG append btns_fb để tránh ép buộc hiển thị list UI rác khi khách chỉ hỏi vu vơ
        return answer, sources, []

    # Không tìm thấy bất cứ thông tin nào liên quan
    return (
        "Tôi chưa tìm thấy thông tin phù hợp với câu hỏi này.\n"
        "Tôi có thể giúp bạn:\n"
        "• Tìm sách theo tên, tác giả, thể loại\n"
        "• Kiểm tra đơn hàng và lịch sử mua\n"
        "• Xem khuyến mãi và chính sách đổi trả\n"
        "Bạn muốn thử lại không?",
        [], [],
    )


# ── Chitchat Handler chi tiết ─────────────────────────────────────────────────
def _handle_chitchat(message: str, user_id: int | None) -> str:
    """Phân biệt các loại chitchat và trả lời phù hợp."""
    msg = message.lower()

    # Farewell
    if any(w in msg for w in ("bye", "tạm biệt", "hẹn gặp", "goodbye", "chào nhé", "gặp lại")):
        return TEMPLATES["farewell"]

    # Cảm ơn
    if any(w in msg for w in ("cảm ơn", "cam on", "thank", "cảm ơn bạn", "thanks")):
        return TEMPLATES["thanks_reply"]

    # AI identity / robot question
    if any(w in msg for w in (
        "bạn là ai", "may là ai", "là robot", "là ai", "là bot", "là ai vậy",
        "có phải ai", "là người thật", "là người không", "có phải người",
        "là chatbot", "là ai không", "are you", "ai not", "bot hay người",
        "bạn có phải", "you a robot", "you human", "real human",
    )):
        return TEMPLATES["ai_identity"]

    # Capabilities question
    if any(w in msg for w in (
        "làm được gì", "giúp gì", "làm gì được", "có thể làm gì",
        "hỗ trợ gì", "giúp tôi", "giúp minh", "chức năng",
        "what can you do", "help me with", "how can you", "danh sách",
    )):
        return (
            TEMPLATES.get("bot_capabilities") or
            "Tôi có thể giúp bạn:\n"
            "• 🔍 **Tìm sách** theo tên, tác giả, thể loại\n"
            "• 📦 **Tra cứu đơn hàng** và trạng thái giao hàng\n"
            "• 💳 **Thanh toán** và xem **khuyến mãi**\n"
            "• 🔄 **Chính sách đổi trả** sách\n"
            "• 📞 Liên hệ CSKH **0353260721** (8h–22h)\n\n"
            "Bạn cần giúp gì hôm nay?"
        )

    # Greeting – phân biệt member vs guest
    if user_id:
        return TEMPLATES["greeting_member"]
    return TEMPLATES["greeting_guest"]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _filter_and_track_books(books: list[dict], context: dict, max_items: int = 4) -> list[dict]:
    """Helper lọc sách trùng lặp và giới hạn số lượng trả về."""
    if not books:
        return []
    already_shown = set(context.get("shown_book_ids", []))
    new_books = [b for b in books if (b.get("book_id") or b.get("id")) not in already_shown]
    
    if not new_books:
        context["shown_book_ids"] = []
        context["_loop_notice"] = True
        display_books = books[:max_items]
        already_shown = set()
    else:
        context["_loop_notice"] = False
        display_books = new_books[:max_items]
        
    context["shown_book_ids"] = list(already_shown | {b.get("book_id") or b.get("id") for b in display_books})
    # Lưu last_shown_books để ordinal resolver dùng ("cuốn đầu tiên", "cuốn thứ 2")
    context["last_shown_books"] = [
        {"title": b.get("title", ""), "price": b.get("price", 0),
         "book_id": b.get("book_id") or b.get("id")}
        for b in display_books
    ]
    return display_books



def _format_books(books: list[dict]) -> str:
    if not books:
        return "Không tìm thấy sách phù hợp."
    lines = []
    
    import re
    def _clean_text(text: str) -> str:
        if not text:
            return ""
        # Xóa newline, gộp khoảng trắng thừa
        cleaned = re.sub(r'\s+', ' ', str(text)).strip()
        # Xóa dấu sao để tránh lỗi markdown
        return cleaned.replace('*', '')

    for i, b in enumerate(books, 1):
        stock_val = b.get("available_quantity", b.get("stock_quantity", b.get("stock", 0)))
        stock_str = "Còn hàng" if stock_val > 0 else "Hết hàng"
        title = _clean_text(b.get('title', 'Sách'))
        author = _clean_text(b.get('author', ''))
        
        # Nếu không có tác giả thì để "Đang cập nhật"
        if not author or author == '?':
            author_str = "Tác giả: Đang cập nhật"
        else:
            author_str = author
            
        # Trích xuất thể loại và chuẩn hóa
        category = _clean_text(b.get('category', b.get('category_name', '')))
        cat_str = f"\n   Thể loại: {category}" if category else ""
        
        lines.append(
            f"{i}. **{title}**{cat_str}\n"
            f"   – {author_str}\n"
            f"   Giá: {b.get('price', 0):,.0f}đ | {stock_str}"
        )
    return "\n\n".join(lines)


def _format_book_detail(book: dict) -> str:
    import re
    def _clean_text(text: str) -> str:
        if not text:
            return ""
        cleaned = re.sub(r'\s+', ' ', str(text)).strip()
        return cleaned.replace('*', '')

    stock_str = "Còn hàng" if book.get("available_quantity", book.get("stock_quantity", book.get("stock", 0))) > 0 else "Hết hàng"
    title = _clean_text(book.get('title', 'Sách'))
    author = _clean_text(book.get('author_name', book.get('author', '')))
    
    if not author or author == '?':
        author = "Đang cập nhật"

    parts = [
        f"**{title}**",
        f"Tác giả: {author}",
        f"Giá: {book.get('price', 0):,.0f}đ",
        f"Tình trạng: {stock_str}",
    ]
    if book.get("category") or book.get("category_name"):
        parts.append(f"Thể loại: {book.get('category', book.get('category_name',''))}")
    if book.get("avg_rating"):
        parts.append(f"Đánh giá: {book.get('avg_rating', 0):.1f}/5 ⭐")
    return "\n".join(parts)


def _format_order(order: dict) -> str:
    from datetime import datetime, timedelta
    STATUS_LABEL = {
        "pending":          "⏳ Đang chờ xử lý",
        "processing":       "🔄 Đang đóng gói & xử lý",
        "confirmed":        "✅ Đã xác nhận",
        "shipped":          "🚚 Đang vận chuyển",
        "delivered":        "✅ Đã giao thành công",
        "cancelled":        "❌ Đã hủy",
        "cancel_requested": "⏳ Yêu cầu hủy đang chờ duyệt",
        "return_requested": "⏳ Yêu cầu trả hàng đang xử lý",
        "returned":         "📦 Đã hoàn trả",
        "failed":           "⚠️ Thất bại",
    }
    # Số ngày cộng thêm từ created_at để ước tính ngày giao
    DELIVERY_OFFSET = {
        "pending":     7,   # chờ xử lý → ước tính 7 ngày
        "processing":  5,   # đang đóng gói → còn ~5 ngày
        "confirmed":   5,
        "shipped":     3,   # đang vận chuyển → còn ~3 ngày
    }

    raw_status = order.get("status", "?")
    status_display = STATUS_LABEL.get(raw_status, raw_status)

    # Tính ngày ước tính giao hàng
    est_delivery_str = ""
    created_at = order.get("created_at")
    if created_at and raw_status in DELIVERY_OFFSET:
        try:
            if isinstance(created_at, str):
                created_dt = datetime.strptime(created_at[:10], "%Y-%m-%d")
            else:
                created_dt = datetime.combine(created_at, datetime.min.time()) if hasattr(created_at, "year") else created_at
            est_dt = created_dt + timedelta(days=DELIVERY_OFFSET[raw_status])
            est_delivery_str = est_dt.strftime("%d/%m/%Y")
        except Exception:
            pass

    lines = [
        f"Đơn hàng **#{order.get('order_id','?')}**",
        f"Trạng thái: {status_display}",
    ]

    # Thêm ngày dự kiến giao cho các đơn đang xử lý
    if est_delivery_str:
        lines.append(f"Dự kiến giao: **{est_delivery_str}**")

    # Thêm địa chỉ giao hàng cho đơn đang ship
    if raw_status == "shipped" and order.get("shipping_address"):
        lines.append(f"Giao đến: {order['shipping_address']}")

    lines += [
        f"Tổng tiền: **{order.get('total_amount', 0):,.0f}đ**",
        f"Ngày đặt: {str(order.get('created_at','?'))[:10]}",
        f"Thanh toán: {order.get('payment_method','?')} – {order.get('payment_status','?')}",
    ]
    items = order.get("items") or []
    if items:
        lines.append("\nSách đã mua:")
        for it in items:
            name  = it.get("book_name") or "?"
            qty   = it.get("quantity", 1)
            price = it.get("total_price") or it.get("unit_price", 0)
            lines.append(f"  • {name} ×{qty} — {price:,.0f}đ")
    return "\n".join(lines)



# ── Trigger words cần bỏ trước khi search ─────────────────────────────────────
_BOOK_TRIGGERS = re.compile(
    r"^(t[ìi]m\s*s[áa]ch|t[ìi]m\s*ki[ếe]m|t[ìi]m\s+"
    r"|search\s*book|find\s*book|cho\s*t[ôo]i|gi[úu]p\s*t[ôo]i\s*t[ìi]m"
    r"|c[óo]\s*s[áa]ch\s*n[àa]o|s[áa]ch\s*v[ềe]|t[ôo]\s*mu[ốo]n\s*t[ìi]m)\s*",
    re.IGNORECASE,
)


def _clean_book_query(message: str) -> str:
    """Loại bỏ trigger words khỏi query book_search."""
    cleaned = _BOOK_TRIGGERS.sub("", message.strip())
    return cleaned.strip() if len(cleaned.strip()) >= 2 else message


def _extract_title_from_message(message: str) -> str:
    """Trích tiêu đề sách đơn giản từ message."""
    import re as _re
    # Tên sách trong dấu ngoặc kép hoặc ngoặc kép tiếng Việt
    quoted = _re.search(r'["\u201c\u2018](.+?)["\u201d\u2019]', message)
    if quoted:
        return quoted.group(1)
    # Sau từ "sách" hoặc "cuốn"
    after_sach = _re.search(r'(?:sách|cuốn)\s+(.+?)(?:\s+(?:giá|có|còn|là|như|bao|nào|không|nhé)|$)',
                             message, _re.IGNORECASE)
    if after_sach:
        return after_sach.group(1).strip()
    return ""


def _handle_confirmation(
    intent: str, context: dict
) -> tuple:
    """
    Xử lý confirmation flow (yes/no) và trả về 3-tuple (text, sources, btns)
    — phù hợp với contract của _process_inner → process().
    """
    pending = context.get("pending_confirmation", {})

    if intent == "confirmation_yes":
        action   = pending.get("action")
        order_id = pending.get("order_id")
        context.pop("pending_confirmation", None)

        if action == "order_cancel" and order_id:
            btn = NavigateButton(
                label=f"📦 Hủy đơn #{order_id} ngay tại đây",
                url=f"/account?tab=orders&orderId={order_id}&action=cancel",
                type="order",
                metadata={"order_id": order_id, "action": "cancel"},
            )
            return (
                f"❗ Để hủy đơn hàng **#{order_id}**, vui lòng xác nhận trên trang quản lý:\n"
                "⚠️ Vì lý do bảo mật, hành động hủy cần được thực hiện trực tiếp bởi bạn.\n"
                "Nhấn nút bên dưới để đi đến trang đơn hàng và hoàn tất hủy:",
                ["mysql:orders"],
                [btn],
            )

        return "✅ Đã xác nhận thành công!", [], []

    elif intent == "confirmation_no":
        context.pop("pending_confirmation", None)
        return "👍 Đã giữ lại đơn hàng. Đơn của bạn vẫn được xử lý bình thường!", [], []

    # Fallback: pending hết hiệu lực
    context.pop("pending_confirmation", None)
    return "⏳ Yêu cầu xác nhận đã hết hiệu lực. Bạn cần hỗ trợ gì thêm không?", [], []


# ── Clarify-First System Helpers (v3) ────────────────────────────────────────

def _is_slot_missing(
    intent: str,
    required_slot: str,
    entities: dict,
    user_id: int | None,
    context: dict,
    message: str = "",
) -> bool:
    """
    Kiểm tra xem slot quan trọng có bị thiếu không.
    Một số intent có fallback tự nhiên (order_status dùng user_id, book_search dùng message gốc)
    nên không cần hỏi lại trong mọi trường hợp.
    """
    # book_search: chỉ hỏi nếu message quá ngắn (< 3 ký tự sau normalize)
    if intent == "book_search":
        query = entities.get("query", "")
        # Nếu có query hoặc genre → không cần hỏi
        if entities.get("query") or entities.get("genre"):
            return False
        # Nếu message gốc đủ dài → dùng luôn làm query
        return False  # book_search tự fallback message gốc → không hỏi

    # order_status: không hỏi nếu user đã đăng nhập (có thể xem đơn gần nhất)
    if intent == "order_status":
        has_id = entities.get("order_id") or context.get("last_order_id")
        return not has_id and not user_id  # chỉ hỏi khi không có cả 2

    # order_cancel: bắt buộc phải có order_id (trừ khi đã đăng nhập)
    if intent == "order_cancel":
        return not entities.get("order_id") and not context.get("last_order_id") and not user_id

    # return_request: cần order_id để xử lý
    if intent == "return_request":
        return not entities.get("order_id")

    # book_detail, book_availability, book_compare: cần tên sách
    if intent in ("book_detail", "book_availability", "book_compare"):
        # FIX-BUG4+BUG3: nếu có OCR books → book_compare dùng last_ocr_books, không cần book_title
        if intent == "book_compare":
            _ocr_cmp = context.get("last_ocr_books", [])
            if len(_ocr_cmp) >= 2:
                return False  # Có OCR log → không cần slot-filling
        return not entities.get("book_title")

    # recommend_gift: nếu không có recipient_type → hỏi để gợi ý chính xác
    if intent == "recommend_gift":
        return not entities.get("recipient_type")

    # recommend_category / recommend_combo: nếu không có genre và không có context → hỏi
    # FIX-C01+C02: nếu có OCR context (last_ocr_books / last_found_title) → KHÔNG hỏi slot
    if intent == "recommend_category" or intent == "recommend_combo":
        _has_ocr = bool(context.get("last_ocr_books") or context.get("last_found_title") or context.get("last_shown_books"))
        return not entities.get("genre") and not context.get("last_category") and not _has_ocr

    # voucher_apply: cần voucher_code — nhưng TRƯỚC HẾT thử auto-extract từ message
    if intent == "voucher_apply":
        if entities.get("voucher_code"):
            return False  # đã có entity → không cần hỏi
        # Auto-extract mã từ message trước khi quyết định hỏi lại
        import re as _re_sf, unicodedata as _ud_sf
        _mn_sf = "".join(
            c for c in _ud_sf.normalize("NFD",
                message.replace("đ","d").replace("Đ","D").upper())
            if _ud_sf.category(c) != "Mn"
        )
        _sf_match = _re_sf.search(
            r'(?:NHAP|DUNG|SU DUNG|AP|KIEM TRA|CHECK|MA|CODE|VOUCHER|COUPON)\s+([A-Z][A-Z0-9]{3,19})\b|'
            r'\b([A-Z][A-Z0-9]{3,19})\b(?=\s*(?:DUOC GIAM|GIAM|DUNG DUOC|AP DUNG|HIEU LUC|LA MA|LA CODE))',
            _mn_sf
        )
        if _sf_match:
            _found_code = (_sf_match.group(1) or _sf_match.group(2) or "").strip()
            if _found_code:
                entities["voucher_code"] = _found_code  # inject vào entities để handler dùng
                return False  # không cần hỏi lại
        return True  # không tìm thấy mã → hỏi lại

    return False


async def _handle_slot_filling_response(
    message: str,
    entities: dict,
    user_id: int | None,
    context: dict,
    history: list[dict],
    user_profile: dict,
) -> tuple[str, list[str], list["NavigateButton"]]:
    """
    Xử lý khi user vừa trả lời câu hỏi slot-filling.
    Merge câu trả lời vào context, sau đó re-process với intent gốc.
    """
    pending    = context.pop("pending_slot_filling")
    target_intent    = pending["target_intent"]
    original_message = pending["original_message"]
    filled_entities  = pending.get("filled_entities", {})
    required_slot    = pending["required_slot"]

    # ── Cancel slot-filling: user nói "không/thôi/giữ lại" để hủy ──
    # Dùng ASCII-normalized để tránh lỗi Unicode regex với tiếng Việt có dấu
    import re as _re_cancel, unicodedata as _uc_cancel
    def _norm_cancel(s):
        s = s.replace("đ","d").replace("Đ","d")
        return "".join(c for c in _uc_cancel.normalize("NFD",s.lower()) if _uc_cancel.category(c)!="Mn")
    _CANCEL_SF_ASCII = _re_cancel.compile(
        r"(khong|thoi|giu lai|bo qua|huy y dinh|khong huy|khong can"
        r"|dung huy|giu nguyen|khong muon huy|dung that|giu don"
        r"|khong dung|khong thuc hien|bo di|thoat)",
        _re_cancel.IGNORECASE
    )
    if target_intent in ("order_cancel", "return_request") and _CANCEL_SF_ASCII.search(_norm_cancel(message)):
        context.pop("pending_intent_confirm", None)
        return (
            "Được rồi! Tôi sẽ **giữ nguyên đơn hàng** của bạn. "
            "Không có thay đổi nào được thực hiện.\n\n"
            "Tôi có thể giúp gì thêm cho bạn không?",
            [], [],
        )

    # ── Topic-change detection: nếu user chuyển sang intent mới hẳn → bỏ slot-filling ──

    from chatbot_app.nlu.customer_intent_classifier import NLUResult as _NLU, detect_intent as _detect
    _new_nlu = _detect(message)
    _new_intent = _new_nlu.intent
    # Các intent "rõ ràng" mà user có thể gõ để thoát slot-filling hiện tại
    _clear_intents = {
        "book_search", "recommend_category", "recommend_gift", "recommend_trending",
        "order_status", "order_history", "cart_help", "store_info",
        "chitchat", "out_of_scope",
    }
    # Chỉ thoát slot-filling khi user chuyển topic RÕ RÀNG (confidence cao)
    # GUARD: không thoát nếu message là câu trả lời trực tiếp slot cần (đVD: mã đơn #1234)
    import re as _re_sf_guard
    _is_direct_order_answer = (
        target_intent in ("order_cancel", "return_request")
        and bool(_re_sf_guard.search(r"#?\d{4,}", message))
    )
    if not _is_direct_order_answer and _new_intent != target_intent and _new_intent in _clear_intents and _new_nlu.confidence >= 0.88:
        # User đã chuyển topic → xử lý theo intent mới
        return await process(
            message=message,
            nlu_result=_new_nlu,
            user_id=user_id,
            context=context,
            history=history,
            user_profile=user_profile,
        )

    # Merge entities: ưu tiên entities mới (từ câu trả lời slot) lên các entities cũ
    merged_entities = {**filled_entities, **entities}

    # Nếu slot vẫn chưa được điền → thử dùng message thô (user gõ tên trực tiếp)
    if required_slot not in merged_entities:
        if target_intent in ("book_detail", "book_availability", "book_compare"):
            merged_entities["book_title"] = message.strip()
        elif target_intent == "order_cancel" or target_intent == "return_request":
            import re as _re
            m = _re.search(r"#?(\d{4,})", message)
            if m:
                merged_entities["order_id"] = m.group(1)
        elif target_intent == "recommend_gift":
            merged_entities["recipient_type"] = _map_recipient_from_text(message)
        elif target_intent == "recommend_category":
            if any(kw in message.lower() for kw in ["khac", "khác", "nua", "nữa", "the loai", "thể loại", "co cai", "có cái", "nao", "nào"]):
                merged_entities["genre"] = None # Trigger lặp hỏi lại với list random mới
            else:
                merged_entities["genre"] = message.strip()
        elif target_intent == "voucher_apply":
            merged_entities["voucher_code"] = message.strip().upper()

    # Tạo NLUResult giả với intent gốc + entities đã merge
    fake_nlu = _NLU(
        intent=target_intent,
        confidence=0.95,   # giả định regex-level confidence sau khi user xác nhận
        entities=merged_entities,
        sentiment="NEUTRAL",
    )

    # Re-process voi message da duoc lam ro bang slot vua dien
    if target_intent == "recommend_category" and merged_entities.get("genre"):
        enriched_message = "Tim sach the loai " + merged_entities["genre"]
    elif target_intent == "recommend_gift" and merged_entities.get("recipient_type"):
        enriched_message = "Tim sach cho " + merged_entities["recipient_type"]
    elif target_intent in ("order_cancel", "return_request") and merged_entities.get("order_id"):
        # [FIX] Giữ keyword để early interceptor không override intent sai
        # "don hang #XXXX" không có return kw → interceptor đổi sang order_status!
        if target_intent == "return_request":
            enriched_message = "muon tra hang don hang #" + merged_entities["order_id"]
        else:
            enriched_message = "huy don hang #" + merged_entities["order_id"]
    else:
        enriched_message = original_message + " " + message
    return await process(
        message=enriched_message,
        nlu_result=fake_nlu,
        user_id=user_id,
        context=context,
        history=history,
        user_profile=user_profile,
    )



def _map_recipient_from_text(text: str) -> str:
    """Map câu trả lời tự do về recipient_type chuẩn."""
    t = text.lower()
    if any(w in t for w in ("bé", "be", "0", "1", "2", "3", "4", "5", "6")):
        return "child_0_6"
    if any(w in t for w in ("7", "8", "9", "10", "11", "12", "thiếu nhi")):
        return "child_7_12"
    if any(w in t for w in ("teen", "thiếu niên", "13", "14", "15", "16", "17")):
        return "teenager"
    if any(w in t for w in ("gái", "phụ nữ", "mẹ", "chị", "nữ")):
        return "adult_female"
    if any(w in t for w in ("trai", "nam", "anh", "bạn trai", "chồng", "bố", "ông")):
        return "adult_male"
    if any(w in t for w in ("già", "lớn tuổi", "ông bà", "cao tuổi", "lão")):
        return "elderly"
    return "adult"


async def _handle_intent_confirm_response(
    message: str,
    intent: str,
    nlu_result,
    user_id: int | None,
    context: dict,
    history: list[dict],
    user_profile: dict,
) -> tuple[str, list[str], list["NavigateButton"]]:
    """
    Xử lý khi user vừa xác nhận (hoặc phủ nhận) intent mà chatbot đoán.

    Các trường hợp:
      - User nói "có", "đúng", "ok"  → tiến hành với guessed_intent
      - User nói "không", "sai"      → hỏi lại rõ hơn
      - User gõ câu mới hoàn toàn    → bỏ pending, re-process câu mới với intent mới
    """
    pending        = context.pop("pending_intent_confirm")
    guessed_intent = pending["guessed_intent"]
    original_msg   = pending["original_message"]

    is_yes = intent in ("confirmation_yes",) or any(
        w in message.lower()
        for w in ("có", "đúng", "ok", "oke", "yes", "phải", "chính xác", "đúng rồi", "vâng", "ừ",
                  "có, tìm", "tìm sách tương tự", "muốn tìm")
    )
    is_no = intent in ("confirmation_no",) or any(
        w in message.lower()
        for w in ("không", "sai", "no", "nope", "không phải", "không cảm ơn", "cảm ơn thôi")
    )

    if is_yes:
        # User xác nhận → process với guessed_intent và original_message
        from chatbot_app.nlu.customer_intent_classifier import NLUResult as _NLU
        confirmed_nlu = _NLU(
            intent=guessed_intent,
            confidence=0.95,
            entities=nlu_result.entities,
            sentiment="NEUTRAL",
        )
        return await process(
            message=original_msg,
            nlu_result=confirmed_nlu,
            user_id=user_id,
            context=context,
            history=history,
            user_profile=user_profile,
        )

    if is_no:
        # User phủ nhận → hỏi lại
        return (
            "Xin lỗi vì tôi hiểu nhầm! Bạn có thể **nói rõ hơn** yêu cầu của mình không?\n"
            "Ví dụ: _\"Tôi muốn tìm sách\"_, _\"Kiểm tra đơn #12345\"_...",
            [],
            [],
        )

    # User gõ câu hoàn toàn mới → xử lý câu mới (intent đã detect từ NLU bình thường)
    return await process(
        message=message,
        nlu_result=nlu_result,
        user_id=user_id,
        context=context,
        history=history,
        user_profile=user_profile,
    )
