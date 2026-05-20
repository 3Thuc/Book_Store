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
import asyncio as _asyncio
import functools as _functools
from chatbot_app.nlu.customer_intent_classifier import NLUResult
from chatbot_app.nlu.ner_extractor import extract_entities
from chatbot_app.retrieval.opensearch_retriever import search_books_semantic, search_knowledge_base
from chatbot_app.retrieval.sql_retriever import (
    get_order_info, get_user_orders, get_books_by_genre, get_book_price
)
from chatbot_app.generation.llm_client import generate
from chatbot_app.models import NavigateButton

# ── CSKH escalation message ──────────────────────────────────────────────────
ESCALATE_MSG = """Để giải quyết nhanh nhất, vui lòng liên hệ đội CSKH của chúng tôi:

**Hotline:** 1800-xxxx (miễn phí, 8h–22h)
**Email:** cskh@bookstore.vn

Chúng tôi sẽ phản hồi trong **2–4 giờ làm việc**."""

# ── Template responses ────────────────────────────────────────────────────────
TEMPLATES = {
    "greeting_guest": (
        "Xin chào! Tôi là trợ lý BookStore.\n"
        "Tôi có thể giúp bạn **tìm sách**, **tra cứu đơn hàng**, **kiểm tra khuyến mãi** "
        "và **tư vấn chính sách đổi trả**.\n"
        "Bạn cần hỗ trợ gì hôm nay?"
    ),
    "greeting_member": (
        "Xin chào! Chào mừng bạn quay lại BookStore.\n"
        "Bạn muốn tìm sách, kiểm tra đơn hàng hay có câu hỏi gì khác?"
    ),
    "farewell":        "Cảm ơn bạn đã sử dụng BookStore! Chúc bạn đọc sách vui vẻ. Hẹn gặp lại!",
    "thanks_reply":    "Không có gì! Tôi luôn sẵn sàng hỗ trợ bạn. Còn điều gì tôi có thể giúp không?",
    "ai_identity": (
        "Tôi là trợ lý ảo BookStore, được thiết kế để hỗ trợ bạn tìm sách "
        "và giải đáp thắc mắc về dịch vụ BookStore. Tôi không phải là người thật, "
        "nhưng tôi sẽ cố gắng hết sức để giúp bạn!"
    ),
    "bot_capabilities": (
        "Tôi có thể giúp bạn:\n"
        "• **Tìm sách** theo tên, tác giả, thể loại hoặc từ khóa\n"
        "• **Gợi ý sách** phù hợp với sở thích hoặc làm quà tặng\n"
        "• **Kiểm tra đơn hàng** và trạng thái giao hàng\n"
        "• **Tra cứu khuyến mãi** và cách áp mã giảm giá\n"
        "• **Giải đáp chính sách** đổi trả, hoàn tiền, thanh toán\n"
        "• **Hỗ trợ tài khoản**: mật khẩu, thông tin cá nhân"
    ),
    "no_auth":       "Vui lòng **đăng nhập** để sử dụng tính năng này.",
    "no_order":      "Tôi không tìm thấy đơn hàng này. Vui lòng kiểm tra lại mã đơn.",
    "out_of_scope": (
        "Xin lỗi, câu hỏi này nằm ngoài phạm vi tôi có thể hỗ trợ.\n"
        "Tôi chuyên về sách và dịch vụ mua sách tại BookStore.\n"
        "Bạn có thể hỏi về: **tìm sách**, **đơn hàng**, **khuyến mãi** hoặc **chính sách đổi trả**."
    ),
    "confirm_pending": "Bạn vui lòng xác nhận: **Có** (tiếp tục) hoặc **Không** (hủy bỏ)?",
    "book_not_found": (
        "Tôi chưa tìm thấy sách phù hợp với từ khóa này. "
        "Bạn thử đổi từ khóa, tìm theo tên tác giả, hoặc chọn thể loại sách nhé!"
    ),
    "negative_empathy": (
        "Tôi hiểu bạn đang không hài lòng và thực sự xin lỗi về điều này. "
        "Hãy cho tôi biết chi tiết hơn để tôi có thể hỗ trợ tốt nhất cho bạn."
    ),
    "account_help_guide": (
        "Tôi có thể hỗ trợ các vấn đề tài khoản sau:\n"
        "• **Quên mật khẩu**: Vào trang đăng nhập → \"Quên mật khẩu\" → nhập email\n"
        "• **Đổi email/SĐT**: Vào phần **Tài khoản → Thông tin cá nhân**\n"
        "• **Tài khoản bị khóa**: Liên hệ hotline **1800-xxxx** để mở khóa\n"
        "• **Đăng ký mới**: Nhấn **Đăng ký** trên trang chủ\n\n"
        "Vấn đề của bạn thuộc loại nào ở trên?"
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
    "promotion_current":   "\n\nBạn muốn tìm sách đang sale hoặc hỏi cách áp mã khuyến mãi?",
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
        if not book_id:
            continue
        title = book.get("title") or "Sách"
        if len(title) > 80:
            title = title[:77] + "..."
            
        price  = book.get("price", 0)
        rating = book.get("avg_rating") or book.get("rating", 0)
        label  = f"{title}"
        if price:
            label += f" – {price:,.0f}đ"
        if rating:
            label += f" ★{float(rating):.1f}"
        buttons.append(NavigateButton(
            label=label,
            url=f"/book/{book_id}",   # FIX: khớp route /book/:id trong App.tsx
            type="book",
            metadata={"book_id": book_id, "price": price, "rating": float(rating) if rating else 0},
        ))
    return buttons


def _make_order_buttons(orders: list[dict] | None = None) -> list[NavigateButton]:
    """Tạo navigate buttons cho đơn hàng → link /account?tab=orders"""
    btns = [NavigateButton(
        label="📦 Xem tất cả đơn hàng",
        url="/account?tab=orders",
        type="order",
    )]
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
                label=f"{emoji} Đơn #{oid} – {amount:,.0f}đ",
                url=f"/account?tab=orders&orderId={oid}",
                type="order",
                metadata={"order_id": oid, "status": status},
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

    # ── [v7] Coreference Resolution (ASCII-normalized) ────────────────
    import unicodedata as _ud7
    _mn7 = "".join(c for c in _ud7.normalize("NFD", message.lower().replace("đ","d")) if _ud7.category(c)!="Mn")
    _ck7 = ["tim hieu them","ve no","cuon do","cuon nay","chi tiet hon",
             "xem them","biet them","gia cuon","bao nhieu tien",
             "sahcs do","sach do","cuon sach do","no la"]
    if intent not in ("book_detail","book_availability","book_review"):
        if any(t in _mn7 for t in _ck7):
            _rf7 = context.get("last_found_title") or context.get("last_search_query")
            if _rf7:
                intent = "book_detail"
                entities["book_title"] = _rf7

    # ── Slot-filling pending ──────────────────────────────────
    if context.get("pending_slot_filling") or context.get("pending_intent_confirm"):
        # Những trường hợp này cần logic phức tạp → delegate về process() an toàn
        answer, sources, btns = await process(
            message=message, nlu_result=nlu_result, user_id=user_id,
            context=context, history=history, user_profile=user_profile,
        )
        return answer, tone, intent, btns, sources, True  # is_template=True

    # ── 0a. CONFIDENCE CHECK ──────────────────────────────────
    is_sbert_result = nlu_result.confidence < 0.95
    in_borderline   = CONFIDENCE_SBERT_MIN <= nlu_result.confidence < CONFIDENCE_CONFIRM_THRESHOLD
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
            "_(Trả lời **Có** để tiếp tục, hoặc nói rõ hơn điều bạn cần)_",
            tone, intent, btns, [], True
        )

    # ── 0b. SLOT-FILLING CHECK ────────────────────────────────────
    if intent in SLOT_FILLING_CONFIG and intent not in NO_CONFIRM_INTENTS:
        required_slot, question, quick_replies = SLOT_FILLING_CONFIG[intent]
        if _is_slot_missing(intent, required_slot, entities, user_id, context):
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
                
            return question, tone, intent, btns, [], True

    # ── EARLY EXITS (template, no LLM needed) ────────────────
    if intent in HARD_ESCALATE_INTENTS:
        prefixes = {
            "complaint_damaged": "Rất tiếc khi nghe sách bạn nhận bị hư hỏng! ",
            "complaint_wrong":   "Xin lỗi vì đã giao sai sách cho bạn! ",
            "payment_issue":     "Tôi rất tiếc vì bạn gặp sự cố thanh toán! ",
        }
        return prefixes.get(intent, "") + ESCALATE_MSG, tone, intent, [], ["escalate:cskh"], True

    if intent in SOFT_ESCALATE_INTENTS:
        btn = NavigateButton(label="👤 Đi đến Trang tài khoản", url="/account", type="page")
        return TEMPLATES["account_help_guide"], tone, intent, [btn], [], True

    if intent == "chitchat":
        return _handle_chitchat(message, user_id), tone, intent, [], [], True

    if intent == "out_of_scope":
        if nlu_result.confidence >= 0.9:
            return TEMPLATES["out_of_scope"], tone, intent, [], [], True
        if len(message.strip()) < 8:
            return (
                "Bạn muốn tìm gì hôm nay? Tôi có thể giúp bạn **tìm sách**, "
                "**kiểm tra đơn hàng** hoặc **tư vấn chính sách** nhé!",
                tone, intent, [], [], True
            )

    if intent == "store_info":
        kb = search_knowledge_base("thông tin cửa hàng liên hệ hotline", top_k=2)
        if kb:
            ctx = "\n\n".join(k["text"] for k in kb)
            return ctx, "ngắn gọn 2-3 câu, trình bày thông tin liên hệ rõ ràng", intent, [], ["opensearch:kb"], False
        return (
            "**Thông tin liên hệ BookStore:**\n"
            "• **Hotline:** 1800-xxxx (miễn phí, 8h–22h)\n"
            "• **Email:** cskh@bookstore.vn\n"
            "• **Website:** www.bookstore.vn\n"
            "• **Giờ hỗ trợ:** Thứ 2–Chủ nhật, 8h–22h",
            tone, intent, [], ["opensearch:kb"], True
        )

    # ── RECOMMENDATION INTENTS – Parallel retrieval ───────────
    neg_count = context.get("negative_count", 0)
    if sentiment == "NEGATIVE":
        context["negative_count"] = neg_count + 1
        if neg_count + 1 >= 3:
            return ESCALATE_MSG, tone, intent, [], ["escalate:cskh"], True
        tone = "đồng cảm sâu sắc, xin lỗi trước, sau đó hỏi rõ vấn đề cụ thể" if neg_count + 1 == 1 else "đồng cảm, đề xuất giải pháp cụ thể, nhắc hotline"
    else:
        context["negative_count"] = 0

    if context.get("pending_confirmation"):
        answer, srcs, btns = _handle_confirmation(intent, context)
        return answer, tone, intent, btns, srcs, True

    # ── Book search / recommendation → parallel OpenSearch ────
    if intent == "book_search":
        raw_query = entities.get("query") or message
        query = _clean_book_query(raw_query)
        price_max = entities.get("price_max")
        price_min = entities.get("price_min")
        genre = entities.get("genre")
        
        # TÌM KIẾM HYBRID OPENSEARCH (SBERT k-NN + BM25)
        books = await _run_in_executor(
            lambda: search_books_semantic(
                query, top_k=8,
                price_max=price_max, price_min=price_min, genre=genre,
                min_score=CONFIDENCE_SBERT_MIN
            )
        )
        
        if not books:
            books = await _run_in_executor(get_books_by_genre, query, 8)
        if not books and genre:
            books = await _run_in_executor(get_books_by_genre, genre, 8)
        if not books:
            return TEMPLATES["book_not_found"], tone, intent, [], [], True
            
        context["last_search_query"] = query
        
        # Kiểm tra mức độ khớp từ khóa trên tập books trả về
        query_keywords = [w for w in raw_query.lower().split() if len(w) >= 2]
        matched_books = []
        for b in books:
            if any(kw in b.get("title", "").lower() for kw in query_keywords):
                matched_books.append(b)
                
        if matched_books:
            # Tìm thấy -> chỉ hiện 1 link duy nhất
            display_books = _filter_and_track_books(matched_books, context, max_items=1)
            book_titles_str = ", ".join([f'"{b.get("title", "")}"' for b in display_books])
            ctx = f"""[HỆ THỐNG]: Khách hỏi sách: "{raw_query}".
Hệ thống TÌM THẤY CHÍNH XÁC sách trong kho: {book_titles_str}.
NHIỆM VỤ: Thông báo vui vẻ, dứt khoát rằng cửa hàng "Có cuốn sách đó", và mời khách tham khảo nút bên dưới. KHÔNG NÓI là "tìm thấy các sách chủ đề tương tự".
LUẬT: Chỉ nhắc đúng tên sách trong danh sách: {book_titles_str}. Không tự bịa thêm tên sách khác."""
        else:
            # Không tìm thấy -> hiện tối đa 4 link gợi ý (fallback)
            display_books = _filter_and_track_books(books, context, max_items=4)
            book_titles_str = ", ".join([f'"{b.get("title", "")}"' for b in display_books])
            ctx = f"""[HỆ THỐNG]: Khách tìm sách: "{raw_query}".
Kho hàng KHÔNG CÓ sách khớp chính xác, hệ thống chỉ gợi ý các sách có thể liên quan: {book_titles_str}.
NHIỆM VỤ: Thú nhận khéo léo rằng chưa có "{raw_query}", sau đó giới thiệu các sách trên như là phương án thay thế đáng tham khảo.
LUẬT: Chỉ dùng tên sách trong danh sách: {book_titles_str}. Không tự bịa tên sách khác."""
        
        if display_books:
            context["last_category"] = display_books[0].get("category", "")
            # Lưu tên cuốn sách chính xác nhất được hiển thị để coref có thể resolve sau
            context["last_found_title"] = display_books[0].get("title", "")
        
        full_tone = tone
        
        btns = _make_book_buttons(display_books)
        return ctx, full_tone, intent, btns, ["opensearch:books"], False

    if intent == "recommend_trending":
        books = await _run_in_executor(search_books_semantic, "sách bán chạy được yêu thích nhất", 8)
        display_books = _filter_and_track_books(books, context, max_items=4)
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách hot."
        btns = _make_book_buttons(display_books)
        full_tone = f"{tone}, viết 1 câu mời xem sách hot bên dưới, KHÔNG tự chế tên sách."
        return ctx, full_tone, intent, btns, ["opensearch:books"], False

    if intent == "recommend_personal":
        if not user_id:
            books = await _run_in_executor(search_books_semantic, _clean_book_query(message), 8)
            display_books = _filter_and_track_books(books, context, max_items=4)
            ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách ưu đãi."
            full_tone = f"{tone}, viết 1 câu dẫn dắt ngắn gọn mời xem thẻ sách bên dưới, nhắc đăng nhập."
            return ctx, full_tone, intent, _make_book_buttons(display_books), ["opensearch:books"], False
        genres = user_profile.get("favorite_genres", [])
        genre  = genres[0] if genres else ""
        fetch = _run_in_executor(get_books_by_genre, genre, 8) if genre else _run_in_executor(search_books_semantic, "sách bán chạy hay đọc nhiều", 8)
        books = await fetch
        display_books = _filter_and_track_books(books, context, max_items=4)
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách gợi ý."
        full_tone = f"{tone}, viết 1 câu dẫn dắt dựa trên sở thích, KHÔNG bịa tên sách."
        return ctx, full_tone, intent, _make_book_buttons(display_books), ["mysql:books"], False

    if intent in ("recommend_combo", "recommend_category"):
        genre = entities.get("genre")
        if not genre and context.get("last_category"):
            genre = context["last_category"]
            
        if genre:
            books = await _run_in_executor(get_books_by_genre, genre, 10)
            context["last_category"] = genre
        else:
            books = await _run_in_executor(search_books_semantic, message, 10)
            
        display_books = _filter_and_track_books(books, context, max_items=4)
        
        if genre:
            ctx = f"[HỆ THỐNG]: Khách muốn tham khảo sách thuộc mảng {genre}. Cửa hàng hiện có {len(display_books)} cuốn nổi bật liên quan."
        else:
            ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách."
            
        full_tone = f"{tone}, 1 câu mời xem thẻ sách cùng thể loại ở bên dưới, KHÔNG tự chế tên sách."
        return ctx, full_tone, intent, _make_book_buttons(display_books), ["opensearch:books"], False

    if intent == "recommend_gift":
        recipient = entities.get("recipient_type", "adult")
        price_max = entities.get("price_max") or entities.get("budget", 300_000)
        genre_map = {
            "child_0_6": "sách tranh thiếu nhi", "child_7_12": "truyện thiếu nhi khoa học",
            "teenager": "kỹ năng sống teen", "adult_female": "văn học tâm lý phụ nữ",
            "adult_male": "kinh tế lịch sử", "adult": "kỹ năng sống văn học", "elderly": "hồi ký sức khỏe",
        }
        gift_genre = genre_map.get(recipient, "kỹ năng sống")
        books = await _run_in_executor(
            lambda: search_books_semantic(f"sách {gift_genre} tặng quà", top_k=8, price_max=price_max)
        )
        display_books = _filter_and_track_books(books, context, max_items=4)
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách phù hợp tặng quà."
        full_tone = f"{tone}, 1 câu nói về việc tặng quà và mời xem sách bên dưới, KHÔNG tự chế tên sách."
        return ctx, full_tone, intent, _make_book_buttons(display_books), ["opensearch:books"], False

    # ── Mọi intent còn lại → delegate về process() thông thường ─
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
    Smart Router: intent → agent → data source → response + link nút bấm

    [v3] Clarify-First flow:
      - pending_slot_filling → user vừa trả lời câu hỏi slot → merge vào context
      - pending_intent_confirm → user xác nhận/từ chối intent → xử lý hoặc hỏi lại
      - confidence borderline (0.52–0.65) → hỏi xác nhận intent
      - thiếu entity quan trọng → hỏi slot-filling
    """
    intent    = nlu_result.intent
    entities  = extract_entities(message, intent)
    tone      = user_profile.get("tone", "thân thiện, ngắn gọn")
    sentiment = nlu_result.sentiment

    # Merge entities từ NLU + entities mới extract
    entities = {**nlu_result.entities, **entities}
    
    # ── [v7] Coreference Resolution (ASCII-normalized) ────────────────
    import unicodedata as _ud7
    _mn7 = "".join(c for c in _ud7.normalize("NFD", message.lower().replace("đ","d")) if _ud7.category(c)!="Mn")
    _ck7 = ["tim hieu them","ve no","cuon do","cuon nay","chi tiet hon",
             "xem them","biet them","gia cuon","bao nhieu tien",
             "sahcs do","sach do","cuon sach do","no la"]
    if intent not in ("book_detail","book_availability","book_review"):
        if any(t in _mn7 for t in _ck7):
            _rf7 = context.get("last_found_title") or context.get("last_search_query")
            if _rf7:
                intent = "book_detail"
                entities["book_title"] = _rf7

    # ══════════════════════════════════════════════════════════════════════
    # [v3] CLARIFY-FIRST: Xử lý các pending state TRƯỚC mọi logic khác
    # ══════════════════════════════════════════════════════════════════════

    # ── A. Đang chờ user điền slot (slot-filling pending) ────────────────
    if context.get("pending_slot_filling"):
        answer, sources, btns = await _handle_slot_filling_response(
            message, entities, user_id, context, history, user_profile
        )
        return answer, sources, btns

    # ── B. Đang chờ user xác nhận intent (intent-confirm pending) ────────
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

    if is_sbert_result and in_borderline and intent not in NO_CONFIRM_INTENTS:
        context["pending_intent_confirm"] = {
            "guessed_intent":   intent,
            "original_message": message,
            "confidence":       nlu_result.confidence,
        }
        desc = INTENT_CONFIRM_DESC.get(intent, intent)
        return (
            f"Bạn đang muốn **{desc}** phải không?\n"
            "_(Trả lời **Có** để tiếp tục, hoặc nói rõ hơn điều bạn cần)_",
            [],
            [
                NavigateButton(label="✅ Đúng rồi!",  url="", type="confirm_yes"),
                NavigateButton(label="❌ Không phải", url="", type="confirm_no"),
            ],
        )

    # ══════════════════════════════════════════════════════════════════════
    # [v3] SLOT-FILLING CHECK: Thiếu entity quan trọng → hỏi trước khi xử lý
    # Chỉ áp dụng khi confidence cao (regex hit hoặc SBERT chắc chắn)
    # ══════════════════════════════════════════════════════════════════════
    if intent in SLOT_FILLING_CONFIG and intent not in NO_CONFIRM_INTENTS:
        required_slot, question, quick_replies = SLOT_FILLING_CONFIG[intent]
        missing = _is_slot_missing(intent, required_slot, entities, user_id, context)
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
        return _handle_confirmation(intent, context)

    # ══ NHÓM A – TÌM KIẾM SÁCH ═════════════════════════════════════════════
    if intent == "book_search":
        raw_query = entities.get("query") or message
        query     = _clean_book_query(raw_query)
        
        # Truy vấn hệ thống Hybrid OpenSearch (BM25 + Semantic Vector)
        price_max = entities.get("price_max")
        price_min = entities.get("price_min")
        genre = entities.get("genre")
        
        books = search_books_semantic(
            query, top_k=8,
            price_max=price_max, price_min=price_min, genre=genre,
            min_score=CONFIDENCE_SBERT_MIN
        )
                
        if not books:
            books = get_books_by_genre(query, limit=8)
        if not books and genre:
            books = get_books_by_genre(genre, limit=8)
        if not books:
            return TEMPLATES["book_not_found"], [], []
            
        context["last_search_query"] = query
        
        # Kiểm tra mức độ khớp từ khóa trên tập books trả về
        query_keywords = [w for w in raw_query.lower().split() if len(w) >= 2]
        matched_books = []
        for b in books:
            if any(kw in b.get("title", "").lower() for kw in query_keywords):
                matched_books.append(b)
                
        if matched_books:
            # Tìm thấy -> chỉ lấy 1 cuốn
            display_books = _filter_and_track_books(matched_books, context, max_items=1)
            book_titles_str = ", ".join([f'"{b.get("title", "")}"' for b in display_books])
            ctx = f"""[HỆ THỐNG]: Khách hỏi sách: "{raw_query}".
Hệ thống TÌM THẤY CHÍNH XÁC sách trong kho: {book_titles_str}.
NHIỆM VỤ: Thông báo vui vẻ, dứt khoát rằng cửa hàng "Có cuốn sách đó", và mời khách tham khảo nút bên dưới. KHÔNG NÓI là "tìm thấy các sách chủ đề tương tự".
LUẬT: Chỉ nhắc đúng tên sách trong danh sách: {book_titles_str}. Không tự bịa thêm tên sách khác."""
        else:
            # Không tìm thấy -> fallback
            display_books = _filter_and_track_books(books, context, max_items=4)
            book_titles_str = ", ".join([f'"{b.get("title", "")}"' for b in display_books])
            ctx = f"""[HỆ THỐNG]: Khách tìm sách: "{raw_query}".
Kho hàng KHÔNG CÓ sách khớp chính xác, hệ thống gợi ý các sách liên quan: {book_titles_str}.
NHIỆM VỤ: Thú nhận khéo léo rằng chưa có "{raw_query}", sau đó giới thiệu các sách trên như là phương án thay thế đáng tham khảo.
LUẬT: Chỉ dùng tên sách trong danh sách: {book_titles_str}. Không tự bịa tên sách khác."""
            
        if display_books:
            context["last_category"]   = display_books[0].get("category", "")
            context["last_found_title"] = display_books[0].get("title", "")
        
        instruction = tone
            
        answer = await generate(message, ctx, history, instruction, intent="book_search")
        btns   = _make_book_buttons(display_books)
        return answer + FOLLOW_UP.get(intent, ""), ["opensearch:books", "mysql:books"], btns

    if intent == "book_detail":
        # Ưu tiên: coref v7 đã inject book_title, nếu không thì extract từ message
        book_title = entities.get("book_title") or _extract_title_from_message(message)
        # Fallback cuối: dùng last_found_title từ context nếu vẫn chưa có
        if not book_title:
            book_title = context.get("last_found_title") or context.get("last_search_query")
        if book_title:
            book = get_book_price(book_title)
            if book:
                ctx = _format_book_detail(book)
            else:
                # Exact match không thấy → semantic search với tên sách
                similar = search_books_semantic(book_title, top_k=3)
                if similar:
                    ctx = (
                        f"Không tìm thấy '{book_title}' chính xác, "
                        f"nhưng đây là một số sách tương tự:\n{_format_books(similar)}"
                    )
                else:
                    ctx = f"Không tìm thấy thông tin sách '{book_title}' trong hệ thống."
        else:
            # Không có book_title gì hết → semantic search bằng toàn bộ message
            books = search_books_semantic(message, top_k=1)
            ctx   = _format_book_detail(books[0]) if books else "Vui lòng cho tôi biết tên sách bạn muốn xem."
        answer = await generate(message, ctx, history, tone, intent="book_detail")
        return answer + FOLLOW_UP.get(intent, ""), ["mysql:books"], []

    if intent == "book_compare":
        hits = search_books_semantic(message, top_k=2)
        ctx  = ("So sánh 2 cuốn:\n" + _format_books(hits)
                if hits else "Không tìm thấy đủ sách để so sánh.")
        answer = await generate(message, ctx, history, tone, intent="book_compare")
        return answer, ["opensearch:books"], []

    if intent == "book_availability":
        book_title = entities.get("book_title") or message
        book = get_book_price(book_title)
        if book:
            status = "Còn hàng" if book["stock_quantity"] > 0 else "Hết hàng"
            ctx = (
                f"**{book['title']}**\n"
                f"Tình trạng: **{status}** ({book['stock_quantity']} cuốn)\n"
                f"Giá: {book['price']:,.0f}đ"
            )
        else:
            ctx = f"Không tìm thấy sách '{book_title}' trong hệ thống."
        answer = await generate(message, ctx, history, tone, intent="book_availability")
        return answer + FOLLOW_UP.get(intent, ""), ["mysql:books"], []

    if intent == "book_review":
        books = search_books_semantic(message, top_k=3)
        display_books = _filter_and_track_books(books, context, max_items=2)
        ctx   = _format_books(display_books) if display_books else "Không tìm thấy sách này."
        answer = await generate(
            message, ctx, history,
            "trình bày đánh giá khách quan, nêu ngắn gọn ưu điểm, KHÔNG liệt kê thành danh sách dài",
            intent="book_review"
        )
        btns = _make_book_buttons(display_books) if display_books else []
        return answer, ["opensearch:books"], btns

    # ══ NHÓM B – GỢI Ý ═════════════════════════════════════════════════════
    if intent == "recommend_personal":
        if not user_id:
            query  = _clean_book_query(message)
            books  = search_books_semantic(query, top_k=8)
            display_books = _filter_and_track_books(books, context, max_items=4)
            ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách ưu đãi." if display_books else ""
            instruction = f"{tone}, viết 1 câu dẫn dắt ngắn gọn mời xem thẻ sách bên dưới, KHÔNG tự chế tên sách. Cuối câu nhắc đăng nhập."
            answer = await generate(message, ctx, history, instruction, intent="recommend_personal")
            btns = _make_book_buttons(display_books)
            return answer + FOLLOW_UP.get(intent, ""), ["opensearch:books"], btns
        
        genres = user_profile.get("favorite_genres", [])
        genre  = genres[0] if genres else ""
        books  = (get_books_by_genre(genre, limit=8) if genre else search_books_semantic("sách bán chạy hay đọc nhiều", top_k=8))
        display_books = _filter_and_track_books(books, context, max_items=4)
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách gợi ý." if display_books else ""
        instruction = f"{tone}, viết 1 câu dẫn dắt dựa trên sở thích, KHÔNG bịa tên sách."
        answer = await generate(message, ctx, history, instruction, intent="recommend_personal")
        btns = _make_book_buttons(display_books)
        return answer + FOLLOW_UP.get(intent, ""), ["mysql:books"], btns

    if intent == "recommend_trending":
        books = search_books_semantic("sách bán chạy được yêu thích nhất", top_k=8)
        display_books = _filter_and_track_books(books, context, max_items=4)
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách hot." if display_books else ""
        instruction = f"{tone}, viết 1 câu mời xem sách hot bên dưới, KHÔNG tự chế tên sách."
        answer = await generate(message, ctx, history, instruction, intent="recommend_trending")
        btns = _make_book_buttons(display_books)
        return answer + FOLLOW_UP.get(intent, ""), ["opensearch:books"], btns

    if intent == "recommend_gift":
        recipient = entities.get("recipient_type", "adult")
        price_max = entities.get("price_max") or entities.get("budget", 300_000)
        genre_map = {
            "child_0_6": "sách tranh thiếu nhi", "child_7_12": "truyện thiếu nhi khoa học",
            "teenager": "kỹ năng sống teen", "adult_female": "văn học tâm lý phụ nữ",
            "adult_male": "kinh tế lịch sử", "adult": "kỹ năng sống văn học", "elderly": "hồi ký sức khỏe",
        }
        gift_genre = genre_map.get(recipient, "kỹ năng sống văn học")
        books = search_books_semantic(f"sách {gift_genre} tặng quà", top_k=8, price_max=price_max)
        display_books = _filter_and_track_books(books, context, max_items=4)
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách phù hợp tặng quà." if display_books else ""
        instruction = f"{tone}, viết 1 câu nói về việc tặng quà và mời xem sách bên dưới, KHÔNG tự chế tên sách."
        answer = await generate(message, ctx, history, instruction, intent="recommend_gift")
        btns = _make_book_buttons(display_books)
        return answer + FOLLOW_UP.get(intent, ""), ["opensearch:books"], btns

    if intent in ("recommend_combo", "recommend_category"):
        genre = entities.get("genre")
        books_raw = search_books_semantic(message, top_k=10, genre=genre)
        
        # Fallback to DB query if semantic search yields empty but we do have a genre
        if not books_raw and genre:
            books_raw = get_books_by_genre(genre, limit=10)
            
        display_books = _filter_and_track_books(books_raw, context, max_items=4)
        if genre:
            context["last_genre"] = genre
            
        if not display_books: # Prevent LLM Hallucination when no context exists
            return f"Rất tiếc, tôi tạm thời chưa có sách phù hợp cho thể loại '{genre}' mà bạn yêu cầu.", [], [NavigateButton(label="Lịch sử", url="", type="quick_reply")]
            
        ctx = f"[HỆ THỐNG]: Có {len(display_books)} sách."
        instruction = f"{tone}, trả lời 1 câu mời xem thẻ sách bên dưới, KHÔNG tự chế tên sách."
        answer = await generate(message, ctx, history, instruction, intent=intent)
        btns = _make_book_buttons(display_books)
        
        # Add dynamic quick reply for recommending another genre implicitly
        btns.append(NavigateButton(label="Đổi thể loại khác", url="", type="quick_reply"))
        return answer, ["opensearch:books", "mysql:books"], btns

    # ══ NHÓM C – ĐƠN HÀNG ══════════════════════════════════════════════════
    if intent == "order_status":
        order_id = entities.get("order_id") or context.get("last_order_id")
        if order_id:
            context["last_order_id"] = order_id  # lưu context
            order = get_order_info(int(order_id))
            ctx   = (_format_order(order) if order
                     else f"Không tìm thấy đơn hàng #{order_id}. Vui lòng kiểm tra lại mã đơn.")
        elif user_id:
            orders = get_user_orders(user_id, limit=3)
            ctx = ("Đơn hàng gần đây của bạn:\n" + "\n".join(
                f"• **#{o['order_id']}**: {o['status']} – {o['total_amount']:,.0f}đ"
                for o in orders
            ) if orders else "Bạn chưa có đơn hàng nào.")
        else:
            return (
                "Vui lòng cung cấp **mã đơn hàng** (VD: #12345) hoặc "
                "**đăng nhập** để xem đơn hàng của bạn.",
                [], []
            )
        answer = await generate(message, ctx, history, tone, intent="order_status")
        orders_for_btn = get_user_orders(user_id, limit=3) if user_id else []
        btns = _make_order_buttons(orders_for_btn)
        return answer + FOLLOW_UP.get(intent, ""), ["mysql:orders"], btns

    if intent == "order_cancel":
        order_id = entities.get("order_id") or context.get("last_order_id")
        if not order_id and user_id:
            orders   = get_user_orders(user_id, limit=1)
            order_id = orders[0]["order_id"] if orders else None
        if order_id:
            order = get_order_info(int(order_id))
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
                    return (
                        f"Đơn hàng **#{order_id}** không thể hủy vì đang ở trạng thái "
                        f"**{order['status']}**.\n"
                        "Nếu cần hỗ trợ, liên hệ hotline **1800-xxxx**.",
                        ["mysql:orders"], []
                    )
            else:
                return f"Không tìm thấy thông tin đơn hàng #{order_id} trong hệ thống.", [], []
        return "Vui lòng cung cấp **mã đơn hàng** để hủy. Ví dụ: `hủy đơn #12345`", [], []

    if intent == "order_history":
        if not user_id:
            return TEMPLATES["no_auth"], [], []
        orders = get_user_orders(user_id, limit=10)
        if not orders:
            return "Bạn chưa có đơn hàng nào.", [], []
        total  = len(orders)
        shown  = orders[:5]
        ctx = (
            f"Lịch sử mua hàng ({total} đơn gần nhất):\n" +
            "\n".join(
                f"• **#{o['order_id']}**: {o['status']} – {o['total_amount']:,.0f}đ – "
                f"{o['book_count']} cuốn – {str(o.get('created_at','?'))[:10]}"
                for o in shown
            )
        )
        if total > 5:
            ctx += f"\n\n_Hiển thị 5/{total} đơn. Hỏi để xem thêm._"
        answer = await generate(message, ctx, history, tone, intent="order_history")
        btns = _make_order_buttons(orders)
        return answer + FOLLOW_UP.get(intent, ""), ["mysql:orders"], btns

    if intent == "cart_help":
        kb  = search_knowledge_base("giỏ hàng mua sách", top_k=1)
        ctx = kb[0]["text"] if kb else "Giỏ hàng lưu sách bạn muốn mua. Truy cập trang web để xem giỏ hàng của bạn."
        answer = await generate(message, ctx, history, tone, intent="cart_help")
        return answer, ["opensearch:kb"], []

    # ══ NHÓM D – THANH TOÁN ═════════════════════════════════════════════════
    if intent in ("payment_method", "payment_issue"):
        kb  = search_knowledge_base(message, top_k=2)
        ctx = "\n\n".join(k["text"] for k in kb) if kb else "Liên hệ hotline để hỗ trợ thanh toán."
        if intent == "payment_issue":
            ctx += "\n\n**Nếu vấn đề vẫn còn:** Hotline **1800-xxxx** (8h–22h)"
        answer = await generate(message, ctx, history, tone, intent=intent)
        return answer, [k["id"] for k in kb], []

    # ══ NHÓM E – ĐỔI TRẢ ════════════════════════════════════════════════════
    if intent == "return_policy":
        kb  = search_knowledge_base("chính sách đổi trả hoàn tiền", top_k=2)
        ctx = "\n\n".join(k["text"] for k in kb) if kb else (
            "**Chính sách đổi trả BookStore:**\n"
            "• Đổi trả trong vòng **7 ngày** kể từ ngày nhận hàng\n"
            "• Sách phải còn nguyên vẹn, chưa tháo bọc (trừ trường hợp lỗi)\n"
            "• Liên hệ CSKH kèm mã đơn hàng và lý do đổi trả"
        )
        answer = await generate(message, ctx, history, tone, intent="return_policy")
        return answer + FOLLOW_UP.get(intent, ""), [k["id"] for k in kb] if kb else [], []

    if intent == "return_request":
        order_id = entities.get("order_id")
        if order_id:
            order = get_order_info(int(order_id))
            ctx   = (
                f"Đơn #{order_id}: **{order['status']}** – {order['total_amount']:,.0f}đ"
                if order else f"Không tìm thấy đơn #{order_id}."
            )
        else:
            ctx = "Cần **mã đơn hàng** để xử lý yêu cầu đổi trả."
        ctx += "\n\n**Thủ tục:** Liên hệ CSKH kèm mã đơn hàng + ảnh sách (nếu bị lỗi) + lý do đổi trả."
        answer = await generate(message, ctx, history, tone, intent="return_request")
        return answer + "\n\n**Hotline:** 1800-xxxx | **Email:** cskh@bookstore.vn", ["mysql:orders"], []

    # ══ NHÓM F – KHUYẾN MÃI ════════════════════════════════════════════════
    if intent == "voucher_apply":
        voucher = entities.get("voucher_code", "")
        kb  = search_knowledge_base(f"voucher mã giảm giá {voucher}", top_k=1)
        ctx = (
            kb[0]["text"] if kb else
            f"Mã **{voucher}** – Để kiểm tra hiệu lực, nhập mã tại **trang Thanh toán → Mã giảm giá**."
        )
        answer = await generate(message, ctx, history, tone, intent="voucher_apply")
        return answer, ["opensearch:kb"], []

    if intent == "promotion_current":
        kb    = search_knowledge_base("khuyến mãi ưu đãi giảm giá", top_k=2)
        books = search_books_semantic("sách đang khuyến mãi giảm giá sale", top_k=3)
        ctx   = (
            "\n\n".join(k["text"] for k in kb) + "\n\n" + _format_books(books)
            if kb else _format_books(books)
        )
        answer = await generate(message, ctx, history, tone, intent="promotion_current")
        return answer + FOLLOW_UP.get(intent, ""), ["opensearch:kb", "opensearch:books"], []

    if intent == "loyalty_points":
        if not user_id:
            return TEMPLATES["no_auth"], [], []
        kb  = search_knowledge_base("tích điểm thưởng loyalty", top_k=1)
        ctx = kb[0]["text"] if kb else (
            "**Chương trình tích điểm BookStore:**\n"
            "• Mỗi **10.000đ** mua hàng = **1 điểm**\n"
            "• **100 điểm** = Giảm **10.000đ** cho lần mua tiếp\n"
            "• Điểm hiệu lực trong **12 tháng** kể từ ngày tích\n"
            "• Xem điểm tại: **Tài khoản → Điểm thưởng**"
        )
        answer = await generate(message, ctx, history, tone, intent="loyalty_points")
        return answer, ["opensearch:kb"], []

    # ══ NHÓM G – HỖ TRỢ ════════════════════════════════════════════════════
    if intent == "store_info":
        kb = search_knowledge_base("thông tin cửa hàng liên hệ hotline", top_k=2)
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
                "• **Hotline:** 1800-xxxx (miễn phí, 8h–22h)\n"
                "• **Email:** cskh@bookstore.vn\n"
                "• **Website:** www.bookstore.vn\n"
                "• **Giờ hỗ trợ:** Thứ 2–Chủ nhật, 8h–22h"
            )
        return answer, ["opensearch:kb"], []


    # ── Chitchat – phân biệt các loại ──────────────────────────────────────
    if intent == "chitchat":
        return _handle_chitchat(message, user_id), [], []

    if intent == "out_of_scope":
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
        _run_in_executor(search_books_semantic, message, 3),
        _run_in_executor(search_knowledge_base, message, 2),
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
        return TEMPLATES["bot_capabilities"]

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
    display_books = new_books[:max_items] if new_books else books[:max_items] # Fallback nếu hết sách mới
    context["shown_book_ids"] = list(already_shown | {b.get("book_id") or b.get("id") for b in display_books})
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
    return (
        f"Đơn hàng **#{order.get('order_id','?')}**\n"
        f"Trạng thái: **{order.get('status','?')}**\n"
        f"Tổng tiền: {order.get('total_amount', 0):,.0f}đ\n"
        f"Ngày đặt: {str(order.get('created_at','?'))[:10]}\n"
        f"Thanh toán: {order.get('payment_method','?')} – {order.get('payment_status','?')}"
    )


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
) -> tuple[str, list[str], list["NavigateButton"]]:
    """
    Xử lý confirmation flow (yes/no) và trả về (answer, sources, navigate_buttons).
    
    Lưu ý: chatbot không gọi API hủy đơn trực tiếp – chỉ hướng dẫn user đến trang quản lý
    để thực hiện hành động đó. Điều này đảm bảo an toàn, xác thực chính xác và kiểm toán đầy đủ.
    """
    pending = context.get("pending_confirmation", {})

    if intent == "confirmation_yes":
        action   = pending.get("action")
        order_id = pending.get("order_id")
        context.pop("pending_confirmation", None)

        if action == "order_cancel" and order_id:
            # Chatbot không tự hủy – hướng dẫn user đến trang quản lý đơn
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

        # Generic yes – không có action rõ ràng
        return "Đã xác nhận thành công!", [], []

    elif intent == "confirmation_no":
        context.pop("pending_confirmation", None)
        return "👍 Đã giữ lại đơn hàng. Đơn của bạn vẫn được xử lý bình thường!", [], []

    return TEMPLATES["confirm_pending"], [], []


# ── Clarify-First System Helpers (v3) ────────────────────────────────────────

def _is_slot_missing(
    intent: str,
    required_slot: str,
    entities: dict,
    user_id: int | None,
    context: dict,
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

    # order_cancel: bắt buộc phải có order_id
    if intent == "order_cancel":
        return not entities.get("order_id") and not context.get("last_order_id")

    # return_request: cần order_id để xử lý
    if intent == "return_request":
        return not entities.get("order_id")

    # book_detail, book_availability, book_compare: cần tên sách
    if intent in ("book_detail", "book_availability", "book_compare"):
        return not entities.get("book_title")

    # recommend_gift: nếu không có recipient_type → hỏi để gợi ý chính xác
    if intent == "recommend_gift":
        return not entities.get("recipient_type")

    # recommend_category / recommend_combo: nếu không có genre và không có context → hỏi
    if intent == "recommend_category" or intent == "recommend_combo":
        return not entities.get("genre") and not context.get("last_category")

    # voucher_apply: cần voucher_code
    if intent == "voucher_apply":
        return not entities.get("voucher_code")

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
    from chatbot_app.nlu.customer_intent_classifier import NLUResult as _NLU
    fake_nlu = _NLU(
        intent=target_intent,
        confidence=0.95,   # giả định regex-level confidence sau khi user xác nhận
        entities=merged_entities,
        sentiment="NEUTRAL",
    )

    # Re-process với message đã được làm rõ bằng slot vừa điền
    if target_intent == "recommend_category" and merged_entities.get("genre"):
        enriched_message = f"Tìm sách thể loại {merged_entities['genre']}"
    elif target_intent == "recommend_gift" and merged_entities.get("recipient_type"):
        enriched_message = f"Tìm sách cho {merged_entities['recipient_type']}"
    else:
        enriched_message = f"{original_message} {message}"
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
        for w in ("có", "đúng", "ok", "oke", "yes", "phải", "chính xác", "đúng rồi", "vâng", "ừ")
    )
    is_no = intent in ("confirmation_no",) or any(
        w in message.lower()
        for w in ("không", "sai", "no", "nope", "khác", "không phải")
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
