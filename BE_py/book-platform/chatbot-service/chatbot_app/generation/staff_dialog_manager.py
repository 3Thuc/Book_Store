"""
staff_dialog_manager.py - Dialog manager cho Staff chatbot.

Xử lý 14 intents staff, trả về (answer, sources).
Có confirm flow nghiêm ngặt cho các actions nguy hiểm (update order status).
"""
from chatbot_app.nlu.staff_intent_classifier import StaffNLUResult
from chatbot_app.retrieval.staff_agents import (
    lookup_order_by_id, lookup_orders_by_status,
    lookup_pending_orders, lookup_return_requests,
    check_book_inventory, get_low_stock_books, get_out_of_stock_books,
    count_out_of_stock, count_low_stock,
    get_today_order_stats, get_period_revenue,
    get_top_selling_books,
    lookup_customer_by_email, lookup_customer_by_name, lookup_customer_recent_orders,
    get_all_time_order_stats,
    update_order_status, update_book_inventory, search_book_inventory,
)
from chatbot_app.generation.dialog_utils import is_ocr_message, is_garbled_query, extract_ocr_book_name
from datetime import date

# ── VALID ORDER STATUS TRANSITIONS ────────────────────────────
VALID_TRANSITIONS = {
    # pending → có thể xử lý hoặc ghi nhận yêu cầu hủy
    "pending":          ["processing", "cancel_requested"],
    # processing → giao đi hoặc ghi nhận yêu cầu hủy
    "processing":       ["shipped", "cancel_requested"],
    # shipped → chỉ được đánh thất bại (delivered tự động khi khách xác nhận)
    "shipped":          ["failed"],
    # cancel_requested → staff duyệt hủy
    "cancel_requested": ["cancelled"],
    # return_requested → staff duyệt trả hàng
    "return_requested": ["returned"],
    # failed → staff có thể re-queue để giao lại (khách gọi lại yêu cầu giao tiếp)
    "failed":           ["pending"],
    # ── View only (không có transition) ─────────────────────────
    # "delivered"  → chỉ xem
    # "cancelled"  → chỉ xem
    # "returned"   → chỉ xem
}

# ── TEMPLATES ─────────────────────────────────────────────────
STAFF_HELP_MSG = """Xin chào! Đây là **Staff Assistant** - chatbot hỗ trợ nghiệp vụ.

**Quản lý đơn hàng:**
- `Đơn #12345` - Xem chi tiết đơn hàng
- `Đơn đang chờ` - Danh sách đơn pending
- `Đơn đổi/trả` - Yêu cầu return/cancel
- `Thống kê hôm nay` - Báo cáo nhanh

**Tồn kho:**
- `Tồn kho sách Đắc Nhân Tâm` - Kiểm tra hàng
- `Sách sắp hết hàng` - Cảnh báo ngưỡng thấp

**Khách hàng:**
- `Tìm khách example@gmail.com` - Tra cứu KH
- `Đơn gần nhất của user 42` - Lịch sử mua

**Báo cáo:**
- `Doanh thu hôm nay` / `tuần này` - Revenue
- `Top sách bán chạy` - Bestseller list"""


def format_order_detail(order: dict) -> str:
    """Định dạng chi tiết đơn hàng cho staff."""
    if not order:
        return "Không tìm thấy đơn hàng."

    items_str = ""
    items_subtotal = 0.0
    for item in order.get("items", []):
        line_total = float(item['unit_price']) * int(item['quantity'])
        items_subtotal += line_total
        items_str += (
            f"\n - **{item['title']}** × {item['quantity']} "
            f"= {line_total:,.0f}đ"
        )

    # DEBUG: log để kiểm tra discrepancy giữa items_subtotal và db total_amount
    print(f"[DEBUG lookup_order #{order.get('order_id')}] "
          f"items={len(order.get('items', []))}, "
          f"items_subtotal={items_subtotal:,.0f}, "
          f"db_total_amount={float(order.get('total_amount', 0) or 0):,.0f}")
    # Tính tổng từ items để tránh dùng total_amount/subtotal không đáng tin trong DB
    # Fallback sang total_amount chỉ khi items rỗng (JOIN lỗi / đơn cũ không có items)
    db_total = float(order.get('total_amount', 0) or 0)
    if order.get("items"):
        # Ưu tiên tính từ items (chính xác hơn DB field có thể bị stale)
        base_total = items_subtotal
    else:
        # items rỗng → không tính được từ items → dùng DB total_amount
        base_total = db_total

    # Chỉ trừ discount khi thực sự có mã giảm giá — tránh trừ nhầm khi DB lưu discount_amount dư
    if order.get('promo_code'):
        discount_val  = float(order.get('discount_amount', 0) or 0)
        total_display = base_total - discount_val
        promo_line = (
            f"Mã giảm giá áp dụng: **{order['promo_code']}**\n"
            f"Tạm tính: {base_total:,.0f}đ → Giảm: -{discount_val:,.0f}đ\n"
        )
    else:
        total_display = base_total
        promo_line = ""

    addr_parts = [
        order.get('shipping_address'),
        order.get('shipping_district'),
        order.get('shipping_city')
    ]
    full_address = ", ".join(p for p in addr_parts if p) or 'N/A'

    # Hiển thị bước tiếp theo theo luồng trạng thái
    _STATUS_LABEL = {
        "pending":          "⏳ Chờ xử lý",
        "processing":       "🔧 Đang xử lý",
        "shipped":          "🚚 Đang giao hàng",
        "delivered":        "✅ Đã giao hàng",
        "cancelled":        "❌ Đã hủy",
        "cancel_requested": "🚫 Yêu cầu hủy",
        "return_requested": "🔄 Yêu cầu trả hàng",
        "returned":         "📦 Đã hoàn trả",
        "failed":           "⚠️ Giao thất bại",
    }
    _TERMINAL_STATES = {"delivered", "cancelled", "returned", "failed"}
    _next = VALID_TRANSITIONS.get(order.get("status", ""), [])
    if _next:
        _next_str = " | ".join(
            f"`{s}` ({_STATUS_LABEL.get(s, s)})" for s in _next
        )
        next_step_line = f"\n💡 **Có thể chuyển sang:** {_next_str}"
    elif order.get("status") in _TERMINAL_STATES:
        next_step_line = "\n🔒 **Đơn đã kết thúc** (không thể thay đổi trạng thái)"
    else:
        next_step_line = ""

    from datetime import datetime, timedelta
    od = order.get('order_date', '?')
    if isinstance(od, datetime):
        order_date_str = (od + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M')
    else:
        order_date_str = str(od)[:16]

    return (
        f"**Đơn hàng #{order['order_id']}**\n"
        f"Trạng thái: `{order['status']}` ({_STATUS_LABEL.get(order.get('status',''), '')})\n"
        f"Tổng tiền: **{total_display:,.0f}đ**\n"
        f"{promo_line}"
        f"Thanh toán: {order.get('payment_method','?')} - {order.get('payment_status','?')}\n"
        f"Khách hàng: **{order.get('recipient_name') or order.get('username','?')}** ({order.get('email','?')})\n"
        f"SĐT: {order.get('address_phone') or order.get('user_phone') or 'N/A'}\n"
        f"Địa chỉ: {full_address}\n"
        f"Ngày đặt: {order_date_str}\n"
        f"Ghi chú: {order.get('note') or 'Không có'}\n"
        f"\nSản phẩm:{items_str if items_str else ' (không có)'}"
        f"{next_step_line}"
    )


def format_order_list(orders: list[dict], title: str = "Danh sách đơn") -> str:
    if not orders:
        return f"Không có đơn nào trong danh sách '{title}'."

    _STATUS_LABEL = {
        "pending":          "⏳ Chờ xử lý",
        "processing":       "🔧 Đang xử lý",
        "shipped":          "🚚 Đang giao hàng",
        "delivered":        "✅ Đã giao hàng",
        "cancelled":        "❌ Đã hủy",
        "cancel_requested": "🚫 Yêu cầu hủy",
        "return_requested": "🔄 Yêu cầu trả hàng",
        "returned":         "📦 Đã hoàn trả",
        "failed":           "⚠️ Giao thất bại",
    }

    from datetime import datetime, timedelta

    lines = [f"**{title}** ({len(orders)} đơn):\n"]
    for o in orders:
        # DB lưu UTC, cộng 7 tiếng để ra giờ VN
        od = o.get('order_date') or o.get('created_at')
        if isinstance(od, datetime):
            order_date_str = (od + timedelta(hours=7)).strftime('%Y-%m-%d')
        else:
            order_date_str = str(od)[:10]

        status_str = _STATUS_LABEL.get(o['status'], o['status'])
        # Ưu tiên full_name → username → email → 'Khách vãng lai'
        _cname = o.get('full_name') or o.get('username') or o.get('email')
        if not _cname or str(_cname) == 'None':
            _cname = 'Khách vãng lai'
        lines.append(
            f"📦 **Đơn #{o['order_id']}** ({order_date_str})\n"
            f"Khách: {_cname}\n"
            f"Trạng thái: {status_str} | {float(o.get('total_amount', 0)):,.0f}đ"
        )
    return "\n\n---\n".join(lines)


def format_inventory(book: dict, identifier: str = "") -> str:
    if not book:
        hint = f'**"{identifier}"**' if identifier else "tên sách đó"
        return (
            f"❌ Không tìm thấy thông tin tồn kho cho {hint}.\n"
            f"💡 Gợi ý: Hỏi `Tồn kho [tên sách]` hoặc kiểm tra trực tiếp trong **Admin Panel → Quản lý sách**."
        )
    stock       = book.get("stock_quantity", 0)
    stock_label = "Còn hàng" if stock > 10 else ("Sắp hết" if stock > 0 else "Hết hàng")
    return (
        f"**{book['title']}** (ID: {book['book_id']})\n"
        f"Tác giả: {book.get('author_name','?')}\n"
        f"Giá: {book.get('price',0):,.0f}đ\n"
        f"Tồn kho: **{stock} cuốn** ({stock_label})\n"
        f"Trạng thái: {book.get('status','?')}"
    )


def format_low_stock_list(books: list[dict], threshold: int = 5, total: int = None) -> str:
    if not books:
        return "Không có sách nào sắp hết hàng."
    if total is None:
        total = len(books)
    lines = [f"**Có tổng cộng {total} sách có tồn kho 1-{threshold} cuốn (sắp hết):**"]
    for b in books[:10]:
        lines.append(
            f"  • **{b['title']}** - Còn **{b['stock_quantity']}** cuốn "
            f"| {b.get('price',0):,.0f}đ"
        )
    if total > 10:
        lines.append(f"  ... và {total - min(10, len(books))} sách khác.")
    return "\n".join(lines)


def format_order_stats(stats: dict) -> str:
    date_str  = stats.get('date', '?')
    total     = int(stats.get('total_orders') or 0)
    delivered = int(stats.get('delivered') or 0)
    pending   = int(stats.get('pending') or 0)
    cancelled = int(stats.get('cancelled') or 0)
    ret_req   = int(stats.get('return_req') or 0)
    revenue   = float(stats.get('total_revenue') or 0)
    confirmed = float(stats.get('confirmed_revenue') or 0)
    return (
        f"📅 **Thống kê đơn hàng — {date_str}**\n"
        f"🧾 Tổng đơn: **{total}**\n"
        f"✅ Đã giao: **{delivered}**\n"
        f"⏳ Đang xử lý / chờ: **{pending}**\n"
        f"❌ Đã hủy: **{cancelled}**\n"
        f"🔄 Yêu cầu trả hàng: **{ret_req}**\n\n"
        f"💰 Tổng doanh thu: **{revenue:,.0f}đ**\n"
        f"💵 Đã giao thực nhận: **{confirmed:,.0f}đ**"
    )


def format_revenue(rev: dict) -> str:
    _PERIOD_VI = {
        "week":  "Tuần này",  "this_week":  "Tuần này",
        "month": "Tháng này", "this_month": "Tháng này",
        "today": "Hôm nay",   "day":        "Hôm nay",
        "year":  "Năm nay",   "quarter":    "Quý này",
        "7days": "7 ngày qua", "30days":     "30 ngày qua",
    }
    label = rev.get("period_label") or rev.get("period", "")
    label = _PERIOD_VI.get(str(label).lower(), label)  # map EN → VI
    total  = int(rev.get('total_orders') or 0)
    cancel = int(rev.get('cancelled_count') or 0)
    cancel_rate = rev.get('cancel_rate') or (round(cancel/total*100, 1) if total > 0 else 0.0)
    cod    = int(rev.get('cod_count') or 0)
    online = int(rev.get('online_count') or 0)
    total_rev    = float(rev.get('total_revenue') or 0)
    confirm_rev  = float(rev.get('confirmed_revenue') or rev.get('delivered_revenue') or 0)
    total_disc   = float(rev.get('total_discounts') or 0)
    returned     = int(rev.get('returned_count') or 0)
    unique_cust  = int(rev.get('unique_customers') or 0)
    return (
        f"**Báo cáo doanh thu {label}**\n"
        f"{rev.get('from_date')} → {rev.get('to_date')}\n\n"
        f"Tổng đơn: **{total}**\n"
        f"Khách duy nhất: {unique_cust}\n"
        f"Tổng doanh thu: **{total_rev:,.0f}đ**\n"
        f"Doanh thu xác nhận: **{confirm_rev:,.0f}đ**\n"
        f"Tổng giảm giá: {total_disc:,.0f}đ\n"
        f"Tỷ lệ hủy: **{cancel_rate:.1f}%** ({cancel}/{total} đơn)\n"
        f"Đơn hủy: {cancel} | Hoàn: {returned}\n"
        f"COD: {cod} | Online: {online}"
    )



def format_top_books(books: list[dict]) -> str:
    if not books:
        return "Chưa có dữ liệu bán hàng."
    lines = ["**Top sách bán chạy:**"]
    for i, b in enumerate(books, 1):
        lines.append(
            f"{i}. **{b['title']}** - {b.get('author_name','?')}\n"
            f"   Bán: **{b.get('total_sold',0)} cuốn** | "
            f"{float(b.get('total_revenue',0)):,.0f}đ | "
            f"Rating: {b.get('avg_rating',0):.1f}"
        )
    return "\n".join(lines)


def format_customer(user: dict) -> str:
    if not user:
        return "Không tìm thấy khách hàng."
    phone = user.get('phone')
    phone_line = f"SĐT: {phone}\n" if phone and phone != 'None' else ""
    return (
        f"👤 **Khách hàng #{user['user_id']}**\n"
        f"Tên: **{user.get('full_name') or user.get('username','?')}**\n"
        f"Email: {user.get('email','?')}\n"
        f"{phone_line}"
        f"Tham gia: {str(user.get('created_at','?'))[:10]}\n"
        f"Tổng đơn: **{user.get('total_orders',0)}** | Tổng chi: **{float(user.get('total_spent',0)):,.0f}đ**"
    )


def _format_order_list(orders: list, title: str = "Đơn hàng:") -> str:
    """Render danh sách đơn hàng đẹp hơn với emoji và label tiếng Việt."""
    _ICON = {
        "pending":          "⏳ Chờ xử lý",
        "processing":       "🔧 Đang xử lý",
        "shipped":          "🚚 Đang giao",
        "delivered":        "✅ Đã giao",
        "cancelled":        "❌ Đã hủy",
        "cancel_requested": "🚫 Yêu cầu hủy",
        "return_requested": "🔄 Yêu cầu trả",
        "returned":         "📦 Hoàn trả",
        "failed":           "⚠️ Giao thất bại",
    }
    result = f"\n\n**{title}**\n"
    for o in orders:
        st = o.get("status", "?")
        label = _ICON.get(st, f"📦 {st}")
        date = str(o.get('order_date', ''))[:10]
        result += (
            f"\n📌 **#{o['order_id']}** — {label}\n"
            f"   💰 {float(o['total_amount']):,.0f}đ | 📅 {date}\n"
        )
    return result


# =============================================================================
# MAIN PROCESS FUNCTION
# =============================================================================

async def process_staff(
    message:    str,
    nlu_result: StaffNLUResult,
    user_id:    int | None,
    context:    dict,
) -> tuple[str, list[str]]:
    """Xử lý message staff và trả về (answer, sources)."""
    intent   = nlu_result.intent
    entities = nlu_result.entities

    # ── [OCR GUARD] Nếu message là OCR upload, bỏ qua NLU intent sai ─────────────
    # VD: [Upload: 10-dieu-ran-lanh-dao.jpg] → NLU có thể classify sai thành
    # recommend_category do keyword "lãnh đạo" trong filename.
    # Solution: khi OCR message được gử tới staff, last_found_title đã được set
    # bởi customer/chat router → dùng để lookup inventory ngay.
    if is_ocr_message(message):
        _ocr_book = (
            context.get("last_found_title")
            or context.get("last_book_name")
            or extract_ocr_book_name(message)  # fallback: extract từ filename
        )
        if _ocr_book:
            book = check_book_inventory(_ocr_book)
            if book:
                context["last_book_id"]   = book.get("book_id")
                context["last_book_name"] = book.get("title", _ocr_book)
                return format_inventory(book, _ocr_book), ["mysql:books"]
            # Sách không có trong kho → hiển thị helpful
            return format_inventory(None, _ocr_book), []
        # Fallback: để luồng bình thường xử lý tiếp

    # ── Normalize message để so sánh keyword (dùng xuyên suốt hàm) ─────────────
    import unicodedata as _ud_s
    _mn_s = "".join(
        c for c in _ud_s.normalize("NFD", message.lower().replace("đ", "d"))
        if _ud_s.category(c) != "Mn"
    )

    # ── [S-V5-02 T5 FIX] Complaint override — bất kể NLU intent là gì ────────────
    _COMPLAINT_DIRECT_KW = [
        "phan nan", "khieu nai", "sach bi rach", "sach bi hong", "sach bi loi",
        "hang bi hu", "sach loi", "sach hong", "hang hong",
    ]
    _RESOLVE_KW = [
        "xu ly", "giai quyet", "lam gi", "phai lam", "the nao", "ra sao",
        "nhu the nao", "huong dan", "quy trinh", "can lam",
    ]
    if any(kw in _mn_s for kw in _COMPLAINT_DIRECT_KW):
        return (
            "📌 **Quy trình xử lý khiếu nại khách nhận sách hư hỏng:**\n\n"
            "1. **Xác nhận thông tin:** Hỏi khách mã đơn hàng + ảnh/video chứng minh\n"
            "2. **Đổi sách mới:** Khách có quyền **đổi** trong vòng **7 ngày** kể từ ngày nhận\n"
            "3. **Chuyển CSKH:** Liên hệ hotline để xử lý nhanh chóng\n"
            "4. **Hoàn tiền:** Nếu khách không muốn đổi, hướng dẫn hoàn qua CSKH\n\n"
            "📞 **Hotline CSKH: 0353260721** (8h–22h)\n"
            "📧 Email: cskh@bookstore.vn"
        ), ["escalate:cskh"]
    # ── end complaint override ────────────────────────────────────────────────────
    # ── [MSG-OVERRIDE FAQ] Giải thích thuật ngữ ──────────────────────────────────
    import re as _re_faq
    if _re_faq.search(r"doanh thu xac nhan( la gi| co nghia la gi| nghia la gi)?", _mn_s, _re_faq.IGNORECASE):
        return (
            "💡 **Doanh thu xác nhận** (hay Đã giao thực nhận) là tổng số tiền thực tế chắc chắn đã thu về từ những đơn hàng được giao thành công (`delivered`) cho khách.\n\n"
            "Chỉ số này đã loại bỏ các đơn đang chờ xử lý, đang giao, bị hủy hoặc bị trả hàng, giúp bạn biết chính xác dòng tiền thực tế đã thu được."
        ), ["mysql:orders"]

    # ── [MSG-OVERRIDE REVENUE] Bắt các câu hỏi thời gian mà NLU có thể classify sai ─────
    # Các dạng: "hôm nay", "hôm qua", "tuần này", "tháng này", "tháng 3", "tháng 1/2026"
    import re as _re_rev
    _TODAY_REVENUE_KW = ["doanh thu hom nay", "hom nay doanh thu", "doanh thu ngay hom nay"]
    _YESTERDAY_KW    = ["hom qua", "ngay hom qua", "ngay qua"]
    _THIS_WEEK_KW    = ["tuan nay", "tuan nay la bao nhieu", "doanh thu tuan", "trong tuan"]
    _LAST_WEEK_KW    = ["tuan truoc", "tuan qua", "tuan vua qua"]
    _THIS_MONTH_KW   = ["thang nay", "doanh thu thang nay", "trong thang nay"]
    _COMPARE_KW      = ["so sanh", "so voi", "va tuan truoc", "va hom qua", "va thang truoc", "tang giam", "bien dong"]
    _SPECIFIC_MONTH  = _re_rev.search(r'th[aá]ng\s+(\d{1,2})(?:/|-)(\d{4})', message, _re_rev.IGNORECASE)
    _SPECIFIC_MONTH2 = _re_rev.search(r'th[aá]ng\s+(\d{1,2})(?:\s+n[aă]m\s+(\d{4}))?', message, _re_rev.IGNORECASE)
    _REVENUE_CTX_KW  = ["doanh thu", "bao nhieu", "la ban nhieu", "tong", "so don", "thong ke"]
    _is_revenue_ctx  = any(kw in _mn_s for kw in _REVENUE_CTX_KW) or context.get("last_query_intent") == "staff_revenue"

    # Hôm nay – override trước khi NLU nhầm thành order_statistics
    if any(kw in _mn_s for kw in _TODAY_REVENUE_KW) and _is_revenue_ctx:
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gprev
        rev = _gprev("today")
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    # So sánh tuần này vs tuần trước
    _is_compare_week = (
        any(kw in _mn_s for kw in _COMPARE_KW)
        and any(kw in _mn_s for kw in _THIS_WEEK_KW + _LAST_WEEK_KW)
    )
    if _is_compare_week and _is_revenue_ctx:
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gprev
        rev_this = _gprev("this_week")
        rev_last = _gprev("last_week")
        context["last_query_intent"] = "staff_revenue"
        this_rev  = float(rev_this.get("total_revenue", 0))
        last_rev  = float(rev_last.get("total_revenue", 0))
        diff      = this_rev - last_rev
        pct       = round(diff / last_rev * 100, 1) if last_rev > 0 else 0.0
        trend     = "📈 Tăng" if diff >= 0 else "📉 Giảm"
        return (
            f"**So sánh doanh thu tuần này vs tuần trước**\n\n"
            f"📅 **Tuần này** ({rev_this.get('from_date')} → {rev_this.get('to_date')}): **{this_rev:,.0f}đ** ({rev_this.get('total_orders')} đơn)\n"
            f"📅 **Tuần trước** ({rev_last.get('from_date')} → {rev_last.get('to_date')}): **{last_rev:,.0f}đ** ({rev_last.get('total_orders')} đơn)\n\n"
            f"{trend}: **{abs(diff):,.0f}đ** ({abs(pct)}%) so với tuần trước"
        ), ["mysql:orders"]

    # So sánh 2 tháng cụ thể: "so sánh tháng 3/2026 và tháng 4/2026"
    # Fallback sang _mn_s (chuẩn hóa không dấu) nếu có typo như "thág" thay vì "tháng"
    _TWO_MONTHS = _re_rev.findall(r'th[aá]ng\s+(\d{1,2})(?:/|-)(\d{4})', message, _re_rev.IGNORECASE)
    if len(_TWO_MONTHS) < 2:
        _TWO_MONTHS = _re_rev.findall(r'thang\s+(\d{1,2})(?:/|-)(\d{4})', _mn_s, _re_rev.IGNORECASE)
    if len(_TWO_MONTHS) == 2 and (any(kw in _mn_s for kw in _COMPARE_KW) or any(kw in _mn_s for kw in ["va", "voi"])) and _is_revenue_ctx:
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gprev
        m1, y1 = _TWO_MONTHS[0]
        m2, y2 = _TWO_MONTHS[1]
        rev1 = _gprev(f"month:{m1}/{y1}")
        rev2 = _gprev(f"month:{m2}/{y2}")
        context["last_query_intent"] = "staff_revenue"
        r1 = float(rev1.get("total_revenue", 0))
        r2 = float(rev2.get("total_revenue", 0))
        diff = r2 - r1
        pct  = round(diff / r1 * 100, 1) if r1 > 0 else 0.0
        trend = "📈 Tăng" if diff >= 0 else "📉 Giảm"
        return (
            f"**So sánh doanh thu Tháng {m1}/{y1} vs Tháng {m2}/{y2}**\n\n"
            f"📅 **Tháng {m1}/{y1}**: **{r1:,.0f}đ** ({rev1.get('total_orders')} đơn)\n"
            f"📅 **Tháng {m2}/{y2}**: **{r2:,.0f}đ** ({rev2.get('total_orders')} đơn)\n\n"
            f"{trend}: **{abs(diff):,.0f}đ** ({abs(pct)}%) từ Tháng {m1}/{y1} sang Tháng {m2}/{y2}"
        ), ["mysql:orders"]

    # So sánh 2 năm: "so sánh năm 2025 và năm 2026"
    _TWO_YEARS = _re_rev.findall(r'n[aă]m\s+(\d{4})', message, _re_rev.IGNORECASE)
    if len(_TWO_YEARS) < 2:
        _TWO_YEARS = _re_rev.findall(r'nam\s+(\d{4})', _mn_s, _re_rev.IGNORECASE)
    if len(_TWO_YEARS) == 2 and (any(kw in _mn_s for kw in _COMPARE_KW) or any(kw in _mn_s for kw in ["va", "voi"])) and _is_revenue_ctx:
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gprev
        y1, y2 = _TWO_YEARS[0], _TWO_YEARS[1]
        rev1 = _gprev(f"year:{y1}")
        rev2 = _gprev(f"year:{y2}")
        context["last_query_intent"] = "staff_revenue"
        r1 = float(rev1.get("total_revenue", 0))
        r2 = float(rev2.get("total_revenue", 0))
        diff = r2 - r1
        pct  = round(diff / r1 * 100, 1) if r1 > 0 else 0.0
        trend = "📈 Tăng" if diff >= 0 else "📉 Giảm"
        return (
            f"**So sánh doanh thu Năm {y1} vs Năm {y2}**\n\n"
            f"📅 **Năm {y1}**: **{r1:,.0f}đ** ({rev1.get('total_orders')} đơn)\n"
            f"📅 **Năm {y2}**: **{r2:,.0f}đ** ({rev2.get('total_orders')} đơn)\n\n"
            f"{trend}: **{abs(diff):,.0f}đ** ({abs(pct)}%) từ Năm {y1} sang Năm {y2}"
        ), ["mysql:orders"]

    # So sánh 2 ngày cụ thể: "so sánh ngày 01/05/2026 và ngày 30/04/2026"
    _TWO_DAYS_DMY = _re_rev.findall(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', message)
    _TWO_DAYS_ISO = _re_rev.findall(r'(\d{4})-(\d{2})-(\d{2})', message)
    _day_list = []
    for d, m, y in _TWO_DAYS_DMY:
        try:
            from datetime import date as _date_cls
            _day_list.append(str(_date_cls(int(y), int(m), int(d))))
        except Exception:
            pass
    for y, m, d in _TWO_DAYS_ISO:
        try:
            from datetime import date as _date_cls
            _day_list.append(str(_date_cls(int(y), int(m), int(d))))
        except Exception:
            pass
    if len(_day_list) >= 2 and (any(kw in _mn_s for kw in _COMPARE_KW) or any(kw in _mn_s for kw in ["va", "voi"])) and _is_revenue_ctx:
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gprev
        d1, d2 = _day_list[0], _day_list[1]
        rev1 = _gprev(f"day:{d1}")
        rev2 = _gprev(f"day:{d2}")
        context["last_query_intent"] = "staff_revenue"
        r1 = float(rev1.get("total_revenue", 0))
        r2 = float(rev2.get("total_revenue", 0))
        diff = r2 - r1
        pct  = round(diff / r1 * 100, 1) if r1 > 0 else 0.0
        trend = "📈 Tăng" if diff >= 0 else "📉 Giảm"
        lbl1 = rev1.get("period_label", d1)
        lbl2 = rev2.get("period_label", d2)
        return (
            f"**So sánh doanh thu {lbl1} vs {lbl2}**\n\n"
            f"📅 **{lbl1}**: **{r1:,.0f}đ** ({rev1.get('total_orders')} đơn)\n"
            f"📅 **{lbl2}**: **{r2:,.0f}đ** ({rev2.get('total_orders')} đơn)\n\n"
            f"{trend}: **{abs(diff):,.0f}đ** ({abs(pct)}%) từ {lbl1} sang {lbl2}"
        ), ["mysql:orders"]

    # So sánh tương đối: "hôm nay vs hôm qua", "hôm qua vs hôm nay"
    _has_today_kw     = any(kw in _mn_s for kw in ["hom nay", "ngay hom nay"])
    _has_yesterday_kw = any(kw in _mn_s for kw in _YESTERDAY_KW)
    _is_compare       = any(kw in _mn_s for kw in _COMPARE_KW + ["va", "voi", "so"])
    if _has_today_kw and _has_yesterday_kw and _is_compare and _is_revenue_ctx:
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gprev
        from datetime import date as _d_cls
        rev_today = _gprev("today")
        rev_yest  = _gprev("yesterday")
        context["last_query_intent"] = "staff_revenue"
        r1 = float(rev_yest.get("total_revenue", 0))
        r2 = float(rev_today.get("total_revenue", 0))
        diff = r2 - r1
        pct  = round(diff / r1 * 100, 1) if r1 > 0 else 0.0
        trend = "📈 Tăng" if diff >= 0 else "📉 Giảm"
        return (
            f"**So sánh doanh thu Hôm qua vs Hôm nay**\n\n"
            f"📅 **Hôm qua** ({rev_yest.get('from_date')}): **{r1:,.0f}đ** ({rev_yest.get('total_orders')} đơn)\n"
            f"📅 **Hôm nay** ({rev_today.get('from_date')}): **{r2:,.0f}đ** ({rev_today.get('total_orders')} đơn)\n\n"
            f"{trend}: **{abs(diff):,.0f}đ** ({abs(pct)}%) so với hôm qua"
        ), ["mysql:orders"]

    if any(kw in _mn_s for kw in _YESTERDAY_KW) and _is_revenue_ctx:
        rev = get_period_revenue("yesterday")
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    if any(kw in _mn_s for kw in _THIS_WEEK_KW) and _is_revenue_ctx:
        rev = get_period_revenue("this_week")
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    if any(kw in _mn_s for kw in _THIS_MONTH_KW) and _is_revenue_ctx:
        rev = get_period_revenue("this_month")
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    if _SPECIFIC_MONTH and _is_revenue_ctx:
        m_val = _SPECIFIC_MONTH.group(1)
        y_val = _SPECIFIC_MONTH.group(2)
        rev = get_period_revenue(f"month:{m_val}/{y_val}")
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    # Câu quá ngắn chỉ có "Tháng này?" or "Tháng 3?" nếu context hiện tại là revenue chain
    if (context.get("last_query_intent") == "staff_revenue"
            and _SPECIFIC_MONTH2 and not any(kw in _mn_s for kw in ["sach", "cuon", "don", "khach"])):
        m_val = _SPECIFIC_MONTH2.group(1)
        y_val = _SPECIFIC_MONTH2.group(2) or str(date.today().year)
        rev = get_period_revenue(f"month:{m_val}/{y_val}")
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    # Trường hợp: "Tháng này?" ngắn khi trong revenue chain context
    if (len(message.strip()) <= 12
            and any(kw in _mn_s for kw in ["thang nay", "tuan nay", "hom qua", "nam nay"])
            and context.get("last_query_intent") == "staff_revenue"):
        _p_map = {"thang nay": "this_month", "tuan nay": "this_week", "hom qua": "yesterday", "nam nay": "year"}
        _p_key = next((v for k,v in _p_map.items() if k in _mn_s), "this_month")
        rev = get_period_revenue(_p_key)
        context["last_query_intent"] = "staff_revenue"
        return format_revenue(rev) + f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người", ["mysql:orders"]

    _BOOK_COREF_KW = [
        "con bao nhieu","con cuon nao","con hang","het hang","het chua",
        "con khong","sach do con","cuon do con","no con",
        # NOTE: 'co khuyen mai' ĐÃ XOÁ: tránh trigger staff_inventory_low
        "gia sach do","gia cuon do",
        "sach vua quet","cuon vua quet","sach nay con","cuon nay con",
        "vua quet con","vua scan con","vua tim con",
        # FIX-S01: advisory khi hỏi nhập thêm không rõ số - biến thể đảo thứ tự
        "nhap them bao nhieu","bao nhieu thi nen nhap","can nhap them",
        "bao nhieu thi dat hang","nen dat bao nhieu","nhap du",
        "bao nhieu de nhap","nhap may cuon","dat may cuon",
        # FIX-S03: "cần nhập thêm" không có COREF nẳm đầu câu
        "can nhap bao nhieu","nhap bao nhieu de du","can nhap them de du",
        "bao nhieu cuon de du","mua them bao nhieu","bao nhieu de du hang",
    ]
    # Ưu tiên: last_book_id → last_book_name → last_found_title → last_search_query
    _coref_identifier = (
        str(context.get("last_book_id")) if context.get("last_book_id") else
        context.get("last_book_name") or
        context.get("last_found_title") or
        context.get("last_search_query")
    )
    # FIX-S03: Nếu có advisory KW nhưng _coref_identifier rỗng → thử lấy từ last_book_id
    _ADV_KW_S03 = ["can nhap bao nhieu","nhap bao nhieu de du","bao nhieu de du","can nhap them de du","bao nhieu cuon de du"]
    if any(kw in _mn_s for kw in _ADV_KW_S03) and not _coref_identifier:
        _last_bid = context.get("last_book_id")
        if _last_bid:
            _coref_identifier = str(_last_bid)

    # [FIX] Extract explicit "(ID: XXXX)" hoặc "ID XXXX" từ message → override _coref_identifier
    # VD: "Đắc Nhân Tâm (ID: 6793) cần nhập thêm..." → dùng "6793" thay vì "Đắc Nhân Tâm"
    import re as _re_id_extract
    _id_in_msg = _re_id_extract.search(r'\(ID:\s*(\d+)\)|id\s*[:#]?\s*(\d+)', message, _re_id_extract.IGNORECASE)
    if _id_in_msg:
        _extracted_id = _id_in_msg.group(1) or _id_in_msg.group(2)
        if _extracted_id:
            _coref_identifier = _extracted_id  # Dùng ID cụ thể, bỏ qua context rộng

    # ── [INV-INTERCEPT] Câu có tên sách cụ thể trong message hiện tại ────────────
    # VD: "Cuốn Đắc Nhân Tâm - Con Đường Thành Công nhập thêm bao nhiêu thì hợp lý"
    # VD: "Đắc Nhân Tâm (ID: 6793) cần nhập thêm bao nhiêu nữa thì hợp lý"
    # Cần extract tên sách cụ thể từ message thay vì dùng _coref_identifier rộng
    import re as _re_inv
    _inv_title_m = _re_inv.search(
        r'(?:cuốn|sách|quyển)\s+(.+?)\s+(?:nhập thêm|cần nhập|nhập bao nhiêu|nhập mấy cuốn|hợp lý|bao nhiêu để đủ|nên nhập|bao nhiêu thì)',
        message, _re_inv.IGNORECASE
    )
    # [FIX] Cũng bắt pattern không có prefix: "TITLE (ID: XXX) cần nhập thêm..."
    if not _inv_title_m:
        _inv_title_m = _re_inv.search(
            r'^([^()]{5,60}?)\s*(?:\(ID:\s*\d+\))?\s+(?:cần nhập|nhập thêm|nhập bao nhiêu|hợp lý|bao nhiêu thì|nên nhập)',
            message, _re_inv.IGNORECASE
        )
    if _inv_title_m:
        _inv_title = _inv_title_m.group(1).strip()
        if len(_inv_title) > 5:  # Tránh bắt nhầm cụm từ ngắn
            _inv_books = search_book_inventory(_inv_title)
            if _inv_books:
                _inv_book = _inv_books[0]  # Tìm cuốn khớp nhất
                context["last_book_id"]   = _inv_book.get("book_id")
                context["last_book_name"] = _inv_book.get("title", _inv_title)
                _stk = int(_inv_book.get("stock_quantity", 0))
                _TARGET_STOCK = 50  # Ngưỡng tồn kho an toàn
                if _stk == 0:
                    _rec = 60           # Hết hàng → nhập gấp (+ buffer)
                elif _stk < _TARGET_STOCK:
                    _rec = max(10, _TARGET_STOCK - _stk)  # Đạt ngưỡng 50
                else:
                    _rec = 0            # Tồn đủ, chưa cần nhập
                if _rec > 0:
                    return (
                        format_inventory(_inv_book, _inv_title) +
                        f"\n\n\U0001f4a1 **Tư vấn nhập hàng:**\n"
                        f"Tồn kho hiện tại: **{_stk} cuốn** | Mục tiêu an toàn: **{_TARGET_STOCK} cuốn**\n"
                        f"\u21d2 Gợi ý đặt nhập: **{_rec} cuốn** "
                        f"(đưa tồn kho lên ~{_stk + _rec} cuốn)"
                    ), ["mysql:books"]
                else:
                    return (
                        format_inventory(_inv_book, _inv_title) +
                        f"\n\n\u2705 **Tồn kho đang đủ** ({_stk} cuốn ≥ {_TARGET_STOCK} mục tiêu).\n"
                        f"Chưa cần nhập thêm."
                    ), ["mysql:books"]
    # ── end inv-intercept ─────────────────────────────────────────────────

    _LIST_QUERY_KW = ["sach nao", "cuon nao", "quyen nao", "nhung sach", "nhung cuon", "danh sach", "thong ke", "bao nhieu sach", "bao nhieu cuon"]
    if any(kw in _mn_s for kw in _BOOK_COREF_KW) and not any(kw in _mn_s for kw in _LIST_QUERY_KW) and intent != "staff_inventory_update" and _coref_identifier:
        books = search_book_inventory(_coref_identifier)
        if books:
            if len(books) > 1 and not str(_coref_identifier).isdigit():
                # Sufficiency question ("nó còn đủ không", "còn không") → tóm tắt thay vì lặp list
                _SUFF_KW = ["con du", "het chua", "no con", "du khong", "co du", "con khong", "du hang"]
                if any(kw in _mn_s for kw in _SUFF_KW):
                    _ok  = sum(1 for b in books if b.get("stock_quantity", 0) > 10)
                    _low = sum(1 for b in books if 0 < b.get("stock_quantity", 0) <= 10)
                    _out = sum(1 for b in books if b.get("stock_quantity", 0) == 0)
                    _lowest = min(books, key=lambda b: b.get("stock_quantity", 0))
                    return (
                        f"📦 **Tổng quan tồn kho \'{_coref_identifier}\'** ({len(books)} phiên bản):\n"
                        f"✅ Còn hàng tốt (>10 cuốn): **{_ok} phiên bản**\n"
                        f"⚠️ Sắp hết (1–10 cuốn): **{_low} phiên bản**\n"
                        f" ❌ Hết hàng: **{_out} phiên bản**\n\n"
                        f"📉 Thấp nhất: **{_lowest['title']}** — chỉ còn **{_lowest.get('stock_quantity',0)} cuốn**\n\n"
                        f"💡 Hỏi chi tiết: `Cuốn [tên sách] còn bao nhiêu?` hoặc `ID [mã] cần nhập thêm?`"
                    ), ["mysql:books"]

                # [FIX] "Các phiên bản sắp hết hàng" → filter chỉ sắp hết/hết hàng
                _SAP_HET_COREF_KW = ["sap het", "gan het", "sap het hang", "phien ban het",
                                     "phien ban sap", "cuon nao sap", "cuon sap het"]
                if any(kw in _mn_s for kw in _SAP_HET_COREF_KW):
                    _low_b = [b for b in books if b.get("stock_quantity", 0) <= 10]
                    if _low_b:
                        result_low = f"⚠️ **{len(_low_b)} phiên bản {_coref_identifier} sắp/đã hết hàng:**\n\n"
                        for b in _low_b:
                            _sq = b.get("stock_quantity", 0)
                            _slb = "Sắp hết" if _sq > 0 else "Hết hàng"
                            result_low += f"  • **{b['title']}** (ID: {b['book_id']}) — còn **{_sq} cuốn** ({_slb})\n"
                        result_low += "\n💡 Cập nhật: `Cập nhật tồn kho sách ID [Mã] lên [Số lượng]`."
                        return result_low, ["mysql:books"]
                    else:
                        return (
                            f"✅ Tất cả phiên bản **{_coref_identifier}** đều còn hàng tốt (>10 cuốn).",
                            ["mysql:books"]
                        )

                # Ambiguous: return list
                result = f"📚 Có **{len(books)} phiên bản sách** khớp với từ khóa \"{_coref_identifier}\". Bạn muốn thao tác trên cuốn nào?\n\n"
                for b in books[:15]: # Show max 15 to avoid text limit
                    stock = b.get("stock_quantity", 0)
                    stock_label = "Còn hàng" if stock > 10 else ("Sắp hết" if stock > 0 else "Hết hàng")
                    result += f"  • **{b['title']}** (ID: {b['book_id']}) — Tồn kho: **{stock} cuốn** ({stock_label})\n"
                if len(books) > 15:
                    result += f"  (... và {len(books) - 15} sách khác)\n"
                result += "\n💡 Bạn có thể hỏi: `ID [Mã] cần nhập thêm bao nhiêu?`"
                return result, ["mysql:books"]
            
            book = books[0]
            context["last_book_id"]   = book.get("book_id")
            context["last_book_name"] = book.get("title", _coref_identifier)

            # FIX-S01: "bao nhiêu thì nên nhập thêm?" không có số target → advisory dựa vào tồn kho
            _ADV_KW = [
                "nhap them bao nhieu","bao nhieu thi nen nhap","can nhap them",
                "bao nhieu thi dat hang","nen dat bao nhieu","nhap du",
                # FIX-S02: thêm biến thể cho "sách vừa quét cần nhập thêm bao nhiêu để đủ N cuốn"
                "can nhap bao nhieu","nhap bao nhieu de du","nhap them de du",
                "bao nhieu de du","bao nhieu cuon de du","mua them bao nhieu",
                "can nhap them de du","can dat them bao nhieu",
            ]
            if any(kw in _mn_s for kw in _ADV_KW):
                _stk_now = int(book.get("stock_quantity", 0))
                import re as _re_adv
                _has_target = bool(_re_adv.search(r'\d+', message))
                if not _has_target:
                    _TARGET_STOCK = 50
                    if _stk_now == 0:
                        _rec_order = 60
                    elif _stk_now < _TARGET_STOCK:
                        _rec_order = max(10, _TARGET_STOCK - _stk_now)
                    else:
                        _rec_order = 0
                    if _rec_order > 0:
                        return (
                            format_inventory(book, _coref_identifier) +
                            f"\n\n\U0001f4a1 **Tư vấn nhập hàng:**\n"
                            f"Tồn kho hiện tại: **{_stk_now} cuốn** | Mục tiêu an toàn: **{_TARGET_STOCK} cuốn**\n"
                            f"\u21d2 Gợi ý đặt nhập: **{_rec_order} cuốn** "
                            f"(đưa tồn kho lên ~{_stk_now + _rec_order} cuốn)"
                        ), ["mysql:books"]
                    else:
                        return (
                            format_inventory(book, _coref_identifier) +
                            f"\n\n\u2705 **Tồn kho đang đủ** ({_stk_now} cuốn ≥ {_TARGET_STOCK} mục tiêu).\n"
                            f"Chưa cần nhập thêm."
                        ), ["mysql:books"]
                else:
                    # FIX-BUG1: có số target rõ → tính toán "target - current"
                    import re as _re_math_b1
                    _tgt_match_b1 = _re_math_b1.search(
                        r'(?:de\s+)?(?:du|dat|toi)\s+(\d+)|'
                        r'(\d+)\s*(?:cuon|quyen)\s*(?:la)?\s*(?:du|la du)',
                        _mn_s
                    )
                    if _tgt_match_b1:
                        _tgt_b1 = int(next(g for g in _tgt_match_b1.groups() if g))
                        _needed_b1 = max(0, _tgt_b1 - _stk_now)
                        return (
                            format_inventory(book, _coref_identifier) +
                            f"\n\n\U0001f4ca **Tính toán nhập hàng:**\n"
                            f"Mục tiêu: **{_tgt_b1} cuốn** | Tồn hiện tại: **{_stk_now} cuốn**\n"
                            f"\u21d2 Cần nhập thêm: **{_needed_b1} cuốn**"
                        ), ["mysql:books"]

            # FIX-BUG2: Cross-context inventory compare
            # "Cuốn sách vừa quét và [Tên sách] cái nào còn nhiều tồn kho hơn?"
            import re as _re_cross
            _cross_cmp_re = _re_cross.compile(
                r'(?:vua quet|vua scan|vua chup|cuon do|sach do|nay|no)\s+(?:va|voi)\s+([a-z0-9 ]+?)(?:\s*,\s*|\s+)(?:cai nao|cuon nao|ton kho|con bao nhieu|nhieu hon|it hon)|'
                r'([a-z0-9 ]+?)\s+(?:va|voi)\s+(?:vua quet|vua scan|vua chup|cuon do|sach do|nay|no)(?:\s*,\s*|\s+)(?:cai nao|cuon nao|ton kho|con bao nhieu|nhieu hon|it hon)'
            )
            _cross_m = _cross_cmp_re.search(_mn_s)
            if _cross_m:
                # Trích xuất từ message gốc (có dấu) bằng span để search DB chính xác hơn
                _start, _end = _cross_m.span(1) if _cross_m.group(1) else _cross_m.span(2)
                _other_book_name = message[_start:_end].strip()
                _other_books = search_book_inventory(_other_book_name)
                
                if _other_books and _other_books[0].get("title") != book.get("title"):
                    _other_book = _other_books[0]
                    _stk_ctx   = int(book.get("stock_quantity", 0))
                    _stk_other = int(_other_book.get("stock_quantity", 0))
                    _winner = book if _stk_ctx >= _stk_other else _other_book
                    
                    return (
                        f"**So sánh tồn kho:**\n"
                        f"Sách **{book.get('title')}**: {_stk_ctx} cuốn\n"
                        f"Sách **{_other_book.get('title')}**: {_stk_other} cuốn\n\n"
                        f"\u21d2 Còn nhiều hơn: **{_winner.get('title')}** "
                        f"({max(_stk_ctx, _stk_other)} cuốn)"
                    ), ["mysql:books"]

            return format_inventory(book, _coref_identifier), ["mysql:books"]
        else:
            # Sách tìm được từ OCR nhưng check_book_inventory không match → thông báo rõ
            return (
                f"📦 Không tìm thấy thông tin tồn kho cho **\"{_coref_identifier}\"** trong hệ thống.\n"
                f"Bạn có thể tìm trực tiếp: hỏi `Tồn kho {_coref_identifier}`."
            ), []


    # ── [S-V5-02 T4 FIX] OCR Rating Compare: "2 cuốn vừa check, cuốn nào đánh giá cao hơn?" ─────
    _OCR_RATING_KW = [
        "danh gia cao hon", "rating cao hon", "review cao hon",
        "cuon nao duoc danh gia", "cuon nao rating", "so sanh danh gia",
        "star cao hon", "sao cao hon", "danh gia tot hon", "cuon nao tot hon",
        "2 cuon vua check", "2 cuon vua quet", "hai cuon vua",
    ]
    _ocr_hist_s = context.get("ocr_history", [])
    if any(kw in _mn_s for kw in _OCR_RATING_KW) and len(_ocr_hist_s) >= 2:
        _b1_name = _ocr_hist_s[-2].get("title", "")
        _b2_name = _ocr_hist_s[-1].get("title", "")
        _b1_book = check_book_inventory(_b1_name) if _b1_name else None
        _b2_book = check_book_inventory(_b2_name) if _b2_name else None
        _b1_rating = float((_b1_book or {}).get("avg_rating", 0) or _ocr_hist_s[-2].get("rating", 0))
        _b2_rating = float((_b2_book or {}).get("avg_rating", 0) or _ocr_hist_s[-1].get("rating", 0))
        _winner_name = _b1_name if _b1_rating >= _b2_rating else _b2_name
        _winner_rating = max(_b1_rating, _b2_rating)
        _loser_name = _b2_name if _b1_rating >= _b2_rating else _b1_name
        _loser_rating = min(_b1_rating, _b2_rating)
        return (
            f"**So sánh đánh giá 2 cuốn sách vừa quét:**\n"
            f"Sách **{_b1_name}** — ⭐ {_b1_rating:.1f}\n"
            f"Sách **{_b2_name}** — ⭐ {_b2_rating:.1f}\n\n"
            f"✅ **{_winner_name}** có đánh giá cao hơn (**{_winner_rating:.1f} sao**)\n"
            f"(so với {_loser_name}: {_loser_rating:.1f} sao)"
        ), ["mysql:books"]
    # ── end OCR rating compare ──────────────────────────────────────────────────

    # ── [FIX BUG-9] Handler cho 'Tổng số sản phẩm trong hệ thống' ────────────
    _TOTAL_PRODUCT_KW = [
        "tong so san pham","tong san pham","bao nhieu san pham",
        "tong so sach","bao nhieu cuon","bao nhieu sach","tong cuon",
        "he thong co bao nhieu",
    ]
    if any(kw in _mn_s for kw in _TOTAL_PRODUCT_KW) and intent != "staff_inventory_check":
        return (
            "Để xem tổng số sản phẩm, vui lòng truy cập:\n"
            "**Admin Panel → Quản lý sách** để xem toàn bộ danh mục.\n\n"
            "Tra cứu nhanh: hỏi `Tồn kho [tên sách]` để kiểm tra từng cuốn.",
            []
        )

    # ── [FIX S-01 T2] "bao nhieu don hang hom nay" / "thong ke hom nay" ──────
    _TODAY_STATS_KW = [
        "hom nay bao nhieu", "bao nhieu don hom nay", "so don hom nay",
        "don hom nay", "thong ke hom nay", "thong ke ngay hom nay",
        "bao nhieu don hang hom", "don hang hom nay",
        "thong ke don hom nay",
        "hom nay co bao nhieu", "cho xu ly hom nay", "don cho hom nay",
    ]
    if any(kw in _mn_s for kw in _TODAY_STATS_KW):
        stats  = get_today_order_stats()
        rev    = get_period_revenue("today")
        answer = format_order_stats(stats)
        answer += f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người"
        return answer, ["mysql:orders"]

    # ── [FIX S-02 T6] "sach do co khuyen mai khong?" → check last_book_name ──
    _PROMO_BOOK_KW = [
        "khuyen mai khong", "giam gia khong", "co sale khong",
        "sach do co khuyen mai", "cuon do co khuyen mai",
    ]
    if any(kw in _mn_s for kw in _PROMO_BOOK_KW) and context.get("last_book_name"):
        book_name = context["last_book_name"]
        return (
            f"Sách **{book_name}** hiện chưa có trong danh sách khuyến mãi."
            f" Bạn có thể kiểm tra trang **Khuyến mãi** trên Admin Panel.",
            ["mysql:promotions"]
        )

    # ── 0. Confirmation pending ────────────────────────────────────────────────
    if context.get("pending_confirmation"):
        # FIX-BUG11: Cho phép thoát confirmation loop khi user rõ ràng chuyển sang yêu cầu khác
        import re as _re_cnf
        import unicodedata as _ud_cnf
        _ESCAPE_INTENTS = {
            "staff_customer_lookup", "staff_order_lookup", "staff_inventory_check",
            "staff_book_lookup", "staff_order_list_pending", "staff_revenue_today",
            "staff_revenue_period", "staff_top_selling",
        }
        _CANCEL_PHRASES = ["khong thoi", "thoi khong", "bo di", "huy di",
                           "bo thoi", "dung lai", "thoat", "thoi bo"]
        _mn_cnf = "".join(
            c for c in _ud_cnf.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_cnf.category(c) != "Mn"
        )
        _has_email = bool(_re_cnf.search(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', message))
        _should_escape = (
            any(ph in _mn_cnf for ph in _CANCEL_PHRASES)
            or intent in _ESCAPE_INTENTS
            or _has_email
        )
        if _should_escape:
            context.pop("pending_confirmation", None)  # clear confirmation, tiếp tục xuống
        else:
            return _handle_staff_confirmation(intent, context), ["confirm_flow"]


    # ── NHÓM S-A: ĐƠN HÀNG ═══════════════════════════════════

    # ── [INTERCEPTOR] "đổi/chuyển đơn #ID sang/thành [status]" ────────────
    # NLU hay nhầm "đổi đơn" → staff_return_handle hoặc staff_order_list_pending
    # Bắt chính xác bằng regex trước khi vào intent routing
    import re as _re_su
    _STATUS_KW_NORM = {
        "pending": "pending", "cho xu ly": "pending", "cho duyet": "pending",
        "processing": "processing", "dang xu ly": "processing", "xu ly": "processing",
        "shipped": "shipped", "dang giao": "shipped", "van chuyen": "shipped",
        "delivered": "delivered", "da giao": "delivered", "giao xong": "delivered", "hoan thanh": "delivered",
        "cancelled": "cancelled", "huy": "cancelled", "da huy": "cancelled",
        "cancel_requested": "cancel_requested", "yeu cau huy": "cancel_requested",
        "return_requested": "return_requested", "yeu cau tra": "return_requested",
        "returned": "returned", "hoan tra": "returned", "tra hang": "returned",
        # failed / thất bại
        "failed": "failed", "that bai": "failed", "giao that bai": "failed", "giao hang that bai": "failed",
    }
    _su_m = _re_su.search(
        r'(?:doi|chuyen|cap nhat|update|sua)\s+(?:don|order)\s*[#\s]*(\d+)'
        r'(?:\s+(?:tu|from)\s+\w+)?'
        r'\s+(?:sang|thanh|to|->|→)'
        r'\s+(?:trang thai\s+(?:la\s+)?)?'  # tuỳ chọn: bỏ qua "trạng thái [là]"
        r'([a-z_\s]+)',
        _mn_s, _re_su.IGNORECASE
    )
    if _su_m:
        _su_oid   = int(_su_m.group(1))
        _su_traw  = _su_m.group(2).strip()
        _su_tstat = next((v for k, v in _STATUS_KW_NORM.items() if k in _su_traw or _su_traw.startswith(k)), None)
        if _su_oid and _su_tstat:
            _su_order = lookup_order_by_id(_su_oid)
            if not _su_order:
                return f"Không tìm thấy đơn hàng #{_su_oid}.", []
            _su_cur = _su_order["status"]
            _su_allowed = VALID_TRANSITIONS.get(_su_cur, [])
            if _su_tstat not in _su_allowed:
                return (
                    f"Không thể chuyển đơn #{_su_oid} từ `{_su_cur}` → `{_su_tstat}`.\n"
                    f"Trạng thái hiện tại **{_su_cur}** cho phép chuyển sang: "
                    f"{', '.join(f'`{s}`' for s in _su_allowed) if _su_allowed else 'không có'}",
                    ["mysql:orders"]
                )
            context["pending_confirmation"] = {
                "action": "update_order_status",
                "order_id": _su_oid,
                "target_status": _su_tstat,
                "current_status": _su_cur,
            }
            context["pending_btns"] = [
                {"type": "confirm_yes", "label": "Có",    "value": "Có",    "url": ""},
                {"type": "confirm_no",  "label": "Không", "value": "Không", "url": ""},
            ]
            context["last_order_id"] = _su_oid
            return (
                f"Xác nhận cập nhật đơn **#{_su_oid}**?\n"
                f"Trạng thái: `{_su_cur}` → `{_su_tstat}`"
            ), ["mysql:orders"]

    if not _su_m:
        _sh_m = _re_su.search(
            r'(duyet huy|xac nhan huy|huy|duyet tra|xac nhan tra|giao thanh cong|giao xong|hoan thanh|that bai|giao that bai)\s+(?:don|order)\s*[#\s]*(\d+)',
            _mn_s, _re_su.IGNORECASE
        )
        if _sh_m:
            _sh_act = _sh_m.group(1).lower().strip()
            _su_oid = int(_sh_m.group(2))
            if "huy" in _sh_act:
                _su_tstat = "cancelled"
            elif "tra" in _sh_act:
                _su_tstat = "returned"
            elif "that bai" in _sh_act:
                _su_tstat = "failed"
            elif "thanh cong" in _sh_act or "xong" in _sh_act or "hoan thanh" in _sh_act:
                _su_tstat = "delivered"
            else:
                _su_tstat = None
                
            if _su_tstat:
                _su_order = lookup_order_by_id(_su_oid)
                if not _su_order:
                    return f"Không tìm thấy đơn hàng #{_su_oid}.", []
                _su_cur = _su_order["status"]
                _su_allowed = VALID_TRANSITIONS.get(_su_cur, [])
                if _su_tstat not in _su_allowed:
                    return (
                        f"Không thể cập nhật đơn #{_su_oid} từ `{_su_cur}` → `{_su_tstat}`.\n"
                        f"Trạng thái hiện tại **{_su_cur}** cho phép chuyển sang: "
                        f"{', '.join(f'`{s}`' for s in _su_allowed) if _su_allowed else 'không có'}",
                        ["mysql:orders"]
                    )
                context["pending_confirmation"] = {
                    "action": "update_order_status",
                    "order_id": _su_oid,
                    "target_status": _su_tstat,
                    "current_status": _su_cur,
                }
                context["pending_btns"] = [
                    {"type": "confirm_yes", "label": "Có",    "value": "Có",    "url": ""},
                    {"type": "confirm_no",  "label": "Không", "value": "Không", "url": ""},
                ]
                context["last_order_id"] = _su_oid
                return (
                    f"Xác nhận cập nhật đơn **#{_su_oid}**?\n"
                    f"Trạng thái: `{_su_cur}` → `{_su_tstat}`"
                ), ["mysql:orders"]

    # ── end interceptor ────────────────────────────────────────────────────

    # FIX-CR01: Coreference "đơn đầu tiên trong đó", "đơn thứ 2" → last_order_list
    _ORDER_ORDINAL_MAP = {
        "dau tien": 0, "thu nhat": 0, "so 1": 0, "first": 0,
        "thu hai": 1, "so 2": 1, "second": 1,
        "thu ba": 2, "so 3": 2, "third": 2,
        "thu tu": 3, "so 4": 3, "cuoi cung": -1, "last": -1,
    }
    _ORDER_COREF_KW = ["trong do", "trong danh sach do", "trong list do", "vua lay", "vua hien"]
    if (
        intent == "staff_order_lookup"
        and not entities.get("order_id")
        and context.get("last_order_list")
        and any(kw in _mn_s for kw in _ORDER_COREF_KW)
    ):
        _resolved_id = None
        for _ordinal_kw, _idx in _ORDER_ORDINAL_MAP.items():
            if _ordinal_kw in _mn_s:
                try:
                    _resolved_id = context["last_order_list"][_idx]
                except IndexError:
                    pass
                break
        if _resolved_id:
            order = lookup_order_by_id(_resolved_id)
            if order:
                context["last_order_id"] = _resolved_id
            return format_order_detail(order), ["mysql:orders"]

    if intent == "staff_order_lookup":
        order_id = entities.get("order_id")
        if not order_id:
            # Thử resolve từ context last_order_id (coreference "đơn đó", "nó")
            _coref_order_kw = ["no","don do","don nay","don tren","cai do","this","it","don vua xem"]
            if any(kw in _mn_s for kw in _coref_order_kw):
                order_id = context.get("last_order_id")
        if not order_id:
            return (
                "Vui lòng cung cấp **mã đơn hàng**, ví dụ: `Đơn #12345`",
                []
            )
        order = lookup_order_by_id(order_id)
        # Luu context de ho tro coreference ("no", "don do", "don nay")
        if order:
            context["last_order_id"] = order_id
        return format_order_detail(order), ["mysql:orders"]

    if intent == "staff_order_status_update":
        order_id      = entities.get("order_id")
        target_status = entities.get("target_status")

        # Coreference: "nó", "đơn đó", "đơn này" → dùng last_order_id nếu có
        if not order_id:
            import re as _re
            _msg_norm = message.lower()
            _coref_kw = ["nó", "đơn đó", "đơn này", "đơn trên", "cái đó", "this", "it"]
            if any(kw in _msg_norm for kw in _coref_kw):
                order_id = context.get("last_order_id")

        if not order_id:
            return "Vui lòng cung cấp **mã đơn hàng** để cập nhật.", []
        if not target_status:
            return (
                f"Đơn hàng #{order_id} - bạn muốn chuyển sang trạng thái nào?\n"
                "Có thể dùng: `processing`, `shipped`, `delivered`, `cancelled`",
                []
            )

        order = lookup_order_by_id(order_id)
        if not order:
            return f"Không tìm thấy đơn hàng #{order_id}.", []

        current_status = order["status"]
        allowed        = VALID_TRANSITIONS.get(current_status, [])

        if target_status not in allowed:
            return (
                f"Không thể chuyển đơn #{order_id} từ `{current_status}` → `{target_status}`.\n"
                f"Trạng thái hiện tại cho phép: {', '.join(allowed) if allowed else 'không có'}",
                ["mysql:orders"]
            )

        context["pending_confirmation"] = {
            "action":         "update_order_status",
            "order_id":       order_id,
            "target_status":  target_status,
            "current_status": current_status,
        }
        context["pending_btns"] = [
            {"type": "confirm_yes", "label": "Có",    "value": "Có",    "url": ""},
            {"type": "confirm_no",  "label": "Không", "value": "Không", "url": ""},
        ]
        return (
            f"Xác nhận cập nhật đơn **#{order_id}**?\n"
            f"Trạng thái: `{current_status}` → `{target_status}`"
        ), ["mysql:orders"]

    if intent == "staff_order_list_pending":
        # ── Phát hiện câu hỏi về SỐ LƯỢNG đơn theo trạng thái ──────────────
        # Phủ đầy đủ 8 trạng thái thực tế trong DB
        _STATUS_COUNT_KW = {
            # pending - chờ xử lý
            "cho xu ly":       "pending",
            "cho duyet":       "pending",
            "dang cho":        "pending",
            "pending":         "pending",
            "chua xu ly":      "pending",
            # processing - đang xử lý
            "dang xu ly":      "processing",
            "processing":      "processing",
            "dang chuan bi":   "processing",
            # shipped - đang giao
            "dang giao":       "shipped",
            "shipped":         "shipped",
            "dang van chuyen": "shipped",
            "dang ship":       "shipped",
            # delivered - đã giao
            "da giao":         "delivered",
            "giao thanh cong": "delivered",
            "delivered":       "delivered",
            "giao xong":       "delivered",
            # cancelled - đã hủy
            "da huy":          "cancelled",
            "bi huy":          "cancelled",
            "cancelled":       "cancelled",
            "huy don":         "cancelled",
            # cancel_requested - yêu cầu hủy
            "yeu cau huy":     "cancel_requested",
            "cancel_requested":"cancel_requested",
            "xin huy":         "cancel_requested",
            "muon huy":        "cancel_requested",
            # return_requested - yêu cầu trả hàng
            "yeu cau tra":     "return_requested",
            "yeu cau hoan":    "return_requested",
            "return_requested":"return_requested",
            "xin tra":         "return_requested",
            "muon tra hang":   "return_requested",
            # returned - đã hoàn hàng
            "hoan hang":       "returned",
            "tra hang":        "returned",
            "returned":        "returned",
            "da tra":          "returned",
            "da hoan":         "returned",
        }
        _COUNT_TRIGGER_KW = ["bao nhieu", "co bao nhieu", "tong", "tong so", "so luong", "dem", "co bao", "co may"]

        _matched_status = None
        for kw, st in _STATUS_COUNT_KW.items():
            if kw in _mn_s:
                _matched_status = st
                break

        _is_count_query = any(kw in _mn_s for kw in _COUNT_TRIGGER_KW)

        # Helper: format bảng thống kê đầy đủ 8 trạng thái
        def _fmt_all_stats(stats: dict, title: str = "Thống kê đơn hàng — Toàn bộ lịch sử") -> str:
            p  = int(stats.get("pending", 0))
            pr = int(stats.get("processing", 0))
            sh = int(stats.get("shipped", 0))
            de = int(stats.get("delivered", 0))
            ca = int(stats.get("cancelled", 0))
            cr = int(stats.get("cancel_requested", 0))
            rr = int(stats.get("return_requested", 0))
            rt = int(stats.get("returned", 0))
            total = int(stats.get("total_orders", 0))
            rev   = float(stats.get("total_revenue", 0))
            d_rev = float(stats.get("delivered_revenue", 0))
            return (
                f"📊 **{title}:**\n\n"
                f"⏳ Chờ xử lý (pending):        **{p:,}**\n"
                f"🔧 Đang xử lý (processing):     **{pr:,}**\n"
                f"🚚 Đang giao (shipped):          **{sh:,}**\n"
                f"✅ Đã giao (delivered):           **{de:,}**\n"
                f"❌ Đã hủy (cancelled):            **{ca:,}**\n"
                f"🚫 Yêu cầu hủy:                 **{cr:,}**\n"
                f"🔄 Yêu cầu trả hàng:             **{rr:,}**\n"
                f"📦 Đã hoàn trả (returned):       **{rt:,}**\n\n"
                f"🧾 **Tổng đơn:** {total:,}\n"
                f"💰 **Tổng doanh thu:** {rev:,.0f}đ\n"
                f"💰 **Doanh thu đã giao:** {d_rev:,.0f}đ"
            )

        # Map trạng thái → nhãn hiển thị
        _STATUS_LABEL_MAP = {
            "pending":          "⏳ Chờ xử lý (pending)",
            "processing":       "🔧 Đang xử lý (processing)",
            "shipped":          "🚚 Đang giao hàng (shipped)",
            "delivered":        "✅ Đã giao thành công (delivered)",
            "cancelled":        "❌ Đã hủy (cancelled)",
            "cancel_requested": "🚫 Yêu cầu hủy (cancel_requested)",
            "return_requested": "🔄 Yêu cầu trả hàng (return_requested)",
            "returned":         "📦 Đã hoàn trả (returned)",
        }

        # Nếu là câu hỏi đếm số lượng → trả thống kê tổng hợp
        if _is_count_query:
            stats = get_all_time_order_stats()
            if _matched_status and _matched_status in _STATUS_LABEL_MAP:
                lbl = _STATUS_LABEL_MAP[_matched_status]
                cnt = int(stats.get(_matched_status, 0))
                return (
                    f"📊 **Thống kê đơn hàng — Toàn bộ lịch sử:**\n\n"
                    f"**{lbl}:** {cnt:,} đơn\n\n"
                    f"Tổng tất cả trạng thái: {int(stats.get('total_orders', 0)):,} đơn"
                ), ["mysql:orders"]
            else:
                return _fmt_all_stats(stats), ["mysql:orders"]

        # ── Phát hiện user muốn xem DANH SÁCH theo khoảng thời gian ─────────
        _DATE_ALL_KW  = ["tat ca", "toan bo", "toan he thong", "tu truoc den nay", "lich su", "all"]
        _DATE_WEEK_KW = ["tuan nay", "7 ngay", "7 ngay qua", "tuan qua", "trong tuan"]

        if any(kw in _mn_s for kw in _DATE_ALL_KW):
            stats = get_all_time_order_stats()
            return (
                _fmt_all_stats(stats) +
                "\n\n_(Muốn xem danh sách cụ thể? Hỏi: \"đơn chờ hôm nay\" hoặc \"đơn chờ tuần này\")_"
            ), ["mysql:orders"]

        elif any(kw in _mn_s for kw in _DATE_WEEK_KW):
            date_filter = "week"
            label = "Đơn chờ & đang xử lý (7 ngày qua)"
        else:
            date_filter = "today"
            label = "Đơn chờ & đang xử lý hôm nay"

        orders = lookup_pending_orders(limit=15, date_filter=date_filter)

        if not orders and date_filter == "today":
            stats = get_all_time_order_stats()
            return (
                f"📭 Hôm nay chưa có đơn hàng mới đang chờ / đang xử lý.\n\n"
                f"📊 **Tổng toàn hệ thống:**\n"
                f"⏳ Chờ xử lý (pending): **{int(stats.get('pending', 0)):,}**\n"
                f"🔧 Đang xử lý (processing): **{int(stats.get('processing', 0)):,}**\n\n"
                f"(Hỏi \"đơn chờ tuần này\" hoặc \"tổng đơn chờ\" để xem thêm)"
            ), ["mysql:orders"]

        if orders:
            context["last_order_list"] = [o["order_id"] for o in orders]
        return format_order_list(orders, label), ["mysql:orders"]

    if intent == "staff_return_handle":
        import re as _re_rh
        # ── Trường hợp 1: User muốn xử lý đơn CỤ THỂ ("Xử lý trả hàng #4984") ──
        _rh_order_id = entities.get("order_id")
        if not _rh_order_id:
            # fallback: tìm số trong message (ví dụ "xử lý #4984")
            _rh_match = _re_rh.search(r"#?(\d{3,6})", message)
            if _rh_match:
                _rh_order_id = int(_rh_match.group(1))

        if _rh_order_id:
            order = lookup_order_by_id(_rh_order_id)
            if not order:
                return f"❌ Không tìm thấy đơn hàng **#{_rh_order_id}**.", []
            cur_status = order.get("status", "")
            if cur_status != "return_requested":
                return (
                    f"⚠️ Đơn **#{_rh_order_id}** có trạng thái `{cur_status}` — "
                    f"không phải `return_requested`.\n"
                    f"Chỉ có thể phê duyệt trả hàng cho đơn đang ở trạng thái `return_requested`."
                ), ["mysql:orders"]
            # Mở confirmation gate
            name  = order.get("recipient_name") or order.get("username") or order.get("email", "?")
            amt   = float(order.get("total_amount") or 0)
            context["pending_confirmation"] = {
                "action":         "update_order_status",
                "order_id":       _rh_order_id,
                "target_status":  "returned",
                "current_status": "return_requested",
            }
            context["last_order_id"] = _rh_order_id
            context["pending_btns"] = [
                {"type": "confirm_yes", "label": "Có",    "value": "Có",    "url": ""},
                {"type": "confirm_no",  "label": "Không", "value": "Không", "url": ""},
            ]
            return (
                f"🔄 **Xác nhận phê duyệt trả hàng?**\n"
                f"📦 Đơn **#{_rh_order_id}** | Khách: **{name}** | "
                f"Giá trị: **{amt:,.0f}đ**\n"
                f"Thao tác: `return_requested` → `returned`\n\n"
                f"Bạn có muốn **phê duyệt** yêu cầu trả hàng này không?"
            ), ["mysql:orders"]

        # ── Trường hợp 2: Xem DANH SÁCH đơn yêu cầu trả hàng (không có order_id) ──
        all_issues = lookup_return_requests(limit=30)
        return_req  = [o for o in all_issues if o.get("status") == "return_requested"]
        cancel_req  = [o for o in all_issues if o.get("status") == "cancel_requested"]

        # Kiểm tra user có hỏi cụ thể về trả hàng hay hủy đơn không
        _is_cancel_q = any(kw in _mn_s for kw in ["huy", "cancel"])
        if _is_cancel_q:
            orders_to_show = cancel_req
            label = "Yêu cầu hủy đơn"
        else:
            # Mặc định: chỉ hiện return_requested
            orders_to_show = return_req
            label = "Yêu cầu trả hàng"

        if orders_to_show:
            context["last_order_list"] = [o["order_id"] for o in orders_to_show]

        result = format_order_list(orders_to_show, label)
        # Gợi ý xử lý nếu có đơn
        if orders_to_show:
            result += "\n\n📌 **Xử lý:** Gõ `Xử lý trả hàng #ID` để phê duyệt từng đơn."
        return result, ["mysql:orders"]

    if intent == "staff_order_list_by_status":
        # Nhận diện trạng thái người dùng muốn xem từ message
        _STATUS_KW_MAP = {
            "yeu cau huy": "cancel_requested", "cancel_requested": "cancel_requested",
            "xin huy": "cancel_requested", "muon huy": "cancel_requested",
            "dang giao": "shipped", "shipped": "shipped",
            "giao hang": "shipped", "van chuyen": "shipped", "dang ship": "shipped",
            "that bai": "failed", "failed": "failed", "giao that bai": "failed",
            "yeu cau tra": "return_requested", "return_requested": "return_requested",
            "da huy": "cancelled", "cancelled": "cancelled", "bi huy": "cancelled",
            "da giao": "delivered", "delivered": "delivered", "giao xong": "delivered",
            "hoan tra": "returned", "returned": "returned", "tra hang": "returned",
            "dang xu ly": "processing", "processing": "processing",
            "cho xu ly": "pending", "pending": "pending",
        }
        _STATUS_LABELS = {
            "cancel_requested": "🚫 Yêu cầu hủy",
            "shipped":          "🚚 Đang giao hàng",
            "failed":           "⚠️ Giao thất bại",
            "return_requested": "🔄 Yêu cầu trả hàng",
            "cancelled":        "❌ Đã hủy",
            "delivered":        "✅ Đã giao",
            "returned":         "📦 Đã hoàn trả",
            "processing":       "🔧 Đang xử lý",
            "pending":          "⏳ Chờ xử lý",
        }

        target_status = None
        for kw, st in _STATUS_KW_MAP.items():
            if kw in _mn_s:
                target_status = st
                break

        if not target_status:
            return (
                "Bạn muốn xem đơn theo trạng thái nào?\n"
                "Ví dụ: *\"đơn yêu cầu hủy\"*, *\"đơn đang giao\"*, *\"đơn thất bại\"*"
            ), []

        orders = lookup_pending_orders(limit=15, date_filter="all", status=target_status)
        label = _STATUS_LABELS.get(target_status, target_status)

        if not orders:
            stats = get_all_time_order_stats()
            count = int(stats.get(target_status, 0))
            return (
                f"📭 Không tìm thấy đơn nào đang **{label}** trong hôm nay.\n"
                f"📊 Tổng toàn hệ thống: **{count:,}** đơn {label}"
            ), ["mysql:orders"]

        context["last_order_list"] = [o["order_id"] for o in orders]
        return format_order_list(orders, f"Danh sách đơn — {label}"), ["mysql:orders"]

    if intent == "staff_order_statistics":
        date_range = entities.get("date_range", "today")
        if date_range == "today":
            stats = get_today_order_stats()
            return format_order_stats(stats), ["mysql:orders"]
        elif date_range == "all":
            stats = get_all_time_order_stats()
            return (
                f"📊 **Thống kê đơn hàng — Toàn bộ lịch sử:**\n\n"
                f"⏳ Chờ xử lý:       **{int(stats.get('pending', 0)):,}**\n"
                f"🔧 Đang xử lý:      **{int(stats.get('processing', 0)):,}**\n"
                f"🚚 Đang giao:        **{int(stats.get('shipped', 0)):,}**\n"
                f"✅ Đã giao:          **{int(stats.get('delivered', 0)):,}**\n"
                f"❌ Đã hủy:           **{int(stats.get('cancelled', 0)):,}**\n"
                f"🚫 Yêu cầu hủy:     **{int(stats.get('cancel_requested', 0)):,}**\n"
                f"🔄 Yêu cầu trả:     **{int(stats.get('return_requested', 0)):,}**\n"
                f"📦 Đã hoàn trả:     **{int(stats.get('returned', 0)):,}**\n\n"
                f"💰 Tổng doanh thu:    **{float(stats.get('total_revenue', 0)):,.0f}đ**"
            ), ["mysql:orders"]
        else:
            rev = get_period_revenue(date_range)
            return format_revenue(rev), ["mysql:orders"]


    # ── NHÓM S-B: TỒN KHO ════════════════════════════════════

    if intent == "staff_inventory_check":
        book_id    = entities.get("book_id")
        # [FIX] Detect "cuốn nào trong kho [TITLE] sắp/gần hết" TRƯỚC _extract_book_name
        # để tránh capture "nào trong kho Đắc Nhân Tâm gần hết hàng" làm identifier sai
        import re as _re_inv
        _LOW_FILTER_PATTERN = _re_inv.search(
            r'(?:cuon|sach|cu[oố]n|s[aá]ch)\s+n[aà]o\s+(?:trong\s+kho\s+|trong\s+s[oố]\s+)'
            r'(.+?)\s+(?:s[aắ]p|g[aầ]n|d[aă]ng|[đd][aã]ng)\s+h[eế]t',
            message, _re_inv.IGNORECASE
        )
        _is_low_filter_query = False
        if _LOW_FILTER_PATTERN and not book_id:
            identifier = _LOW_FILTER_PATTERN.group(1).strip()
            _is_low_filter_query = True  # Sẽ filter chỉ sắp hết sau khi search
        else:
            identifier = str(book_id) if book_id else _extract_book_name(message)
        if not identifier:
            # Coreference: "nó", "cuốn đó", "sách đó", "ảnh vừa quét", "cuốn vừa quét"
            # Normalize để bắt cả phiên bản có/không dấu tiếng Việt
            import unicodedata as _ud_ic
            _msg_norm2 = "".join(
                c for c in _ud_ic.normalize("NFD", message.lower().replace("đ", "d").replace("Đ", "D"))
                if _ud_ic.category(c) != "Mn"
            )
            _coref_book = [
                "no", "cuon do", "sach do", "cuon nay", "sach nay", "cai do",
                "anh vua", "cuon vua", "vua quet", "vua check",
                "nhap them", "can nhap", "de du", "nhap de du",
            ]
            if any(kw in _msg_norm2 for kw in _coref_book):
                # Ưu tiên last_found_title (cuốn vừa tìm/quét) trước last_book_id (stale)
                _ctx_title = context.get("last_found_title") or context.get("last_book_name")
                _ctx_id    = context.get("last_book_id")
                if _ctx_title:
                    identifier = _ctx_title
                elif _ctx_id:
                    identifier = str(_ctx_id)

        if not identifier:
            # 1. Đếm tổng chính xác (fast COUNT queries)
            total_out = count_out_of_stock()
            total_low = count_low_stock(threshold=5)

            if total_out == 0 and total_low == 0:
                return (
                    "📦 **Tồn kho tổng quan:**\n\n"
                    "✅ Hiện không có sách nào sắp hết hàng.\n\n"
                    "Hỏi tên sách cụ thể để kiểm tra tồn kho."
                ), ["mysql:books"]

            # 2. Dòng tổng quan
            result = (
                "📦 **Tồn kho tổng quan:**\n"
                f"⚠️ Hết hàng: **{total_out} sách**  |  "
                f"🟡 Sắp hết: **{total_low} sách**\n\n"
            )

            # 3. Top 10 sách hết hàng
            if total_out > 0:
                books_out = get_out_of_stock_books(limit=10)
                result += "⚠️ **Hết hàng — cần nhập ngay:**\n"
                for b in books_out:
                    title = b['title'][:55] + "..." if len(b['title']) > 55 else b['title']
                    result += f"  • {title}\n"
                if total_out > 10:
                    result += f"  (... và {total_out - 10} sách khác)\n"
                result += "\n"

            # 4. Top 10 sách sắp hết
            if total_low > 0:
                books_low = get_low_stock_books(threshold=5, limit=10)
                result += "🟡 **Sắp hết — ưu tiên bổ sung:**\n"
                for b in books_low:
                    title = b['title'][:50] + "..." if len(b['title']) > 50 else b['title']
                    result += f"  • {title} — còn {b['stock_quantity']} cuốn\n"
                if total_low > 10:
                    result += f"  (... và {total_low - 10} sách khác)\n"

            result += "\nHỏi tên sách để xem chi tiết tồn kho."
            return result, ["mysql:books"]
        
        books = search_book_inventory(identifier)
        if not books:
            book = None
        else:
            if len(books) > 1:
                # [FIX] Nếu là low-filter query → chỉ show sắp hết/hết hàng
                _display_books = books
                if _is_low_filter_query:
                    _display_books = [b for b in books if b.get("stock_quantity", 0) <= 10]
                    if not _display_books:
                        context["last_book_name"] = identifier
                        return (
                            f"✅ Tất cả phiên bản **{identifier}** đều còn hàng tốt (>10 cuốn).\n"
                            f"Hỏi chi tiết từng cuốn nếu cần xem số lượng cụ thể.",
                            ["mysql:books"]
                        )
                    _low_variants = [b for b in _display_books if 0 < b.get("stock_quantity", 0) <= 10]
                    _out_variants = [b for b in _display_books if b.get("stock_quantity", 0) == 0]
                    result = f"⚠️ Phiên bản **{identifier}** sắp/đã hết hàng ({len(_display_books)} phiên bản):\n\n"
                    if _low_variants:
                        result += "🟡 **Sắp hết:**\n"
                        for b in _low_variants:
                            result += f"  • {b['title']} (ID: {b['book_id']}) — còn **{b['stock_quantity']} cuốn**\n"
                    if _out_variants:
                        result += "\n❌ **Hết hàng:**\n"
                        for b in _out_variants:
                            result += f"  • {b['title']} (ID: {b['book_id']})\n"
                    result += "\n💡 Cập nhật: `Cập nhật tồn kho sách ID [Mã] lên [Số lượng]`."
                    context["last_book_name"] = identifier
                    return result, ["mysql:books"]
                # Hiện list thông thường
                result = f"📚 Có **{len(_display_books)} phiên bản sách** khớp với từ khóa \"{identifier}\":\n\n"
                for b in _display_books:
                    stock = b.get("stock_quantity", 0)
                    stock_label = "Còn hàng" if stock > 10 else ("Sắp hết" if stock > 0 else "Hết hàng")
                    title = b['title']
                    result += f"  • **{title}** (ID: {b['book_id']}) — Tồn kho: **{stock} cuốn** ({stock_label})\n"
                result += "\n💡 Bạn có thể hỏi: `Cập nhật tồn kho sách ID [Mã] lên [Số lượng]`."
                context["last_book_name"] = identifier
                return result, ["mysql:books"]
            
            book = books[0]

        if book:
            context["last_book_id"]   = book.get("book_id")
            context["last_book_name"] = book.get("title", identifier)

            # FIX-BUG12+23 (cải thiện): Phát hiện "nhập thêm bao nhiêu để đủ N" → tính toán luôn
            import re as _re_stk
            _target_num = None
            # Pattern A: "đủ/tới N cuốn" - số ở cuối câu (FIX: bỏ $ anchor - _mn_s còn giữ dấu ?)
            _pmA = _re_stk.search(
                r'(?:de\s+)?(?:du|toi|dat)\s+(\d+)\s*(?:cuon|quyen|cai)?'
                r'|(?:du|toi)\s{0,3}(\d+)',
                _mn_s
            )
            # Pattern B: "nhập thêm N"
            _pmB = _re_stk.search(
                r'(?:nhap|them|dat|order).{0,15}(\d+)\s*(?:cuon|quyen)?'
                r'|(?:nhap|them).{0,5}(\d+)',
                _mn_s
            )
            # Pattern D (NEW): "de du N / dat N cuon / du N cuon" - bắt rõ hơn Pattern A
            _pmD = _re_stk.search(
                r'de\s+du\s+(\d+)|dat\s+(\d+)\s*cuon|du\s+(\d+)\s*cuon|toi\s+(\d+)\s*cuon',
                _mn_s
            )
            # Pattern C: số bất kỳ khi có keyword đủ/nhập
            _pmC_nums = None
            _ORDER_KWS2 = ["nhap them","can nhap","de du","dat du","toi du","dat them","du so","du cuon","toi cuon"]
            if any(kw in _mn_s for kw in _ORDER_KWS2):
                _pmC_nums = _re_stk.findall(r'\d+', message)

            for _pm in [_pmD, _pmA, _pmB]:
                if _pm:
                    for grp in (_pm.groups() or []):
                        if grp and str(grp).isdigit():
                            _target_num = int(grp)
                            break
                if _target_num:
                    break
            if not _target_num and _pmC_nums:
                # Lấy số lớn nhất (thường là target, không phải số ngày/%)
                _candidates = [int(n) for n in _pmC_nums if int(n) > 1]
                if _candidates:
                    _target_num = max(_candidates)

            if _target_num and _target_num > 0:
                _current_stock = int(book.get("stock_quantity", 0))
                _need = max(0, _target_num - _current_stock)
                _inv_txt = format_inventory(book, identifier)
                return (
                    _inv_txt + f"\n\n📊 **Tính toán nhập hàng:**\n"
                    f"Mục tiêu: **{_target_num} cuốn** | Tồn hiện tại: **{_current_stock} cuốn**\n"
                    f"⇒ Cần nhập thêm: **{_need} cuốn**"
                ), ["mysql:books"]

        return format_inventory(book, identifier), ["mysql:books"]


    if intent == "staff_inventory_low":
        threshold = entities.get("threshold", 5)
        # [FIX Bug 1] Nếu có context book đang được hỏi → filter low-stock theo book đó
        # thay vì list toàn bộ kho (796 sách)
        _ctx_book_name = context.get("last_book_name")
        # [FIX] Global indicators: "toan kho", "sach nao", "cuon nao" → KHÔNG filter theo context
        _GLOBAL_KW = ["toan kho", "toan bo", "tat ca sach", "toan he thong", "global", "tong",
                      "het sach nao", "sach nao", "cuon nao", "quyen nao", "nhung sach nao", "nhung cuon nao"]
        import unicodedata as _ud_low
        _mn_low = "".join(
            c for c in _ud_low.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_low.category(c) != "Mn"
        )
        _is_global_query = any(kw in _mn_low for kw in _GLOBAL_KW)
        if _ctx_book_name and not _is_global_query:
            # [FIX] Dùng threshold=10 thống nhất với "Sắp hết (1-10 cuốn)" trong inventory_check
            _low_threshold = 10
            # Filter low-stock cho book trong context
            books_all = search_book_inventory(_ctx_book_name)
            if books_all:
                _low = [b for b in books_all if 0 < b.get("stock_quantity", 0) <= _low_threshold]
                _out = [b for b in books_all if b.get("stock_quantity", 0) == 0]
                if not _low and not _out:
                    return (
                        f"✅ Tất cả phiên bản **{_ctx_book_name}** đều còn hàng tốt (>10 cuốn).\n"
                        f"Tổng: {len(books_all)} phiên bản.",
                        ["mysql:books"]
                    )
                result = f"⚠️ Phiên bản **{_ctx_book_name}** sắp/đã hết hàng:\n\n"
                if _low:
                    result += f"🟡 **Sắp hết ({len(_low)} phiên bản):**\n"
                    for b in _low:
                        result += f"  • {b['title']} (ID: {b['book_id']}) — còn **{b['stock_quantity']} cuốn**\n"
                if _out:
                    result += f"\n❌ **Hết hàng ({len(_out)} phiên bản):**\n"
                    for b in _out:
                        result += f"  • {b['title']} (ID: {b['book_id']})\n"
                result += "\n💡 Cập nhật: `Cập nhật tồn kho sách ID [Mã] lên [Số lượng]`."
                return result, ["mysql:books"]
        # Không có context book hoặc là global query → list toàn kho như cũ
        total_low = count_low_stock(threshold=threshold)
        total_out = count_out_of_stock()
        books_low = get_low_stock_books(threshold=threshold, limit=10)
        books_out = get_out_of_stock_books(limit=5)
        result = format_low_stock_list(books_low, threshold=threshold, total=total_low)
        if total_out > 0:
            result += f"\n\n⚠️ **Có tổng cộng {total_out} sách đã hết hàng hoàn toàn:**"
            for b in books_out:
                result += f"\n  • **{b['title']}**"
            if total_out > len(books_out):
                result += f"\n  ... và {total_out - len(books_out)} sách khác."
        return result, ["mysql:books"]

    if intent == "staff_inventory_update":
        import re as _re2
        _msg_lower = message.lower()

        # ── Bước 1: Xác định sách cần cập nhật ───────────────────────────────
        # Ưu tiên: book_id từ entities → coreference context → tên sách trong message
        book_id  = entities.get("book_id")
        book_name = None

        # Normalize message để kiểm tra coreference (bỏ dấu tiếng Việt)
        import unicodedata as _ud_inv
        _msg_norm = "".join(
            c for c in _ud_inv.normalize("NFD", _msg_lower.replace("đ", "d").replace("Đ", "D"))
            if _ud_inv.category(c) != "Mn"
        )

        # Kiểm tra coreference ("sách vừa quét", "nó", "cuốn đó", "cuốn vừa quét")
        _coref_kws = [
            "vua quet", "vua scan",
            "cuon do", "sach do",
            "no ", " no",
            "cuon nay", "sach nay",
            "cuon vua", "sach vua",
            "cuon kia", "sach kia",
            "do", "cuon do"   # fallback ngắn – chỉ match khi phrase khác không bắt được
        ]
        _is_coref = any(kw in _msg_norm for kw in _coref_kws)

        # Bắt ID trực tiếp từ câu nếu người dùng gõ theo format bot gợi ý: "sách ID 11002" hoặc "(ID: 11002)"
        _id_m = _re2.search(r"id:?\s*(\d+)", _msg_lower)
        if _id_m:
            book_id = _id_m.group(1)

        # ── CẢI TIẾN: Ưu tiên bắt tên sách mới từ câu lệnh trước khi fallback về context
        if not book_id and not _is_coref:
            # Pattern 1: "cập nhật [tên sách] lên N" (action trước, title sau)
            _name_m = _re2.search(
                r"(cap nhat|update|tang|giam|sua|cập nhật|tăng|giảm|sửa)\s+(ton kho\s+|stock\s+|tồn kho\s+)?(sach\s+|cuon\s+|sách\s+|cuốn\s+)?(.+?)(?:\s+(len|thanh|xuong|to|=|lên|thành|xuống)\s+\d+|$)",
                _msg_lower
            )
            if _name_m:
                _potential_name = _name_m.group(4).strip()
                # Bộ lọc: từ chối nếu potential name là:
                # 1. Chỉ số hoặc số + đơn vị (VD: "50", "50 cuốn")
                # 2. Bắt đầu bằng từ chỉ số lượng/hướng ("lên", "len", "thành", "thanh", "xuống", "xuong", "to")
                # 3. Quá ngắn (< 3 ký tự) – không phải tên sách
                _qty_keywords = ["len ", "lên ", "thanh ", "thành ", "xuong ", "xuống ", "to ", "= "]
                _is_qty = (
                    _re2.match(r"^\d+\s*(cuon|quyen|sach|cuốn|quyển|sách)?$", _potential_name)
                    or any(_potential_name.startswith(kw) for kw in _qty_keywords)
                    or len(_potential_name) < 3
                )
                if not _is_qty:
                    book_name = _potential_name

            # [FIX] Pattern 2: "TITLE cập nhật lên N" (title trước, action sau)
            # Chạy KHI book_name vẫn None (Pattern 1 không bắt được / bị reject là qty)
            # VD: "Đắc Nhân Tâm (Tái Bản 2021) cập nhật lên 20 cuốn"
            if not book_name:
                _name_m2 = _re2.search(
                    r"^(.+?)\s+(?:cap nhat|cập nhật|update|tang len|tăng lên|giam|giảm)\s+(?:ton kho\s+|tồn kho\s+)?(?:len|lên|thanh|thành|to|=)\s+\d+",
                    _msg_lower, _re2.IGNORECASE
                )
                if _name_m2:
                    _potential_name2 = _name_m2.group(1).strip()
                    # Loại bỏ các cụm từ thừa ở đầu ("sách", "cuốn", "quyển")
                    _potential_name2 = _re2.sub(r'^(sach|cuon|quyen|sách|cuốn|quyển)\s+', '', _potential_name2, flags=_re2.IGNORECASE)
                    if len(_potential_name2) > 3:
                        book_name = _potential_name2


        # Nếu vẫn không có book_id và book_name: fallback về context
        # Ưu tiên last_found_title (cuốn vừa tìm kiếm/quét) trước last_book_id (cuốn được cập nhật lần trước)
        if not book_id and not book_name:
            _ctx_title = context.get("last_found_title") or context.get("last_book_name")
            _ctx_id    = context.get("last_book_id")

            if _ctx_title:
                # Có tên sách cụ thể trong context → dùng tên để lookup (chính xác hơn ID cũ)
                book_name = _ctx_title
            elif _ctx_id:
                book_id = _ctx_id


        # ── Bước 2: Xác định số lượng mới ────────────────────────────────────
        new_qty = None
        # Tìm số cuối cùng trong message (VD: "lên 50", "thành 200", "= 100")
        _qty_m = _re2.search(r"(len|thanh|xuong|to|=|lên|thành|xuống)\s*(\d+)", _msg_lower)
        if not _qty_m:
            _qty_m = _re2.search(r"(\d+)\s*(cuon|quyen|sach)?$", _msg_lower)
        if _qty_m:
            new_qty = int(_qty_m.group(2) if _qty_m.lastindex >= 2 else _qty_m.group(1))

        # ── Bước 3: Validate và thực thi ─────────────────────────────────────
        if not book_id and book_name:
            # Thử tìm book_id theo tên
            book_info = check_book_inventory(book_name)
            if book_info:
                book_id   = book_info["book_id"]
                book_name = book_info["title"]

        if not book_id:
            return (
                "📚 Bạn muốn cập nhật tồn kho sách nào?\n"
                "Ví dụ: *\"Cập nhật tồn kho Đắc Nhân Tâm lên 50\"*\n"
                "hoặc: *\"Cập nhật sách vừa quét lên 100\"* (sau khi quét ảnh bìa)"
            ), []

        if new_qty is None:
            book_info = check_book_inventory(str(book_id))
            title = book_info["title"] if book_info else f"ID {book_id}"
            return (
                f"📦 Bạn muốn cập nhật tồn kho **{title}** lên bao nhiêu cuốn?\n"
                f"Ví dụ: *\"Cập nhật lên 50\"*"
            ), []

        if new_qty < 0:
            return "❌ Số lượng tồn kho không thể âm.", []

        # Yêu cầu xác nhận trước khi thực thi
        book_info = check_book_inventory(str(book_id))
        title = book_info["title"] if book_info else f"ID {book_id}"
        old_q = book_info["stock_quantity"] if book_info else "?"

        context["pending_confirmation"] = {
            "action": "update_inventory",
            "book_id": book_id,
            "new_qty": new_qty,
            "title": title,
            "old_qty": old_q
        }
        context["pending_btns"] = [
            {"type": "confirm_yes", "label": "Có",    "value": "Có",    "url": ""},
            {"type": "confirm_no",  "label": "Không", "value": "Không", "url": ""},
        ]

        return (
            f"⚠️ **Xác nhận cập nhật tồn kho?**\n"
            f"📚 Sách: **{title}**\n"
            f"📦 Tồn kho: **{old_q}** → **{new_qty}** cuốn"
        ), []

    # ── NHÓM S-C: HỖ TRỢ KHÁCH HÀNG ═════════════════════════

    if intent == "staff_escalated_issues":
        # Bot truy vấn DB lấy đơn có status: cancel_requested + return_requested
        issues = lookup_return_requests(limit=50)

        cancel_req = [o for o in issues if o.get("status") == "cancel_requested"]
        return_req = [o for o in issues if o.get("status") == "return_requested"]

        # ── Kiểm tra có lọc theo loại cụ thể không ──────────────────────────
        status_filter = entities.get("status_filter")  # "cancel_requested" | "return_requested" | None

        # Không có đơn nào ở loại được hỏi
        def _no_issues_msg():
            stats = get_all_time_order_stats()
            cr = int(stats.get("cancel_requested", 0))
            rr = int(stats.get("return_requested", 0))
            return (
                "✅ **Hiện không có vấn đề tồn đọng cần xử lý.**\n\n"
                "📊 **Thống kê toàn hệ thống:**\n"
                f"🚫 Yêu cầu hủy đơn: **{cr}** đơn\n"
                f"🔄 Yêu cầu trả hàng: **{rr}** đơn\n\n"
                "Bot kiểm tra các đơn cancel_requested và return_requested trong DB."
            ), ["mysql:orders"]

        def _fmt_order_line(o: dict) -> str:
            date_str = str(o.get("order_date", o.get("created_at", "?")))[:10]
            name = o.get("full_name") or o.get("username") or o.get("email", "?")
            return (
                f"  • Đơn #{o['order_id']} | "
                f"{name} | "
                f"{float(o.get('total_amount', 0)):,.0f}đ | {date_str}"
            )

        # ── Hỏi riêng về yêu cầu HỦY ────────────────────────────────────────
        if status_filter == "cancel_requested":
            if not cancel_req:
                return f"✅ **Hiện không có yêu cầu hủy đơn nào.** (0 đơn cancel_requested)\n\n🔄 Yêu cầu trả hàng: **{len(return_req)}** đơn.", ["mysql:orders"]
            lines = [f"🚫 **Yêu cầu hủy đơn ({len(cancel_req)} đơn):**"]
            for o in cancel_req:
                lines.append(_fmt_order_line(o))
            lines.append("\n📌 **Xử lý:** Xác nhận với `Duyệt hủy đơn #ID` hoặc Admin Panel.")
            return "\n".join(lines), ["mysql:orders"]

        # ── Hỏi riêng về yêu cầu TRẢ HÀNG ──────────────────────────────────
        if status_filter == "return_requested":
            if not return_req:
                return f"✅ **Hiện không có yêu cầu trả hàng nào.** (0 đơn return_requested)\n\n🚫 Yêu cầu hủy đơn: **{len(cancel_req)}** đơn.", ["mysql:orders"]
            lines = [f"🔄 **Yêu cầu trả hàng ({len(return_req)} đơn):**"]
            for o in return_req:
                lines.append(_fmt_order_line(o))
            lines.append("\n📌 **Xử lý:** Xác nhận với `Chuyển đơn #ID sang returned` hoặc Admin Panel.")
            return "\n".join(lines), ["mysql:orders"]

        # ── Hỏi tổng hợp (không lọc) ─────────────────────────────────────────
        if not issues:
            return _no_issues_msg()

        # Có đơn cần xử lý → hiển thị phân loại đầy đủ
        lines = ["🔔 **Khách hàng cần hỗ trợ — Vấn đề tồn đọng:**\n"]

        if cancel_req:
            lines.append(f"🚫 **Yêu cầu hủy đơn ({len(cancel_req)} đơn):**")
            for o in cancel_req:
                lines.append(_fmt_order_line(o))

        if return_req:
            if cancel_req:
                lines.append("")
            lines.append(f"🔄 **Yêu cầu trả hàng ({len(return_req)} đơn):**")
            for o in return_req:
                lines.append(_fmt_order_line(o))

        lines.append(
            "\n📌 **Xử lý tại:** Admin Panel → Quản lý đơn hàng → Cập nhật trạng thái"
        )
        return "\n".join(lines), ["mysql:orders"]


    if intent == "staff_customer_lookup":
        email = entities.get("email") or context.get("last_customer_email")

        # Tìm theo tên khi không có email
        if not email:
            import re as _re_cust
            # Extract tên khách sau "đạt hàng/tìm/tên khách hàng" pattern
            _cname_m = _re_cust.search(
                r'(?:khách hàng|khách|người dùng|user|customer)\s+([\w\s]+?)'
                r'(?:\s+(?:đặt|đơn|đƣ|mua|gần nhất|lịch sử|thông tin|là ai|là gì|nào)|$)',
                message, _re_cust.IGNORECASE
            )
            if _cname_m:
                _cname = _cname_m.group(1).strip()
                if len(_cname) >= 3:
                    _matched = lookup_customer_by_name(_cname)
                    if not _matched:
                        return f"Không tìm thấy khách hàng nào tên **{_cname}**.", []
                    if len(_matched) > 1:
                        # Nhiều kết quả → liệt kê để staff chọn
                        _lst = f"👥 Tìm thấy **{len(_matched)} khách hàng** tên “{_cname}”:\n\n"
                        for _u in _matched:
                            _lst += (
                                f"  • **{_u.get('full_name') or _u.get('username')}** "
                                f"(`{_u.get('email')}`) — "
                                f"{_u.get('total_orders', 0)} đơn | "
                                f"{float(_u.get('total_spent', 0)):,.0f}đ\n"
                            )
                        _lst += "\n💡 Dùng email để xem chi tiết: `Tìm khách example@gmail.com`"
                        return _lst, ["mysql:users"]
                    # Duy nhất 1 kết quả
                    user = _matched[0]
                    context["last_customer_email"] = user.get("email")
                    answer = format_customer(user)
                    _order_limit = min(int(user.get("total_orders", 3)), 5) if int(user.get("total_orders", 0)) <= 5 else 5
                    orders = lookup_customer_recent_orders(user["user_id"], limit=_order_limit)
                    if orders:
                        _title = "Đơn hàng (đầy đủ):" if int(user.get("total_orders", 0)) <= 5 else "Đơn gần nhất:"
                        answer += _format_order_list(orders, title=_title)
                    return answer, ["mysql:users", "mysql:orders"]

            return (
                "Vui lòng cung cấp **tên** hoặc **email** khách hàng, ví dụ:\n"
                "`Tìm khách Nguyen Trong Nghia` | `Tìm khách example@gmail.com`",
                []
            )

        user = lookup_customer_by_email(email)
        context["last_customer_email"] = email
        if not user:
            return f"Không tìm thấy khách hàng với email `{email}`.", []
        answer = format_customer(user)
        # Hiển thị toàn bộ đơn nếu tổng đơn ≤ 5, ngược lại lấy 5 gần nhất
        _order_limit = min(int(user.get("total_orders", 3)), 5) if int(user.get("total_orders", 0)) <= 5 else 5
        orders = lookup_customer_recent_orders(user["user_id"], limit=_order_limit)
        if orders:
            _title = "Đơn hàng (đầy đủ):" if int(user.get("total_orders", 0)) <= 5 else "Đơn gần nhất:"
            answer += _format_order_list(orders, title=_title)
        return answer, ["mysql:users", "mysql:orders"]

    if intent == "staff_complaint_resolve":
        order_id = entities.get("order_id")
        if not order_id:
            return (
                "Vui lòng cung cấp **mã đơn hàng** để ghi nhận giải quyết.\n"
                "Ví dụ: `Đã giải quyết xong khiếu nại đơn #12345`\n\n"
                "📌 **Quy trình xử lý:**\n"
                "1. Khách có quyền **đổi** sách mới trong vòng 7 ngày.\n"
                "2. Chuyển cho bộ phận **cskh** nếu cần.\n"
                "📞 Hotline CSKH: 0353260721",
                ["escalate:cskh"]
            )
        return (
            f"Đã ghi nhận giải quyết khiếu nại cho đơn **#{order_id}**.\n"
            "Nếu cần: cập nhật trạng thái đơn hoặc liên hệ CSKH: **0353260721**",
            []
        )

    # ── NHÓM S-D: TRA CỨU & THỐNG KÊ ════════════════════════

    if intent == "staff_book_lookup":
        book_id    = entities.get("book_id")
        identifier = str(book_id) if book_id else _extract_book_name(message)
        if not identifier:
            # Coreference fallback
            _msg_norm4 = message.lower()
            _coref_book4 = ["nó", "cuốn đó", "sách đó", "cuốn này", "sách này"]
            if any(kw in _msg_norm4 for kw in _coref_book4):
                identifier = str(context.get("last_book_id")) if context.get("last_book_id") else context.get("last_book_name")
        if not identifier:
            return "Vui lòng cung cấp **tên sách** hoặc **ID sách**.", []
        book = check_book_inventory(identifier)
        if book:
            context["last_book_id"]   = book.get("book_id")
            context["last_book_name"] = book.get("title", identifier)
        return format_inventory(book, identifier), ["mysql:books"]

    if intent == "staff_revenue_today":
        stats  = get_today_order_stats()
        rev    = get_period_revenue("today")
        answer = format_order_stats(stats)
        answer += f"\n\nKhách duy nhất: {rev.get('unique_customers', 0)} người"
        context["last_query_intent"] = "staff_revenue"  # set context cho revenue chain
        return answer, ["mysql:orders"]

    if intent == "staff_top_selling":
        books = get_top_selling_books(limit=10)
        # Lưu sách bán chạy #1 vào context để hỗ trợ follow-up tồn kho
        if books:
            context["last_book_id"]   = books[0].get("book_id")
            context["last_book_name"] = books[0].get("title", "")
        return format_top_books(books), ["mysql:order_details"]

    # ── S-D4: QUY TRÌNH HOÀN HÀNG (Knowledge Base policy) ────
    # -- S-D4: QUY TRINH HOAN HANG hoac CHINH SACH GIAO HANG (KB policy) ----

    if intent == "staff_return_workflow":

        # FIX-E: Phan biet giao hang vs hoan hang

        import unicodedata as _ud_sw

        _mn_sw = "".join(

            c for c in _ud_sw.normalize("NFD", message.lower().replace("\u0111","d"))

            if _ud_sw.category(c) != "Mn"

        )

        _DELIVERY_KW = [

            "giao hang", "van chuyen", "phi ship", "phi giao", "thoi gian giao",

            "bao lau giao", "may ngay giao", "don vi van chuyen", "shipping",

            "giao toi khach", "chinh sach giao", "quy dinh giao",

        ]

        _is_delivery_q = any(kw in _mn_sw for kw in _DELIVERY_KW)

        if _is_delivery_q:

            return (

                "**Ch\u00ednh s\u00e1ch giao h\u00e0ng t\u1ea1i BookStore:**\n\n"

                "\u23f0 **Th\u1eddi gian giao h\u00e0ng:**\n"

                "  \u2022 N\u1ed9i th\u00e0nh H\u00e0 N\u1ed9i / TP.HCM: **1-2 ng\u00e0y l\u00e0m vi\u1ec7c**\n"

                "  \u2022 T\u1ec9nh th\u00e0nh kh\u00e1c: **3-5 ng\u00e0y l\u00e0m vi\u1ec7c**\n"

                "  \u2022 V\u00f9ng xa/h\u1ea3i \u0111\u1ea3o: **5-7 ng\u00e0y l\u00e0m vi\u1ec7c**\n\n"

                "\U0001f4b0 **Ph\u00ed giao h\u00e0ng:**\n"

                "  \u2022 Mi\u1ec5n ph\u00ed v\u1edbi \u0111\u01a1n t\u1eeb **150,000\u0111**\n"

                "  \u2022 D\u01b0\u1edbi 150,000\u0111: ph\u00ed ship t\u1eeb **15,000\u0111-30,000\u0111** t\u00f9y khu v\u1ef1c\n\n"

                "\U0001f69a **\u0110\u01a1n v\u1ecb v\u1eadn chuy\u1ec3n:**\n"

                "  \u2022 GHTK, GHN, ViettelPost\n\n"

                "\U0001f4e6 **Quy tr\u00ecnh x\u1eed l\u00fd:**\n"

                "  \u2022 Sau khi \u0111\u1eb7t h\u00e0ng: ki\u1ec3m tra v\u00e0 \u0111\u00f3ng g\u00f3i trong **1-2h**\n"

                "  \u2022 B\u00e0n giao \u0111\u01a1n v\u1ecb v\u1eadn chuy\u1ec3n: c\u1eadp nh\u1eadt tr\u1ea1ng th\u00e1i `shipped`\n\n"

                "\U0001f4de Li\u00ean h\u1ec7 CSKH n\u1ebfu kh\u00e1ch h\u1ecfi v\u1ec1 ship: **0353260721** (8h\u201322h)"

            ), ["kb:delivery_policy"]

        return (

            "**Quy tr\u00ecnh x\u1eed l\u00fd ho\u00e0n h\u00e0ng t\u1ea1i BookStore:**\n\n"

            "**B\u01b0\u1edbc 1 \u2013 Ti\u1ebfp nh\u1eadn y\u00eau c\u1ea7u:**\n"

            "  \u2022 Staff x\u00e1c nh\u1eadn \u0111\u01a1n c\u00f3 tr\u1ea1ng th\u00e1i `return_requested`\n"

            "  \u2022 Ki\u1ec3m tra l\u00fd do kh\u00e1ch tr\u1ea3 h\u00e0ng (h\u1ecfng, sai s\u1ea3n ph\u1ea9m, \u0111\u1ed5i \u00fd)\n\n"

            "**B\u01b0\u1edbc 2 \u2013 Duy\u1ec7t ho\u00e0n h\u00e0ng:**\n"

            "  \u2022 N\u1ebfu h\u1ee3p l\u1ec7 (\u22647 ng\u00e0y, c\u00f2n nguy\u00ean v\u1eb9n) \u2192 Approve\n"

            "  \u2022 Chuy\u1ec3n tr\u1ea1ng th\u00e1i: `return_requested` \u2192 `returned`\n\n"

            "**B\u01b0\u1edbc 3 \u2013 Ho\u00e0n ti\u1ec1n:**\n"

            "  \u2022 COD: Chuy\u1ec3n kho\u1ea3n l\u1ea1i cho kh\u00e1ch trong 3-5 ng\u00e0y l\u00e0m vi\u1ec7c\n"

            "  \u2022 Online: Ho\u00e0n v\u1ec1 t\u00e0i kho\u1ea3n g\u1ed1c trong 5-7 ng\u00e0y\n"

            "  \u2022 Th\u1ef1c hi\u1ec7n qua: **Admin Panel \u2192 \u0110\u01a1n h\u00e0ng \u2192 Ho\u00e0n ti\u1ec1n**\n\n"

            "**B\u01b0\u1edbc 4 \u2013 Nh\u1eadp l\u1ea1i kho:**\n"

            "  \u2022 S\u00e1ch tr\u1ea3 v\u1ec1 c\u00f2n t\u1ed1t \u2192 C\u1eadp nh\u1eadt `stock_quantity + quantity`\n"

            "  \u2022 S\u00e1ch b\u1ecb h\u1ecfng \u2192 Ghi nh\u1eadn v\u00e0o b\u00e1o c\u00e1o h\u00e0ng h\u1ecfng\n\n"

            "\U0001f4de Li\u00ean h\u1ec7 CSKH n\u1ebfu kh\u00e1ch khi\u1ebfu n\u1ea1i: **0353260721** (8h\u201322h)"

        ), ["kb:return_policy"]



    # ── S-D5: THỐNG KÊ NGƯỜI DÙNG ────────────────────────────
    if intent == "staff_user_stats":
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN role='customer' THEN 1 ELSE 0 END) as customers,
                    SUM(CASE WHEN role='staff' THEN 1 ELSE 0 END) as staff_count,
                    SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active_count
                FROM users
            """)
            row = cur.fetchone() or {}
            cur.close(); conn.close()
            return (
                f"**Thống kê tài khoản hệ thống:**\n\n"
                f"Tổng tài khoản: **{row.get('total',0)}**\n"
                f"  Khách hàng: **{row.get('customers',0)}**\n"
                f"  Nhân viên (staff): **{row.get('staff_count',0)}**\n"
                f"  Active: **{row.get('active_count',0)}**\n\n"
                f"Xem chi tiết: **Admin Panel → Quản lý Users**"
            ), ["mysql:users"]
        except Exception as e:
            return f"Không thể truy vấn thống kê users: {e}", []

    # ── CHITCHAT / HELP / OUT OF SCOPE ────────────────────────
    if intent == "staff_chitchat":
        return STAFF_HELP_MSG, []

    if intent == "staff_out_of_scope":
        return (
            "🚫 **Yêu cầu không hợp lệ**\n"
            "Tính năng này nằm ngoài phạm vi hỗ trợ của Staff AI (ví dụ: thao tác trên tài khoản khách hàng, cài đặt hệ thống). "
            "Vui lòng thực hiện trên Admin Panel hoặc liên hệ quản trị viên.",
            []
        )

    return (
        "Tôi không hiểu yêu cầu này.\n"
        "Gõ `help` để xem danh sách các lệnh hỗ trợ.",
        []
    )



# ── Confirmation Handler ──────────────────────────────────────
def _handle_staff_confirmation(intent: str, context: dict) -> str:
    pending = context.get("pending_confirmation", {})
    action  = pending.get("action")

    if intent == "confirmation_yes":
        if action == "update_order_status":
            order_id      = pending.get("order_id")
            target_status = pending.get("target_status")
            old_status    = pending.get("current_status", "?")
            context.pop("pending_confirmation", None)
            context.pop("pending_btns", None)          # ← xóa buttons thừa
            # ── Thực thi DB update thật sự ──
            result = update_order_status(int(order_id), target_status)
            if result.get("success"):
                return (
                    f"✅ **Đã cập nhật đơn #{order_id} thành công!**\n"
                    f"Trạng thái: `{old_status}` → `{target_status}`"
                )
            else:
                return (
                    f"❌ **Cập nhật đơn #{order_id} thất bại.**\n"
                    f"Lý do: {result.get('message', 'Lỗi không xác định')}\n"
                    f"Vui lòng thử lại hoặc cập nhật thủ công qua **Admin Panel**."
                )
        elif action == "update_inventory":
            book_id = pending.get("book_id")
            new_qty = pending.get("new_qty")
            title   = pending.get("title")
            old_q   = pending.get("old_qty")
            
            context.pop("pending_confirmation", None)
            context.pop("pending_btns", None)
            
            result = update_book_inventory(int(book_id), new_qty)
            if result.get("success"):
                # Cập nhật context để tránh leakage sang câu sau
                context["last_book_id"]   = int(book_id)
                context["last_book_name"] = title
                
                return (
                    f"✅ **Đã cập nhật tồn kho thành công!**\n"
                    f"📚 Sách: **{title}**\n"
                    f"📦 Tồn kho: **{old_q}** → **{new_qty}** cuốn"
                )
            else:
                return (
                    f"❌ **Cập nhật tồn kho thất bại.**\n"
                    f"Lý do: {result.get('message', 'Lỗi không xác định')}\n"
                    f"Vui lòng thử lại hoặc cập nhật thủ công qua **Admin Panel → Quản lý kho**."
                )
        context.pop("pending_confirmation", None)
        context.pop("pending_btns", None)              # ← xóa buttons thừa
        return "Đã xác nhận thao tác."

    elif intent == "confirmation_no":
        context.pop("pending_confirmation", None)
        context.pop("pending_btns", None)              # ← xóa buttons thừa
        return "Đã hủy. Không có thay đổi nào được thực hiện."

    return "Bạn vui lòng xác nhận: **Có** (tiếp tục) hoặc **Không** (hủy bỏ)?"


# ── Helper ────────────────────────────────────────────────────
def _extract_book_name(message: str) -> str:
    """Trích tên sách từ message (fallback đơn giản)."""
    import re
    quoted = re.search(r'[""](.+?)[""]', message)
    if quoted:
        return quoted.group(1)
    after = re.search(
        r"(?:sách|cuốn|quyen|quyển|tồn kho|kiểm tra|check)(?:\s+(?:số lượng|tồn kho))?(?:\s+(?:sách|cuốn|quyen|quyển))?\s+(.+?)(?:\s+(?:còn|có|là|như|bao nhiêu)|\s*$)",
        message, re.IGNORECASE
    )
    if after:
        name = after.group(1).strip()
        if name.lower() in ["sách", "sach", "cuốn", "cuon", "quyển", "quyen", "truyện", "truyen"]:
            return ""
        return name
    return ""
