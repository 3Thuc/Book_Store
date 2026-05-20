"""
admin_dialog_manager.py – Dialog manager cho Admin chatbot.

Xử lý 21 intents admin với confirm flow cực kỳ nghiêm ngặt
cho các actions ảnh hưởng đến user/tài khoản.
"""
from chatbot_app.nlu.admin_intent_classifier import AdminNLUResult
from chatbot_app.retrieval.admin_agents import (
    get_dashboard_overview, get_revenue_report,
    lookup_user_by_email_or_id, get_user_statistics, get_all_staff_accounts,
    get_promotions, check_voucher_code, get_expiring_promotions,
    check_system_health,
    get_low_rating_books, get_category_stats, get_top_books_admin,
    get_top_books_admin as get_top_books,
    admin_update_user_status, admin_update_user_role,
)
from chatbot_app.retrieval.staff_agents import (
    lookup_return_requests,
    get_top_selling_books,
    check_book_inventory,
    search_book_inventory,
    update_book_inventory,
    # Đơn hàng
    lookup_order_by_id, lookup_orders_by_status,
    lookup_pending_orders, update_order_status,
    get_all_time_order_stats, get_today_order_stats, get_period_revenue,
    # Tồn kho
    get_low_stock_books, get_out_of_stock_books,
    count_out_of_stock, count_low_stock,
    # Khách hàng
    lookup_customer_by_email, lookup_customer_by_name, lookup_customer_recent_orders,
)
from chatbot_app.generation.dialog_utils import is_ocr_message, extract_ocr_book_name

# ── ADMIN HELP MESSAGE ────────────────────────────────────────
ADMIN_HELP_MSG = """Xin chào **Admin**! Đây là **Admin Assistant**.

**Dashboard & Báo cáo:**
- `Dashboard` – Tổng quan hệ thống
- `Doanh thu tuần này` / `tháng này` – Revenue report
- `Thống kê đơn hàng` – Order statistics
- `Top sách bán chạy` – Bestseller admin

**Quản lý User:**
- `Tìm user example@gmail.com` – Tra cứu tài khoản
- `Thống kê người dùng` – User stats
- `Danh sách staff` – Tất cả tài khoản staff
- `Đổi role user 42 thành staff` – Thay đổi quyền *(yêu cầu xác nhận)*
- `Khóa tài khoản user 42` – Lock account *(yêu cầu xác nhận)*

**Quản lý Sách:**
- `Sách rating thấp` – Review sách dưới 3 sao
- `Thống kê danh mục` – Category analytics
- `Top sách doanh thu cao` – Bestseller full report

**Khuyến mãi:**
- `Danh sách voucher` – Active promotions
- `Kiểm tra mã SALE30` – Validate voucher
- `Promotion sắp hết hạn` – Expiring alerts

**Hệ thống:**
- `Kiểm tra service` – System health check
- `Khiếu nại chờ xử lý` – Escalated issues"""


# ── FORMATTERS ────────────────────────────────────────────────

def fmt_dashboard(d: dict) -> str:
    # Revenue trend arrow (text, not emoji)
    rev_today = float(d.get('revenue_today', 0))
    rev_yest  = float(d.get('revenue_yesterday', 0))
    if rev_yest > 0:
        diff_pct = round((rev_today - rev_yest) / rev_yest * 100, 1)
        arrow    = "↑" if rev_today >= rev_yest else "↓"
        rev_note = f" ({arrow}{abs(diff_pct)}% so hôm qua: {rev_yest:,.0f}đ)"
    elif rev_today > 0:
        rev_note = " (hôm qua: 0đ)"
    else:
        rev_note = f" (hôm qua: {rev_yest:,.0f}đ)" if rev_yest > 0 else ""

    # Action items (chỉ hiện khi có việc cần làm)
    action_lines = []
    ret_req = int(d.get('return_requests', 0))
    pend24  = int(d.get('pending_over_24h', 0))
    oos     = int(d.get('out_of_stock_active', 0))
    if ret_req: action_lines.append(f"• Trả hàng chờ duyệt: **{ret_req}**")
    if pend24:  action_lines.append(f"• Đơn pending >24h: **{pend24}**")
    if oos:     action_lines.append(f"• Hết hàng nhưng đang bán: **{oos} sách**")

    action_block = ""
    if action_lines:
        action_block = "\n⚡ **CẦN XỬ LÝ NGAY:**\n" + "\n".join(action_lines) + "\n"

    low_stock = int(d.get('low_stock_count', 0))
    low_note  = f" | Sắp hết (≤5): {low_stock}" if low_stock > 0 else ""

    return (
        f"📊 **DASHBOARD TỔNG QUAN** — {d.get('date','?')}\n"
        f"{action_block}\n"
        f"**1. HIỆU SUẤT HÔM NAY**\n"
        f"• Đơn mới: **{d.get('orders_today',0)}**\n"
        f"• Doanh thu: **{rev_today:,.0f}đ**{rev_note}\n"
        f"• Người dùng mới: **{d.get('new_users_today',0)}**\n\n"
        f"**2. THỐNG KÊ TỔNG THỂ**\n"
        f"• Doanh thu (all-time): **{float(d.get('total_revenue_all_time',0)):,.0f}đ**\n"
        f"• Đơn đã bán: **{d.get('total_orders_all_time',0):,}** (Hợp lệ: {d.get('active_orders_all_time',0):,})\n"
        f"• Kho sách: **{d.get('total_books',0):,}**{low_note}\n"
        f"• Khách hàng: **{d.get('active_customers',0)}** (Mới tháng này: {d.get('new_users_this_month',0)})\n"
        f"• Đơn đang xử lý: **{d.get('pending_orders',0)}**"
    )


def fmt_revenue(r: dict) -> str:
    _PERIOD_VI = {
        "week":  "Tuần này",  "this_week":  "Tuần này",
        "month": "Tháng này", "this_month": "Tháng này",
        "today": "Hôm nay",   "day":        "Hôm nay",
        "year":  "Năm nay",   "quarter":    "Quý này",
        "7days": "7 ngày qua", "30days":    "30 ngày qua",
    }
    label     = r.get("period_label") or r.get("period", "")
    label     = _PERIOD_VI.get(str(label).lower(), label)
    total     = int(r.get('total_orders') or 0)
    cancelled = int(r.get('cancelled_count') or 0)
    returned  = int(r.get('returned_count') or 0)
    cancel_rate = r.get('cancel_rate', round(cancelled / total * 100, 1) if total > 0 else 0.0)

    top_days = r.get("top_days", [])
    top_str = ""
    if top_days:
        top_str = "\n\n🗓️ **Top ngày doanh thu cao:**"
        for d in top_days[:3]:
            top_str += f"\n\u2022 {d['day']}: **{float(d['revenue']):,.0f}đ** ({d['orders']} đơn)"

    return (
        f"📊 **Doanh thu {label}**\n"
        f"📅 {r.get('from_date')} → {r.get('to_date')}\n\n"
        f"📦 Tổng đơn: **{total}** | Khách: {r.get('unique_customers',0)}\n"
        f"💰 Doanh thu: **{float(r.get('gross_revenue',0)):,.0f}đ**\n"
        f"✅ Xác nhận: **{float(r.get('confirmed_revenue',0)):,.0f}đ**\n"
        f"🏷️ Giảm giá: {float(r.get('total_discounts',0)):,.0f}đ\n"
        f"🚫 Hủy: {cancelled} ({cancel_rate:.1f}%) | Hoàn: {returned}\n"
        f"💳 COD: {int(r.get('cod_count') or 0)} | Online: {int(r.get('online_payment_count') or 0)}"
        f"{top_str}"
    )


def fmt_user(u: dict) -> str:
    if not u:
        return "Không tìm thấy người dùng."
    return (
        f"**User #{u['user_id']}**\n"
        f"{u.get('full_name') or u.get('username','?')} ({u.get('username','')})\n"
        f"{u.get('email','?')} | {u.get('phone','N/A')}\n"
        f"Role: `{u.get('role','?')}` | Status: `{u.get('status','?')}`\n"
        f"Tham gia: {str(u.get('created_at','?'))[:10]}\n"
        f"Đơn hàng: **{u.get('total_orders',0)}** | "
        f"Tổng chi: **{float(u.get('total_spent',0)):,.0f}đ**"
    )


def fmt_user_stats(s: dict) -> str:
    locked = int(s.get('locked_users', 0))
    locked_warn = f"⚠️ {locked}" if locked > 0 else str(locked)
    return (
        f"👥 **Thống kê người dùng**\n\n"
        f"• Tổng: **{s.get('total_users',0)}** | Active: {s.get('active_users',0)} | Khóa: {locked_warn}\n"
        f"• KH: {s.get('customers',0)} | Staff: {s.get('staff_count',0)} | Admin: {s.get('admin_count',0)}\n\n"
        f"🗓️ Mới hôm nay: **{s.get('new_today',0)}** | Mới tháng này: **{s.get('new_this_month',0)}**"
    )


def fmt_promotions(promos: list[dict]) -> str:
    if not promos:
        return "ℹ️ Không có khuyến mãi nào đang active."
    lines = [f"🎟️ **Khuyến mãi đang hoạt động** ({len(promos)} mã):\n"]
    for p in promos:
        days     = p.get("days_remaining", 0)
        exp_warn = " ⚠️ [SẮP HẾT HẠN]" if days <= 7 else ""
        lines.append(
            f"• `{p['code']}` — Giảm **{p.get('discount_percent',0):.0f}%**\n"
            f"  HSD: {str(p.get('end_date','?'))}{exp_warn} (còn {days} ngày)\n"
        )
    return "\n".join(lines).strip()


def fmt_voucher_check(v: dict, code: str) -> str:
    if not v:
        return f"Không tìm thấy mã `{code}` trong hệ thống."
    validity   = v.get("validity", "unknown")
    status_map = {
        "valid":       "Hợp lệ",
        "expired":     "Đã hết hạn",
        "not_started": "Chưa có hiệu lực",
        "inactive":    "Đã vô hiệu hóa",
        "deleted":     "Đã xóa",
    }
    status_label = status_map.get(validity, validity)
    return (
        f"**Mã voucher: `{v['code']}`**\n"
        f"Trạng thái: {status_label}\n"
        f"Giảm: **{v.get('discount_percent',0):.0f}%**\n"
        f"Hiệu lực: {v.get('start_date','?')} → {v.get('end_date','?')}\n"
        f"Còn: **{v.get('days_remaining',0)} ngày**\n"
        f"Đã dùng: **{v.get('times_used',0)} lần**"
    )


def fmt_system_health(health: dict) -> str:
    lines = ["**System Health Check**"]
    for service, info in health.items():
        status = info.get("status", "unknown")
        lines.append(f"  {service.upper()}: {status}")
        if "error" in info and status == "❌ offline":
            lines.append(f"    Chi tiết lỗi: {info['error']}")
        if "http" in info and status == "⚠️ degraded":
            lines.append(f"    HTTP Code: {info['http']}")
        if "models" in info:
            if isinstance(info['models'], list):
                lines.append(f"    Models: {', '.join(info['models'])}")
            else:
                lines.append(f"    Models: {info['models']}")
        if "cluster_status" in info:
            lines.append(f"    Cluster: {info['cluster_status']}")
    return "\n".join(lines)


def fmt_low_rating_books(books: list[dict]) -> str:
    if not books:
        return "✅ Không có sách nào có rating thấp đáng lo ngại."
    lines = [f"⭐ **Sách có rating thấp** (cần review):"]
    for i, b in enumerate(books[:5], 1):
        lines.append(
            f"{i}. **{b['title']}** (ID: {b['book_id']})\n"
            f"   ★ {b.get('avg_rating',0):.1f}/5 — {b.get('rating_count',0)} đánh giá | "
            f"Status: `{b.get('status','?')}`"
        )
    lines.append("\n💡 Xét duyệt hoặc tạm ẩn sách nếu rating < 3.0 ảnh hưởng trải nghiệm khách hàng.")
    return "\n".join(lines)


def fmt_top_books_admin(books: list[dict]) -> str:
    if not books:
        return "Chưa có dữ liệu bán hàng."
    lines = ["**Top sách – Admin Report:**"]
    for i, b in enumerate(books, 1):
        lines.append(
            f"{i}. **{b['title']}** (ID: {b['book_id']})\n"
            f"   Bán: {b.get('total_sold',0)} | "
            f"{float(b.get('total_revenue',0)):,.0f}đ | "
            f"Rating: {b.get('avg_rating',0):.1f} | "
            f"Kho: {b.get('stock_quantity',0)}"
        )
    return "\n".join(lines)


# =============================================================================
# MAIN PROCESS FUNCTION
# =============================================================================

async def process_admin(
    message:    str,
    nlu_result: AdminNLUResult,
    user_id:    int | None,
    context:    dict,
) -> tuple[str, list[str], list[dict]]:
    """Xử lý message admin và trả về (answer, sources, navigate_buttons)."""
    intent   = nlu_result.intent
    entities = nlu_result.entities

    def _with_confirm_buttons(answer: str, sources: list) -> tuple[str, list, list]:
        """Wrap answer + buttons Xác nhận/Hủy khi có pending_confirmation."""
        btns = [
            {"label": "Có",   "url": "", "type": "confirm_yes"},
            {"label": "Không", "url": "", "type": "confirm_no"},
        ]
        return answer, sources, btns

    # ── [MSG-OVERRIDE] Bypass stale NLU – detect quarterly/retention từ message ──
    # Cần thiết vì uvicorn --reload đôi khi không reload admin_intent_classifier.py
    # kịp thời, dẫn đến NLU trả admin_out_of_scope cho "Quý 2", "giữ chân KH", v.v.
    import re as _re_ovr, unicodedata as _ud_ovr
    _mn_ovr = "".join(
        c for c in _ud_ovr.normalize("NFD", message.lower().replace("đ","d"))
        if _ud_ovr.category(c) != "Mn"
    )
    # Override 1: Quarterly/monthly revenue ("quý 1", "q2", "Q3 doanh thu"...)
    _Q_PAT = _re_ovr.compile(r'(?:quy|q)\s*[1-4]', _re_ovr.IGNORECASE)
    if _Q_PAT.search(_mn_ovr) and intent not in ("admin_revenue_stats", "admin_revenue_report"):
        intent = "admin_revenue_stats"

    # Override 1b: Explicit revenue/order stats with time queries ("Doanh thu hôm qua", "Thống kê đơn hàng tuần này")
    if intent not in ("admin_revenue_stats", "admin_order_stats", "admin_revenue_report", "admin_revenue_ytd"):
        _time_kws = ["hom qua", "hom nay", "tuan nay", "thang nay", "thang truoc", "nam nay", "quy nay", "dau nam"]
        if any(kw in _mn_ovr for kw in _time_kws):
            if "doanh thu" in _mn_ovr:
                intent = "admin_revenue_stats"
            elif "thong ke" in _mn_ovr or "don hang" in _mn_ovr:
                intent = "admin_order_stats"

    # Override 2: Retention/loyalty strategy ("giữ chân khách" + "chiến lược")
    _RETENTION_KW = ["giu chan","loyalty","trung thanh","retention","giu khach","tru khach"]
    _STRATEGY_KW  = ["chien luoc","ke hoach","giai phap","can lam","nen lam","de xuat","lam gi","cach nao"]
    if (any(kw in _mn_ovr for kw in _RETENTION_KW)
            and any(kw in _mn_ovr for kw in _STRATEGY_KW)
            and intent not in ("admin_marketing_advice",)):
        intent = "admin_marketing_advice"

    # Override 3: "Doanh thu tháng X/YYYY" → admin_revenue_stats + inject _month_override
    import re as _re_mo_ovr
    _mo_match = _re_mo_ovr.search(r'th[aá]ng\s+(\d{1,2})[/\-](\d{4})', message, _re_mo_ovr.IGNORECASE)
    if _mo_match:
        if intent not in ("admin_revenue_stats", "admin_revenue_report"):
            intent = "admin_revenue_stats"
        entities = dict(entities)  # copy để không mutate NLU result
        entities["_month_override"] = f"month:{_mo_match.group(1)}/{_mo_match.group(2)}"

    # Override 3b: Short time queries ("tháng này", "ngày hôm qua") when context is revenue or orders
    if len(message.strip()) <= 15 and context.get("last_query_intent") in ("admin_revenue", "admin_order_stats"):
        if any(kw in _mn_ovr for kw in ["thang nay", "tuan nay", "hom qua", "nam nay", "hom nay", "thang truoc"]):
            intent = "admin_revenue_stats" if context.get("last_query_intent") == "admin_revenue" else "admin_order_stats"

    # Override 4: "Tỷ lệ hủy / hoàn trả" → admin_order_stats
    _CANCEL_RATE_KW = ["ty le huy", "ty le hoan", "phan tram huy", "hoan tra bao nhieu", "cancel rate"]
    if any(kw in _mn_ovr for kw in _CANCEL_RATE_KW) and intent not in ("admin_order_stats", "admin_revenue_stats"):
        intent = "admin_order_stats"

    # Override 5: Revenue comparison (so sánh doanh thu theo kỳ)
    if any(kw in _mn_ovr for kw in ["so sanh", "so voi"]):
        if any(kw in _mn_ovr for kw in ["doanh thu", "bao nhieu", "tong", "thong ke"]) or context.get("last_query_intent") == "admin_revenue":
            intent = "admin_revenue_compare"

    # Override 6: Khuyến mãi đang chạy
    if any(kw in _mn_ovr for kw in ["khuyen mai dang chay", "voucher dang chay", "khuyen mai dang hoat dong", "ma giam gia dang chay"]):
        intent = "admin_promotion_list"

    # ── end MSG-OVERRIDE ──────────────────────────────────────────────────────────


    # ── [OCR GUARD] Nếu message là OCR upload, bỏ qua NLU intent sai ─────────────
    # VD: [Upload: 10-sai-lam-lon-nhat-cua-nguoi-lanh-dao.jpg] → NLU có thể classify sai
    # thành intent khác do keyword trong filename. OCR pipeline đã xử lý book lookup
    # trước khi gửi vào đây, context.last_found_title đã có sẵn.
    if is_ocr_message(message):
        _ocr_book = (
            context.get("last_found_title")
            or context.get("last_book_name")
            or extract_ocr_book_name(message)  # fallback: extract từ filename
        )
        if _ocr_book:
            try:
                book = check_book_inventory(_ocr_book)
                if book:
                    context["last_book_name"] = book.get("title", _ocr_book)
                    context["last_looked_up_book"] = {
                        "book_id": book.get("book_id"),
                        "title":   book.get("title", _ocr_book),
                    }
                    _stock = book.get('stock_quantity', 0)
                    _stock_lbl = "Còn hàng" if _stock > 10 else ("Sắp hết" if _stock > 0 else "Hết hàng")
                    lines = [
                        f"**{book.get('title', _ocr_book)}** (ID: {book.get('book_id','')})",
                        f"Tác giả: {book.get('author_name', book.get('author','?'))}",
                        f"Giá: {float(book.get('price',0)):,.0f}đ",
                        f"Tồn kho: **{_stock} cuốn** ({_stock_lbl})",
                        f"Trạng thái: {book.get('status','?')}",
                    ]
                    return "\n".join(lines), ["mysql:books"]
                else:
                    return (
                        f"❌ Không tìm thấy sách **\"{_ocr_book}\"** trong kị sản phẩm.\n"
                        f"💡 Kiểm tra trực tiếp: **Admin Panel → Quản lý sách**"
                    ), []
            except Exception:
                pass  # fallback to normal flow

    # ── [FIX BUG-10] Keyword-based handlers cho admin queries không rõ intent ──
    import unicodedata as _ud_a
    _mn_a = "".join(c for c in _ud_a.normalize("NFD", message.lower().replace("đ","d")) if _ud_a.category(c)!="Mn")

    # ── HARD BLOCK #1: password / credential request (PHẢI ĐẶT ĐẦU TIÊN) ─────
    # Chặn trước NẾU message chứa keyword nhạy cảm, bất kể intent là gì
    _SENSITIVE_KWS = ["mat khau", "password", "passwd", "ma pin", "token", "secret_key",
                      "api_key", "access_key", "encryption_key", "private_key"]
    if any(kw in _mn_a for kw in _SENSITIVE_KWS):
        return (
            "⛔ **Thông tin bảo mật (mật khẩu, token, key) được mã hóa và không thể truy cập qua chatbot.**\n\n"
            "Để reset mật khẩu người dùng, dùng:\n"
            "→ **Admin Panel → Quản lý Users → [User ID] → Reset Password**\n\n"
            "Chatbot không bao giờ hiển thị mật khẩu hoặc thông tin xác thực."
        ), []

    # Overstocked / ton kho nhieu nhat
    if any(kw in _mn_a for kw in ["ton kho nhieu nhat","hang thua","qua nhieu hang","du hang nhat"]):
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT b.book_id, b.title, b.stock_quantity, b.price
                FROM books b WHERE b.status='active'
                ORDER BY b.stock_quantity DESC LIMIT 10
            """)
            books = cur.fetchall()
            cur.close(); conn.close()
            if books:
                lines = ["**Sách tồn kho nhiều nhất:**"]
                for i, b in enumerate(books, 1):
                    lines.append(f"{i}. **{b['title']}** – Kho: **{b['stock_quantity']} cuốn** | {float(b['price']):,.0f}đ")
                return "\n".join(lines), ["mysql:books"]
        except Exception:
            pass

    # Slow-moving: sach nao khong ai mua X ngay / san pham khong co don hang
    _SLOW_MOVING_KWS = [
        "sach nao khong", "khong ai mua", "chua ai mua", "ban cham", "it ban nhat", "sach e",
        "khong co don hang", "khong co don", "san pham khong co don", "khong ban duoc",
        "chua co don", "chua ban duoc", "30 ngay", "trong 30", "thang qua khong"
    ]
    # FIX-G: Loai tru neu user hoi ve chien luoc tang doanh so (neu co tu tang/chien luoc/lam gi)
    # -> nhung cau nay nen route sang admin_marketing_advice, khong nen tra danh sach sach e
    _MARKETING_OVERRIDE_KW = ["tang doanh so", "chien luoc", "lam gi de", "can lam gi", "nen lam gi",
                               "giai phap", "cach nao", "tang ban", "day manh", "kich cau"]
    _is_marketing_q = any(kw in _mn_a for kw in _MARKETING_OVERRIDE_KW)
    if any(kw in _mn_a for kw in _SLOW_MOVING_KWS) and not _is_marketing_q:
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            import re
            m = re.search(r"(\d+)\s*(ngày|ngay)", message, re.IGNORECASE)
            days = int(m.group(1)) if m else 30
            
            cur.execute("""
                SELECT b.book_id, b.title, b.stock_quantity, b.price,
                       COALESCE(s.total_sold,0) as total_sold
                FROM books b
                LEFT JOIN (
                    SELECT od.book_id, SUM(od.quantity) as total_sold
                    FROM order_details od
                    JOIN orders o ON od.order_id=o.order_id
                    WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                    GROUP BY od.book_id
                ) s ON b.book_id=s.book_id
                WHERE b.status='active' AND b.stock_quantity > 0
                  AND COALESCE(s.total_sold,0)=0
                ORDER BY b.stock_quantity DESC LIMIT 10
            """, (days,))
            slow = cur.fetchall()
            cur.close(); conn.close()
            if slow:
                lines = [f"**📦 Sách chưa có đơn hàng trong {days} ngày qua (có tồn kho):**"]
                for b in slow:
                    lines.append(f"• **{b['title']}** – Kho: {b['stock_quantity']} cuốn | {float(b['price']):,.0f}đ")
                lines.append("\n💡 Gợi ý: Cân nhắc chạy khuyến mãi hoặc điều chỉnh giá để kích cầu.")
                return "\n".join(lines), ["mysql:books"]
            return f"🎉 Tuyệt vời! Không có sách nào chưa bán được trong {days} ngày qua!", []
        except Exception:
            pass

    # Export report request → hướng dẫn dùng admin panel
    _EXPORT_KWS = ["xuat bao cao", "xuat excel", "download bao cao", "export bao cao",
                   "tai bao cao", "lay file bao cao", "bao cao excel", "xuat log", "xuat file", "export file"]
    if any(kw in _mn_a for kw in _EXPORT_KWS):
        if "log" in _mn_a:
            return (
                "⛔ **Không thể xuất file log qua chatbot.**\n\n"
                "Log của hệ thống Chatbot/Backend được lưu trữ trực tiếp trên Server.\n"
                "Để xem log, vui lòng truy cập vào thư mục mã nguồn hoặc kiểm tra qua Docker console."
            ), []
        
        return (
            "⛔ **Chức năng xuất file chưa được hỗ trợ.**\n\n"
            "Hiện tại hệ thống BookStore chưa hỗ trợ tính năng xuất file (Excel/CSV/PDF) trực tiếp qua Chatbot hay Admin Panel.\n"
            "Chatbot chỉ hỗ trợ xem thống kê tóm tắt và tra cứu dữ liệu nhanh trực tiếp trên khung chat."
        ), []


    # Active users most
    if any(kw in _mn_a for kw in ["nguoi dung nao hoat dong","user nao hoat dong","nguoi mua nhieu nhat","khach hang nao mua nhieu","top user","top nguoi dung"]):
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT u.user_id, u.username, u.full_name, u.email,
                       COUNT(DISTINCT o.order_id) as total_orders,
                       COALESCE(SUM(od.total_price), 0) as total_spent
                FROM users u
                JOIN orders o ON u.user_id=o.user_id
                LEFT JOIN order_details od ON o.order_id = od.order_id
                WHERE o.status IN ('delivered','processing','shipped')
                GROUP BY u.user_id
                ORDER BY total_spent DESC LIMIT 10
            """)
            users = cur.fetchall()
            cur.close(); conn.close()
            if users:
                lines = ["**Top người dùng hoạt động nhất:**"]
                for i, u in enumerate(users, 1):
                    lines.append(f"{i}. **{u.get('full_name') or u.get('username','?')}** ({u['email']}) – {u['total_orders']} đơn | {float(u['total_spent']):,.0f}đ")
                return "\n".join(lines), ["mysql:users"]
        except Exception:
            pass

    # Total products in system
    if any(kw in _mn_a for kw in ["bao nhieu san pham","tong so san pham","tong san pham","bao nhieu sach","tong so sach","he thong co bao nhieu sach"]):
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("SELECT COUNT(*) as total, SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active FROM books")
            r = cur.fetchone()
            cur.close(); conn.close()
            if r:
                return (f"📚 Hệ thống hiện có **{r['total']} sách** trong database.\n"
                        f"  • Đang bán (active): **{r['active']} cuốn**\n"
                        f"  • Ẩn/ngừng bán: **{int(r['total'])-int(r['active'])} cuốn**"), ["mysql:books"]
        except Exception:
            pass

    # ── 0. Confirmation pending ──────────────────────────────
    if context.get("pending_confirmation"):
        # Bất kỳ intent nào không phải yes/no → clear pending và tiếp tục với intent mới
        if intent not in ["confirmation_yes", "confirmation_no"]:
            context.pop("pending_confirmation", None)
        else:
            _conf_answer = _handle_admin_confirmation(intent, context)
            # Nếu đã xử lý confirm_yes/no → không cần buttons nữa
            _still_pending = bool(context.get("pending_confirmation"))
            _btns = [
                {"label": "Có",   "url": "", "type": "confirm_yes"},
                {"label": "Không", "url": "", "type": "confirm_no"},
            ] if _still_pending else []
            return _conf_answer, ["confirm_flow"], _btns
    # ── NHÓM A-A: DASHBOARD & THỐNG KÊ ═══════════════════════

    if intent == "admin_dashboard_summary":
        data = get_dashboard_overview()
        return fmt_dashboard(data), ["mysql:stats"]

    if intent == "admin_revenue_compare":
        from chatbot_app.retrieval.staff_agents import get_period_revenue as _gpr_compare
        import re as _re_cmp, calendar as _cal_cmp
        from datetime import date as _d_cmp

        _mn_cmp = "".join(c for c in __import__("unicodedata").normalize("NFD", message.lower().replace("đ","d")) if __import__("unicodedata").category(c)!="Mn")
        _COMPARE_KW = ["so sanh", "so voi", "va", "voi"]
        _is_cmp = any(kw in _mn_cmp for kw in _COMPARE_KW)

        def _fmt_compare(label1, data1, label2, data2):
            r1 = float(data1.get("total_revenue", 0))
            r2 = float(data2.get("total_revenue", 0))
            o1 = int(data1.get("total_orders", 0))
            o2 = int(data2.get("total_orders", 0))
            r_diff = r2 - r1
            o_diff = o2 - o1
            r_pct = round(abs(r_diff) / r1 * 100, 1) if r1 > 0 else (100.0 if r2 > 0 else 0.0)
            o_pct = round(abs(o_diff) / o1 * 100, 1) if o1 > 0 else (100.0 if o2 > 0 else 0.0)
            r_icon = "📈 Tăng" if r_diff >= 0 else "📉 Giảm"
            o_icon = "📈 Tăng" if o_diff >= 0 else "📉 Giảm"
            return (
                f"⚖️ **So sánh doanh thu: {label1} vs {label2}**\n\n"
                f"📅 **{label1}:**\n"
                f"📦 Đơn hàng: **{o1}** đơn | 💰 Doanh thu: **{r1:,.0f}đ**\n\n"
                f"📅 **{label2}:**\n"
                f"📦 Đơn hàng: **{o2}** đơn | 💰 Doanh thu: **{r2:,.0f}đ**\n\n"
                f"📈 **Phân tích chênh lệch:**\n"
                f"• Số đơn hàng: {o_icon} **{o_pct}%** ({o_diff:+d} đơn)\n"
                f"• Doanh thu: {r_icon} **{r_pct}%** ({r_diff:+,.0f}đ)"
            )

        # Priority 1: Quý cụ thể – phải bắt trước, tránh regex năm can thiệp
        _TWO_Q = _re_cmp.findall(r'(?:quy|q)\s*([1-4])(?:/|-)(\d{4})', _mn_cmp)
        if len(_TWO_Q) >= 2 and _is_cmp:
            q1n, y1 = int(_TWO_Q[0][0]), int(_TWO_Q[0][1])
            q2n, y2 = int(_TWO_Q[1][0]), int(_TWO_Q[1][1])
            s1 = str(_d_cmp(y1, (q1n-1)*3+1, 1))
            e1 = str(_d_cmp(y1, (q1n-1)*3+3, _cal_cmp.monthrange(y1, (q1n-1)*3+3)[1]))
            s2 = str(_d_cmp(y2, (q2n-1)*3+1, 1))
            e2 = str(_d_cmp(y2, (q2n-1)*3+3, _cal_cmp.monthrange(y2, (q2n-1)*3+3)[1]))
            d1 = _gpr_compare(f"range:{s1}/{e1}")
            d2 = _gpr_compare(f"range:{s2}/{e2}")
            context["last_query_intent"] = "admin_revenue"
            return _fmt_compare(f"Q{q1n}/{y1}", d1, f"Q{q2n}/{y2}", d2), ["mysql:orders"]

        # Priority 2: Tháng cụ thể – "tháng 3/2026 và tháng 4/2026" hoặc "tháng 3/2026 với 4/2026"
        _TWO_M = _re_cmp.findall(r'th[aá]ng\s+(\d{1,2})(?:/|-)(\d{4})', message, _re_cmp.IGNORECASE)
        if len(_TWO_M) < 2:
            _TWO_M = _re_cmp.findall(r'thang\s+(\d{1,2})/(\d{4})', _mn_cmp)
        # Nếu chỉ tìm được 1 tháng đầy đủ, thử bắt tháng rút gọn (vd: "tháng 3/2026 với 4/2026")
        if len(_TWO_M) == 1:
            _yr_from_first = _TWO_M[0][1]
            # Tìm tất cả "số/năm" trong message, loại trừ match đầu tiên đã có
            _all_my = _re_cmp.findall(r'\b(\d{1,2})/(\d{4})\b', message)
            for _m_val, _y_val in _all_my:
                if (_m_val, _y_val) != _TWO_M[0] and 1 <= int(_m_val) <= 12:
                    _TWO_M = [_TWO_M[0], (_m_val, _y_val)]
                    break
        if len(_TWO_M) >= 2 and _is_cmp:
            m1, y1 = _TWO_M[0]; m2, y2 = _TWO_M[1]
            d1 = _gpr_compare(f"month:{m1}/{y1}"); d2 = _gpr_compare(f"month:{m2}/{y2}")
            context["last_query_intent"] = "admin_revenue"
            return _fmt_compare(f"Tháng {m1}/{y1}", d1, f"Tháng {m2}/{y2}", d2), ["mysql:orders"]

        # Priority 3: Năm cụ thể – "năm 2025 với 2026"
        _TWO_Y = _re_cmp.findall(r'n[aă]m\s+(\d{4})', message, _re_cmp.IGNORECASE)
        if len(_TWO_Y) < 2:
            _TWO_Y = _re_cmp.findall(r'\b(20\d{2})\b', message)
        if len(_TWO_Y) >= 2 and _is_cmp:
            y1, y2 = _TWO_Y[0], _TWO_Y[1]
            d1 = _gpr_compare(f"year:{y1}"); d2 = _gpr_compare(f"year:{y2}")
            context["last_query_intent"] = "admin_revenue"
            return _fmt_compare(f"Năm {y1}", d1, f"Năm {y2}", d2), ["mysql:orders"]

        # Priority 4: Tuần tương đối
        if "tuan nay" in _mn_cmp and "tuan truoc" in _mn_cmp:
            d1 = _gpr_compare("this_week"); d2 = _gpr_compare("last_week")
            context["last_query_intent"] = "admin_revenue"
            return _fmt_compare("Tuần này", d1, "Tuần trước", d2), ["mysql:orders"]

        # Priority 5: Ngày tương đối
        if "hom nay" in _mn_cmp and "hom qua" in _mn_cmp:
            d1 = _gpr_compare("today"); d2 = _gpr_compare("yesterday")
            context["last_query_intent"] = "admin_revenue"
            return _fmt_compare("Hôm nay", d1, "Hôm qua", d2), ["mysql:orders"]

        # Default fallback: Tháng này vs tháng trước
        d1 = _gpr_compare("this_month"); d2 = _gpr_compare("last_month")
        context["last_query_intent"] = "admin_revenue"
        return _fmt_compare("Tháng này", d1, "Tháng trước", d2), ["mysql:orders"]

    if intent in ["admin_revenue_report", "admin_revenue_stats"]:
        # P1 FIX: So sánh doanh thu 2 sách từ OCR context
        _ocr_books_ctx = context.get("last_ocr_books", [])
        _compare_kws = ["so sanh", "compare", "cuon nao", "cai nao", "nhieu hon", "cao hon", "doanh thu", "ban duoc nhieu hon"]
        _mn_rev = "".join(c for c in __import__("unicodedata").normalize("NFD", message.lower().replace("đ","d")) if __import__("unicodedata").category(c)!="Mn")
        if len(_ocr_books_ctx) >= 2 and any(kw in _mn_rev for kw in _compare_kws):
            b1, b2 = _ocr_books_ctx[-2], _ocr_books_ctx[-1]
            try:
                from chatbot_app.db import get_connection
                conn = get_connection()
                cur  = conn.cursor(dictionary=True)
                results = []
                for bk in [b1, b2]:
                    if bk.get("book_id"):
                        cur.execute("""
                            SELECT b.title, COALESCE(SUM(oi.quantity),0) as total_sold,
                                   COALESCE(SUM(oi.quantity * oi.discounted_price),0) as total_revenue
                            FROM books b
                            LEFT JOIN order_items oi ON b.book_id = oi.book_id
                            LEFT JOIN orders o ON oi.order_id = o.order_id
                               AND o.created_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
                               AND o.status NOT IN ('cancelled')
                            WHERE b.book_id = %s GROUP BY b.book_id
                        """, (bk["book_id"],))
                        row = cur.fetchone()
                        if row:
                            results.append(row)
                cur.close(); conn.close()
                if len(results) == 2:
                    r1, r2 = results[0], results[1]
                    winner = r1["title"] if r1["total_revenue"] >= r2["total_revenue"] else r2["title"]
                    return (
                        f"**📊 So sánh doanh thu 90 ngày qua:**\n"
                        f"• **{r1['title']}**: {int(r1['total_sold'])} bản | {float(r1['total_revenue']):,.0f}đ\n"
                        f"• **{r2['title']}**: {int(r2['total_sold'])} bản | {float(r2['total_revenue']):,.0f}đ\n\n"
                        f"🏆 **`{winner}`** có doanh thu cao hơn trong 90 ngày qua."
                    ), ["mysql:orders"]
            except Exception:
                pass
            return (
                f"📊 Để so sánh doanh thu **{b1.get('title','sách 1')}** và **{b2.get('title','sách 2')}**,\n"
                "vui lòng tra cứu trong **Admin Panel → Quản lý Đơn hàng → Lọc theo sản phẩm**."
            ), []

        # FIX-A01~A04: Extract quarterly/monthly từ message trực tiếp (entities rỗng)
        import re as _re_rev
        _q_match = _re_rev.search(r'(?:qu[y|y]|q|quy)\s*([1-4])', _mn_rev)
        _yr_match = _re_rev.search(r'(20\d{2})', message)
        _q_num    = int(_q_match.group(1)) if _q_match else None
        _yr       = int(_yr_match.group(1)) if _yr_match else __import__("datetime").date.today().year

        # Case 1: "Tháng nào trong Q1/Q2 có doanh thu cao nhất?"
        _MONTH_BEST_KW = ["thang nao","thang may","thang nao trong","month nao","chiem cao nhất","cao nhat trong"]
        if _q_num and any(kw in _mn_rev for kw in _MONTH_BEST_KW):
            try:
                from chatbot_app.db import get_connection
                _q_start_m = (_q_num - 1) * 3 + 1
                _q_months  = [_q_start_m, _q_start_m+1, _q_start_m+2]
                conn = get_connection(); cur = conn.cursor(dictionary=True)
                cur.execute("""
                    SELECT MONTH(order_date) AS m,
                           COUNT(*) AS total_orders,
                           COALESCE(SUM(total_amount),0) AS revenue,
                           COALESCE(SUM(CASE WHEN status='delivered' THEN total_amount ELSE 0 END),0) AS confirmed_rev
                    FROM orders
                    WHERE YEAR(order_date) = %s AND MONTH(order_date) IN (%s,%s,%s)
                    GROUP BY MONTH(order_date) ORDER BY revenue DESC
                """, (_yr, *_q_months))
                rows = cur.fetchall(); cur.close(); conn.close()
                if rows:
                    _MONTH_VI = {1:"Tháng 1",2:"Tháng 2",3:"Tháng 3",4:"Tháng 4",
                                 5:"Tháng 5",6:"Tháng 6",7:"Tháng 7",8:"Tháng 8",
                                 9:"Tháng 9",10:"Tháng 10",11:"Tháng 11",12:"Tháng 12"}
                    _best = rows[0]
                    lines = [f"**📊 Doanh thu từng tháng trong Q{_q_num}/{_yr}:**"]
                    for row in rows:
                        _flag = " ⭐ **Cao nhất**" if row['m'] == _best['m'] else ""
                        lines.append(
                            f"  {_MONTH_VI[row['m']]}: **{float(row['revenue']):,.0f}đ** "
                            f"({row['total_orders']} đơn | Xác nhận: {float(row['confirmed_rev']):,.0f}đ){_flag}"
                        )
                    lines.append(f"\n🏆 **{_MONTH_VI[_best['m']]}** có doanh thu cao nhất trong Q{_q_num}/{_yr}.")
                    return "\n".join(lines), ["mysql:orders"]
            except Exception as _e_rev:
                pass

        # Case 2: "Tỷ lệ hoàn trả trong Q1/Q2" → admin_order_stats logic
        _RETURN_RATE_KW = ["ty le hoan tra", "ty le tra hang", "don hoan","don tra","return rate","hoan tra"]
        if _q_num and any(kw in _mn_rev for kw in _RETURN_RATE_KW):
            try:
                from chatbot_app.db import get_connection
                _q_start_m = (_q_num - 1) * 3 + 1
                _q_end_m   = _q_start_m + 2
                import datetime as _dt
                _q_start_date = _dt.date(_yr, _q_start_m, 1)
                _q_end_date   = _dt.date(_yr, _q_end_m, 28)  # conservative end
                conn = get_connection(); cur = conn.cursor(dictionary=True)
                cur.execute("""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN status IN ('returned','return_requested') THEN 1 ELSE 0 END) AS returned,
                           SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) AS cancelled,
                           COALESCE(SUM(total_amount),0) AS gross_revenue
                    FROM orders WHERE DATE(order_date) BETWEEN %s AND %s
                """, (_q_start_date, _q_end_date))
                row = cur.fetchone() or {}; cur.close(); conn.close()
                _total   = int(row.get("total",0)) or 1
                _ret     = int(row.get("returned",0))
                _can     = int(row.get("cancelled",0))
                _pct_ret = round(_ret / _total * 100, 2)
                _pct_can = round(_can / _total * 100, 2)
                return (
                    f"📊 **Thống kê đơn hàng Q{_q_num}/{_yr}**\n"
                    f"📅 {_q_start_date} → {_q_end_date}\n\n"
                    f"📦 Tổng đơn: **{_total}**\n"
                    f"💰 Doanh thu gộp: **{float(row.get('gross_revenue',0)):,.0f}đ**\n"
                    f"🚫 Hủy: **{_can}** ({_pct_can}%) | Hoàn trả: **{_ret}** ({_pct_ret}%)"
                ), ["mysql:orders"]
            except Exception:
                pass

        # Case 3: "Quý 2 tính đến hiện tại" hoặc "Q2 doanh thu" → gọi get_revenue_report với q{N}
        # Case 0: Month override từ MSG-OVERRIDE ("tháng 1/2026") – dùng get_period_revenue trực tiếp
        if entities.get("_month_override"):
            try:
                from chatbot_app.retrieval.staff_agents import get_period_revenue as _gpr_admin
                _mo_rev = _gpr_admin(entities["_month_override"])
                _total_mo  = int(_mo_rev.get("total_orders", 0))
                _cancel_mo = int(_mo_rev.get("cancelled_count", 0))
                _cancel_rate_mo = _mo_rev.get("cancel_rate", round(_cancel_mo/_total_mo*100,1) if _total_mo > 0 else 0.0)
                _mo_label = _mo_rev.get("period_label", entities["_month_override"])
                context["last_admin_revenue"] = _mo_rev
                context["last_query_intent"]  = "admin_revenue"
                return (
                    f"📊 **Doanh thu {_mo_label}**\n"
                    f"📅 {_mo_rev.get('from_date')} → {_mo_rev.get('to_date')}\n\n"
                    f"📦 Tổng đơn: **{_total_mo}** | Khách: {_mo_rev.get('unique_customers', 0)}\n"
                    f"💰 Doanh thu: **{float(_mo_rev.get('total_revenue',0)):,.0f}đ**\n"
                    f"✅ Xác nhận: **{float(_mo_rev.get('delivered_revenue',0)):,.0f}đ**\n"
                    f"🏷️ Giảm giá: {float(_mo_rev.get('total_discounts',0)):,.0f}đ\n"
                    f"🚫 Hủy: {_cancel_mo} ({_cancel_rate_mo:.1f}%) | Hoàn: {int(_mo_rev.get('returned_count', 0))}\n"
                    f"💳 COD: {int(_mo_rev.get('cod_count',0))} | Online: {int(_mo_rev.get('online_count',0))}"
                ), ["mysql:orders"]
            except Exception:
                pass  # fallback to normal flow below

        if _q_num:
            date_range = f"q{_q_num}"
        else:
            date_range = entities.get("date_range", "week")
            # Thêm fallback từ message: "tháng này", "hôm nay", "tuần này"
            if date_range == "week":
                _DR_KEYWORDS = {
                    "today": ["hom nay", "ngay hom nay", "hien tai ngay", "today"],
                    "yesterday": ["hom qua", "ngay hom qua", "hom kia"],
                    "last_month": ["thang truoc", "thang vua roi", "thang roi"],
                    "last_week": ["tuan truoc", "tuan vua roi"],
                    "month": ["thang nay", "thang hien tai"],
                    "week": ["tuan nay", "tuan hien tai"],
                    "year": ["nam nay", "ytd", "ca nam", "dau nam"],
                    "quarter": ["quy nay", "quy hien tai"],
                }
                for _dr, _kws in _DR_KEYWORDS.items():
                    if any(kw in _mn_rev for kw in _kws):
                        date_range = _dr; break
        data = get_revenue_report(date_range)
        context["last_query_intent"] = "admin_revenue"
        return fmt_revenue(data), ["mysql:orders"]



    if intent == "admin_order_stats":
        date_range   = entities.get("date_range", "month")
        if date_range == "month":
            _DR_KEYWORDS = {
                "today": ["hom nay", "ngay hom nay", "hien tai ngay", "today"],
                "yesterday": ["hom qua", "ngay hom qua", "hom kia"],
                "last_month": ["thang truoc", "thang vua roi", "thang roi"],
                "last_week": ["tuan truoc", "tuan vua roi"],
                "month": ["thang nay", "thang hien tai", "thang nay"],
                "week": ["tuan nay", "tuan hien tai"],
                "year": ["nam nay", "ytd", "ca nam", "dau nam", "tu dau nam"],
                "quarter": ["quy nay", "quy hien tai"],
            }
            import unicodedata as _ud_os
            _mn_os = "".join(c for c in _ud_os.normalize("NFD", message.lower().replace("đ","d")) if _ud_os.category(c)!="Mn")
            for _dr, _kws in _DR_KEYWORDS.items():
                if any(kw in _mn_os for kw in _kws):
                    date_range = _dr; break
        data         = get_revenue_report(date_range)
        cancelled    = float(data.get('cancelled_count') or 0)
        returned     = float(data.get('returned_count') or 0)
        total_orders = max(int(data.get('total_orders') or 1), 1)

        answer = (
            f"📊 **Thống kê đơn hàng – {data.get('period_label','')}**\n"
            f"📅 {data.get('from_date')} → {data.get('to_date')}\n\n"
            f"📦 Tổng đơn: **{data.get('total_orders',0)}**\n"
            f"🚫 Hủy: **{int(cancelled)}** ({(cancelled / total_orders * 100):.1f}%) | Hoàn: **{int(returned)}**"
        )
        context["last_query_intent"] = "admin_order_stats"
        return answer, ["mysql:orders"]

    if intent == "admin_top_books":
        books = get_top_books_admin(limit=10)
        return fmt_top_books_admin(books), ["mysql:order_details"]

    if intent == "admin_user_stats":
        # [FIX A-01 T8] Sub-intent: top user theo chi tiêu
        import unicodedata as _ud_a2, re as _re_a2
        _mn_a2 = "".join(c for c in _ud_a2.normalize("NFD", message.lower().replace("đ","d")) if _ud_a2.category(c)!="Mn")
        _TOP_SPEND_KW = [
            "chi tieu nhieu nhat", "chi nhieu nhat", "mua nhieu nhat",
            "top chi tieu", "top buyer", "dat don nhieu nhat",
            "khach hang chi nhieu", "nguoi chi nhieu", "spent most",
            "biggest spender", "top spender", "gia tri don cao nhat",
        ]
        if any(kw in _mn_a2 for kw in _TOP_SPEND_KW):
            # Query top 10 user by total spending từ DB
            try:
                from chatbot_app.db import get_connection
                conn = get_connection()
                cur  = conn.cursor(dictionary=True)
                cur.execute("""
                    SELECT u.user_id, u.username, u.email,
                           COUNT(o.order_id) as total_orders,
                           SUM(o.total_amount) as total_spent
                    FROM users u
                    JOIN orders o ON u.user_id = o.user_id
                    WHERE o.status NOT IN ('cancelled')
                    GROUP BY u.user_id
                    ORDER BY total_spent DESC
                    LIMIT 10
                """)
                top_users = cur.fetchall()
                cur.close(); conn.close()
                if top_users:
                    lines = ["**🏆 Top 10 khách hàng chi tiêu nhiều nhất:**", ""]
                    for i, u in enumerate(top_users, 1):
                        lines.append(
                            f"{i}. **{u.get('username','?')}** ({u.get('email','?')})\n"
                            f"   Tổng chi: **{float(u.get('total_spent',0)):,.0f}đ** | "
                            f"Số đơn: {u.get('total_orders',0)}"
                        )
                    return "\n".join(lines), ["mysql:orders"]
            except Exception as _e:
                pass  # Fallback xuống thống kê tổng quát

        _NEW_USERS_KW = [
            "danh sach user dang ky", "user moi dang ky", "khach hang moi", "tai khoan moi",
            "danh sach khach hang moi", "user dang ky trong", "danh sach user moi", "nguoi dung dang ky"
        ]
        if any(kw in _mn_a2 for kw in _NEW_USERS_KW) and "danh sach" in _mn_a2:
            import re as _re_days
            days = None
            m = _re_days.search(r"(\d+)\s*ngay", _mn_a2)
            if m:
                days = int(m.group(1))
            elif "thang" in _mn_a2:
                days = 30
            elif "tuan" in _mn_a2:
                days = 7
            elif "hom nay" in _mn_a2:
                days = 1
                
            try:
                from chatbot_app.db import get_connection
                conn = get_connection()
                cur  = conn.cursor(dictionary=True)
                
                if days is not None:
                    cur.execute("""
                        SELECT user_id, username, email, created_at
                        FROM users
                        WHERE role = 'customer'
                          AND created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
                        ORDER BY created_at DESC
                        LIMIT 20
                    """, (days,))
                else:
                    cur.execute("""
                        SELECT user_id, username, email, created_at
                        FROM users
                        WHERE role = 'customer'
                        ORDER BY created_at DESC
                        LIMIT 15
                    """)
                    
                new_users = cur.fetchall()
                cur.close(); conn.close()
                if new_users:
                    if days is not None:
                        lines = [f"**🆕 Danh sách User đăng ký trong {days} ngày qua:**", ""]
                    else:
                        lines = ["**🆕 Danh sách User đăng ký gần đây:**", ""]
                    for u in new_users:
                        date_str = str(u.get('created_at', ''))[:10]
                        lines.append(f"• `#{u.get('user_id')}` **{u.get('username','?')}** ({u.get('email','?')}) – Đăng ký: {date_str}")
                    return "\n".join(lines), ["mysql:users"]
                else:
                    if days is not None:
                        return f"Không có người dùng nào đăng ký trong {days} ngày qua.", ["mysql:users"]
                    else:
                        return "Chưa có người dùng nào đăng ký.", ["mysql:users"]
            except Exception as _e:
                pass

        stats = get_user_statistics()
        return fmt_user_stats(stats), ["mysql:users"]

    # ── P2 FIX: admin_inactive_users – users chưa mua bao giờ ──────────────────
    if intent == "admin_inactive_users":
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT u.user_id, u.username, u.email, u.created_at
                FROM users u
                LEFT JOIN orders o ON u.user_id = o.user_id
                WHERE o.order_id IS NULL AND u.role = 'customer'
                ORDER BY u.created_at DESC LIMIT 15
            """)
            inactive = cur.fetchall()
            cur.execute("SELECT COUNT(*) as total FROM users WHERE role='customer'")
            total_row = cur.fetchone()
            cur.close(); conn.close()
            total_customers = total_row["total"] if total_row else 0
            if inactive:
                lines = [f"**👤 Khách hàng chưa mua lần nào** ({len(inactive)}/{total_customers} khách được hiển thị):"]
                for i, u in enumerate(inactive[:10], 1):
                    created = str(u.get("created_at",""))[:10]
                    lines.append(f"{i}. **{u.get('username','?')}** ({u.get('email','?')}) – Đăng ký: {created}")
                lines.append(f"\n💡 Có thể gửi email marketing hoặc voucher chào mừng để kích hoạt nhóm này.")
                return "\n".join(lines), ["mysql:users"]
            return "✅ Tất cả khách hàng đều đã có ít nhất một đơn hàng!", []
        except Exception:
            return "Không thể truy vấn danh sách user inactive lúc này. Thử lại sau hoặc kiểm tra **Admin Panel → Users → Filter: no_orders**.", []

    # ── P2 FIX: admin_top_loyal_customers – top khách hàng trung thành ─────────
    if intent == "admin_top_loyal_customers":
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT u.user_id, u.username, u.email,
                       COUNT(DISTINCT o.order_id) as total_orders,
                       COALESCE(SUM(o.total_amount), 0) as total_spent
                FROM users u
                JOIN orders o ON u.user_id = o.user_id
                WHERE o.status NOT IN ('cancelled')
                GROUP BY u.user_id
                ORDER BY total_orders DESC, total_spent DESC
                LIMIT 10
            """)
            top_loyal = cur.fetchall()
            cur.close(); conn.close()
            if top_loyal:
                lines = ["**🏅 Top 10 khách hàng trung thành nhất (nhiều đơn nhất):**", ""]
                for i, u in enumerate(top_loyal, 1):
                    email = u.get("email","?")
                    lines.append(
                        f"• **#{i} {u.get('username','?')}** ({email})\n"
                        f"  ↳ **{u['total_orders']} đơn** | Chi **{float(u['total_spent']):,.0f}đ**"
                    )
                return "\n".join(lines), ["mysql:orders"]
            return "Chưa có dữ liệu đủ để xếp hạng khách hàng trung thành.", []
        except Exception:
            return "Không thể truy xuất dữ liệu lúc này. Xem trong **Admin Panel → Analytics → Top Customers**.", []

    # ── P2 FIX: admin_revenue_ytd – doanh thu năm đến nay (Year-to-Date) ───────
    if intent == "admin_revenue_ytd":
        try:
            from chatbot_app.db import get_connection
            import datetime
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            year = datetime.datetime.now().year
            cur.execute("""
                SELECT
                    MONTH(CONVERT_TZ(o.order_date, '+00:00', '+07:00')) as month,
                    SUM(CASE WHEN o.status NOT IN ('cancelled', 'returned', 'failed') THEN 1 ELSE 0 END) as total_orders,
                    COALESCE(SUM(CASE WHEN o.status NOT IN ('cancelled', 'returned', 'failed') THEN
                        (SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) - 
                        COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                    ELSE 0 END), 0) as revenue,
                    COALESCE(SUM(CASE WHEN o.status='delivered' THEN
                        (SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) - 
                        COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                    ELSE 0 END),0) as confirmed_revenue
                FROM orders o
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE YEAR(CONVERT_TZ(o.order_date, '+00:00', '+07:00')) = %s
                GROUP BY MONTH(CONVERT_TZ(o.order_date, '+00:00', '+07:00'))
                ORDER BY month
            """, (year,))
            rows = cur.fetchall()
            cur.execute("""
                SELECT 
                    COUNT(o.order_id) as ytd_orders,
                    COALESCE(SUM(CASE WHEN o.status NOT IN ('cancelled', 'returned', 'failed') THEN
                        (SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) - 
                        COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                    ELSE 0 END), 0) as ytd_total
                FROM orders o 
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE YEAR(CONVERT_TZ(o.order_date, '+00:00', '+07:00')) = %s
            """, (year,))
            ytd = cur.fetchone()
            cur.close(); conn.close()

            ytd_total = float(ytd["ytd_total"] if ytd else 0)
            ytd_orders = int(ytd["ytd_orders"] if ytd else 0)
            lines = [f"📊 **Doanh thu từ đầu năm {year} đến nay (YTD):**", "",
                     f"📦 Tổng đơn: **{ytd_orders}** | 💰 Tổng doanh thu: **{ytd_total:,.0f}đ**", ""]
            if rows:
                _MONTH_VI = ["","Tháng 1","Tháng 2","Tháng 3","Tháng 4","Tháng 5","Tháng 6",
                             "Tháng 7","Tháng 8","Tháng 9","Tháng 10","Tháng 11","Tháng 12"]
                lines.append("**Chi tiết theo tháng:**")
                for r in rows:
                    m = int(r["month"])
                    lines.append(f"  {_MONTH_VI[m]}: {int(r['total_orders'])} đơn | {float(r['revenue']):,.0f}đ (xác nhận: {float(r['confirmed_revenue']):,.0f}đ)")
            return "\n".join(lines), ["mysql:orders"]
        except Exception:
            return "Không thể truy xuất dữ liệu YTD lúc này. Kiểm tra **Admin Panel → Báo cáo → Năm nay**.", []

    # ── P2 FIX: admin_category_order_analysis – thể loại ít/nhiều đơn nhất ────
    if intent == "admin_category_order_analysis":
        import unicodedata as _ud_c
        _mn_c = "".join(c for c in _ud_c.normalize("NFD", message.lower().replace("đ","d")) if _ud_c.category(c)!="Mn")
        _LEAST_KWS = ["it don","it nhat","ban cham","khong ban","ban kem","least","fewest","thap nhat"]
        _sort = "ASC" if any(kw in _mn_c for kw in _LEAST_KWS) else "DESC"
        _label = "ít đơn hàng nhất" if _sort == "ASC" else "nhiều đơn hàng nhất"
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute(f"""
                SELECT c.category_name as category, COUNT(DISTINCT od.order_id) as total_orders, COALESCE(SUM(od.quantity),0) as total_sold
                FROM categories c
                JOIN book_categories bc ON c.category_id = bc.category_id
                JOIN books b ON bc.book_id = b.book_id
                LEFT JOIN order_details od ON b.book_id = od.book_id
                LEFT JOIN orders o ON od.order_id = o.order_id AND o.status NOT IN ('cancelled')
                WHERE b.status = 'active'
                GROUP BY c.category_id, c.category_name
                ORDER BY total_orders {_sort}
                LIMIT 10
            """)
            cats = cur.fetchall()
            cur.close(); conn.close()
            if cats:
                lines = [f"📂 **Thể loại sách {_label}:**", ""]
                for i, c in enumerate(cats, 1):
                    lines.append(f"{i}. **{c['category'] or 'Chưa phân loại'}** – {int(c['total_orders'])} đơn | {int(c['total_sold'])} cuốn bán")
                if _sort == "ASC":
                    lines.append("\n💡 Gợi ý: Cân nhắc chạy promotion hoặc review lại nội dung các thể loại này.")
                return "\n".join(lines), ["mysql:orders"]
            return "Chưa có đủ dữ liệu đơn hàng để phân tích theo thể loại.", []
        except Exception:
            return "Không thể truy vấn dữ liệu lúc này. Xem trong **Admin Panel → Analytics → Category**.", []

    # ── P2 FIX: admin_marketing_advice – tư vấn tăng doanh số / giữ chân khách ──
    if intent == "admin_marketing_advice":
        import unicodedata as _ud_mk
        _mn_mk = "".join(
            c for c in _ud_mk.normalize("NFD", message.lower().replace("đ","d"))
            if _ud_mk.category(c) != "Mn"
        )
        _RETENTION_KW = [
            "giu chan", "loyalty", "trung thanh", "kh trung thanh", "kh cu",
            "khach hang trung thanh", "khach trung thanh", "chien luoc giu",
            "tai sao khach", "khach bo di", "khach quay lai", "tang tiep can",
            "retention", "giu khach", "duy tri khach",
        ]
        _is_retention = any(kw in _mn_mk for kw in _RETENTION_KW)
        if _is_retention:
            return (
                "**🤝 Chiến lược giữ chân khách hàng trung thành:**\n\n"
                "**1. Loyalty Program (Tích điểm):**\n"
                "   • Mỗi 10,000đ mua hàng = 1 điểm thưởng\n"
                "   • Đổi điểm lấy voucher giảm giá hoặc sách miễn phí\n"
                "   • Hạng thành viên: **Bronze → Silver → Gold → Platinum**\n\n"
                "**2. Ưu đãi khách hàng VIP (top 10% chi tiêu):**\n"
                "   • Giảm 15% cho mọi đơn hàng tiếp theo\n"
                "   • Ưu tiên giao hàng nhanh miễn phí\n"
                "   • Được xem trước sách mới ra mắt\n\n"
                "**3. Email & Push Notification cá nhân hóa:**\n"
                "   • Gửi gợi ý sách dựa trên lịch sử mua (recommendation engine)\n"
                "   • Nhắc nhở khi sách yêu thích giảm giá\n"
                "   • Chúc mừng sinh nhật + voucher tặng quà\n\n"
                "**4. Community & Engagement:**\n"
                "   • Tạo club đọc sách online, review contest\n"
                "   • Tặng badge 'BookLover' cho KH review ≥ 5 sách\n\n"
                "**5. Phân tích churn risk:**\n"
                "   • KH không mua hàng > 60 ngày → tự động gửi voucher 'Nhớ bạn'\n"
                "   • Dùng lệnh `Khách chưa mua 30 ngày` để xem danh sách cần chăm sóc\n\n"
                "💡 Dữ liệu gợi ý: Xem top khách hàng chi tiêu cao nhất qua lệnh `Top khách hàng`."
            ), []
        # Default: chiến lược marketing tăng doanh số sách ế
        return (
            "**📣 Chiến lược tăng doanh số cho sách ế / tồn kho cao:**\n\n"
            "**1. Khuyến mãi có mục tiêu:**\n"
            "   • Tạo mã giảm giá dành riêng cho danh mục ế: `Admin Panel → Khuyến mãi → Tạo mã`\n"
            "   • Flash sale 24h cho top 10 sách tồn kho nhiều nhất\n"
            "   • Combo bundle: kết hợp sách ế với sách bán chạy\n\n"
            "**2. Tăng visibility:**\n"
            "   • Đặt sách ế vào banner trang chủ với tag `⭐ Giảm giá đặc biệt`\n"
            "   • Đề xuất trong phần \"Có thể bạn thích\" dựa trên lịch sử mua\n\n"
            "**3. Re-pricing:**\n"
            "   • Xem xét điều chỉnh giá xuống 10–20% cho sách tồn quá 90 ngày\n"
            "   • So sánh giá với đối thủ cạnh tranh cùng thể loại\n\n"
            "**4. Email marketing:**\n"
            "   • Gửi email giới thiệu sách ế đến nhóm khách chưa mua 30 ngày\n\n"
            "💡 Dùng `Sách không có đơn 30 ngày` để xem danh sách sách cần xử lý."
        ), []

    # ── NHÓM A-B: QUẢN LÝ USER ════════════════════════════════


    if intent == "admin_user_lookup":
        # Ưu tiên kiểm tra xem admin có đang tra cứu chính mình không
        if any(kw in _mn_a for kw in ["chinh minh", "cua toi", "cua minh", "ban than"]):
            query = str(user_id) if user_id else None
        else:
            query = (
                entities.get("target_email") or
                str(entities.get("target_user_id", "")) or
                _extract_query_from_message(message)
            )

        if not query:
            return "Vui lòng cung cấp **email** hoặc **ID** user để tra cứu.", []
        user = lookup_user_by_email_or_id(query)
        if user:
            # Lưu vào context để dùng cho thao tác tiếp theo (khóa, đổi role...)
            context["last_looked_up_user"] = {
                "user_id":  user.get("user_id"),
                "username": user.get("username"),
                "email":    user.get("email"),
            }
        return fmt_user(user), ["mysql:users"]

    if intent == "admin_user_update_role":
        _nlu_target = entities.get("target_user_id") or entities.get("target_email")
        target_role = entities.get("target_role")
        
        # Lọc bõ NLU bóc nhầm từ coref
        def is_coref(text):
            if not text: return False
            words = str(text).lower().replace("tài khoản", "taikhoan").replace("người dùng", "nguoidung").split()
            corefs = {"n", "ho", "do", "nay", "ay", "kia", "user", "nguoi", "họ", "đó", "này", "ấy", "kia", "tk", "acc", "taikhoan", "nguoidung"}
            return all(w in corefs for w in words)

        if _nlu_target and (len(str(_nlu_target).strip()) <= 2 or is_coref(_nlu_target)):
            _nlu_target = None
            
        # Fallback regex nếu NLU không bóc được target_user
        if not _nlu_target:
            import re
            match = re.search(r"(?:user|tài khoản|tk|account|người dùng)\s+([a-zA-Z0-9_\.\-\s@]+?)(?:\s+(?:lên|thành|sang|role|quyền|đi|luôn|ngay|lại)|$)", message, re.IGNORECASE)
            if match:
                ext = match.group(1).strip()
                if len(ext) > 2 and not is_coref(ext):
                    _nlu_target = ext

        _ctx_user   = context.get("last_looked_up_user", {})
        target_user = _nlu_target or str(_ctx_user.get("user_id", "")) or None
        target_name = _ctx_user.get("username", target_user) if not _nlu_target else target_user

        if not target_user:
            return "Vui lòng cung cấp **user ID** hoặc **email** hoặc **tên** để thực hiện đổi role.", [], []
            
        # Tìm role mục tiêu từ message nếu NLU không bắt được
        if not target_role:
            _msg_lower = message.lower()
            if "staff" in _msg_lower or "nhân viên" in _msg_lower:
                target_role = "staff"
            elif "admin" in _msg_lower or "quản trị" in _msg_lower:
                target_role = "admin"
            else:
                return f"Bạn muốn đổi role user `{target_user}` thành role nào? (`staff`/`customer`)", [], []

        # BẢO MẬT: Chặn cấp quyền admin
        if target_role == "admin":
            return "❌ **Không thể đổi role lên admin qua chatbot** vì lý do bảo mật.\n\nThực hiện tại: `Admin Panel → Users → [ID] → Đổi Role`", [], []

        current_user_info = lookup_user_by_email_or_id(target_user)
        if current_user_info:
            db_user_id = current_user_info.get("user_id")
            db_username = current_user_info.get("username")
            current_role = current_user_info.get("role")
            target_user = str(db_user_id)
            target_name = db_username
            if current_role == target_role:
                return f"ℹ️ Tài khoản `{db_username}` (ID: {db_user_id}) hiện tại đã có role **{target_role}** rồi.", ["mysql:users"], []
        else:
            return f"❌ Không tìm thấy user với ID/Email/Tên: `{target_user}`.", ["mysql:users"], []

        context["pending_confirmation"] = {
            "action":      "update_user_role",
            "target_user": target_user,
            "target_role": target_role,
        }
        return _with_confirm_buttons(
            f"**Xác nhận thay đổi role?**\n"
            f"User: `{target_name}` (ID: {target_user})\n"
            f"Role hiện tại: `{current_role}` → Role mới: `{target_role}`",
            ["mysql:users"]
        )

    if intent == "admin_user_lock_unlock":
        # Ưu tiên entity từ NLU, fallback về user vừa tra cứu trong context
        _nlu_target = entities.get("target_user_id") or entities.get("target_email")
        
        def is_coref(text):
            if not text: return False
            words = str(text).lower().replace("tài khoản", "taikhoan").replace("người dùng", "nguoidung").split()
            corefs = {"n", "ho", "do", "nay", "ay", "kia", "user", "nguoi", "họ", "đó", "này", "ấy", "kia", "tk", "acc", "taikhoan", "nguoidung"}
            return all(w in corefs for w in words)

        if _nlu_target and (len(str(_nlu_target).strip()) <= 2 or is_coref(_nlu_target)):
            _nlu_target = None
            
        if not _nlu_target:
            import re
            match = re.search(
                r"(?:user|tài khoản|tk|account|người dùng)\s+([\w][a-zA-Z0-9_\.\- ]+?)\s*(?:(?:khoa|mo khoa|lock|unlock|ban|unban|cap nhat|doi|reset)|$)",
                message, re.IGNORECASE
            )
            if not match:
                match = re.search(r"(?:user|tài khoản|tk|account|người dùng)\s+(.+?)\s*$", message, re.IGNORECASE)
            if match:
                ext = match.group(1).strip()
                if len(ext) > 1 and not is_coref(ext):
                    _nlu_target = ext
                    
        _ctx_user   = context.get("last_looked_up_user", {})
        target_user = _nlu_target or str(_ctx_user.get("user_id", "")) or None
        target_user = target_user if target_user else None  # "" → None
        target_name = _ctx_user.get("username", target_user) if not _nlu_target else target_user

        # Phát hiẹn unlock vs lock: kiẻm tra "mở khóa" / "unlock" TRUỚC
        # đẻ tránh "khóa" trong "mở khóa" bị detect nhàm
        _is_unlock = any(w in _mn_ovr for w in ["mo khoa", "unlock", "kich hoat",
                                                 "mo lai", "bo khoa", "go khoa", "unban"])
        _is_lock   = (not _is_unlock) and any(w in _mn_ovr for w in
                                              ["khoa", "lock", "ban", "vo hieu", "chan"])
        target_status = "active" if _is_unlock else "locked"

        if not target_user:
            return "Vui lòng cung cấp **user ID** hoặc **email** hoặc **tên** để thực hiện.", [], []

        # Kiểm tra trạng thái hiẹn tại từ DB trước khi confirm
        current_user_info = lookup_user_by_email_or_id(target_user)
        if not current_user_info:
            return f"❌ Không tìm thấy user với ID/Email: `{target_user}`.", [], []

        db_user_id = current_user_info.get("user_id")
        db_username = current_user_info.get("username")
        current_status = current_user_info.get("status")
        
        # Security Check: Prevent locking/unlocking admin accounts
        if current_user_info.get("role") == "admin":
            return f"⛔ **Không thể khóa hay thay đổi trạng thái tài khoản ADMIN (`{db_username}`) qua chatbot.**", ["mysql:users"], []
            
        if current_status == target_status:
            state_vi = "bị khóa" if current_status == "locked" else ("chưa xác thực" if current_status == "unverified" else "đang hoạt động (đã mở khóa)")
            return f"⚠️ Tài khoản của user `{db_username}` (ID: {db_user_id}) hiện tại đã **{state_vi}** rồi.", ["mysql:users"], []

        action_label = "khóa" if target_status == "locked" else "mở khóa"
        context["pending_confirmation"] = {
            "action":        "update_user_status",
            "target_user":   db_user_id,
            "target_status": target_status,
        }
        return _with_confirm_buttons(
            f"**Xác nhận {action_label} tài khoản?**\n"
            f"User: `{db_username}` (ID: {db_user_id})\n"
            f"Trạng thái hiện tại: `{current_status}` → Trạng thái mới: `{target_status}`",
            ["mysql:users"]
        )

    if intent == "admin_user_reset_password":
        _nlu_target = entities.get("target_user_id") or entities.get("target_email")
        
        def is_coref(text):
            if not text: return False
            words = str(text).lower().replace("tài khoản", "taikhoan").replace("người dùng", "nguoidung").split()
            corefs = {"n", "ho", "do", "nay", "ay", "kia", "user", "nguoi", "họ", "đó", "này", "ấy", "kia", "tk", "acc", "taikhoan", "nguoidung"}
            return all(w in corefs for w in words)
            
        if _nlu_target and (len(str(_nlu_target).strip()) <= 2 or is_coref(_nlu_target)):
            _nlu_target = None
            
        _ctx_user = context.get("last_looked_up_user", {})
        target_user = _nlu_target or str(_ctx_user.get("user_id", "")) or None
        
        if not target_user:
            return "Vui lòng cung cấp **user ID**, **email** hoặc tìm user trước để reset password.", [], []

        # Security Check: Prevent resetting admin accounts
        current_user_info = lookup_user_by_email_or_id(target_user)
        if current_user_info and current_user_info.get("role") == "admin":
            return f"⛔ **Không thể reset password tài khoản ADMIN (`{current_user_info.get('username')}`) qua chatbot.**", ["mysql:users"], []

        context["pending_confirmation"] = {
            "action":      "reset_user_password",
            "target_user": target_user,
        }
        return _with_confirm_buttons(
            f"**Xác nhận reset password?**\n"
            f"User: `{target_user}`\n"
            f"Mật khẩu sẽ được đặt lại về mặc định.\n\n"
            f"**Có** – Xác nhận | **Không** – Hủy bỏ",
            ["mysql:users"]
        )

    if intent == "admin_list_staff":
        is_admin_query = "admin" in message.lower() or "quan tri" in message.lower() or "quản trị" in message.lower()
        role_to_fetch = 'admin' if is_admin_query else 'staff'
        role_label = "Admin" if is_admin_query else "Staff"
        
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT user_id, full_name, username, email, status FROM users WHERE role = %s ORDER BY user_id", (role_to_fetch,))
            staff = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            return f"❌ Lỗi khi lấy danh sách {role_label}: {e}", []

        if not staff:
            return f"Chưa có tài khoản {role_label} nào.", []
            
        lines = [f"**Danh sách {role_label}** ({len(staff)} tài khoản):"]
        for s in staff:
            lines.append(
                f"- **#{s['user_id']}** {s.get('full_name') or s.get('username','?')} "
                f"({s.get('email','?')}) – {s.get('status','?')}"
            )
        return "\n".join(lines), ["mysql:users"]

    if intent == "admin_list_customers":
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT user_id, full_name, username, email, status FROM users WHERE role = 'customer' ORDER BY user_id DESC LIMIT 20")
            customer_list = cur.fetchall()
            cur.execute("SELECT COUNT(*) as total FROM users WHERE role = 'customer'")
            total_customers = cur.fetchone()["total"]
            cur.close()
            conn.close()
            
            if not customer_list:
                return "Hiện tại không có tài khoản khách hàng nào trong hệ thống.", []
                
            lines = [f"**Danh sách Khách hàng mới nhất** ({len(customer_list)}/{total_customers} tài khoản):"]
            for c in customer_list:
                name = c.get("full_name") or c.get("username") or "Unknown"
                st = c.get("status", "unknown")
                lines.append(f"• `#{c['user_id']}` **{name}** ({c['email']}) – `{st}`")
            
            if total_customers > len(customer_list):
                lines.append(f"\n💡 Còn **{total_customers - len(customer_list)}** khách hàng khác. Xem đầy đủ tại: **Admin Panel → Quản lý Users**.")
            return "\n".join(lines), ["mysql:users"]
        except Exception as e:
            return f"❌ Lỗi khi lấy danh sách khách hàng: {e}", []

    if intent == "admin_banned_users":
        # Lấy dữ liệu từ bảng thống kê user (có sẵn trường locked_users)
        stats = get_user_statistics()
        locked_count = int(stats.get("locked_users") or 0)
        if locked_count == 0:
            return (
                "✅ Hiện tại **không có** tài khoản nào bị khóa trong hệ thống.\n"
                f"Tổng user: **{stats.get('total_users', 0)}** | Active: {stats.get('active_users', 0)}",
                ["mysql:users"]
            )
        # Lấy danh sách các tài khoản bị khóa
        try:
            from chatbot_app.db import get_connection
            conn = get_connection()
            cur  = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT user_id, username, full_name, email, role, created_at
                FROM users
                WHERE status = 'locked'
                ORDER BY created_at DESC
                LIMIT 20
            """)
            locked_users = cur.fetchall()
            cur.close(); conn.close()
        except Exception:
            locked_users = []
        lines = [f"🔒 **Tài khoản đang bị khóa** ({locked_count} tài khoản):"]
        for u in locked_users:
            lines.append(
                f"- **#{u['user_id']}** {u.get('full_name') or u.get('username','?')} "
                f"({u.get('email','?')}) – {u.get('role','?')}"
            )
        lines.append(f"\n→ Dùng `Khóa/Mở khóa user <ID>` để thực hiện thao tác.")
        return "\n".join(lines), ["mysql:users"]

    # ── NHÓM A-C: QUẢN LÝ SÁCH ════════════════════════════════

    if intent == "admin_book_add_guide":
        return (
            "**Hướng dẫn thêm sách mới:**\n\n"
            "1. Vào **Admin Panel → Quản lý Sách → Thêm sách**\n"
            "2. Điền đầy đủ: Tên, Tác giả, NXB, Giá, Số lượng, Thể loại\n"
            "3. Upload ảnh bìa (tùy chọn)\n"
            "4. Click **Lưu** để xuất bản sách\n\n"
            "API endpoint: `POST /admin/books`\n"
            "Các trường bắt buộc: `title`, `author_id`, `price`, `stock_quantity`",
            []
        )

    if intent == "admin_book_status_change":
        book_id       = entities.get("book_id")
        target_status = entities.get("target_status", "inactive")
        if not book_id:
            return "Vui lòng cung cấp **ID sách** để thay đổi trạng thái.", []
        action_label = "kích hoạt" if target_status == "active" else "vô hiệu hóa"
        context["pending_confirmation"] = {
            "action":        "update_book_status",
            "book_id":       book_id,
            "target_status": target_status,
        }
        return _with_confirm_buttons(
            f"**Xác nhận {action_label} sách?**\n"
            f"Sách ID: `{book_id}` → Status: `{target_status}`\n\n"
            f"**Có** – Xác nhận | **Không** – Hủy bỏ",
            ["mysql:books"]
        )

    # ── CẬP NHẬT TỒN KHO SÁCH (dùng lại hàm staff) ──────────
    if intent == "admin_update_book_stock":
        import re as _re2
        # Trích xuất số lượng mới từ câu hỏi
        qty_match = _re2.search(r"(?:len|thanh|them|xuong|=|\bset\b|\bla\b)?\s*(\d+)\s*(?:cuon|quyen|ban|co|item)?", message, _re2.IGNORECASE)
        new_qty = int(qty_match.group(1)) if qty_match else None

        # Trích xuất tên/ID sách từ entities hoặc context lần tra cứu trước
        target_book = (
            entities.get("book_id")
            or entities.get("book_name")
            or (context.get("last_looked_up_book") or {}).get("title")
            or (context.get("last_looked_up_book") or {}).get("book_id")
        )

        if not target_book:
            return "Vui lòng cung cấp **tên sách** hoặc **ID sách** để cập nhật tồn kho.", [], []
        if new_qty is None:
            return "Vui lòng cho biết **số lượng mới** cần cập nhật (ví dụ: *cập nhật lên 50 cuốn*).", [], []

        # Dùng check_book_inventory của staff – có fuzzy match tốt hơn
        _book = check_book_inventory(str(target_book))
        if not _book:
            return f"❌ Không tìm thấy sách `{target_book}` trong hệ thống.", [], []

        context["pending_confirmation"] = {
            "action":     "update_book_stock",
            "book_id":    _book["book_id"],
            "book_title": _book["title"],
            "new_qty":    new_qty,
            "old_qty":    _book.get("stock_quantity", 0),
        }
        return _with_confirm_buttons(
            f"**Xác nhận cập nhật tồn kho?**\n"
            f"📖 Sách: **{_book['title']}** (ID: {_book['book_id']})\n"
            f"📦 Tồn kho: **{_book.get('stock_quantity', 0)} cuốn** → **{new_qty} cuốn**",
            ["mysql:books"]
        )

    # ── ADMIN ORDER STATUS UPDATE (kế thừa staff) ─────────────
    if intent == "admin_order_status_update":
        import re as _re_asu
        _VALID_TRANSITIONS = {
            "pending":          ["processing", "cancelled"],
            "processing":       ["shipped", "cancelled"],
            "shipped":          ["delivered", "failed"],
            "delivered":        ["return_requested"],
            "cancel_requested": ["cancelled", "processing"],
            "return_requested": ["returned", "shipped"],
            "failed":           ["processing"],
            "cancelled":        [],
            "returned":         [],
        }
        _STATUS_MAP = {
            "processing": ["processing", "xu ly", "dang xu ly", "dang chuan bi", "xac nhan don", "duyet don"],
            "shipped":    ["shipped", "ship", "da ship", "giao hang", "dang giao", "van chuyen", "xuat kho"],
            "delivered":  ["delivered", "da giao", "giao xong", "hoan thanh", "giao thanh cong", "khach da nhan"],
            "cancelled":  ["cancelled", "da huy", "duyet huy", "xac nhan huy", "huy chinh thuc"],
            "cancel_requested": ["cancel_requested", "yeu cau huy", "huy don", "xin huy", "muon huy"],
            "return_requested": ["return_requested", "yeu cau tra", "yeu cau hoan", "xin tra", "muon tra hang"],
            "returned":   ["returned", "da tra", "duyet tra", "hoan hang", "tra hang xong"],
            "failed":     ["failed", "that bai", "giao that bai", "khong giao duoc"],
            "pending":    ["pending", "cho xu ly", "cho duyet", "chua xu ly"],
        }
        import unicodedata as _ud_asu
        _mn_asu = "".join(
            c for c in _ud_asu.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_asu.category(c) != "Mn"
        )
        order_id = entities.get("order_id")
        if not order_id:
            _oid_m = _re_asu.search(r"#?(\d{3,6})", message)
            if _oid_m:
                order_id = int(_oid_m.group(1))
        if not order_id:
            order_id = context.get("last_order_id")
        if not order_id:
            return "Vui lòng cung cấp **mã đơn hàng** để cập nhật trạng thái.", [], []

        target_status = None
        for st, kws in _STATUS_MAP.items():
            if any(kw in _mn_asu for kw in kws):
                target_status = st
                break
        if not target_status:
            return (
                f"Đơn **#{order_id}** — bạn muốn chuyển sang trạng thái nào?\n"
                "Ví dụ: `processing`, `shipped`, `delivered`, `cancelled`"
            ), [], []

        order = lookup_order_by_id(order_id)
        if not order:
            return f"Không tìm thấy đơn hàng **#{order_id}**.", [], []
        current_status = order["status"]
        allowed = _VALID_TRANSITIONS.get(current_status, [])
        if target_status not in allowed:
            return (
                f"Không thể chuyển đơn **#{order_id}** từ `{current_status}` → `{target_status}`.\n"
                f"Trạng thái hiện tại cho phép: {', '.join(f'`{s}`' for s in allowed) if allowed else 'không có'}"
            ), [], []

        context["pending_confirmation"] = {
            "action":         "update_order_status",
            "order_id":       order_id,
            "target_status":  target_status,
            "current_status": current_status,
        }
        return _with_confirm_buttons(
            f"Xác nhận cập nhật đơn **#{order_id}**?\n"
            f"Trạng thái: `{current_status}` → `{target_status}`",
            ["mysql:orders"]
        )

    # ── ADMIN ORDER LIST PENDING ───────────────────────────────
    if intent == "admin_order_list_pending":
        import unicodedata as _ud_alp
        _mn_alp = "".join(
            c for c in _ud_alp.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_alp.category(c) != "Mn"
        )
        _COUNT_KW = ["bao nhieu", "co bao nhieu", "tong", "tong so", "so luong", "dem"]
        _WEEK_KW  = ["tuan nay", "7 ngay", "tuan qua", "trong tuan"]
        _ALL_KW   = ["tat ca", "toan bo", "toan he thong", "lich su", "all"]

        if any(kw in _mn_alp for kw in _COUNT_KW):
            stats = get_all_time_order_stats()
            p  = int(stats.get("pending", 0))
            pr = int(stats.get("processing", 0))
            total = int(stats.get("total_orders", 0))
            return (
                f"📊 **Thống kê đơn hàng — Toàn bộ lịch sử:**\n\n"
                f"⏳ Chờ xử lý (pending):   **{p:,}**\n"
                f"🔧 Đang xử lý (processing): **{pr:,}**\n\n"
                f"🧾 Tổng tất cả trạng thái: **{total:,} đơn**"
            ), [], ["mysql:orders"]

        if any(kw in _mn_alp for kw in _ALL_KW):
            stats = get_all_time_order_stats()
            return (
                f"📊 **Thống kê toàn hệ thống:**\n\n"
                f"⏳ Pending: **{int(stats.get('pending',0)):,}**  "
                f"🔧 Processing: **{int(stats.get('processing',0)):,}**  "
                f"🚚 Shipped: **{int(stats.get('shipped',0)):,}**\n"
                f"✅ Delivered: **{int(stats.get('delivered',0)):,}**  "
                f"❌ Cancelled: **{int(stats.get('cancelled',0)):,}**\n"
                f"🚫 Cancel req: **{int(stats.get('cancel_requested',0)):,}**  "
                f"🔄 Return req: **{int(stats.get('return_requested',0)):,}**  "
                f"📦 Returned: **{int(stats.get('returned',0)):,}**\n\n"
                f"💰 Tổng doanh thu: **{float(stats.get('total_revenue',0)):,.0f}đ**"
            ), [], ["mysql:orders"]

        date_filter = "week" if any(kw in _mn_alp for kw in _WEEK_KW) else "today"
        label = "Đơn chờ & đang xử lý (7 ngày qua)" if date_filter == "week" else "Đơn chờ & đang xử lý hôm nay"
        orders = lookup_pending_orders(limit=15, date_filter=date_filter)
        if orders:
            context["last_order_list"] = [o["order_id"] for o in orders]
        if not orders:
            stats = get_all_time_order_stats()
            return (
                f"📭 Hôm nay chưa có đơn chờ mới.\n\n"
                f"📊 Tổng hệ thống: ⏳ Pending **{int(stats.get('pending',0)):,}** | "
                f"🔧 Processing **{int(stats.get('processing',0)):,}**"
            ), [], ["mysql:orders"]
        lines = [f"📋 **{label} ({len(orders)} đơn):**\n"]
        for o in orders:
            name = o.get("full_name") or o.get("username") or o.get("email", "?")
            amt  = float(o.get("total_amount") or 0)
            lines.append(
                f"  • Đơn **#{o['order_id']}** | {name} | "
                f"{amt:,.0f}đ | `{o['status']}`"
            )
        return "\n".join(lines), [], ["mysql:orders"]

    # ── ADMIN ORDER LIST BY STATUS ─────────────────────────────
    if intent == "admin_order_list_by_status":
        import unicodedata as _ud_albs
        _mn_albs = "".join(
            c for c in _ud_albs.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_albs.category(c) != "Mn"
        )
        _ST_KW = {
            "yeu cau huy": "cancel_requested", "cancel_requested": "cancel_requested", "xin huy": "cancel_requested",
            "dang giao": "shipped", "shipped": "shipped", "giao hang": "shipped", "van chuyen": "shipped",
            "that bai": "failed", "failed": "failed",
            "yeu cau tra": "return_requested", "return_requested": "return_requested",
            "da huy": "cancelled", "cancelled": "cancelled", "bi huy": "cancelled",
            "da giao": "delivered", "delivered": "delivered", "giao xong": "delivered",
            "hoan tra": "returned", "returned": "returned", "tra hang": "returned",
            "dang xu ly": "processing", "processing": "processing",
            "cho xu ly": "pending", "pending": "pending",
        }
        _ST_LABELS = {
            "cancel_requested": "🚫 Yêu cầu hủy", "shipped": "🚚 Đang giao hàng",
            "failed": "⚠️ Giao thất bại", "return_requested": "🔄 Yêu cầu trả hàng",
            "cancelled": "❌ Đã hủy", "delivered": "✅ Đã giao",
            "returned": "📦 Đã hoàn trả", "processing": "🔧 Đang xử lý", "pending": "⏳ Chờ xử lý",
        }
        target_status = next((st for kw, st in _ST_KW.items() if kw in _mn_albs), None)
        if not target_status:
            return (
                "Bạn muốn xem đơn theo trạng thái nào?\n"
                "Ví dụ: *\"đơn yêu cầu hủy\"*, *\"đơn đang giao\"*, *\"đơn thất bại\"*"
            ), [], []
        orders = lookup_pending_orders(limit=15, date_filter="all", status=target_status)
        label  = _ST_LABELS.get(target_status, target_status)
        if not orders:
            stats = get_all_time_order_stats()
            cnt   = int(stats.get(target_status, 0))
            return (
                f"📭 Không tìm thấy đơn nào đang **{label}** gần đây.\n"
                f"📊 Tổng toàn hệ thống: **{cnt:,}** đơn {label}"
            ), [], ["mysql:orders"]
        context["last_order_list"] = [o["order_id"] for o in orders]
        lines = [f"📋 **Danh sách đơn — {label} ({len(orders)} đơn):**\n"]
        for o in orders:
            name = o.get("full_name") or o.get("username") or o.get("email", "?")
            amt  = float(o.get("total_amount") or 0)
            lines.append(f"  • Đơn **#{o['order_id']}** | {name} | {amt:,.0f}đ")
        return "\n".join(lines), [], ["mysql:orders"]

    # ── ADMIN RETURN HANDLE ────────────────────────────────────
    if intent == "admin_return_handle":
        import re as _re_arh
        _rh_id = entities.get("order_id")
        if not _rh_id:
            _rh_m = _re_arh.search(r"#?(\d{3,6})", message)
            if _rh_m:
                _rh_id = int(_rh_m.group(1))
        if _rh_id:
            order = lookup_order_by_id(_rh_id)
            if not order:
                return f"❌ Không tìm thấy đơn hàng **#{_rh_id}**.", [], []
            cur_status = order.get("status", "")
            if cur_status != "return_requested":
                return (
                    f"⚠️ Đơn **#{_rh_id}** có trạng thái `{cur_status}` — "
                    f"không phải `return_requested`.\n"
                    f"Chỉ phê duyệt trả hàng cho đơn đang `return_requested`."
                ), [], ["mysql:orders"]
            name = order.get("recipient_name") or order.get("username") or order.get("email", "?")
            amt  = float(order.get("total_amount") or 0)
            context["pending_confirmation"] = {
                "action": "update_order_status", "order_id": _rh_id,
                "target_status": "returned", "current_status": "return_requested",
            }
            context["last_order_id"] = _rh_id
            return _with_confirm_buttons(
                f"🔄 **Xác nhận phê duyệt trả hàng?**\n"
                f"📦 Đơn **#{_rh_id}** | Khách: **{name}** | Giá trị: **{amt:,.0f}đ**\n"
                f"Thao tác: `return_requested` → `returned`",
                ["mysql:orders"]
            )
        # Xem danh sách hoàn trả
        all_issues  = lookup_return_requests(limit=30)
        return_req  = [o for o in all_issues if o.get("status") == "return_requested"]
        cancel_req  = [o for o in all_issues if o.get("status") == "cancel_requested"]
        import unicodedata as _ud_rh
        _mn_rh = "".join(
            c for c in _ud_rh.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_rh.category(c) != "Mn"
        )
        orders_to_show = cancel_req if any(kw in _mn_rh for kw in ["huy", "cancel"]) else return_req
        label = "Yêu cầu hủy đơn" if any(kw in _mn_rh for kw in ["huy", "cancel"]) else "Yêu cầu trả hàng"
        if orders_to_show:
            context["last_order_list"] = [o["order_id"] for o in orders_to_show]
        if not orders_to_show:
            return f"✅ Hiện không có **{label}** nào cần xử lý.", [], ["mysql:orders"]
        lines = [f"📋 **{label} ({len(orders_to_show)} đơn):**\n"]
        for o in orders_to_show:
            name = o.get("full_name") or o.get("username") or o.get("email", "?")
            amt  = float(o.get("total_amount") or 0)
            lines.append(f"  • Đơn **#{o['order_id']}** | {name} | {amt:,.0f}đ")
        lines.append("\n📌 Gõ `Xử lý trả hàng #ID` để phê duyệt từng đơn.")
        return "\n".join(lines), [], ["mysql:orders"]

    # ── ADMIN RETURN WORKFLOW ──────────────────────────────────
    if intent == "admin_return_workflow":
        import unicodedata as _ud_arw
        _mn_arw = "".join(
            c for c in _ud_arw.normalize("NFD", message.lower().replace("đ", "d"))
            if _ud_arw.category(c) != "Mn"
        )
        _DELIVERY_KW = ["giao hang", "van chuyen", "phi ship", "phi giao", "thoi gian giao",
                        "bao lau giao", "may ngay giao", "don vi van chuyen", "shipping",
                        "chinh sach giao", "quy dinh giao"]
        if any(kw in _mn_arw for kw in _DELIVERY_KW):
            return (
                "**Chính sách giao hàng tại BookStore:**\n\n"
                "⏰ **Thời gian giao hàng:**\n"
                "  • Nội thành HN/TP.HCM: **1-2 ngày làm việc**\n"
                "  • Tỉnh thành khác: **3-5 ngày làm việc**\n"
                "  • Vùng xa/hải đảo: **5-7 ngày làm việc**\n\n"
                "💰 **Phí giao hàng:**\n"
                "  • Miễn phí với đơn từ **150,000đ**\n"
                "  • Dưới 150,000đ: phí ship **15,000đ-30,000đ** tùy khu vực\n\n"
                "🚚 **Đơn vị vận chuyển:** GHTK, GHN, ViettelPost\n\n"
                "📞 Liên hệ CSKH nếu khách hỏi về ship: **0353260721** (8h–22h)"
            ), [], ["kb:delivery_policy"]
        return (
            "**Quy trình xử lý hoàn hàng tại BookStore:**\n\n"
            "**Bước 1 – Tiếp nhận yêu cầu:**\n"
            "  • Xác nhận đơn có trạng thái `return_requested`\n"
            "  • Kiểm tra lý do khách trả hàng (hỏng, sai sản phẩm, đổi ý)\n\n"
            "**Bước 2 – Duyệt hoàn hàng:**\n"
            "  • Nếu hợp lệ (≤7 ngày, còn nguyên vẹn) → Approve\n"
            "  • Chuyển trạng thái: `return_requested` → `returned`\n\n"
            "**Bước 3 – Hoàn tiền:**\n"
            "  • COD: Chuyển khoản lại trong 3-5 ngày làm việc\n"
            "  • Online: Hoàn về tài khoản gốc trong 5-7 ngày\n"
            "  • Thực hiện qua: **Admin Panel → Đơn hàng → Hoàn tiền**\n\n"
            "**Bước 4 – Nhập lại kho:**\n"
            "  • Sách trả về còn tốt → Cập nhật `stock_quantity + quantity`\n"
            "  • Sách bị hỏng → Ghi nhận vào báo cáo hàng hỏng\n\n"
            "📞 Liên hệ CSKH nếu khách khiếu nại: **0353260721** (8h–22h)"
        ), [], ["kb:return_policy"]

    # ── ADMIN INVENTORY CHECK ──────────────────────────────────
    if intent == "admin_inventory_check":
        book_id    = entities.get("book_id")
        import re as _re_aic
        identifier = str(book_id) if book_id else None
        if not identifier:
            # Thử trích tên sách từ context last_looked_up_book
            _ctx_book = context.get("last_looked_up_book") or {}
            identifier = (
                _ctx_book.get("title")
                or str(_ctx_book.get("book_id", "")) or None
            )
        if not identifier:
            # Tổng quan kho
            total_out = count_out_of_stock()
            total_low = count_low_stock(threshold=5)
            result = (
                f"📦 **Tổng quan tồn kho:**\n"
                f"⚠️ Hết hàng: **{total_out} sách**  |  🟡 Sắp hết (≤5): **{total_low} sách**\n\n"
            )
            if total_out > 0:
                for b in get_out_of_stock_books(limit=5):
                    result += f"  ⚠️ {b['title'][:55]}\n"
            if total_low > 0:
                for b in get_low_stock_books(threshold=5, limit=5):
                    result += f"  🟡 {b['title'][:50]} — còn {b['stock_quantity']} cuốn\n"
            result += "\nHỏi tên sách cụ thể để xem chi tiết."
            return result, [], ["mysql:books"]
        book = check_book_inventory(identifier)
        if book:
            context["last_looked_up_book"] = {"book_id": book["book_id"], "title": book["title"]}
            stock = int(book.get("stock_quantity") or 0)
            status_label = "✅ Còn hàng" if stock > 10 else ("⚠️ Sắp hết" if stock > 0 else "❌ Hết hàng")
            return (
                f"📦 **Tồn kho sách:**\n"
                f"📖 **{book['title']}** (ID: {book['book_id']})\n"
                f"👤 Tác giả: {book.get('author_name', 'N/A')}\n"
                f"💰 Giá: {float(book.get('price') or 0):,.0f}đ\n"
                f"📦 Tồn kho: **{stock} cuốn** — {status_label}\n"
                f"🏷️ Trạng thái: {book.get('status', 'N/A')}"
            ), [], ["mysql:books"]
        return f"❌ Không tìm thấy sách `{identifier}` trong hệ thống.", [], []

    # ── ADMIN INVENTORY LOW ────────────────────────────────────
    if intent == "admin_inventory_low":
        threshold = entities.get("threshold", 5)
        total_low = count_low_stock(threshold=threshold)
        total_out = count_out_of_stock()
        books_low = get_low_stock_books(threshold=threshold, limit=15)
        books_out = get_out_of_stock_books(limit=5)
        lines = [f"⚠️ **Cảnh báo tồn kho — Sắp hết hàng (≤{threshold} cuốn):**\n"]
        if not books_low and total_out == 0:
            return "✅ Tất cả sách đều còn hàng đầy đủ. Không có cảnh báo tồn kho.", [], ["mysql:books"]
        for b in books_low:
            lines.append(f"  🟡 **{b['title'][:55]}** — còn **{b['stock_quantity']} cuốn**")
        if total_low > len(books_low):
            lines.append(f"  ... và {total_low - len(books_low)} sách khác")
        if total_out > 0:
            lines.append(f"\n⚠️ **Hết hàng hoàn toàn ({total_out} sách):**")
            for b in books_out:
                lines.append(f"  🔴 **{b['title'][:55]}**")
        lines.append(f"\n💡 Gõ `Cập nhật tồn kho [tên sách] lên [N]` để nhập hàng.")
        return "\n".join(lines), [], ["mysql:books"]

    # ── ADMIN BOOK LOOKUP ──────────────────────────────────────
    if intent == "admin_book_lookup":
        book_id    = entities.get("book_id")
        identifier = str(book_id) if book_id else None
        if not identifier:
            import re as _re_abl
            _name_m = _re_abl.search(
                r"(?:tim|xem|check|tra cuu|thong tin|chi tiet|cuon|sach|book)\s+(.+?)(?:\s*$)",
                message, _re_abl.IGNORECASE
            )
            if _name_m:
                _raw = _name_m.group(1).strip()
                _skip = ["theo", "ma", "id", "ten", "isbn", "admin", "trong he thong"]
                if not any(_raw.lower().startswith(s) for s in _skip) and len(_raw) >= 3:
                    identifier = _raw
        if not identifier:
            _ctx = context.get("last_looked_up_book") or {}
            identifier = _ctx.get("title") or (str(_ctx.get("book_id")) if _ctx.get("book_id") else None)
        if not identifier:
            return "Vui lòng cung cấp **tên sách** hoặc **ID sách** để tra cứu.", [], []
        book = check_book_inventory(identifier)
        if book:
            context["last_looked_up_book"] = {"book_id": book["book_id"], "title": book["title"]}
            stock = int(book.get("stock_quantity") or 0)
            stock_label = "✅ Còn hàng" if stock > 10 else ("⚠️ Sắp hết" if stock > 0 else "❌ Hết hàng")
            return (
                f"📚 **Thông tin sách:**\n"
                f"📖 **{book['title']}** (ID: {book['book_id']})\n"
                f"👤 Tác giả: {book.get('author_name', 'N/A')}\n"
                f"💰 Giá: {float(book.get('price') or 0):,.0f}đ\n"
                f"📦 Tồn kho: **{stock} cuốn** — {stock_label}\n"
                f"🏷️ Trạng thái: {book.get('status', 'N/A')}"
            ), [], ["mysql:books"]
        return f"❌ Không tìm thấy sách `{identifier}` trong hệ thống.", [], []

    if intent == "admin_category_manage":
        cats = get_category_stats()
        if not cats:
            return "Chưa có danh mục nào.", []
        lines = ["**Thống kê danh mục:**"]
        for c in cats[:15]:
            lines.append(
                f"- **{c['category_name']}**: {c.get('book_count',0)} sách | "
                f"Rating: {float(c.get('avg_rating') or 0):.1f} | "
                f"Kho: {c.get('total_stock',0) or 0}"
            )
        return "\n".join(lines), ["mysql:categories"]

    if intent == "admin_book_low_rating":
        threshold = entities.get("rating_threshold", 3.0)
        books     = get_low_rating_books(threshold=threshold)
        return fmt_low_rating_books(books), ["mysql:books"]

    # ── NHÓM A-D: KHUYẾN MÃI ════════════════════════════════

    if intent == "admin_promotion_list":
        promo_status = entities.get("promo_status", "active")
        if promo_status == "deleted":
            promo_status = "inactive"
            
        promos_data = get_promotions(status=promo_status)
        total_count = promos_data[0]
        promos = promos_data[1]
        
        if promo_status == "inactive":
            if total_count == 0:
                return "ℹ️ Không có khuyến mãi nào đang không hoạt động.", []
            lines = [f"🎟️ **Tổng quan:** Hiện có **{total_count} mã khuyến mãi không hoạt động**.", ""]
            lines.append(f"Danh sách {len(promos)} mã gần nhất:\n")
        else:
            if total_count == 0:
                return "ℹ️ Không có khuyến mãi nào đang active.", []
            lines = [f"🎟️ **Tổng quan:** Hiện có **{total_count} mã khuyến mãi đang hoạt động**.", ""]
            lines.append(f"Danh sách {len(promos)} mã gần nhất:\n")
            
        for p in promos:
            days     = p.get("days_remaining", 0)
            if days < 0:
                lines.append(
                    f"• `{p['code']}` — Giảm **{p.get('discount_percent',0):.0f}%**\n"
                    f"  HSD: {str(p.get('end_date','?'))}\n"
                )
            else:
                exp_warn = " ⚠️ [SẮP HẾT HẠN]" if days <= 7 and promo_status == "active" else ""
                lines.append(
                    f"• `{p['code']}` — Giảm **{p.get('discount_percent',0):.0f}%**\n"
                    f"  HSD: {str(p.get('end_date','?'))}{exp_warn} (còn {days} ngày)\n"
                )
        return "\n".join(lines).strip(), ["mysql:promotions"]

    if intent == "admin_promotion_check":
        code = entities.get("voucher_code") or _extract_voucher_from_message(message)
        if not code:
            return "Vui lòng cung cấp **mã voucher** để kiểm tra. Ví dụ: `Kiểm tra mã SALE30`", []
        voucher = check_voucher_code(code)
        return fmt_voucher_check(voucher, code), ["mysql:promotions"]

    if intent == "admin_promotion_create_guide":
        return (
            "**Hướng dẫn tạo Promotion mới:**\n\n"
            "1. Vào **Admin Panel → Khuyến mãi → Tạo mới**\n"
            "2. Điền: Mã code (VD: SALE30), % giảm, Ngày bắt đầu, Ngày kết thúc\n"
            "3. Click **Lưu** để kích hoạt\n\n"
            "Mã phải duy nhất và viết IN HOA (VD: `BOOKFEST50`)\n"
            "Giảm giá tối đa: 100% (nhập 100 = miễn phí)",
            []
        )

    if intent == "admin_promotion_expiring":
        import re
        m = re.search(r"(\d+)\s*(ngày|ngay)", message, re.IGNORECASE)
        days = int(m.group(1)) if m else 7
            
        promos = get_expiring_promotions(days=days)
        if not promos:
            return f"Không có promotion nào sắp hết hạn trong {days} ngày tới.", []
        lines = [f"**Promotion sắp hết hạn trong {days} ngày tới:**"]
        for p in promos:
            lines.append(
                f"- `{p['code']}` – {p.get('discount_percent',0):.0f}% | "
                f"Hết hạn: {str(p.get('end_date','?'))} (**{p.get('days_remaining',0)} ngày**)"
            )
        return "\n".join(lines), ["mysql:promotions"]

    # ── NHÓM A-E: HỆ THỐNG ════════════════════════════════════

    if intent == "admin_system_health":
        health = await check_system_health()
        return fmt_system_health(health), ["system:health"]

    if intent == "admin_escalated_customers":
        returns = lookup_return_requests(limit=10)
        if not returns:
            return "Không có vấn đề nào đang chờ xử lý.", []
        lines = [f"**Vấn đề escalated** ({len(returns)} đơn):"]
        for o in returns:
            lines.append(
                f"- Đơn **#{o['order_id']}** | {o['status']} | "
                f"{str(o.get('created_at','?'))[:10]} | "
                f"{o.get('username', o.get('email','?'))}"
            )
        return "\n".join(lines), ["mysql:orders"]

    # ── CHITCHAT / HELP ───────────────────────────────────────
    if intent == "admin_chitchat":
        return ADMIN_HELP_MSG, []

    # ── HARD BLOCK: password / credential request ─────────────
    _SENSITIVE_KWS = ["mat khau", "password", "passwd", "ma pin", "token", "secret_key",
                      "api_key", "access_key", "encryption_key", "private_key"]
    if any(kw in _mn_a for kw in _SENSITIVE_KWS):
        return (
            "⛔ **Thông tin bảo mật (mật khẩu, token, key) được mã hóa và không thể truy cập qua chatbot.**\n\n"
            "Để reset mật khẩu người dùng, dùng:\n"
            "→ **Admin Panel → Quản lý Users → [User ID] → Reset Password**\n\n"
            "Chatbot không bao giờ hiển thị mật khẩu hoặc thông tin xác thực."
        ), []

    # ── OUT OF SCOPE / DESTRUCTIVE BLOCK ──────────────────────
    if intent == "admin_out_of_scope":
        _msg_low = message.lower()
        if "blacklist" in _msg_low or "ip " in _msg_low or "chan ip" in _msg_low:
            return (
                "⛔ **Thao tác bảo mật bị từ chối.**\n\n"
                "Việc chặn IP (Blacklist) yêu cầu quyền can thiệp sâu ở cấp độ Firewall hoặc WAF của Server.\n"
                "Vui lòng thực hiện cấu hình này trực tiếp qua Nginx/Apache hoặc hệ thống Security Group của server."
            ), []
        
        return (
            "⛔ **Không thể thực hiện thao tác xóa hàng loạt qua chatbot.**\n\n"
            "Các thao tác xóa/hủy dữ liệu hàng loạt cần được thực hiện trực tiếp qua:\n"
            "→ **Admin Panel → Quản lý Đơn hàng/Users** (với xác thực 2FA)\n\n"
            "Chatbot chỉ hỗ trợ: xem thống kê, tra cứu, và các thao tác đơn lẻ có xác nhận."
        ), []

    return (
        "Tôi không hiểu yêu cầu này.\n"
        "Gõ `help` để xem danh sách các lệnh admin.",
        []
    )



# ── Confirmation Handler ──────────────────────────────────────
def _handle_admin_confirmation(intent: str, context: dict) -> str:
    pending = context.get("pending_confirmation", {})
    action  = pending.get("action")

    if intent == "confirmation_yes":
        context.pop("pending_confirmation", None)

        if action == "update_user_role":
            target = pending.get("target_user")
            role   = pending.get("target_role")
            # ── Thực thi DB thật sự ──
            result = admin_update_user_role(str(target), role)
            if result.get("success"):
                return (
                    f"✅ **Đã đổi role thành công!**\n"
                    f"User `{result.get('username', target)}` (ID: {result.get('user_id')})\n"
                    f"Role: `{result.get('old_role')}` → `{role}`"
                )
            return (
                f"❌ **Đổi role thất bại.**\n"
                f"Lý do: {result.get('message')}\n"
                f"→ Thực hiện thủ công: **Admin Panel → Quản lý Users → [User ID] → Đổi Role**"
            )

        if action == "update_user_status":
            target = pending.get("target_user")
            status = pending.get("target_status")
            label  = "Đã khóa" if status == "locked" else "Đã mở khóa"
            # ── Thực thi DB thật sự ──
            result = admin_update_user_status(str(target), status)
            if result.get("success"):
                return (
                    f"✅ **{label} tài khoản thành công!**\n"
                    f"User `{result.get('username', target)}` (ID: {result.get('user_id')}) → `{status}`"
                )
            return (
                f"❌ **{label} thất bại.**\n"
                f"Lý do: {result.get('message')}\n"
                f"→ Thực hiện thủ công: **Admin Panel → Quản lý Users → [User ID] → Đổi Status**"
            )

        if action == "reset_user_password":
            u = pending.get("target_user")
            return (
                f"⚠️ **Reset mật khẩu** không thể thực hiện qua chatbot (bảo mật).\n"
                f"→ Thực hiện tại: **Admin Panel → Quản lý Users → [{u}] → Reset Password**"
            )

        if action == "update_book_status":
            bid, s = pending.get("book_id"), pending.get("target_status")
            return (
                f"⚠️ Đổi trạng thái sách #{bid} → `{s}` cần thực hiện qua:\n"
                f"→ **Admin Panel → Quản lý Sách → [Sách #{bid}] → Đổi Status**"
            )

        if action == "update_book_stock":
            book_id    = pending.get("book_id")
            new_qty    = pending.get("new_qty")
            old_qty    = pending.get("old_qty", 0)
            book_title = pending.get("book_title", f"ID #{book_id}")
            # Dùng lại hàm update_book_inventory của staff
            result = update_book_inventory(int(book_id), int(new_qty))
            if result.get("success"):
                diff  = int(new_qty) - int(old_qty or 0)
                arrow = f"+{diff}" if diff > 0 else str(diff)
                return (
                    f"✅ **Đã cập nhật tồn kho thành công!**\n"
                    f"📖 Sách: **{book_title}** (ID: {book_id})\n"
                    f"📦 Tồn kho: **{old_qty} cuốn** → **{new_qty} cuốn** ({arrow})"
                )
            return f"❌ Cập nhật thất bại: {result.get('message')}"

        if action == "update_order_status":
            order_id       = pending.get("order_id")
            target_status  = pending.get("target_status")
            current_status = pending.get("current_status", "?")
            result = update_order_status(order_id, target_status)
            if result.get("success"):
                return (
                    f"✅ **Đã cập nhật trạng thái đơn hàng thành công!**\n"
                    f"📦 Đơn **#{order_id}**: `{current_status}` → `{target_status}`"
                )
            return f"❌ Cập nhật thất bại: {result.get('message')}"

        return "Đã xác nhận thao tác."

    elif intent == "confirmation_no":
        context.pop("pending_confirmation", None)
        return "Đã hủy. Không có thay đổi nào được thực hiện."

    return "Bạn chưa xác nhận. Vui lòng nhấn **Có** hoặc **Không**."


# ── Helpers ───────────────────────────────────────────────────
def _extract_query_from_message(message: str) -> str:
    """Trích email hoặc từ khóa tìm kiếm từ message."""
    import re
    email = re.search(r"[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}", message)
    if email:
        return email.group(0)
    after = re.search(
        r"(?:tìm|tra cứu|xem|user|tài khoản|người dùng)\s+(.+?)(?:\s*$)",
        message, re.IGNORECASE
    )
    return after.group(1).strip() if after else ""


def _extract_voucher_from_message(message: str) -> str:
    """Trích mã voucher (chuỗi IN HOA gồm chữ và số) từ message."""
    import re
    match = re.search(r"\b([A-Z][A-Z0-9]{2,19})\b", message)
    return match.group(1) if match else ""
