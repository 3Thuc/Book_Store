"""
staff_agents.py – Các agent truy vấn dữ liệu cho Staff chatbot.

Agents:
  - OrderLookupAgent   : tra cứu đơn hàng theo nhiều điều kiện
  - OrderUpdateAgent   : cập nhật trạng thái đơn (cần confirm flow)
  - InventoryAgent     : kiểm tra tồn kho, cảnh báo hàng sắp hết
  - StatisticsAgent    : thống kê nhanh đơn/doanh thu hôm nay/tuần
  - CustomerLookupAgent: tra cứu thông tin và lịch sử mua của khách

NOTE: Tất cả hàm đều có try/except để tránh crash khi DB lỗi.
      Trả về None / [] / {} thay vì throw exception.
"""
from chatbot_app.db import get_connection
from datetime import date, timedelta, datetime
import traceback


def _safe_query(query_fn):
    """Decorator bắt mọi DB exception, log và trả về None."""
    def wrapper(*args, **kwargs):
        try:
            return query_fn(*args, **kwargs)
        except Exception as e:
            print(f"⚠️ [DB ERROR] {query_fn.__name__}: {e}")
            traceback.print_exc()
            return None
    return wrapper


# ═══════════════════════════════════════════════════════════════
#  ORDER LOOKUP AGENT
# ═══════════════════════════════════════════════════════════════

def lookup_order_by_id(order_id: int) -> dict | None:
    """Lấy chi tiết đơn hàng kèm danh sách sách."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    o.order_id,
                    o.status,
                    o.payment_method,
                    o.payment_status,
                    o.subtotal,
                    o.discount_amount,
                    COALESCE((SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id), o.total_amount) AS total_amount,
                    o.note,
                    o.order_date,
                    o.created_at,
                    u.username,
                    u.email,
                    u.phone AS user_phone,
                    a.address_text AS shipping_address,
                    a.district AS shipping_district,
                    a.city AS shipping_city,
                    a.phone_number AS address_phone,
                    a.recipient_name,
                    p.code AS promo_code
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.user_id
                LEFT JOIN addresses a ON o.address_id = a.address_id
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE o.order_id = %s
                LIMIT 1
            """, (order_id,))
            order = cur.fetchone()
            if not order:
                return None

            cur.execute("""
                SELECT
                    od.quantity,
                    od.unit_price AS unit_price,
                    b.title,
                    b.book_id
                FROM order_details od
                JOIN books b ON od.book_id = b.book_id
                WHERE od.order_id = %s
            """, (order_id,))
            order["items"] = cur.fetchall()
            return order
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_order_by_id] DB error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
#  ORDER UPDATE AGENT (WRITE)
# ═══════════════════════════════════════════════════════════════

def _evict_java_cache_async(order_id: int, new_status: str) -> None:
    """
    Gọi Java Spring Boot POST /admin/cache/evict để xóa Spring @Cacheable (adminOrders, adminDashboard).
    Từ trong Docker container, host machine = host.docker.internal (Docker Desktop Windows/Mac).
    Chạy background thread — không block chatbot response.
    """
    import threading
    def _call():
        try:
            import ssl
            import urllib.request
            from chatbot_app.config import ADMIN_API_KEY

            api_key = ADMIN_API_KEY or "bookstore-internal-key"
            # host.docker.internal → host machine từ bên trong Docker container (Docker Desktop)
            java_url = "https://host.docker.internal:8443/bookdb/admin/cache/evict"

            req = urllib.request.Request(
                java_url,
                data=b"{}",
                headers={
                    "Content-Type": "application/json",
                    "X-Admin-Key": api_key,
                },
                method="POST"
            )
            # SSL self-signed cert → bỏ qua verify
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            with urllib.request.urlopen(req, context=ctx, timeout=5) as resp:
                print(f"[cache-evict] ✅ Spring cache evicted cho đơn #{order_id} → {new_status} (HTTP {resp.status})")
        except Exception as e:
            print(f"[cache-evict] ⚠️ Không thể evict Spring cache: {e}")

    threading.Thread(target=_call, daemon=True).start()


def update_order_status(order_id: int, new_status: str) -> dict:
    """
    Cập nhật trạng thái đơn hàng vào DB.
    Trả về dict: {success: bool, message: str, old_status: str | None}
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            # Lấy trạng thái hiện tại để log
            cur.execute(
                "SELECT status FROM orders WHERE order_id = %s",
                (order_id,)
            )
            row = cur.fetchone()
            if not row:
                return {"success": False, "message": f"Không tìm thấy đơn #{order_id}.", "old_status": None}

            old_status = row["status"]
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = NOW() WHERE order_id = %s",
                (new_status, order_id)
            )
            conn.commit()
            affected = cur.rowcount
            if affected > 0:
                # Trigger Java @CacheEvict ngầm để FE nhận data mới ngay
                _evict_java_cache_async(order_id, new_status)
                return {"success": True, "message": "OK", "old_status": old_status}
            return {"success": False, "message": "Không có dòng nào được cập nhật.", "old_status": old_status}
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [update_order_status] DB error: {e}")
        traceback.print_exc()
        return {"success": False, "message": str(e), "old_status": None}


def lookup_orders_by_status(status: str, limit: int = 20) -> list[dict]:
    """Lấy danh sách đơn hàng theo trạng thái."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    o.order_id,
                    o.status,
                    COALESCE((SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id), o.total_amount) AS total_amount,
                    o.payment_method,
                    o.created_at,
                    u.username,
                    u.email
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.user_id
                WHERE o.status = %s
                ORDER BY o.created_at DESC
                LIMIT %s
            """, (status, limit))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_orders_by_status] DB error: {e}")
        return []


def lookup_pending_orders(limit: int = 15, date_filter: str = "today", status: str = None) -> list[dict]:
    """Danh sách đơn theo trạng thái.

    Args:
        limit: số đơn tối đa trả về (mặc định 15)
        date_filter: 'today' (hôm nay) | 'week' (7 ngày) | 'all' (toàn bộ)
        status: lọc theo trạng thái cụ thể (None = pending + processing)
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        today = date.today()

        # Xây dựng điều kiện lọc ngày
        if date_filter == "today":
            date_clause = "AND DATE(o.order_date) = %s"
            params = (today, limit)
        elif date_filter == "week":
            week_start = today - timedelta(days=6)
            date_clause = "AND DATE(o.order_date) >= %s"
            params = (week_start, limit)
        else:  # 'all' - toàn bộ lịch sử
            date_clause = ""
            params = (limit,)

        # Lọc theo trạng thái cụ thể hoặc mặc định pending+processing
        if status:
            status_clause = f"o.status = '{status}'"
        else:
            status_clause = "o.status IN ('pending', 'processing')"

        try:
            cur.execute(f"""
                SELECT
                    o.order_id,
                    o.status,
                    COALESCE(SUM(od.total_price), o.total_amount) AS total_amount,
                    o.payment_method,
                    o.order_date,
                    COALESCE(u.full_name, u.username) AS full_name,
                    u.username,
                    u.email,
                    COUNT(od.book_id) AS item_count
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.user_id
                LEFT JOIN order_details od ON o.order_id = od.order_id
                WHERE {status_clause}
                {date_clause}
                GROUP BY o.order_id, u.full_name, u.username, u.email
                ORDER BY o.order_date DESC
                LIMIT %s
            """, params)
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_pending_orders] DB error: {e}")
        return []


def lookup_return_requests(limit: int = 15) -> list[dict]:
    """Danh sách đơn đang yêu cầu đổi/trả."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    o.order_id,
                    o.status,
                    COALESCE(SUM(od.total_price), o.total_amount) AS total_amount,
                    o.order_date,
                    COALESCE(u.full_name, u.username) AS full_name,
                    u.username,
                    u.email,
                    u.phone
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.user_id
                LEFT JOIN order_details od ON o.order_id = od.order_id
                WHERE o.status IN ('cancel_requested', 'return_requested')
                GROUP BY o.order_id, u.full_name, u.username, u.email, u.phone
                ORDER BY o.order_date DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_return_requests] DB error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
#  INVENTORY AGENT
# ═══════════════════════════════════════════════════════════════

def check_book_inventory(identifier: str) -> dict | None:
    """Kiểm tra tồn kho sách theo tên hoặc ID."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if str(identifier).isdigit():
                cur.execute("""
                    SELECT b.book_id, b.title, b.stock_quantity, b.price,
                           b.status, a.author_name
                    FROM books b LEFT JOIN authors a ON b.author_id = a.author_id
                    WHERE b.book_id = %s
                """, (int(identifier),))
                result = cur.fetchone()
                return result
            else:
                # [FIX BUG-E] Exact match trước, LIKE fallback sau để tránh trả sai sách
                cur.execute("""
                    SELECT b.book_id, b.title, b.stock_quantity, b.price,
                           b.status, a.author_name
                    FROM books b LEFT JOIN authors a ON b.author_id = a.author_id
                    WHERE b.title = %s AND b.status != 'deleted'
                    LIMIT 1
                """, (identifier,))
                result = cur.fetchone()
                if not result:
                    # Fallback: LIKE fuzzy match – ưu tiên title ngắn/exact
                    cur.execute("""
                        SELECT b.book_id, b.title, b.stock_quantity, b.price,
                               b.status, a.author_name
                        FROM books b LEFT JOIN authors a ON b.author_id = a.author_id
                        WHERE b.title LIKE %s AND b.status != 'deleted'
                        ORDER BY CASE WHEN b.title = %s THEN 0 ELSE 1 END,
                                 LENGTH(b.title) ASC
                        LIMIT 1
                    """, (f"%{identifier}%", identifier))
                    result = cur.fetchone()
                return result

        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [check_book_inventory] DB error: {e}")
        return None

def search_book_inventory(identifier: str) -> list[dict]:
    """Tìm tất cả sách khớp với tên hoặc ID để kiểm tra tồn kho (trả về list)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if str(identifier).isdigit():
                cur.execute("""
                    SELECT b.book_id, b.title, b.stock_quantity, b.price,
                           b.status, a.author_name
                    FROM books b LEFT JOIN authors a ON b.author_id = a.author_id
                    WHERE b.book_id = %s
                """, (int(identifier),))
                return cur.fetchall()
            else:
                cur.execute("""
                    SELECT b.book_id, b.title, b.stock_quantity, b.price,
                           b.status, a.author_name
                    FROM books b LEFT JOIN authors a ON b.author_id = a.author_id
                    WHERE b.title LIKE %s AND b.status != 'deleted'
                    ORDER BY CASE WHEN b.title = %s THEN 0 ELSE 1 END,
                             LENGTH(b.title) ASC
                    LIMIT 50
                """, (f"%{identifier}%", identifier))
                return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [search_book_inventory] DB error: {e}")
        return []


def update_book_inventory(book_id: int, new_quantity: int) -> dict:
    """Cập nhật tồn kho sách trực tiếp vào DB.

    Args:
        book_id: ID sách cần cập nhật
        new_quantity: số lượng mới (không âm)
    Returns:
        dict với 'success', 'old_quantity', 'new_quantity', 'title'
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            # Lấy thông tin hiện tại
            cur.execute("""
                SELECT book_id, title, stock_quantity FROM books WHERE book_id = %s
            """, (book_id,))
            book = cur.fetchone()
            if not book:
                return {"success": False, "message": f"Không tìm thấy sách ID {book_id}"}

            old_qty = int(book["stock_quantity"] or 0)
            # Thực hiện update
            cur.execute("""
                UPDATE books SET stock_quantity = %s WHERE book_id = %s
            """, (new_quantity, book_id))
            conn.commit()
            print(f"✅ [update_book_inventory] Book {book_id} stock: {old_qty} → {new_quantity}")
            return {
                "success": True,
                "book_id": book_id,
                "title": book["title"],
                "old_quantity": old_qty,
                "new_quantity": new_quantity,
            }
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [update_book_inventory] DB error: {e}")
        return {"success": False, "message": str(e)}


def get_low_stock_books(threshold: int = 5, limit: int = 20) -> list[dict]:
    """Lấy sách có available_quantity 1..threshold (sắp hết, chưa hết hẳn)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT * FROM (
                    SELECT b.book_id, b.title, b.price, a.author_name,
                           (b.stock_quantity - COALESCE((
                               SELECT SUM(od.quantity)
                               FROM order_details od
                               JOIN orders o ON od.order_id = o.order_id
                               WHERE od.book_id = b.book_id AND o.status IN ('pending', 'processing', 'shipped')
                           ), 0)) AS stock_quantity,
                           GROUP_CONCAT(c.category_name SEPARATOR ', ') AS categories
                    FROM books b
                    LEFT JOIN authors a ON b.author_id = a.author_id
                    LEFT JOIN book_categories bc ON b.book_id = bc.book_id
                    LEFT JOIN categories c ON bc.category_id = c.category_id
                    WHERE b.status = 'active'
                    GROUP BY b.book_id, b.title, b.stock_quantity, b.price, a.author_name
                ) as temp
                WHERE stock_quantity BETWEEN 1 AND %s
                ORDER BY stock_quantity ASC
                LIMIT %s
            """, (threshold, limit))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_low_stock_books] DB error: {e}")
        return []


def get_out_of_stock_books(limit: int = 10) -> list[dict]:
    """Lấy sách đã hết hàng hoàn toàn (available_quantity <= 0)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT * FROM (
                    SELECT b.book_id, b.title, b.price, a.author_name,
                           (b.stock_quantity - COALESCE((
                               SELECT SUM(od.quantity)
                               FROM order_details od
                               JOIN orders o ON od.order_id = o.order_id
                               WHERE od.book_id = b.book_id AND o.status IN ('pending', 'processing', 'shipped')
                           ), 0)) AS stock_quantity
                    FROM books b
                    LEFT JOIN authors a ON b.author_id = a.author_id
                    WHERE b.status = 'active'
                ) as temp
                WHERE stock_quantity <= 0
                ORDER BY stock_quantity ASC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_out_of_stock_books] DB error: {e}")
        return []


def count_out_of_stock() -> int:
    """Đếm tổng số sách hết hàng (available_quantity <= 0)."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT b.book_id,
                           (b.stock_quantity - COALESCE((
                               SELECT SUM(od.quantity)
                               FROM order_details od
                               JOIN orders o ON od.order_id = o.order_id
                               WHERE od.book_id = b.book_id AND o.status IN ('pending', 'processing', 'shipped')
                           ), 0)) AS available_quantity
                    FROM books b
                    WHERE b.status = 'active'
                ) as temp
                WHERE available_quantity <= 0
            """)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [count_out_of_stock] DB error: {e}")
        return 0


def count_low_stock(threshold: int = 5) -> int:
    """Đếm tổng số sách sắp hết hàng (1 <= available_quantity <= threshold)."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        try:
            cur.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT b.book_id,
                           (b.stock_quantity - COALESCE((
                               SELECT SUM(od.quantity)
                               FROM order_details od
                               JOIN orders o ON od.order_id = o.order_id
                               WHERE od.book_id = b.book_id AND o.status IN ('pending', 'processing', 'shipped')
                           ), 0)) AS available_quantity
                    FROM books b
                    WHERE b.status = 'active'
                ) as temp
                WHERE available_quantity BETWEEN 1 AND %s
            """, (threshold,))
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [count_low_stock] DB error: {e}")
        return 0




# ═══════════════════════════════════════════════════════════════
#  STATISTICS AGENT
# ═══════════════════════════════════════════════════════════════

def get_all_time_order_stats() -> dict:
    """Thống kê tổng số đơn theo trạng thái — toàn bộ lịch sử (không lọc ngày)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_orders,
                    SUM(CASE WHEN o.status = 'pending'          THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN o.status = 'processing'       THEN 1 ELSE 0 END) AS processing,
                    SUM(CASE WHEN o.status = 'shipped'          THEN 1 ELSE 0 END) AS shipped,
                    SUM(CASE WHEN o.status = 'delivered'        THEN 1 ELSE 0 END) AS delivered,
                    SUM(CASE WHEN o.status = 'cancelled'        THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN o.status = 'cancel_requested' THEN 1 ELSE 0 END) AS cancel_requested,
                    SUM(CASE WHEN o.status = 'return_requested' THEN 1 ELSE 0 END) AS return_requested,
                    SUM(CASE WHEN o.status = 'returned'         THEN 1 ELSE 0 END) AS returned,
                    COALESCE(SUM(
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * (1 - COALESCE(p.discount_percent, 0) / 100)
                        * CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN 1 ELSE 0 END
                    ), 0) AS total_revenue,
                    COALESCE(SUM(CASE WHEN o.status = 'delivered' THEN
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * (1 - COALESCE(p.discount_percent, 0) / 100)
                    ELSE 0 END), 0) AS delivered_revenue
                FROM orders o
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
            """)
            row = cur.fetchone() or {}
            return row
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_all_time_order_stats] DB error: {e}")
        return {}


def get_today_order_stats() -> dict:
    """Thống kê đơn hàng hôm nay."""
    today = date.today()
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_orders,
                    SUM(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
                    SUM(CASE WHEN o.status IN ('pending','processing') THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN o.status = 'return_requested' THEN 1 ELSE 0 END) AS return_req,
                    COALESCE(SUM(
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * (1 - COALESCE(p.discount_percent, 0) / 100)
                        * CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN 1 ELSE 0 END
                    ), 0) AS total_revenue,
                    COALESCE(SUM(CASE WHEN o.status = 'delivered' THEN
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * (1 - COALESCE(p.discount_percent, 0) / 100)
                    ELSE 0 END), 0) AS confirmed_revenue
                FROM orders o
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE DATE(DATE_ADD(o.order_date, INTERVAL 7 HOUR)) = %s
            """, (today,))
            row = cur.fetchone()
            if row:
                row["date"] = str(today)
                return row
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_today_order_stats] DB error: {e}")
    return {
        "date": str(today), "total_orders": 0, "delivered": 0,
        "pending": 0, "cancelled": 0, "return_req": 0,
        "total_revenue": 0, "confirmed_revenue": 0,
    }


def get_period_revenue(date_range: str = "today") -> dict:
    """Doanh thu theo khoảng thời gian.
    Hỗ trợ: today / yesterday / week / this_week / last_week / month / this_month
            month:M/YYYY / quarter:Q/YYYY / year:YYYY / day:YYYY-MM-DD
    """
    today = date.today()

    # ── Parse period ──────────────────────────────────────────────────────────
    if date_range in ("week", "this_week"):
        start = today - timedelta(days=today.weekday())  # Thứ 2 tuần này
        period_label = "Tuần này"
    elif date_range == "last_week":
        this_week_start = today - timedelta(days=today.weekday())
        start = this_week_start - timedelta(days=7)
        today = this_week_start - timedelta(days=1)  # Chủ nhật tuần trước
        period_label = "Tuần trước"
    elif date_range in ("month", "this_month"):
        start = today.replace(day=1)
        period_label = "Tháng này"
    elif date_range == "yesterday":
        start = today - timedelta(days=1)
        today = start  # cả start và end đều là hôm qua
        period_label = "Hôm qua"
    elif date_range.startswith("month:"):
        # Format: "month:1/2026" hoặc "month:01/2026"
        try:
            parts = date_range[6:].split("/")
            m, y = int(parts[0]), int(parts[1])
            start = date(y, m, 1)
            # End = ngày cuối tháng
            if m == 12:
                end_day = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                end_day = date(y, m + 1, 1) - timedelta(days=1)
            today = end_day
            period_label = f"Tháng {m}/{y}"
        except Exception:
            start = today.replace(day=1)
            period_label = "Tháng này"
    elif date_range.startswith("quarter:"):
        # Format: "quarter:1/2026"
        try:
            parts = date_range[8:].split("/")
            q, y = int(parts[0]), int(parts[1])
            start_month = (q - 1) * 3 + 1
            start = date(y, start_month, 1)
            end_month = start_month + 2
            if end_month > 12:
                end_day = date(y + 1, 1, 1) - timedelta(days=1)
            else:
                end_day = date(y, end_month + 1, 1) - timedelta(days=1)
            today = end_day
            period_label = f"Q{q}/{y}"
        except Exception:
            start = today.replace(day=1)
            period_label = "Tháng này"
    elif date_range.startswith("year:"):
        # Format: "year:2025"
        try:
            y = int(date_range[5:])
            start = date(y, 1, 1)
            today = date(y, 12, 31)
            period_label = f"Năm {y}"
        except Exception:
            start = date(today.year, 1, 1)
            period_label = f"Năm {today.year}"
    elif date_range.startswith("day:"):
        # Format: "day:2026-05-01"
        try:
            d = date.fromisoformat(date_range[4:])
            start = d
            today = d
            period_label = d.strftime("%d/%m/%Y")
        except Exception:
            start = today
            period_label = "Hôm nay"
    elif date_range.startswith("range:"):
        # Format: "range:2026-01-01/2026-03-31"
        try:
            parts = date_range[6:].split("/")
            start = date.fromisoformat(parts[0])
            today = date.fromisoformat(parts[1])
            period_label = f"{parts[0]} → {parts[1]}"
        except Exception:
            start = today.replace(day=1)
            period_label = "Kỳ này"
    else:  # today / default
        start = today
        period_label = "Hôm nay"

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_orders,
                    COALESCE(SUM(
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * (1 - COALESCE(p.discount_percent, 0) / 100)
                        * CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN 1 ELSE 0 END
                    ), 0) AS total_revenue,
                    COALESCE(SUM(CASE WHEN o.status = 'delivered' THEN
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * (1 - COALESCE(p.discount_percent, 0) / 100)
                    ELSE 0 END), 0) AS delivered_revenue,
                    COUNT(DISTINCT o.user_id) AS unique_customers,
                    SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    SUM(CASE WHEN o.status IN ('return_requested','returned') THEN 1 ELSE 0 END) AS returned_count,
                    SUM(CASE WHEN o.payment_method = 'cod' THEN 1 ELSE 0 END) AS cod_count,
                    SUM(CASE WHEN o.payment_method != 'cod' THEN 1 ELSE 0 END) AS online_count,
                    COALESCE(SUM(CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN
                        (SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id)
                        * COALESCE(p.discount_percent, 0) / 100
                    ELSE 0 END), 0) AS total_discounts
                FROM orders o
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE DATE(DATE_ADD(o.order_date, INTERVAL 7 HOUR)) BETWEEN %s AND %s
            """, (start, today))
            row = cur.fetchone() or {}
            total = int(row.get("total_orders") or 0)
            cancelled = int(row.get("cancelled_count") or 0)
            row["cancel_rate"]  = round(cancelled / total * 100, 1) if total > 0 else 0.0
            row["period"]       = date_range
            row["period_label"] = period_label
            row["from_date"]    = str(start)
            row["to_date"]      = str(today)
            return row
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_period_revenue] DB error: {e}")
    return {
        "period": date_range, "period_label": period_label,
        "from_date": str(start), "to_date": str(today),
        "total_orders": 0, "total_revenue": 0, "delivered_revenue": 0,
        "unique_customers": 0, "cancelled_count": 0, "returned_count": 0,
        "cancel_rate": 0.0, "cod_count": 0, "online_count": 0, "total_discounts": 0,
    }


def get_top_selling_books(limit: int = 10) -> list[dict]:
    """Sách bán chạy nhất (dựa trên order_details)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    b.book_id, b.title, a.author_name,
                    b.price,
                    SUM(od.quantity) AS total_sold,
                    SUM(od.quantity * od.unit_price) AS total_revenue,
                    b.avg_rating
                FROM order_details od
                JOIN books b ON od.book_id = b.book_id
                LEFT JOIN authors a ON b.author_id = a.author_id
                JOIN orders o ON od.order_id = o.order_id
                WHERE o.status IN ('delivered', 'shipped', 'processing')
                GROUP BY b.book_id, b.title, a.author_name, b.price, b.avg_rating
                ORDER BY total_sold DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_top_selling_books] DB error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
#  CUSTOMER LOOKUP AGENT
# ═══════════════════════════════════════════════════════════════

def lookup_customer_by_email(email: str) -> dict | None:
    """Tra cứu thông tin khách hàng theo email."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT u.user_id, u.username, u.full_name, u.email, u.phone,
                       u.role, u.status, u.created_at,
                       COUNT(DISTINCT o.order_id) AS total_orders,
                       COALESCE(SUM(o.total_amount), 0) AS total_spent
                FROM users u
                LEFT JOIN orders o ON u.user_id = o.user_id
                WHERE u.email LIKE %s
                GROUP BY u.user_id
                LIMIT 1
            """, (f"%{email}%",))
            return cur.fetchone()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_customer_by_email] DB error: {e}")
        return None


def lookup_customer_by_name(name: str) -> list[dict]:
    """Tra cứu khách hàng theo tên (full_name hoặc username), trả về tối đa 5 kết quả.
    Nếu không khớp tên đầy đủ, tự động thử từng từ (fuzzy fallback).
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            # 1) Thử tìm tên đầy đủ trước
            cur.execute("""
                SELECT u.user_id, u.username, u.full_name, u.email, u.phone,
                       u.role, u.status, u.created_at,
                       COUNT(DISTINCT o.order_id) AS total_orders,
                       COALESCE(SUM(o.total_amount), 0) AS total_spent
                FROM users u
                LEFT JOIN orders o ON u.user_id = o.user_id
                WHERE u.full_name LIKE %s OR u.username LIKE %s
                GROUP BY u.user_id
                ORDER BY u.full_name
                LIMIT 5
            """, (f"%{name}%", f"%{name}%"))
            rows = cur.fetchall()
            if rows:
                return rows

            # 2) Fuzzy fallback: tách từng từ (bỏ từ < 3 ký tự) và OR search
            words = [w for w in name.split() if len(w) >= 3]
            if not words:
                return []
            where_clauses = " OR ".join(
                ["u.full_name LIKE %s OR u.username LIKE %s"] * len(words)
            )
            params = []
            for w in words:
                params += [f"%{w}%", f"%{w}%"]
            cur.execute(f"""
                SELECT u.user_id, u.username, u.full_name, u.email, u.phone,
                       u.role, u.status, u.created_at,
                       COUNT(DISTINCT o.order_id) AS total_orders,
                       COALESCE(SUM(o.total_amount), 0) AS total_spent
                FROM users u
                LEFT JOIN orders o ON u.user_id = o.user_id
                WHERE {where_clauses}
                GROUP BY u.user_id
                ORDER BY u.full_name
                LIMIT 5
            """, params)
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_customer_by_name] DB error: {e}")
        return []


def lookup_customer_recent_orders(user_id: int, limit: int = 5) -> list[dict]:
    """Lịch sử đơn hàng gần nhất của một khách."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT o.order_id, o.status,
                       COALESCE((SELECT SUM(od2.total_price) FROM order_details od2 WHERE od2.order_id = o.order_id), o.total_amount) AS total_amount,
                       o.payment_method, o.payment_status, o.order_date
                FROM orders o
                WHERE o.user_id = %s
                ORDER BY o.order_date DESC
                LIMIT %s
            """, (user_id, limit))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_customer_recent_orders] DB error: {e}")
        return []
