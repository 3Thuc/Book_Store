"""
admin_agents.py – Các agent truy vấn dữ liệu cho Admin chatbot.

Agents:
  - DashboardAgent     : tổng hợp dashboard overview
  - RevenueAgent       : doanh thu theo ngày/tuần/tháng/quý
  - UserManagementAgent: tra cứu + thống kê user
  - PromotionAgent     : quản lý voucher/promotion
  - SystemHealthAgent  : kiểm tra trạng thái các service
  - BookAdminAgent     : sách rating thấp, thống kê danh mục

NOTE: Tất cả hàm đều wrap try/except để không crash khi DB lỗi.
"""
import httpx
import traceback
from chatbot_app.db import get_connection
from chatbot_app.config import (
    OLLAMA_HOST, OPENSEARCH_HOST, OPENSEARCH_PORT,
    OPENSEARCH_USER, OPENSEARCH_PASSWORD, OPENSEARCH_USE_SSL,
    MYSQL_HOST, MYSQL_PORT,
)
from datetime import date, timedelta


# ═══════════════════════════════════════════════════════════════
#  DASHBOARD AGENT
# ═══════════════════════════════════════════════════════════════

def get_dashboard_overview() -> dict:
    """Tổng quan dashboard: counts + doanh thu hôm nay + đơn pending + action items."""
    today     = date.today()
    yesterday = today - timedelta(days=1)
    default = {
        "date": str(today), "total_users": 0, "active_customers": 0,
        "new_users_today": 0, "total_books": 0, "low_stock_count": 0,
        "orders_today": 0, "revenue_today": 0, "revenue_yesterday": 0,
        "pending_orders": 0, "return_requests": 0,
        "pending_over_24h": 0, "out_of_stock_active": 0,
    }
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM users WHERE status != 'deleted') AS total_users,
                    (SELECT COUNT(*) FROM users WHERE role = 'customer' AND status = 'active') AS active_customers,
                    (SELECT COUNT(*) FROM users WHERE DATE(DATE_ADD(created_at, INTERVAL 7 HOUR)) = %s AND role = 'customer') AS new_users_today,
                    (SELECT COUNT(*) FROM users
                     WHERE DATE_FORMAT(DATE_ADD(created_at, INTERVAL 7 HOUR), '%%Y-%%m') = DATE_FORMAT(%s, '%%Y-%%m') AND role = 'customer'
                    ) AS new_users_this_month,
                    (SELECT COUNT(*) FROM books WHERE status = 'active') AS total_books,
                    (SELECT COUNT(*) FROM (
                        SELECT b.stock_quantity - COALESCE((
                            SELECT SUM(od.quantity) FROM order_details od
                            JOIN orders o ON od.order_id = o.order_id
                            WHERE od.book_id = b.book_id AND o.status IN ('pending', 'processing', 'shipped')
                        ), 0) AS available_qty
                        FROM books b WHERE b.status = 'active'
                     ) AS t WHERE available_qty <= 5 AND available_qty > 0) AS low_stock_count,
                    (SELECT COUNT(*) FROM (
                        SELECT b.stock_quantity - COALESCE((
                            SELECT SUM(od.quantity) FROM order_details od
                            JOIN orders o ON od.order_id = o.order_id
                            WHERE od.book_id = b.book_id AND o.status IN ('pending', 'processing', 'shipped')
                        ), 0) AS available_qty
                        FROM books b WHERE b.status = 'active'
                     ) AS t WHERE available_qty <= 0) AS out_of_stock_active,
                    (SELECT COUNT(*) FROM orders WHERE DATE(DATE_ADD(order_date, INTERVAL 7 HOUR)) = %s) AS orders_today,
                    (SELECT COALESCE(SUM(total_amount), 0) FROM orders
                     WHERE DATE(DATE_ADD(order_date, INTERVAL 7 HOUR)) = %s
                       AND status IN ('delivered','shipped')) AS revenue_today,
                    (SELECT COALESCE(SUM(total_amount), 0) FROM orders
                     WHERE DATE(DATE_ADD(order_date, INTERVAL 7 HOUR)) = %s
                       AND status IN ('delivered','shipped')) AS revenue_yesterday,
                    (SELECT COUNT(*) FROM orders WHERE status IN ('pending','processing')) AS pending_orders,
                    (SELECT COUNT(*) FROM orders WHERE status = 'return_requested') AS return_requests,
                    (SELECT COUNT(*) FROM orders
                     WHERE status IN ('pending','processing')
                       AND order_date < DATE_SUB(NOW(), INTERVAL 24 HOUR)) AS pending_over_24h,
                    (SELECT COUNT(*) FROM orders) AS total_orders_all_time,
                    (SELECT COUNT(*) FROM orders WHERE status NOT IN ('cancelled', 'returned', 'failed')) AS active_orders_all_time,
                    (SELECT COALESCE(SUM(total_amount), 0)
                     FROM orders o
                     WHERE o.status NOT IN ('cancelled', 'returned', 'failed')) AS total_revenue_all_time
            """, (today, today, today, today, yesterday))
            stats = cur.fetchone()
            if stats:
                stats["date"] = str(today)
                return stats
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_dashboard_overview] DB error: {e}")
    return default


# ═══════════════════════════════════════════════════════════════
#  REVENUE AGENT
# ═══════════════════════════════════════════════════════════════

def get_revenue_report(date_range: str = "week") -> dict:
    """Báo cáo doanh thu chi tiết theo khoảng thời gian."""
    today = date.today()
    # FIX-A01: Hỗ trợ q1/q2/q3/q4 cụ thể
    if date_range in ("q1","q2","q3","q4"):
        _qn = int(date_range[1])
        _yr = today.year
        _sm = (_qn - 1) * 3 + 1
        import calendar as _cal
        _em = _sm + 2
        _ed = _cal.monthrange(_yr, _em)[1]
        start = date(_yr, _sm, 1)
        end_q = date(_yr, _em, _ed)
        # Nếu quý chưa kết thúc → cắt tại hôm nay
        end_q = min(end_q, today)
        label = f"Q{_qn}/{_yr}"
    elif date_range == "today":
        start = today; end_q = today; label = "Hôm nay"
    elif date_range == "yesterday":
        start = today - timedelta(days=1); end_q = start; label = "Hôm qua"
    elif date_range == "week":
        start = today - timedelta(days=today.weekday()); end_q = today; label = "Tuần này"
    elif date_range == "last_week":
        this_week_start = today - timedelta(days=today.weekday())
        start = this_week_start - timedelta(days=7); end_q = this_week_start - timedelta(days=1); label = "Tuần trước"
    elif date_range == "month":
        start = today.replace(day=1); end_q = today; label = "Tháng này"
    elif date_range == "last_month":
        this_month_start = today.replace(day=1)
        end_q = this_month_start - timedelta(days=1)
        start = end_q.replace(day=1); label = "Tháng trước"
    elif date_range == "quarter":
        quarter_start_month = ((today.month - 1) // 3) * 3 + 1
        start = today.replace(month=quarter_start_month, day=1); end_q = today; label = "Quý này"
    elif date_range == "year":
        start = today.replace(month=1, day=1); end_q = today; label = "Năm nay"
    else:
        start = today - timedelta(days=6); end_q = today; label = "7 ngày qua"

    default = {
        "period_label": label, "from_date": str(start), "to_date": str(end_q),
        "total_orders": 0, "unique_customers": 0, "gross_revenue": 0,
        "confirmed_revenue": 0, "total_discounts": 0, "cancelled_count": 0,
        "returned_count": 0, "cod_count": 0, "online_payment_count": 0,
        "top_days": [],
    }
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    SUM(CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN 1 ELSE 0 END) AS total_orders,
                    COUNT(DISTINCT o.user_id) AS unique_customers,
                    COALESCE(SUM(CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN
                        (SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) - 
                        COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                    ELSE 0 END), 0) AS gross_revenue,
                    COALESCE(SUM(CASE WHEN o.status = 'delivered' THEN
                        (SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) - 
                        COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                    ELSE 0 END), 0) AS confirmed_revenue,
                    COALESCE(SUM(CASE WHEN o.status NOT IN ('failed', 'cancelled', 'returned') THEN
                        COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                    ELSE 0 END), 0) AS total_discounts,
                    SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
                    SUM(CASE WHEN o.status = 'returned' THEN 1 ELSE 0 END) AS returned_count,
                    SUM(CASE WHEN o.payment_method = 'COD' THEN 1 ELSE 0 END) AS cod_count,
                    SUM(CASE WHEN o.payment_method != 'COD' THEN 1 ELSE 0 END) AS online_payment_count
                FROM orders o
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE DATE(CONVERT_TZ(o.order_date, '+00:00', '+07:00')) BETWEEN %s AND %s
            """, (start, end_q))
            row = cur.fetchone() or {}
            row["period_label"] = label
            row["from_date"]    = str(start)
            row["to_date"]      = str(end_q)

            cur.execute("""
                SELECT DATE(CONVERT_TZ(o.order_date, '+00:00', '+07:00')) AS day,
                       COUNT(DISTINCT o.order_id) AS orders,
                       COALESCE(SUM(CASE WHEN o.status NOT IN ('failed','cancelled','returned') THEN 
                           (SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) - 
                           COALESCE((SELECT SUM(od.total_price) FROM order_details od WHERE od.order_id = o.order_id) * p.discount_percent / 100.0, 0)
                       ELSE 0 END),0) AS revenue
                FROM orders o
                LEFT JOIN promotions p ON o.promo_id = p.promo_id
                WHERE DATE(CONVERT_TZ(o.order_date, '+00:00', '+07:00')) BETWEEN %s AND %s
                GROUP BY DATE(CONVERT_TZ(o.order_date, '+00:00', '+07:00'))
                ORDER BY revenue DESC
                LIMIT 5
            """, (start, end_q))
            row["top_days"] = [
                {"day": str(r["day"]), "orders": r["orders"], "revenue": float(r["revenue"])}
                for r in cur.fetchall()
            ]
            return row
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_revenue_report] DB error: {e}")
    return default


# ═══════════════════════════════════════════════════════════════
#  USER MANAGEMENT AGENT
# ═══════════════════════════════════════════════════════════════

def lookup_user_by_email_or_id(query: str) -> dict | None:
    """Tra cứu user theo email hoặc user_id."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if query.isdigit():
                cur.execute("""
                    SELECT u.user_id, u.username, u.full_name, u.email, u.phone,
                           u.role, u.status, u.created_at,
                           COUNT(DISTINCT o.order_id) AS total_orders,
                           COALESCE(SUM(o.total_amount),0) AS total_spent
                    FROM users u
                    LEFT JOIN orders o ON u.user_id = o.user_id AND o.status = 'delivered'
                    WHERE u.user_id = %s
                    GROUP BY u.user_id
                """, (int(query),))
            else:
                cur.execute("""
                    SELECT u.user_id, u.username, u.full_name, u.email, u.phone,
                           u.role, u.status, u.created_at,
                           COUNT(DISTINCT o.order_id) AS total_orders,
                           COALESCE(SUM(o.total_amount),0) AS total_spent
                    FROM users u
                    LEFT JOIN orders o ON u.user_id = o.user_id AND o.status = 'delivered'
                    WHERE u.email LIKE %s OR u.username LIKE %s OR u.full_name LIKE %s
                    GROUP BY u.user_id
                    LIMIT 1
                """, (f"%{query}%", f"%{query}%", f"%{query}%"))
            return cur.fetchone()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [lookup_user_by_email_or_id] DB error: {e}")
        return None


def get_user_statistics() -> dict:
    """Thống kê tổng quan người dùng trong hệ thống."""
    today = date.today()
    this_month_start = today.replace(day=1)
    default = {
        "total_users": 0, "customers": 0, "staff_count": 0, "admin_count": 0,
        "active_users": 0, "locked_users": 0, "new_today": 0, "new_this_month": 0,
    }
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS total_users,
                    SUM(CASE WHEN role = 'customer' THEN 1 ELSE 0 END) AS customers,
                    SUM(CASE WHEN role = 'staff' THEN 1 ELSE 0 END) AS staff_count,
                    SUM(CASE WHEN role = 'admin' THEN 1 ELSE 0 END) AS admin_count,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_users,
                    SUM(CASE WHEN status = 'locked' THEN 1 ELSE 0 END) AS locked_users,
                    SUM(CASE WHEN DATE(created_at) = %s THEN 1 ELSE 0 END) AS new_today,
                    SUM(CASE WHEN DATE(created_at) BETWEEN %s AND %s THEN 1 ELSE 0 END) AS new_this_month
                FROM users
            """, (today, this_month_start, today))
            return cur.fetchone() or default
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_user_statistics] DB error: {e}")
        return default


def get_all_staff_accounts() -> list[dict]:
    """Danh sách tất cả tài khoản có role=staff."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT user_id, username, full_name, email, phone, status, created_at
                FROM users
                WHERE role = 'staff'
                ORDER BY created_at DESC
            """)
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_all_staff_accounts] DB error: {e}")
        return []


def get_order_statistics(date_range: str = "today") -> dict:
    """Thống kê đơn hàng theo khoảng thời gian."""
    today = date.today()
    if date_range == "week":
        start = today - timedelta(days=today.weekday())
    elif date_range == "month":
        start = today.replace(day=1)
    elif date_range == "year":
        start = today.replace(month=1, day=1)
    else:
        start = today
    default = {
        "date_range": date_range, "from_date": str(start), "to_date": str(today),
        "total": 0, "delivered": 0, "pending": 0, "cancelled": 0,
        "returned": 0, "completion_rate": 0,
    }
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered,
                    SUM(CASE WHEN status IN ('pending','processing','shipped') THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN status IN ('returned','return_requested') THEN 1 ELSE 0 END) AS returned
                FROM orders
                WHERE DATE(order_date) BETWEEN %s AND %s
            """, (start, today))
            row = cur.fetchone() or {}
            t = row.get("total", 0) or 1
            row["completion_rate"] = round((row.get("delivered", 0) or 0) / t * 100, 1)
            row["date_range"] = date_range
            row["from_date"]  = str(start)
            row["to_date"]    = str(today)
            return row
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_order_statistics] DB error: {e}")
        return default


# ═══════════════════════════════════════════════════════════════
#  PROMOTION AGENT
# ═══════════════════════════════════════════════════════════════

def get_promotions(status: str = 'active') -> tuple[int, list[dict]]:
    """Danh sách khuyến mãi theo trạng thái. Trả về (total_count, items)."""
    today = date.today()
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if status == 'active':
                # Đếm tổng
                cur.execute("""
                    SELECT COUNT(*) as total FROM promotions
                    WHERE status = 'active' AND end_date >= %s AND start_date <= %s
                """, (today, today))
                total = cur.fetchone()['total']
                
                # Lấy 10 dòng
                cur.execute("""
                    SELECT promo_id, code, discount_percent, start_date, end_date, status,
                           DATEDIFF(end_date, %s) AS days_remaining
                    FROM promotions
                    WHERE status = 'active'
                      AND end_date >= %s
                      AND start_date <= %s
                    ORDER BY end_date ASC
                    LIMIT 10
                """, (today, today, today))
                items = cur.fetchall()
                
            elif status == 'inactive':
                # Đếm tổng
                cur.execute("""
                    SELECT COUNT(*) as total FROM promotions
                    WHERE NOT (status = 'active' AND end_date >= %s AND start_date <= %s)
                """, (today, today))
                total = cur.fetchone()['total']
                
                # Lấy 10 dòng
                cur.execute("""
                    SELECT promo_id, code, discount_percent, start_date, end_date, status,
                           DATEDIFF(end_date, %s) AS days_remaining
                    FROM promotions
                    WHERE NOT (status = 'active' AND end_date >= %s AND start_date <= %s)
                    ORDER BY end_date DESC
                    LIMIT 10
                """, (today, today, today))
                items = cur.fetchall()
                
            return total, items
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_promotions] DB error: {e}")
        return 0, []


def check_voucher_code(code: str) -> dict | None:
    """Kiểm tra chi tiết mã voucher cụ thể."""
    today = date.today()
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT p.promo_id, p.code, p.discount_percent, p.start_date, p.end_date,
                       p.status, p.created_at,
                       CASE
                         WHEN p.end_date < %s THEN 'expired'
                         WHEN p.start_date > %s THEN 'not_started'
                         WHEN p.status = 'inactive' THEN 'inactive'
                         ELSE 'valid'
                       END AS validity,
                       DATEDIFF(p.end_date, %s) AS days_remaining,
                       (SELECT COUNT(*) FROM orders o WHERE o.promo_id = p.promo_id) AS times_used
                FROM promotions p
                WHERE p.code = %s
            """, (today, today, today, code.upper()))
            return cur.fetchone()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [check_voucher_code] DB error: {e}")
        return None


def get_expiring_promotions(days: int = 7) -> list[dict]:
    """Promotions sắp hết hạn trong N ngày tới."""
    today = date.today()
    end_check = today + timedelta(days=days)
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT promo_id, code, discount_percent, end_date,
                       DATEDIFF(end_date, %s) AS days_remaining
                FROM promotions
                WHERE status = 'active'
                  AND end_date BETWEEN %s AND %s
                ORDER BY end_date ASC
            """, (today, today, end_check))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_expiring_promotions] DB error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
#  SYSTEM HEALTH AGENT
# ═══════════════════════════════════════════════════════════════

async def check_system_health() -> dict:
    """Kiểm tra trạng thái MySQL, Ollama, OpenSearch."""
    results = {}

    # ── MySQL ─────────────────────────────────────────────────
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close(); conn.close()
        results["mysql"] = {"status": "✅ online", "host": f"{MYSQL_HOST}:{MYSQL_PORT}"}
    except Exception as e:
        results["mysql"] = {"status": "❌ offline", "error": str(e)}

    # ── Ollama ─────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{OLLAMA_HOST}/api/tags")
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                results["ollama"] = {"status": "✅ online", "models": models, "host": OLLAMA_HOST}
            else:
                results["ollama"] = {"status": "⚠️ degraded", "http": resp.status_code}
    except Exception as e:
        results["ollama"] = {"status": "❌ offline", "error": str(e)[:80]}

    # ── Java Backend API ───────────────────────────────────────
    try:
        from chatbot_app.config import JAVA_API_BASE
        _base_err = []
        _active_url = None
        _resp = None
        
        async with httpx.AsyncClient(timeout=5, verify=False) as client:
            _base = JAVA_API_BASE.replace('/bookdb', '').rstrip('/')
            
            # Tạo danh sách URL cần thử - thứ tự rất quan trọng:
            # 1. https://127.0.0.1:8443 (IPv4 trực tiếp, tránh lỗi IPv6 của localhost trên Windows)
            # 2. https://localhost:8443 (Cấu hình gốc)
            # 3. http://127.0.0.1:8080 (Dự phòng Dev mode không SSL)
            # 4. http://localhost:8080
            _base_ip = _base.replace("localhost", "127.0.0.1")
            _base_docker = _base.replace("localhost", "host.docker.internal")
            if "https" in _base and "8443" in _base:
                _try_bases = [
                    _base_ip,  # https://127.0.0.1:8443 TRƯỚC
                    _base_docker, # https://host.docker.internal:8443
                    _base,     # https://localhost:8443
                    _base_ip.replace("https", "http").replace("8443", "8080"),
                    _base_docker.replace("https", "http").replace("8443", "8080"),
                    _base.replace("https", "http").replace("8443", "8080"),
                ]
            else:
                _try_bases = [_base_ip, _base_docker, _base]
            
            # Với mỗi base URL thử nhiều path endpoints
            _try_paths = ["/actuator/health", "/"]
            
            _last_err = None
            for _base_url in _try_bases:
                for _path in _try_paths:
                    _full_url = _base_url + _path
                    _base_err.append(_full_url)
                    try:
                        _resp = await client.get(_full_url)
                        _active_url = _base_url
                        break  # Có phản hồi -> thoát loop path
                    except httpx.ConnectError as e:
                        _last_err = e
                        break  # ConnectError ở base này -> không thử path khác, sang base tiếp theo
                    except Exception as e:
                        _last_err = e
                        continue
                if _active_url:
                    break  # Thoát loop base
            
            if _resp is not None:
                if _resp.status_code in [200, 404, 401, 403]:
                    results["java_api"] = {"status": "✅ online", "url": _active_url}
                else:
                    results["java_api"] = {"status": "⚠️ degraded", "http": _resp.status_code}
            else:
                raise _last_err if _last_err else Exception("No valid endpoints responded.")
                
    except Exception as e:
        results["java_api"] = {"status": "❌ offline", "error": f"{type(e).__name__}: All {len(_base_err)} endpoints failed"}

    # ── OpenSearch ─────────────────────────────────────────────
    try:
        scheme = "https" if OPENSEARCH_USE_SSL else "http"
        async with httpx.AsyncClient(timeout=5, verify=False) as client:
            auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD) if OPENSEARCH_USER else None
            resp = await client.get(
                f"{scheme}://{OPENSEARCH_HOST}:{OPENSEARCH_PORT}/_cluster/health",
                auth=auth,
            )
            if resp.status_code == 200:
                data = resp.json()
                results["opensearch"] = {
                    "status": "✅ online",
                    "cluster_status": data.get("status"),
                    "host": f"{OPENSEARCH_HOST}:{OPENSEARCH_PORT}",
                }
            else:
                results["opensearch"] = {"status": "⚠️ degraded", "http": resp.status_code}
    except Exception as e:
        results["opensearch"] = {"status": "❌ offline", "error": str(e)[:80]}

    return results


# ═══════════════════════════════════════════════════════════════
#  BOOK ADMIN AGENT
# ═══════════════════════════════════════════════════════════════

def get_low_rating_books(threshold: float = 3.0, limit: int = 10) -> list[dict]:
    """Sách có avg_rating <= threshold và có đủ lượt đánh giá."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT b.book_id, b.title, b.avg_rating, b.rating_count,
                       b.price, b.status, a.author_name,
                       GROUP_CONCAT(c.category_name SEPARATOR ', ') AS categories
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN book_categories bc ON b.book_id = bc.book_id
                LEFT JOIN categories c ON bc.category_id = c.category_id
                WHERE b.avg_rating <= %s AND b.rating_count >= 5
                  AND b.status != 'deleted'
                GROUP BY b.book_id, b.title, b.avg_rating, b.rating_count,
                         b.price, b.status, a.author_name
                ORDER BY b.avg_rating ASC, b.rating_count DESC
                LIMIT %s
            """, (threshold, limit))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_low_rating_books] DB error: {e}")
        return []


def get_category_stats() -> list[dict]:
    """Thống kê số sách và doanh số theo danh mục."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT c.category_name,
                       COUNT(DISTINCT b.book_id) AS book_count,
                       AVG(b.avg_rating) AS avg_rating,
                       SUM(b.stock_quantity) AS total_stock
                FROM categories c
                LEFT JOIN book_categories bc ON c.category_id = bc.category_id
                LEFT JOIN books b ON bc.book_id = b.book_id AND b.status = 'active'
                GROUP BY c.category_id, c.category_name
                ORDER BY book_count DESC
            """)
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_category_stats] DB error: {e}")
        return []


def get_top_books_admin(limit: int = 10) -> list[dict]:
    """Top sách bán chạy với thông tin đầy đủ cho admin."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    b.book_id, b.title, a.author_name, b.price,
                    b.avg_rating, b.rating_count, b.stock_quantity, b.status,
                    COALESCE(SUM(od.quantity), 0) AS total_sold,
                    COALESCE(SUM(od.quantity * od.unit_price), 0) AS total_revenue
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN order_details od ON b.book_id = od.book_id
                LEFT JOIN orders o ON od.order_id = o.order_id
                    AND o.status IN ('delivered','shipped')
                GROUP BY b.book_id, b.title, a.author_name, b.price,
                         b.avg_rating, b.rating_count, b.stock_quantity, b.status
                ORDER BY total_sold DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_top_books_admin] DB error: {e}")
        return []


def get_escalated_customers(limit: int = 20) -> list[dict]:
    """Khách hàng có đơn cần escalate (cancel/return requests)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT o.order_id, o.status, o.total_amount, o.created_at,
                       u.user_id, u.username, u.email, u.phone
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.user_id
                WHERE o.status IN ('cancel_requested', 'return_requested')
                ORDER BY o.created_at DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [get_escalated_customers] DB error: {e}")
        return []


# ═══════════════════════════════════════════════════════════════
#  USER MANAGEMENT WRITE AGENTS (Admin only)
# ═══════════════════════════════════════════════════════════════

def admin_update_user_status(user_id_or_name: str, new_status: str) -> dict:
    """
    Khóa hoặc mở khóa tài khoản user.
    new_status: 'locked' | 'active'
    Trả về dict: {success: bool, message: str, username: str | None}
    """
    import traceback as _tb
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            # Tìm user theo ID hoặc username/email
            if str(user_id_or_name).isdigit():
                cur.execute(
                    "SELECT user_id, username, role, status FROM users WHERE user_id = %s",
                    (int(user_id_or_name),)
                )
            else:
                cur.execute(
                    "SELECT user_id, username, role, status FROM users WHERE username = %s OR email = %s LIMIT 1",
                    (user_id_or_name, user_id_or_name)
                )
            user = cur.fetchone()
            if not user:
                return {"success": False, "message": f"Không tìm thấy user `{user_id_or_name}`.", "username": None}

            # Bảo vệ: không cho khóa admin
            if user["role"] == "admin":
                return {"success": False, "message": "Không thể khóa tài khoản Admin qua chatbot.", "username": user["username"]}

            cur.execute(
                "UPDATE users SET status = %s, updated_at = NOW() WHERE user_id = %s",
                (new_status, user["user_id"])
            )
            conn.commit()
            return {"success": True, "message": "OK", "username": user["username"], "user_id": user["user_id"]}
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [admin_update_user_status] DB error: {e}")
        _tb.print_exc()
        return {"success": False, "message": str(e), "username": None}


def admin_update_user_role(user_id_or_name: str, new_role: str) -> dict:
    """
    Đổi role của user (customer ↔ staff).
    new_role: 'customer' | 'staff'  (không cho set 'admin' qua chatbot)
    Trả về dict: {success: bool, message: str, username: str | None}
    """
    import traceback as _tb
    # Bảo vệ: chỉ cho phép customer ↔ staff
    if new_role not in ("customer", "staff"):
        return {"success": False, "message": "Chỉ có thể đổi role thành `customer` hoặc `staff` qua chatbot.", "username": None}
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if str(user_id_or_name).isdigit():
                cur.execute(
                    "SELECT user_id, username, role, status FROM users WHERE user_id = %s",
                    (int(user_id_or_name),)
                )
            else:
                cur.execute(
                    "SELECT user_id, username, role, status FROM users WHERE username = %s OR email = %s LIMIT 1",
                    (user_id_or_name, user_id_or_name)
                )
            user = cur.fetchone()
            if not user:
                return {"success": False, "message": f"Không tìm thấy user `{user_id_or_name}`.", "username": None}

            # Không đổi role Admin
            if user["role"] == "admin":
                return {"success": False, "message": "Không thể đổi role tài khoản Admin qua chatbot.", "username": user["username"]}

            cur.execute(
                "UPDATE users SET role = %s, updated_at = NOW() WHERE user_id = %s",
                (new_role, user["user_id"])
            )
            conn.commit()
            return {
                "success": True, "message": "OK",
                "username": user["username"],
                "user_id": user["user_id"],
                "old_role": user["role"],
            }
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [admin_update_user_role] DB error: {e}")
        _tb.print_exc()
        return {"success": False, "message": str(e), "username": None}


def admin_update_book_stock(book_id_or_name: str, new_quantity: int) -> dict:
    """
    Cập nhật tồn kho (stock_quantity) của một cuốn sách.
    book_id_or_name: book_id (số) hoặc tên sách (string)
    new_quantity: số lượng mới (>= 0)
    Trả về dict: {success, message, book_title, book_id, old_qty, new_qty}
    """
    import traceback as _tb
    if new_quantity < 0:
        return {"success": False, "message": "Số lượng tồn kho không được âm.", "book_title": None}
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if str(book_id_or_name).isdigit():
                cur.execute(
                    "SELECT book_id, title, stock_quantity FROM books WHERE book_id = %s",
                    (int(book_id_or_name),)
                )
            else:
                cur.execute(
                    "SELECT book_id, title, stock_quantity FROM books WHERE title LIKE %s LIMIT 1",
                    (f"%{book_id_or_name}%",)
                )
            book = cur.fetchone()
            if not book:
                return {"success": False, "message": f"Không tìm thấy sách `{book_id_or_name}`.", "book_title": None}

            old_qty = book["stock_quantity"]
            cur.execute(
                "UPDATE books SET stock_quantity = %s, updated_at = NOW() WHERE book_id = %s",
                (new_quantity, book["book_id"])
            )
            conn.commit()
            return {
                "success":    True,
                "message":    "OK",
                "book_title": book["title"],
                "book_id":    book["book_id"],
                "old_qty":    old_qty,
                "new_qty":    new_quantity,
            }
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"⚠️ [admin_update_book_stock] DB error: {e}")
        _tb.print_exc()
        return {"success": False, "message": str(e), "book_title": None}
