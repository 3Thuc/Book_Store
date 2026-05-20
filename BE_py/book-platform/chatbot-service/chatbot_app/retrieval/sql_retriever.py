"""
sql_retriever.py – Lấy dữ liệu realtime từ MySQL.
Dùng cho: đơn hàng, sách theo thể loại, giá, tồn kho.

FIX: Tất cả hàm bọc trong try/except → trả None/[] thay vì crash khi DB lỗi.
FIX: get_discounted_books không dùng cột discount_price (không tồn tại trong schema).
"""
from chatbot_app.db import get_connection


def get_order_info(order_id: int, user_id: int | None = None) -> dict | None:
    """Lấy thông tin đơn hàng theo order_id.
    
    user_id: nếu cung cấp, chỉ trả kết quả khi đơn thuộc về user đó (bảo mật).
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            if user_id is not None:
                cur.execute("""
                    SELECT o.order_id, o.status, o.total_amount, o.created_at,
                           o.payment_method, o.payment_status,
                           a.address_text AS shipping_address
                    FROM orders o
                    LEFT JOIN addresses a ON o.address_id = a.address_id
                    WHERE o.order_id = %s AND o.user_id = %s
                    LIMIT 1
                """, (order_id, user_id))
            else:
                cur.execute("""
                    SELECT o.order_id, o.status, o.total_amount, o.created_at,
                           o.payment_method, o.payment_status,
                           a.address_text AS shipping_address
                    FROM orders o
                    LEFT JOIN addresses a ON o.address_id = a.address_id
                    WHERE o.order_id = %s
                    LIMIT 1
                """, (order_id,))
            order = cur.fetchone()
            if order:
                # Fetch order items
                cur.execute("""
                    SELECT od.book_name, od.quantity, od.unit_price, od.total_price
                    FROM order_details od
                    WHERE od.order_id = %s
                """, (order_id,))
                order["items"] = cur.fetchall() or []
            return order
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_order_info] DB error: {e}")
        return None


def get_user_orders(user_id: int, limit: int = 5) -> list[dict]:
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT o.order_id, o.status, o.created_at,
                       COALESCE(SUM(od.total_price), o.total_amount, 0) AS total_amount,
                       COUNT(od.book_id) AS book_count
                FROM orders o
                LEFT JOIN order_details od ON o.order_id = od.order_id
                WHERE o.user_id = %s
                GROUP BY o.order_id
                ORDER BY o.created_at DESC
                LIMIT %s
            """, (user_id, limit))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_user_orders] DB error: {e}")
        return []


def get_user_order_summary(user_id: int) -> dict:
    """Trả về tổng số đơn hàng và phân nhóm theo status.
    Dùng cho câu hỏi 'Tổng đơn hàng tôi có bao nhiêu?'
    Returns: {total, by_status: {status: count}, total_spent}
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT status, COUNT(*) AS cnt,
                       COALESCE(SUM(total_amount), 0) AS status_total
                FROM orders
                WHERE user_id = %s
                GROUP BY status
            """, (user_id,))
            rows = cur.fetchall()
            by_status = {r["status"]: int(r["cnt"]) for r in rows}
            total = sum(by_status.values())
            total_spent = sum(float(r["status_total"]) for r in rows)
            return {"total": total, "by_status": by_status, "total_spent": total_spent}
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_user_order_summary] DB error: {e}")
        return {"total": 0, "by_status": {}, "total_spent": 0}



def get_user_orders_by_status(user_id: int, status: str | None = None, limit: int = 10) -> list[dict]:
    """Lấy đơn hàng của user, có thể lọc theo status.
    status: single value như 'pending' | 'cancelled' | 'failed' | 'return_requested' | 'returned'
            hoặc pipe-separated multi-value: 'return_requested|returned'
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            base_select = """
                SELECT o.order_id, o.status, o.created_at,
                       COALESCE(SUM(od.total_price), o.total_amount, 0) AS total_amount,
                       COUNT(od.book_id) AS book_count
                FROM orders o
                LEFT JOIN order_details od ON o.order_id = od.order_id
            """
            order_suffix = "GROUP BY o.order_id ORDER BY o.created_at DESC LIMIT %s"

            if status:
                # Hỗ trợ pipe-separated: "return_requested|returned" → IN ('return_requested', 'returned')
                status_list = [s.strip() for s in status.split("|") if s.strip()]
                if len(status_list) > 1:
                    placeholders = ", ".join(["%s"] * len(status_list))
                    sql = f"{base_select} WHERE o.user_id = %s AND o.status IN ({placeholders}) {order_suffix}"
                    cur.execute(sql, (user_id, *status_list, limit))
                else:
                    sql = f"{base_select} WHERE o.user_id = %s AND o.status = %s {order_suffix}"
                    cur.execute(sql, (user_id, status_list[0], limit))
            else:
                sql = f"{base_select} WHERE o.user_id = %s {order_suffix}"
                cur.execute(sql, (user_id, limit))

            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_user_orders_by_status] DB error: {e}")
        return []


def get_loyalty_points(user_id: int) -> dict:
    """Tính điểm tích lũy chính xác từ TOÀN BỘ đơn delivered (không dùng LIMIT)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    COALESCE(SUM(total_amount), 0) AS total_spent,
                    COUNT(*) AS delivered_count
                FROM orders
                WHERE user_id = %s AND status = 'delivered'
            """, (user_id,))
            row = cur.fetchone() or {}
            total_spent = float(row.get("total_spent") or 0)
            points = int(total_spent / 10_000)
            return {
                "total_spent": total_spent,
                "delivered_count": int(row.get("delivered_count") or 0),
                "points": points,
                "redeem_value": (points // 100) * 10_000,
            }
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_loyalty_points] DB error: {e}")
        return {"total_spent": 0, "delivered_count": 0, "points": 0, "redeem_value": 0}


def get_books_by_genre(genre: str, limit: int = 5) -> list[dict]:
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT b.book_id, b.title, a.author_name, b.price,
                       b.avg_rating, b.stock_quantity, c.category_name,
                       COALESCE((
                           SELECT SUM(od.quantity)
                           FROM order_details od
                           JOIN orders o ON od.order_id = o.order_id
                           WHERE od.book_id = b.book_id
                             AND o.status IN ('delivered', 'shipped', 'processing')
                       ), 0) AS total_sold,
                       (b.stock_quantity - COALESCE((
                           SELECT SUM(od.quantity)
                           FROM order_details od
                           JOIN orders o ON od.order_id = o.order_id
                           WHERE od.book_id = b.book_id
                             AND o.status IN ('pending', 'processing', 'shipped')
                       ), 0)) AS available_quantity
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN book_categories bc ON bc.book_id = b.book_id
                LEFT JOIN categories c ON c.category_id = bc.category_id
                WHERE c.category_name LIKE %s
                  AND b.status = 'active'
                HAVING available_quantity > 0
                ORDER BY total_sold DESC, b.avg_rating DESC
                LIMIT %s
            """, (f"%{genre}%", limit))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_books_by_genre] DB error: {e}")
        return []

def search_books_by_keyword_sql(keyword: str, limit: int = 5) -> list[dict]:
    """Tìm sách chính xác theo keyword (tên sách) để bổ khuyết cho SBERT vector search."""
    if not keyword or len(keyword.strip()) < 2:
        return []
        
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            # Split keyword to multiple LIKE conditions for robust title matching
            words = keyword.strip().split()
            if not words: return []
            
            # Tiêu đề chứa tất cả các từ
            title_conditions = " AND ".join(["b.title LIKE %s"] * len(words))
            title_params = [f"%{w}%" for w in words]
            
            # Hoặc tác giả chứa tất cả các từ
            author_conditions = " AND ".join(["a.author_name LIKE %s"] * len(words))
            author_params = [f"%{w}%" for w in words]
            
            sql = f"""
                SELECT b.book_id, b.title, a.author_name, b.price,
                       b.stock_quantity - COALESCE(SUM(oi.quantity), 0) AS available_quantity,
                       c.category_name
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN order_details oi ON b.book_id = oi.book_id
                LEFT JOIN orders o ON oi.order_id = o.order_id AND o.status IN ('pending', 'processing')
                LEFT JOIN book_categories bc ON bc.book_id = b.book_id
                LEFT JOIN categories c ON bc.category_id = c.category_id
                WHERE (({title_conditions}) OR ({author_conditions})) AND b.status = 'active'
                GROUP BY b.book_id
                LIMIT %s
            """
            
            # Hợp nhất danh sách parameters
            params = title_params + author_params + [limit]
            
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            
            results = []
            for row in rows:
                results.append({
                    "book_id":  row["book_id"],
                    "title":    row["title"],
                    "author":   row["author_name"],
                    "price":    row["price"],
                    "stock":    int(row["available_quantity"] or 0),
                    "category": row["category_name"],
                    "score":    1.0,  # keyword exact match có score cố định = 1.0
                })
            return results
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[search_books_by_keyword_sql] DB error: {e}")
        return []


def get_book_price(title: str) -> dict | None:
    """Tìm sách theo tên — thử nhiều chiến lược để xử lý dấu tiếng Việt."""
    if not title or len(title.strip()) < 2:
        return None
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            # Chiến lược 1: LIKE trực tiếp (khi DB và input cùng encoding)
            # Ưu tiên kết quả khớp chính xác nhất bằng cách ORDER BY độ dài tiêu đề
            cur.execute("""
                SELECT b.book_id, b.title, b.price, b.stock_quantity,
                       a.author_name, a.author_id
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                WHERE b.title LIKE %s
                  AND b.status = 'active'
                ORDER BY CASE WHEN b.title = %s THEN 0 ELSE 1 END, LENGTH(b.title) ASC
                LIMIT 1
            """, (f"%{title}%", title))
            row = cur.fetchone()
            if row:
                return row

            # Chiến lược 2: Tìm theo từng từ riêng lẻ (bỏ qua các từ ngắn < 3 ký tự)
            words = [w for w in title.split() if len(w) >= 3]
            if words:
                for word in words:
                    cur.execute("""
                        SELECT b.book_id, b.title, b.price, b.stock_quantity,
                               a.author_name, a.author_id
                        FROM books b
                        LEFT JOIN authors a ON b.author_id = a.author_id
                        WHERE b.title LIKE %s
                          AND b.status = 'active'
                        ORDER BY b.stock_quantity DESC
                        LIMIT 1
                    """, (f"%{word}%",))
                    row = cur.fetchone()
                    if row:
                        return row

            return None
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_book_price] DB error: {e}")
        return None


def get_real_stock(book_ids: list[int]) -> dict[int, int]:
    if not book_ids:
        return {}
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            format_strings = ','.join(['%s'] * len(book_ids))
            query = f"""
                SELECT b.book_id,
                       (b.stock_quantity - COALESCE((
                           SELECT SUM(od.quantity)
                           FROM order_details od
                           JOIN orders o ON od.order_id = o.order_id
                           WHERE od.book_id = b.book_id
                             AND o.status IN ('pending', 'processing', 'shipped')
                       ), 0)) AS available_quantity
                FROM books b
                WHERE b.book_id IN ({format_strings})
            """
            cur.execute(query, tuple(book_ids))
            return {row["book_id"]: row["available_quantity"] for row in cur.fetchall()}
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_real_stock] DB error: {e}")
        return {}


def get_book_realtime(book_id: int) -> dict | None:
    """Lấy dữ liệu REALTIME từ MySQL: avg_rating, rating_count, available_quantity.
    Dùng để override OpenSearch stale index trong book_review / book_detail handler.
    """
    if not book_id:
        return None
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT
                    b.book_id,
                    b.title,
                    b.price,
                    b.avg_rating,
                    b.rating_count,
                    b.stock_quantity,
                    a.author_name,
                    (b.stock_quantity - COALESCE((
                        SELECT SUM(od.quantity)
                        FROM order_details od
                        JOIN orders o ON od.order_id = o.order_id
                        WHERE od.book_id = b.book_id
                          AND o.status IN ('pending', 'processing', 'shipped')
                    ), 0)) AS available_quantity
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                WHERE b.book_id = %s
                LIMIT 1
            """, (book_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "book_id":          row["book_id"],
                "title":            row["title"],
                "price":            float(row["price"] or 0),
                "avg_rating":       float(row["avg_rating"]) if row["avg_rating"] else 0,
                "review_count":     int(row["rating_count"] or 0),
                "author":           row["author_name"] or "Đang cập nhật",
                "available_quantity": int(row["available_quantity"] or 0),
                "stock_quantity":   int(row["stock_quantity"] or 0),
            }
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_book_realtime] DB error: {e}")
        return None


def get_discounted_books(limit: int = 5) -> list[dict]:
    """
    Lấy sách nổi bật/bán chạy từ MySQL.
    NOTE: Bảng books không có cột discount_price trong schema thực tế.
    Trả sách bán chạy nhất + rating cao làm gợi ý thay thế.

    Returns: list[dict] với keys: book_id, title, author_name, price, total_sold, avg_rating
    """
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT b.book_id, b.title, b.price,
                       a.author_name,
                       COALESCE(SUM(od.quantity), 0) AS total_sold,
                       b.avg_rating
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN order_details od ON od.book_id = b.book_id
                LEFT JOIN orders o ON od.order_id = o.order_id
                    AND o.status IN ('delivered', 'shipped')
                WHERE b.status = 'active'
                  AND b.stock_quantity > 0
                GROUP BY b.book_id
                ORDER BY total_sold DESC, b.avg_rating DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_discounted_books] DB error: {e}")
        return []


def get_books_by_author_id(author_id: int, exclude_title: str = "", limit: int = 8) -> list[dict]:
    """
    Lấy các sách cùng tác giả theo author_id chính xác (không dùng text search).
    Đây là cách duy nhất đảm bảo kết quả đúng tác giả, tránh việc OpenSearch
    match tên tác giả trong tiêu đề sách (VD: “Cùng Dale Carnegie Tiến Tới”).
    """
    if not author_id:
        return []
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT b.book_id, b.title, a.author_name, b.price,
                       b.avg_rating, b.stock_quantity, c.category_name
                FROM books b
                LEFT JOIN authors a ON b.author_id = a.author_id
                LEFT JOIN book_categories bc ON bc.book_id = b.book_id
                LEFT JOIN categories c ON bc.category_id = c.category_id
                WHERE b.author_id = %s
                  AND b.status = 'active'
                  AND b.stock_quantity > 0
                  AND (%s = '' OR b.title NOT LIKE %s)
                ORDER BY b.avg_rating DESC, b.stock_quantity DESC
                LIMIT %s
            """, (author_id, exclude_title, f"%{exclude_title}%" if exclude_title else "", limit))
            rows = cur.fetchall()
            return [
                {
                    "book_id":     r["book_id"],
                    "title":       r["title"],
                    "author_name": r["author_name"],
                    "price":       float(r["price"]),
                    "rating":      float(r["avg_rating"]) if r["avg_rating"] else 0,
                    "stock":       int(r["stock_quantity"]),
                    "category":    r["category_name"],
                    "score":       1.0,
                }
                for r in rows
            ]
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_books_by_author_id] DB error: {e}")
        return []


def get_voucher_info(code: str) -> dict | None:
    """Tra cứu thông tin mã giảm giá từ bảng promotions."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT code, discount_percent, start_date, end_date,
                       is_active, status, is_deleted
                FROM promotions
                WHERE code = %s
                LIMIT 1
            """, (code.strip().upper(),))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "code":             row["code"],
                "discount_percent": float(row["discount_percent"]) if row["discount_percent"] else 0,
                "type":             "percent",
                "start_date":       str(row["start_date"]) if row["start_date"] else None,
                "end_date":         str(row["end_date"])   if row["end_date"]   else None,
                "is_active":        bool(row["is_active"]),
                "status":           row["status"],
                "is_deleted":       bool(row["is_deleted"]),
            }
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_voucher_info] DB error: {e}")
        return None


def get_all_vouchers(active_only: bool = True) -> list[dict]:
    """Lấy danh sách mã giảm giá (mặc định chỉ lấy mã đang hoạt động)."""
    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)
        try:
            query = """
                SELECT code, discount_percent, start_date, end_date, status
                FROM promotions
                WHERE is_deleted = 0
            """
            if active_only:
                query += " AND status = 'active'"
            query += " ORDER BY promo_id DESC LIMIT 20"
            cur.execute(query)
            rows = cur.fetchall()
            return [
                {
                    "code":             r["code"],
                    "discount_percent": float(r["discount_percent"]) if r["discount_percent"] else 0,
                    "end_date":         str(r["end_date"]) if r["end_date"] else None,
                    "status":           r["status"],
                }
                for r in rows
            ]
        finally:
            cur.close(); conn.close()
    except Exception as e:
        print(f"[get_all_vouchers] DB error: {e}")
        return []
