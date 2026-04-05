"""
sql_retriever.py – Lấy dữ liệu realtime từ MySQL.
Dùng cho: đơn hàng, sách theo thể loại, giá, tồn kho.
"""
from chatbot_app.db import get_connection


def get_order_info(order_id: int) -> dict | None:
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT o.order_id, o.status, o.total_price, o.created_at,
               o.shipping_address
        FROM orders o
        WHERE o.order_id = %s
        LIMIT 1
    """, (order_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row


def get_user_orders(user_id: int, limit: int = 5) -> list[dict]:
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT o.order_id, o.status, o.total_price, o.created_at,
               COUNT(oi.book_id) AS book_count
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.user_id = %s
        GROUP BY o.order_id
        ORDER BY o.created_at DESC
        LIMIT %s
    """, (user_id, limit))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_books_by_genre(genre: str, limit: int = 5) -> list[dict]:
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT b.book_id, b.title, a.author_name, b.price
        FROM books b
        LEFT JOIN authors a ON b.author_id = a.author_id
        LEFT JOIN book_categories bc ON bc.book_id = b.book_id
        LEFT JOIN categories c ON c.category_id = bc.category_id
        WHERE c.category_name LIKE %s
          AND b.status = 'active'
          AND b.stock_quantity > 0
        ORDER BY b.sold_quantity DESC
        LIMIT %s
    """, (f"%{genre}%", limit))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_book_price(title: str) -> dict | None:
    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT b.book_id, b.title, b.price, b.stock_quantity,
               a.author_name
        FROM books b
        LEFT JOIN authors a ON b.author_id = a.author_id
        WHERE b.title LIKE %s AND b.status = 'active'
        LIMIT 1
    """, (f"%{title}%",))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row
