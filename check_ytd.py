import sys; sys.path.insert(0, '/app')
from chatbot_app.db import get_connection
conn = get_connection()
cur = conn.cursor(dictionary=True)
cur.execute('''
    SELECT
        MONTH(order_date) as month,
        SUM(CASE WHEN status NOT IN ('cancelled', 'returned', 'failed') THEN 1 ELSE 0 END) as total_orders,
        COALESCE(SUM(CASE WHEN status NOT IN ('cancelled', 'returned', 'failed') THEN total_amount ELSE 0 END), 0) as revenue,
        COALESCE(SUM(CASE WHEN status = 'delivered' THEN total_amount ELSE 0 END), 0) as confirmed_revenue
    FROM orders
    WHERE YEAR(order_date) = 2026
    GROUP BY MONTH(order_date)
    ORDER BY month
''')
print('Monthly:', cur.fetchall())

cur.execute('''
    SELECT
        COALESCE(SUM(total_amount), 0) as ytd_total,
        COUNT(order_id) as ytd_orders
    FROM orders
    WHERE YEAR(order_date) = 2026 AND status NOT IN ('cancelled', 'returned', 'failed')
''')
print('YTD:', cur.fetchone())
cur.close(); conn.close()

