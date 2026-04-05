"""
db.py – Kết nối MySQL cho chatbot service.
Dùng chung database bookstore với hệ thống hiện tại.
"""
import mysql.connector
from mysql.connector import pooling
from chatbot_app.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

# Connection pool để tránh tạo connection mới mỗi request
_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="chatbot_pool",
            pool_size=5,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
        )
    return _pool

def get_connection():
    return get_pool().get_connection()
