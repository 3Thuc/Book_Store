import mysql.connector
from mysql.connector import pooling
from .config import settings

# Tạo connection pool để tái sử dụng connection, tránh connect/disconnect liên tục
# pool_size=20: Hỗ trợ ~20 request đồng thời (mỗi request cần 1 connection)
# pool_reset_session=True: Reset session vars sau mỗi lần dùng (tránh dirty state)
connection_pool = pooling.MySQLConnectionPool(
    pool_name="bookstore_pool",
    pool_size=20,
    pool_reset_session=True,
    host=settings.MYSQL_HOST,
    port=settings.MYSQL_PORT,
    user=settings.MYSQL_USER,
    password=settings.MYSQL_PASSWORD,
    database=settings.MYSQL_DB,
    connection_timeout=30,      # Timeout lấy connection từ pool (30s)
    connect_timeout=10,         # Timeout kết nối TCP đến MySQL (10s)
)


def get_connection():
    return connection_pool.get_connection()
