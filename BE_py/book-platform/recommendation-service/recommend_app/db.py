import mysql.connector
from mysql.connector import pooling
from .config import settings

# Tạo connection pool để tái sử dụng connection, tránh connect/disconnect liên tục
connection_pool = pooling.MySQLConnectionPool(
    pool_name="bookstore_pool",
    pool_size=5,
    pool_reset_session=True,
    host=settings.MYSQL_HOST,
    port=settings.MYSQL_PORT,
    user=settings.MYSQL_USER,
    password=settings.MYSQL_PASSWORD,
    database=settings.MYSQL_DB,
)


def get_connection():
    return connection_pool.get_connection()
