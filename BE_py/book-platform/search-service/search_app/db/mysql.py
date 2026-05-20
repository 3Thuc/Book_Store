import logging
import os

import pymysql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("search.db")

# ── Connection Pool (DBUtils) ─────────────────────────────────────────────────
# Pool tái sử dụng kết nối TCP thay vì tạo mới mỗi lần → giảm ~10-20ms overhead
# mỗi lần index, tránh resource leak khi connection không được close đúng cách.

_pool = None


def _build_pool():
    """Khởi tạo connection pool lần đầu (lazy init)."""
    global _pool
    if _pool is not None:
        return _pool

    host     = os.getenv("MYSQL_HOST", "localhost")
    port     = int(os.getenv("MYSQL_PORT", "3306"))
    user     = os.getenv("MYSQL_USER", "root")
    db       = os.getenv("MYSQL_DB", "bookstore")
    password = os.getenv("MYSQL_PASSWORD") or ""

    creator_kwargs = {
        "host":        host,
        "port":        port,
        "user":        user,
        "database":    db,
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit":  True,
    }
    if password:
        creator_kwargs["password"] = password

    try:
        from dbutils.pooled_db import PooledDB  # type: ignore

        _pool = PooledDB(
            creator=pymysql,
            maxconnections=10,   # Tối đa 10 connection đồng thời (khớp OS pool=10)
            mincached=2,         # Giữ sẵn 2 connection idle để giảm latency
            maxcached=5,         # Tối đa 5 connection idle trong cache
            blocking=True,       # Block thay vì raise khi hết pool (safer)
            **creator_kwargs,
        )
        logger.info("[MySQL] PooledDB initialized (maxconn=10, mincached=2)")
    except ImportError:
        # Fallback: DBUtils chưa cài → dùng factory function (không pool)
        logger.warning(
            "[MySQL] dbutils không tìm thấy → dùng connection factory (không pool). "
            "Cài thêm: pip install dbutils"
        )
        _pool = None  # Đánh dấu không có pool

    return _pool


def get_mysql_conn():
    """
    Trả về MySQL connection từ pool (hoặc tạo mới nếu pool không khả dụng).

    Connection trả về tương thích context manager:
        conn = get_mysql_conn()
        try:
            with conn.cursor() as cur:
                ...
        finally:
            conn.close()   # Trả về pool, không đóng TCP thật sự

    Raises:
        RuntimeError: Nếu không thể kết nối MySQL.
    """
    pool = _build_pool()

    if pool is not None:
        # Connection từ pool — .close() trả về pool, không đóng TCP
        return pool.connection()

    # Fallback: tạo connection thường (khi dbutils chưa cài)
    host     = os.getenv("MYSQL_HOST", "localhost")
    port     = int(os.getenv("MYSQL_PORT", "3306"))
    user     = os.getenv("MYSQL_USER", "root")
    db       = os.getenv("MYSQL_DB", "bookstore")
    password = os.getenv("MYSQL_PASSWORD")

    kwargs = {
        "host":        host,
        "port":        port,
        "user":        user,
        "database":    db,
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit":  True,
    }
    if password is not None and password != "":
        kwargs["password"] = password

    try:
        return pymysql.connect(**kwargs)
    except pymysql.err.OperationalError as e:
        raise RuntimeError(
            f"MySQL connection failed for user='{user}'. "
            f"Password used={'YES' if password else 'NO'}"
        ) from e
