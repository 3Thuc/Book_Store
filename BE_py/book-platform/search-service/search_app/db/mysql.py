import os
import pymysql
from dotenv import load_dotenv

load_dotenv()


def get_mysql_conn():
    host = os.getenv("MYSQL_HOST", "localhost")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    db = os.getenv("MYSQL_DB", "bookstore")

    password = os.getenv("MYSQL_PASSWORD")

    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "database": db,
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
    }

    # ✅ CHỈ set password nếu THỰC SỰ có
    if password is not None and password != "":
        kwargs["password"] = password

    try:
        return pymysql.connect(**kwargs)
    except pymysql.err.OperationalError as e:
        raise RuntimeError(
            f"MySQL connection failed for user='{user}'. "
            f"Password used={'YES' if password else 'NO'}"
        ) from e
