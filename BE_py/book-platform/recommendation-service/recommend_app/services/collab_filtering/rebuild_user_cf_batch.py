import os
import sys
import time
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "bookstore")


def get_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
    )


def fetch_active_user_ids(conn, days: int):
    """
    Lấy danh sách user có hành vi gần đây (view/add_to_cart/purchase).
    Có thể thay bằng orders nếu bạn muốn.
    """
    sql = """
        SELECT DISTINCT user_id
        FROM user_actions
        WHERE user_id IS NOT NULL
          AND action_date >= NOW() - INTERVAL %s DAY
        ORDER BY user_id;
    """
    cur = conn.cursor()
    cur.execute(sql, (days,))
    user_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    return user_ids


def rebuild_for_user(conn, user_id: int, days: int, topn: int):
    """
    Gọi stored procedure:
      CALL sp_rebuild_user_cf_implicit(user_id, days, topn)
    """
    cur = conn.cursor()
    cur.callproc("sp_rebuild_user_cf_implicit", [user_id, days, topn])
    conn.commit()
    cur.close()


def main():
    # - active_days: lấy user active trong N ngày gần đây (default 30)
    # - history_days: window để tính CF (default 90)
    # - topn: số gợi ý lưu/user (default 50)
    active_days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    history_days = int(sys.argv[2]) if len(sys.argv) > 2 else 90
    topn = int(sys.argv[3]) if len(sys.argv) > 3 else 50

    print(
        f"[BatchCF] active_days={active_days}, history_days={history_days}, topn={topn}")

    conn = get_connection()
    try:
        user_ids = fetch_active_user_ids(conn, active_days)
        print(f"[BatchCF] active users: {len(user_ids)}")

        ok = 0
        fail = 0
        start = time.time()

        for i, uid in enumerate(user_ids, start=1):
            try:
                rebuild_for_user(conn, int(uid), history_days, topn)
                ok += 1
                if i % 50 == 0:
                    print(
                        f"[BatchCF] progress {i}/{len(user_ids)} ok={ok} fail={fail}")
            except Exception as e:
                fail += 1
                print(f"[BatchCF][ERR] user_id={uid} err={e}")

        elapsed = time.time() - start
        print(f"[BatchCF] done. ok={ok} fail={fail} time={elapsed:.1f}s")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
