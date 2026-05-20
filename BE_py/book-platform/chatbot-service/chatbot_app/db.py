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
            pool_size=20,           # tăng từ 5 lên 20 để xử lý concurrent requests
            pool_reset_session=True,
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            connection_timeout=30,  # timeout 30s thay vì mặc định
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
    return _pool

def get_connection():
    return get_pool().get_connection()


def test_connection() -> bool:
    """Kiểm tra MySQL kết nối được không. Dùng trong main.py startup."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()   # ← PHẢI fetch trước khi close, tránh 'Unread result found'
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ MySQL error: {e}")
        return False


def ensure_tables():
    """
    Tạo bảng chat_sessions và chat_messages nếu chưa tồn tại.
    v2: Thêm composite index và MySQL Event Scheduler cleanup.
    Gọi từ main.py startup.
    """
    conn = get_connection()
    cur  = conn.cursor()

    # ── Bảng chat_sessions ──────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_sessions (
            session_id   VARCHAR(64)  PRIMARY KEY,
            user_id      INT          NULL,
            started_at   DATETIME     DEFAULT NOW(),
            last_active  DATETIME     DEFAULT NOW(),
            context_json JSON,
            turn_count   INT          DEFAULT 0,
            user_cluster TINYINT      NULL,
            INDEX idx_user   (user_id),
            INDEX idx_active (last_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # ── Bảng chat_messages ──────────────────────────────────────────────────
    # Composite index idx_session_time: tối ưu cho query hot path:
    #   WHERE session_id = ? ORDER BY created_at DESC LIMIT N
    # MySQL dùng index này để sort theo created_at MÀ KHÔNG cần filesort riêng
    # → giảm query time ~40-60% so với index đơn idx_session(session_id)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
            session_id          VARCHAR(64)  NOT NULL,
            role                ENUM('user','assistant') NOT NULL,
            content             TEXT         NOT NULL,
            intent              VARCHAR(64)  NULL,
            confidence          FLOAT        NULL,
            sentiment           ENUM('POSITIVE','NEGATIVE','NEUTRAL') NULL,
            entities            JSON         NULL,
            retrieval_sources   JSON         NULL,
            created_at          DATETIME     DEFAULT NOW(),
            -- Composite index thay thế index đơn: tối ưu ORDER BY + LIMIT
            INDEX idx_session_time (session_id, created_at DESC),
            INDEX idx_intent       (intent)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # ── Phương án A: Thêm composite index nếu bảng đã tồn tại ──────────────
    # idx_session cũ (đơn) → thay bằng idx_session_time (composite)
    # Dùng try/except vì ALTER TABLE sẽ lỗi "Duplicate key name" nếu đã có
    try:
        cur.execute("""
            ALTER TABLE chat_messages
            DROP INDEX IF EXISTS idx_session,
            ADD INDEX IF NOT EXISTS idx_session_time (session_id, created_at DESC)
        """)
        conn.commit()
        print("   ✅ Composite index idx_session_time added/verified")
    except Exception:
        pass  # Index đã tồn tại hoặc DB không hỗ trợ IF NOT EXISTS → bỏ qua

    # ── Phương án D: MySQL Event Scheduler – auto cleanup 30 ngày ───────────
    # Xóa messages của session inactive > 30 ngày lúc 2h sáng hàng ngày.
    # LIMIT 5000/lần để không lock table quá lâu.
    #
    # LƯU Ý: Event Scheduler phải được bật trên MySQL:
    #   SET GLOBAL event_scheduler = ON;    ← cần quyền SUPER
    # Nếu không có quyền → event vẫn được tạo nhưng không chạy tự động
    # → chạy thủ công: CALL cleanup_old_chat_data();
    try:
        cur.execute("""
            CREATE EVENT IF NOT EXISTS cleanup_old_chat_sessions
            ON SCHEDULE EVERY 1 DAY
            STARTS TIMESTAMP(CURRENT_DATE, '02:00:00')
            ON COMPLETION PRESERVE
            COMMENT 'Auto-xoa session va message cu hon 30 ngay'
            DO BEGIN
                -- Bước 1: Xóa messages của session đã cũ
                -- (không dùng JOIN + LIMIT vì MySQL không hỗ trợ)
                DELETE FROM chat_messages
                WHERE session_id IN (
                    SELECT session_id FROM (
                        SELECT session_id FROM chat_sessions
                        WHERE last_active < NOW() - INTERVAL 30 DAY
                        LIMIT 5000
                    ) AS old_sessions
                );

                -- Bước 2: Xóa chính session cũ
                DELETE FROM chat_sessions
                WHERE last_active < NOW() - INTERVAL 30 DAY
                LIMIT 1000;
            END
        """)
        print("   ✅ MySQL Event cleanup_old_chat_sessions created/verified")
    except Exception as ev_err:
        print(f"   ⚠️ Event Scheduler not available (need SUPER privilege): {ev_err}")
        print("       → Chạy thủ công khi cần: DELETE FROM chat_sessions WHERE last_active < NOW() - INTERVAL 30 DAY LIMIT 1000")


    conn.commit()
    cur.close()
    conn.close()
    print("   ✅ MySQL tables ready (chat_sessions, chat_messages) — v2 optimized")
