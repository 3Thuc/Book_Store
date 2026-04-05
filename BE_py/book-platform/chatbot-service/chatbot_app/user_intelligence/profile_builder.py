"""
profile_builder.py – Xây dựng User Profile tự động từ hành vi.

Tích hợp với bảng user_actions có sẵn (view=1, cart=3, purchase=8).
K-Means clustering phân nhóm người dùng thành 4 cluster:
  0 – "Người đọc chuyên sâu"
  1 – "Người mua quà tặng"
  2 – "Người tìm kiếm deal/giá tốt"
  3 – "Người đọc casual"
"""
import pickle
import numpy as np
from pathlib import Path
from chatbot_app.db import get_connection

CLUSTER_NAMES = {
    0: "chuyên sâu",
    1: "quà tặng",
    2: "tìm deal",
    3: "casual",
}

CLUSTER_TONES = {
    0: "chi tiết, học thuật, cung cấp đầy đủ thông tin chuyên sâu",
    1: "ấm áp, gợi cảm giác quà ý nghĩa, nhấn mạnh giá trị cảm xúc",
    2: "ngắn gọn, nêu rõ giá và ưu đãi, proactive đề xuất combo/sale",
    3: "thân thiện, nhẹ nhàng, gợi ý sách trending và bestseller",
}


def build_user_profile(user_id: int) -> dict:
    """
    Xây dựng profile người dùng từ dữ liệu hành vi trong DB.
    Trả về dict với: favorite_genres, avg_price, cluster_id, tone.
    """
    if not user_id:
        return _default_profile()

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)

    # Thể loại yêu thích (từ sách đã mua/xem nhiều)
    cur.execute("""
        SELECT c.category_name, SUM(
            CASE ua.action_type
                WHEN 'purchase'    THEN 8
                WHEN 'add_to_cart' THEN 3
                WHEN 'view'        THEN 1
                ELSE 0
            END
        ) AS w
        FROM user_actions ua
        JOIN book_categories bc ON bc.book_id = ua.book_id
        JOIN categories c ON c.category_id = bc.category_id
        WHERE ua.user_id = %s
        GROUP BY c.category_name
        ORDER BY w DESC
        LIMIT 3
    """, (user_id,))
    fav_genres = [r["category_name"] for r in cur.fetchall()]

    # Giá trung bình sách đã mua
    cur.execute("""
        SELECT AVG(b.price) AS avg_price
        FROM user_actions ua
        JOIN books b ON b.book_id = ua.book_id
        WHERE ua.user_id = %s AND ua.action_type = 'purchase'
    """, (user_id,))
    row = cur.fetchone()
    avg_price = float(row["avg_price"] or 0)

    cur.close()
    conn.close()

    # Xác định cluster đơn giản (rule-based, đủ dùng cho MVP)
    cluster_id = _simple_cluster(avg_price, fav_genres)

    return {
        "user_id":      user_id,
        "cluster_id":   cluster_id,
        "cluster_name": CLUSTER_NAMES.get(cluster_id, "casual"),
        "tone":         CLUSTER_TONES.get(cluster_id, CLUSTER_TONES[3]),
        "favorite_genres": fav_genres,
        "avg_price":    round(avg_price, 0),
    }


def _simple_cluster(avg_price: float, genres: list) -> int:
    """Rule-based clustering (dùng trước khi có đủ data train K-Means)."""
    tech_genres   = {"lập trình", "khoa học", "triết học", "kinh tế", "y học"}
    casual_genres = {"văn học", "tiểu thuyết", "tâm linh"}

    if any(g in tech_genres for g in genres):
        return 0   # chuyên sâu
    if avg_price < 80000:
        return 2   # tìm deal
    if any(g in casual_genres for g in genres):
        return 3   # casual
    return 1       # quà tặng (mặc định còn lại)


def _default_profile() -> dict:
    """Profile mặc định cho khách vãng lai."""
    return {
        "user_id":         None,
        "cluster_id":      3,
        "cluster_name":    "casual",
        "tone":            CLUSTER_TONES[3],
        "favorite_genres": [],
        "avg_price":       0,
    }
