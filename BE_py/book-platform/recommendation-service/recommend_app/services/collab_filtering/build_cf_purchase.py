"""
build_cf_purchase.py – CF Purchase-Based dùng ALS
====================================================
Khác biệt so với CF_IMPLICIT:
  - Tín hiệu: Số lượng đặt hàng thực tế (từ bảng orders/order_details).
  - Signal mạnh hơn (Explicit purchase intent), không lẫn tạp tín hiệu view.
  - Kết quả cho section "Người mua sách này cũng đã mua..."
  - Tag: CF_PURCHASE

Thuật toán: Giống CF_IMPLICIT, dùng ALS thay Brute Force kNN.
"""

import os
import time
import numpy as np
import mysql.connector
import pandas as pd
from scipy import sparse
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Fix OpenBLAS multi-thread warning
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

load_dotenv()

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB       = os.getenv("MYSQL_DB", "bookstore")

FACTORS        = int(os.getenv("CF_FACTORS", "64"))
ITERATIONS     = int(os.getenv("CF_ITERATIONS", "20"))
REGULARIZATION = float(os.getenv("CF_REG", "0.05"))
ALPHA          = float(os.getenv("CF_ALPHA", "10.0"))  # Purchase signal mạnh → alpha cao hơn
TOP_K          = int(os.getenv("CF_TOPK", "50"))


def get_engine():
    url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    return create_engine(url)


def load_purchase_interactions() -> pd.DataFrame:
    engine = get_engine()
    sql = """
        SELECT
            o.user_id, od.book_id,
            SUM(od.quantity) AS raw_score
        FROM orders o
        JOIN order_details od ON o.order_id = od.order_id
        JOIN books b ON b.book_id = od.book_id
        WHERE o.status IN ('processing', 'shipped', 'delivered')
          AND o.user_id IS NOT NULL
          AND b.status = 'active'
          AND b.stock_quantity > 0
        GROUP BY o.user_id, od.book_id
        HAVING raw_score > 0
    """
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df


def build_confidence_matrix(df: pd.DataFrame):
    user_ids = df["user_id"].astype("category")
    book_ids = df["book_id"].astype("category")

    user_idx  = dict(enumerate(user_ids.cat.categories))
    book_idx  = dict(enumerate(book_ids.cat.categories))

    row   = user_ids.cat.codes.to_numpy()
    col   = book_ids.cat.codes.to_numpy()
    conf  = (1 + ALPHA * df["raw_score"].to_numpy()).astype(np.float32)

    C = sparse.csr_matrix(
        (conf, (row, col)),
        shape=(len(user_idx), len(book_idx)),
    )
    return C, user_idx, book_idx


def fit_als(C: sparse.csr_matrix) -> np.ndarray:
    try:
        from implicit.cpu.als import AlternatingLeastSquares
    except ImportError:
        raise ImportError("Cài thư viện: pip install implicit")

    model = AlternatingLeastSquares(
        factors=FACTORS,
        iterations=ITERATIONS,
        regularization=REGULARIZATION,
        random_state=42,
    )
    # implicit.fit() nhận User×Item (không phải Item×User)!
    model.fit(C)   # C shape (n_users, n_items) → item_factors: (n_items, FACTORS) ✓
    return model.item_factors


def compute_top_k_similar(item_factors: np.ndarray, book_idx: dict) -> list:
    norms = np.linalg.norm(item_factors, axis=1, keepdims=True) + 1e-9
    V = item_factors / norms

    n_items = V.shape[0]
    rows_to_insert = []
    batch_size = 512

    for start in range(0, n_items, batch_size):
        end = min(start + batch_size, n_items)
        sim = V[start:end] @ V.T

        for i_local, i_global in enumerate(range(start, end)):
            scores = sim[i_local]
            top_indices = np.argpartition(scores, -(TOP_K + 1))[-(TOP_K + 1):]
            top_indices = top_indices[np.argsort(scores[top_indices])[::-1]]

            src_book_id = book_idx[i_global]
            for j in top_indices:
                if j == i_global:
                    continue
                s = float(scores[j])
                if s < 0.05:
                    break
                rows_to_insert.append(
                    (int(src_book_id), int(book_idx[j]), round(s, 6), "CF_PURCHASE")
                )

    return rows_to_insert


def save_to_db(rows: list) -> None:
    import mysql.connector as mc
    conn = mc.connect(
        host=MYSQL_HOST, port=int(MYSQL_PORT),
        user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB,
    )
    cur  = conn.cursor()
    cur.execute("DELETE FROM similar_books WHERE algo_type='CF_PURCHASE'")
    conn.commit()

    sql = """
        INSERT INTO similar_books(book_id, similar_book_id, score, algo_type)
        VALUES (%s, %s, %s, %s) AS new_row
        ON DUPLICATE KEY UPDATE score = new_row.score
    """
    BATCH = 10_000
    for i in range(0, len(rows), BATCH):
        cur.executemany(sql, rows[i: i + BATCH])
        conn.commit()

    cur.close()
    conn.close()


def main():
    t0 = time.time()
    print(f"[ALS-CF-PURCHASE] factors={FACTORS} iterations={ITERATIONS} alpha={ALPHA} topk={TOP_K}")

    print("[1/4] Tải Purchase data từ orders/order_details...")
    df = load_purchase_interactions()
    print(f"      Interactions: {len(df):,}  |  Users: {df['user_id'].nunique():,}  |  Books: {df['book_id'].nunique():,}")

    if df.empty:
        print("[WARN] Không có đơn hàng. Bỏ qua CF_PURCHASE.")
        return

    print("[2/4] Build Confidence Matrix...")
    C, user_idx, book_idx = build_confidence_matrix(df)
    print(f"      Shape: {C.shape}  |  Sparsity: {1 - C.nnz / (C.shape[0]*C.shape[1]):.2%}")

    print("[3/4] Huấn luyện ALS...")
    item_factors = fit_als(C)

    print("[4/4] Tính Top-K & Lưu DB...")
    rows = compute_top_k_similar(item_factors, book_idx)
    print(f"      Tổng rows: {len(rows):,}")
    save_to_db(rows)

    elapsed = time.time() - t0
    print(f"\n[DONE] CF_PURCHASE hoàn tất trong {elapsed:.1f}s")


if __name__ == "__main__":
    main()
