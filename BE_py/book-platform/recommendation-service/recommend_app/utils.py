# app/utils.py
# ❖ Tự động chuyển exception MySQL thành HTTPException(500)
# ❖ Ghi log (hoặc print nếu chưa setup logging)
import logging
from contextlib import contextmanager
from typing import List
from fastapi import HTTPException
from recommend_app.db import get_connection
from recommend_app.models import BookRecommendation
import mysql.connector

logger = logging.getLogger("recommend.db")


@contextmanager
def _get_conn():
    """
    Context manager đảm bảo connection luôn được trả về pool sau khi dùng.
    Dùng với: `with _get_conn() as conn:`
    """
    conn = None
    try:
        conn = get_connection()
        yield conn
    finally:
        if conn:
            try:
                conn.close()   # Trả về pool, không đóng thực sự
            except Exception:
                pass


def _call_proc(proc_name: str, args: list) -> List[dict]:
    """
    Gọi stored procedure và trả về list dict.
    Tự động trả HTTP 500 khi có lỗi DB hoặc lỗi proc.
    """
    try:
        with _get_conn() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.callproc(proc_name, args)
                rows: List[dict] = []
                for result in cursor.stored_results():
                    rows.extend(result.fetchall())
                return rows
            except mysql.connector.Error as e:
                logger.error("[DB ERROR] %s: %s", proc_name, e)
                raise HTTPException(
                    status_code=500,
                    detail=f"Database error while executing {proc_name}: {str(e)}"
                )
            except Exception as e:
                logger.error("[SYSTEM ERROR] %s: %s", proc_name, e)
                raise HTTPException(
                    status_code=500,
                    detail=f"Server error while executing {proc_name}: {str(e)}"
                )
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
    except HTTPException:
        raise
    except mysql.connector.errors.PoolError as e:
        logger.error("[POOL EXHAUSTED] %s: %s", proc_name, e)
        raise HTTPException(
            status_code=503,
            detail="Server đang bận, vui lòng thử lại sau."
        )
    except Exception as e:
        logger.error("[CONN ERROR] %s: %s", proc_name, e)
        raise HTTPException(status_code=500, detail=str(e))


def _call_query(query: str, args: tuple = ()) -> List[dict]:
    """Gọi một raw select SQL thay vì stored procedure."""
    try:
        with _get_conn() as conn:
            cursor = conn.cursor(dictionary=True)
            try:
                cursor.execute(query, args)
                return cursor.fetchall()
            except Exception as e:
                logger.error("[DB ERROR] Query: %s", e)
                raise HTTPException(status_code=500, detail=str(e))
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
    except HTTPException:
        raise
    except mysql.connector.errors.PoolError as e:
        logger.error("[POOL EXHAUSTED] Query: %s", e)
        raise HTTPException(status_code=503, detail="Server đang bận, vui lòng thử lại sau.")
    except Exception as e:
        logger.error("[CONN ERROR] Query: %s", e)
        raise HTTPException(status_code=500, detail=str(e))



def _rows_to_recommendations(rows: List[dict], default_reason: str) -> List[BookRecommendation]:
    """
    Convert MySQL rows -> Pydantic model list.
    """
    recs: List[BookRecommendation] = []

    for row in rows:
        reason = row.get("reason") or row.get("reasons") or default_reason

        rec = BookRecommendation(
            book_id=row["book_id"],
            title=row.get("title", ""),
            price=float(row.get("price") or 0.0),
            main_image=row.get("main_image"),
            author_name=row.get("author_name"),
            avg_rating=float(row.get("avg_rating") or 0),
            rating_count=int(row.get("rating_count") or 0),
            reason=reason,
            total_sold=row.get("total_sold"),
            final_score=float(row["final_score"]) if row.get(
                "final_score") is not None else None,
            co_purchase_count=row.get("co_purchase_count"),
            view_count=row.get("view_count"),
            category_name=row.get("category_name"),  # P2.3: MMR diversity grouping
        )
        recs.append(rec)

    return recs
