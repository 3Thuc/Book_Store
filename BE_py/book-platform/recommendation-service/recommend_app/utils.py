# app/utils.py
# ✦ Tự động chuyển exception MySQL thành HTTPException(500)
# ✦ Ghi log (hoặc print nếu chưa setup logging)
from typing import List
from fastapi import HTTPException
from recommend_app.db import get_connection
from recommend_app.models import BookRecommendation
import mysql.connector


def _call_proc(proc_name: str, args: list) -> List[dict]:
    """
    Gọi stored procedure và trả về list dict.
    Tự động trả HTTP 500 khi có lỗi DB hoặc lỗi proc.
    """
    conn = get_connection()
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.callproc(proc_name, args)

        rows: List[dict] = []
        for result in cursor.stored_results():
            rows.extend(result.fetchall())

        return rows

    except mysql.connector.Error as e:
        print(f"[DB ERROR] {proc_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Database error while executing {proc_name}: {str(e)}"
        )

    except Exception as e:
        print(f"[SYSTEM ERROR] {proc_name}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Server error while executing {proc_name}: {str(e)}"
        )

    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass

        try:
            conn.close()
        except:
            pass


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
            view_count=row.get("view_count")
        )
        recs.append(rec)

    return recs
