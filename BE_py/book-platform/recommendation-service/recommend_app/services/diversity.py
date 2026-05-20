"""
MMR Diversity Filter – P2.3
Maximal Marginal Relevance: Can bang relevance va diversity trong ket qua goi y.

Van de giai quyet:
  - /recommend/user/{id}/for-you thuong tra toan sach cung 1 the loai (filter bubble)
  - Nguoi dung mua toan "Ky nang song" -> chi thay the loai nay mai

Giai phap:
  - MMR: Chon sach tiep theo bang cong thuc can bang relevance & da dang
  - max_same_category: Khong cho qua N sach cung the loai trong ket qua cuoi
"""
import logging
from typing import List, Any

logger = logging.getLogger("recommend.diversity")


def _category_overlap(rec_a: Any, rec_b: Any) -> float:
    """
    Do overlap category giua 2 sach.
    rec_a, rec_b la BookRecommendation objects.
    Neu cung category_name -> 1.0, nguoc lai -> 0.0.
    """
    cat_a = getattr(rec_a, "category_name", None) or getattr(rec_a, "reason", "")
    cat_b = getattr(rec_b, "category_name", None) or getattr(rec_b, "reason", "")
    # Simple: same category = 1, khac = 0
    if cat_a and cat_b and cat_a == cat_b:
        return 1.0
    return 0.0


def mmr_diversify(
    recs: list,
    k: int = 20,
    lambda_: float = 0.7,
    max_same_category: int = 3,
) -> list:
    """
    Ap dung Maximal Marginal Relevance de da dang hoa danh sach goi y.

    Args:
        recs:               Danh sach BookRecommendation ban dau (da ranked)
        k:                  So ket qua mong muon tra ve
        lambda_:            He so: cao -> uu tien relevance, thap -> uu tien diversity.
                            0.7 = 70% relevance + 30% diversity (recommended)
        max_same_category:  Toi da bao nhieu sach cung category duoc phep xuat hien

    Returns:
        Danh sach da duoc re-rank de can bang relevance + diversity

    Vi du:
        Input (sorted by CF score):
          1. Ky nang song A (score=0.9)
          2. Ky nang song B (score=0.88)
          3. Ky nang song C (score=0.85)
          4. Tam ly hoc D  (score=0.82)
          5. Van hoc E     (score=0.80)

        Output (MMR diverse=True, max_same_category=2):
          1. Ky nang song A  <- best relevance
          2. Tam ly hoc D    <- diverse pick (khac category)
          3. Ky nang song B  <- 2nd ky nang (con duoc, chua qua 2)
          4. Van hoc E       <- diverse pick
          5. Ky nang song C  <- bi skip (da co 2 ky nang, den luot sach khac truoc)
    """
    if not recs or len(recs) <= 1:
        return recs[:k]

    # Lay max score de normalize
    scores = [getattr(r, "final_score", None) or 0.0 for r in recs]
    max_score = max(scores) if scores else 1.0
    if max_score == 0:
        max_score = 1.0

    selected    = []
    remaining   = list(recs)
    cat_count   = {}  # category_name -> so lan da chon

    for _ in range(min(k, len(recs))):
        if not remaining:
            break

        best_item  = None
        best_score = float("-inf")

        for candidate in remaining:
            # Relevance score (normalized 0-1)
            raw_score = getattr(candidate, "final_score", None) or 0.0
            rel_score = raw_score / max_score

            # Category diversity penalty
            # Neu category da chon qua max -> penalty = 1 (rat cao)
            cat = getattr(candidate, "category_name", None) or "unknown"
            cat_times = cat_count.get(cat, 0)

            if cat_times >= max_same_category:
                # Khong chon sach nay neu da du the loai
                continue

            # Similarity voi nhung sach da chon (tinh theo category overlap)
            if selected:
                max_sim = max(_category_overlap(candidate, s) for s in selected)
            else:
                max_sim = 0.0

            # MMR score
            mmr_score = lambda_ * rel_score - (1 - lambda_) * max_sim

            if mmr_score > best_score:
                best_score = mmr_score
                best_item  = candidate

        if best_item is None:
            # Tat ca remaining bi block boi max_same_category -> lay cai cao nhat con lai
            remaining_sorted = sorted(
                remaining,
                key=lambda r: getattr(r, "final_score", None) or 0.0,
                reverse=True
            )
            best_item = remaining_sorted[0]

        selected.append(best_item)
        remaining.remove(best_item)

        # Cap nhat category count
        cat = getattr(best_item, "category_name", None) or "unknown"
        cat_count[cat] = cat_count.get(cat, 0) + 1

    logger.debug(
        "[MMR] diversify: %d input -> %d output | lambda=%.1f | max_cat=%d",
        len(recs), len(selected), lambda_, max_same_category
    )
    return selected
