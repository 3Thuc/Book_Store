"""
P3.3 – BK-Tree cho pHash matching
=================================
Thay the vong lap O(n) trong find_similar_book() bang BK-Tree O(log n).

BK-Tree (Burkhard-Keller Tree):
  - Cay tim kiem tren khong gian metric (Hamming distance).
  - Moi node luu: hash gia tri + dict {distance: child_node}.
  - Tim kiem: xuat phat tu root, chi di vao node con
    co distance nam trong [d-threshold, d+threshold] -> cat bot nhanh.

Do phuc tap:
  - Build: O(n log n) trung binh
  - Search: O(log n) trung binh, O(n) worst case (khi threshold lon)
  - Voi threshold = 10/64 bit (15%) -> thuc te O(log n)

Voi 20.000 sach:
  - Linear O(n):   ~2-5ms   (20.000 comparisons)
  - BK-Tree O(logn): ~0.1-0.5ms (200-500 comparisons trung binh)
"""
from __future__ import annotations
import logging
from typing import Optional

logger = logging.getLogger("ocr.bktree")


class _BKNode:
    """Mot node trong BK-Tree."""
    __slots__ = ("hash_val", "entry", "children")

    def __init__(self, hash_val: int, entry):
        self.hash_val = hash_val
        self.entry    = entry
        self.children: dict[int, "_BKNode"] = {}


class BKTree:
    """
    BK-Tree tren Hamming distance cho 64-bit pHash.

    Vi du:
        tree = BKTree()
        for entry in _hash_index:
            tree.add(entry.phash, entry)

        # Tim tat ca entry co dist <= 10
        results = tree.search(query_hash, threshold=10)
    """

    def __init__(self):
        self._root: Optional[_BKNode] = None
        self._size: int = 0

    @staticmethod
    def _hamming(a: int, b: int) -> int:
        return bin(a ^ b).count("1")

    def add(self, hash_val: int, entry) -> None:
        """Them 1 entry vao cay."""
        if self._root is None:
            self._root = _BKNode(hash_val, entry)
            self._size += 1
            return

        node = self._root
        while True:
            d = self._hamming(hash_val, node.hash_val)
            if d == 0:
                # Hash trung nhau -> ghi de entry (upsert)
                node.entry = entry
                return
            if d not in node.children:
                node.children[d] = _BKNode(hash_val, entry)
                self._size += 1
                return
            node = node.children[d]

    def search(self, query_hash: int, threshold: int) -> list[tuple[int, object]]:
        """
        Tim tat ca entry co Hamming distance <= threshold voi query_hash.

        Returns:
            List[(distance, entry)] sap xep theo distance tang dan.
        """
        if self._root is None:
            return []

        results: list[tuple[int, object]] = []
        stack  : list[_BKNode] = [self._root]

        while stack:
            node = stack.pop()
            d    = self._hamming(query_hash, node.hash_val)

            if d <= threshold:
                results.append((d, node.entry))

            # Chi xet cac nhanh co kha nang match
            # Neu distance d_parent -> query = d, thi voi moi child co key k:
            #   Hamming(child, query) >= |d - k| (bat dang thuc tam giac)
            # -> Chi di vao child neu |d - k| <= threshold
            lo = max(0, d - threshold)
            hi = d + threshold
            for k, child in node.children.items():
                if lo <= k <= hi:
                    stack.append(child)

        results.sort(key=lambda x: x[0])
        return results

    def find_best(self, query_hash: int, threshold: int) -> Optional[tuple[int, object]]:
        """
        Tim entry gan nhat trong threshold.
        Nhanh hon search() doi voi use-case chi can 1 ket qua.

        Returns:
            (best_distance, best_entry) hoac None.
        """
        if self._root is None:
            return None

        best_dist  = threshold + 1
        best_entry = None
        stack      = [self._root]

        while stack:
            node = stack.pop()
            d    = self._hamming(query_hash, node.hash_val)

            if d < best_dist:
                best_dist  = d
                best_entry = node.entry

            # Pruning tich cuc: chi xet nhanh co the cai thien best
            lo = max(0, d - best_dist)
            hi = d + best_dist
            for k, child in node.children.items():
                if lo <= k <= hi:
                    stack.append(child)

        if best_entry is None or best_dist > threshold:
            return None
        return best_dist, best_entry

    @property
    def size(self) -> int:
        return self._size

    def __len__(self) -> int:
        return self._size
