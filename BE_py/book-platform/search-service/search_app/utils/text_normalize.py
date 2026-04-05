import re
import unicodedata


def fold_vi(text: str) -> str:
    if not text:
        return ""
    s = text.strip().lower()
    s = s.replace("đ", "d").replace("Đ", "d")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_query(q: str) -> str:
    # bạn có thể mở rộng synonym nhẹ ở đây nếu muốn
    return fold_vi(q)
