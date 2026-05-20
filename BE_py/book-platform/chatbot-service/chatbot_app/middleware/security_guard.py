"""
security_guard.py – Lớp bảo vệ chatbot trước NLU pipeline.

Áp dụng cho TẤT CẢ role: guest / customer / staff / admin.
Chạy TRƯỚC intent classifier → không ảnh hưởng logic nghiệp vụ.

Phân loại vi phạm:
  BLOCK – Trả về ngay, không xử lý tiếp
  WARN  – Ghi log, nhắc nhở, vẫn tiếp tục (tối đa 1 lần / session)

Nhóm vi phạm:
  1. Jailbreak / Prompt Injection
  2. Chửi tục / Ngôn ngữ thô lỗ (VI + EN)
  3. Nội dung nhạy cảm / Không phù hợp
  4. SQL / Code Injection
  5. Spam / Flood (dùng context session)
"""

import re
import unicodedata
import time
from typing import Optional


# ── Normalize helper ──────────────────────────────────────────────────────────
def _norm(text: str) -> str:
    """Chuẩn hoá: lowercase, bỏ dấu tiếng Việt, bỏ ký tự đặc biệt."""
    text = text.lower().replace("đ", "d").replace("Đ", "d")
    nfd = unicodedata.normalize("NFD", text)
    no_accent = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    # Chuẩn hoá khoảng trắng
    return re.sub(r"\s+", " ", no_accent).strip()


# ── 1. JAILBREAK / PROMPT INJECTION ─────────────────────────────────────────
_JAILBREAK_PATTERNS = [
    # Ignore instructions
    r"(ignore|forget|disregard|bo qua|bỏ qua).{0,25}(previous|all|prior|truoc|trước).{0,25}"
    r"(instruction|rule|prompt|limit|huong dan|quy tac|gioi han)",
    # Act as / roleplay bypass
    r"(act as|pretend|roleplay|dong vai|đóng vai|gia vo|giả vờ).{0,25}"
    r"(ai|bot|assistant|dan|evil|khong co|không có|unrestricted)",
    # Đặt lại nhân cách
    r"(tu nay|from now|ke tu|từ giờ|dorénavant).{0,15}"
    r"(may|mày|you|bot|ai|he thong|hệ thống).{0,15}(la|là|is|are|be|tro thanh|trở thành)",
    # System prompt leak
    r"(system prompt|system message|initial prompt|base prompt|hidden prompt|cau lenh goc)",
    r"(xem|show|hien thi|hiện thị|reveal|print|display).{0,20}"
    r"(system|internal|hidden|goc|lệnh gốc|prompt|cau lenh)",
    # No restriction
    r"(no (restriction|limit|rule|filter|censorship|boundary)|khong co gioi han|"
    r"khong gioi han|without (limit|restriction|filter))",
    # Jailbreak keywords
    r"\b(jailbreak|bypass|override|hack|crack|exploit).{0,20}(bot|ai|system|filter|chatbot)\b",
    # DAN / Developer mode
    r"\b(dev mode|developer mode|god mode|deus mode|unrestricted mode|chaos mode|evil mode)\b"
    r"|\b(jailbreak mode|dan mode|dan prompt)\b"
    r"|\b(you are|be|am|tro thanh|bay gio la) dan\b"
    r"|\bdan (model|bot|ai|version|mode|prompt)\b",
    # In a story / fictional framing
    r"(trong boi canh|trong truyen|in the (context|story|fiction|roleplay|scenario)).{0,60}"
    r"(bot|ai|assistant|he thong)",
    # Claim to be admin/developer
    r"(toi la|i am|im).{0,10}(admin that|real admin|developer|creator|owner|chu he thong)",
    # Access all / root
    r"(cap quyen|grant|give me).{0,15}(admin|root|full access|superuser|all permission)",
    # Token smuggling
    r"(\[INST\]|\[SYS\]|<\|im_start\|>|<<SYS>>|SYSTEM:)",
]

_JAILBREAK_RE = [re.compile(p, re.IGNORECASE) for p in _JAILBREAK_PATTERNS]


def _is_jailbreak(norm_msg: str) -> bool:
    for pat in _JAILBREAK_RE:
        if pat.search(norm_msg):
            return True
    return False


# ── 2. PROFANITY (VI + EN) ───────────────────────────────────────────────────
# Từ điển từ tục/thô lỗ — dùng word boundary hoặc substring cho tiếng Việt
_PROFANITY_EXACT_VI = {
    "dm", "đm", "dit", "địt", "lon", "lồn", "cặc", "buoi", "buồi",
    "deo", "đéo", "clm", "đụ", "vl", "wtf",
    "cc", "dcm", "vcl", "đkm", "dkm", "đmm", "dmm", "đbm", "dbm",
    "mmdc", "mdc", "ngucl",
}
# FIX: bỏ "cac" (false positive với "các"), "vai"/"vãi" (vai trò), "du" ("đủ"→"du")
# NOTE: "cl" bỏ khỏi exact set (quá ngắn, false positive) → chỉ bắt qua substring
_PROFANITY_SUBSTR_VI = [
    "du ma", "duma", "du me", "dit me", "địt mẹ",
    "ngu vl", "ngu vcl", "con chó̀ ngu", "con chó chặt",   # chỉ match khi đi kèm từ thô tục
    "do ngu", "bot ngu", "bot dkm", "con bot dit", "chúng mày ngu",
    "thằng ngu", "con cac", "cai cac",
]
_PROFANITY_EN = [
    r"\bf+u+c+k+(ing|er|ed|s)?\b",    # fuck, fucking, fucker, fucked
    r"\bf[\.\*]c+k\b",                  # f.ck, f*ck
    r"\bsh[i1]t(ty)?\b",               # shit, shitty, sh1t
    r"\bsh[\.\*]t\b",                  # sh*t, sh.t
    r"\bb[i1]tc+h(es)?\b",             # bitch, bitches
    r"\bass+hole\b",                    # asshole
    r"\bb[a4]st[a4]rd\b",              # bastard
    r"\bdamn\s+(you|it|this)\b",       # damn you/it (chó dùng standalone)
    r"\bcr[a4]p\b",
    r"\bmoron\b", r"\bpiece\s+of\s+(shit|crap)\b",
    r"\bstupid\s+(bot|ai|ass)\b",      # stupid bot/ai
    r"\bworthless\s+(bot|ai)\b",
    r"\bpiss\s+off\b",
    r"\bgo\s+to\s+hell\b",
]
_PROFANITY_EN_RE = [re.compile(p, re.IGNORECASE) for p in _PROFANITY_EN]


def _is_profanity(norm_msg: str, original_msg: str) -> bool:
    tokens = set(re.split(r"[\s,.!?;:\"'\-/\\]+", norm_msg))
    if tokens & _PROFANITY_EXACT_VI:
        return True
    for sub in _PROFANITY_SUBSTR_VI:
        if sub in norm_msg:
            return True
    for pat in _PROFANITY_EN_RE:
        if pat.search(original_msg):
            return True
    return False


# ── 3. INJECTION (SQL / Code / Path traversal) ───────────────────────────────
_INJECTION_PATTERNS = [
    r"\b(drop|truncate|alter|create)\s+(table|database|schema|index)\b",
    r"\bdelete\s+from\s+\w+",                       # DELETE FROM <table>
    r"\b(select|insert|update)\s+.{0,20}\s+(from|into|where|set)\s+",
    r"\b(union\s+select|having\s+1=1|exec\s+xp_|execute\s+sp_)\b",
    r"(--|#|/\*|\*/).{0,5}(drop|select|delete|insert|update)",
    r"<script[\s>]|javascript\s*:|on\w+\s*=\s*[\"']",
    r"\.\./\.\.|\.\.\\|\.\./|etc/passwd|cmd\.exe|/bin/(sh|bash)",
    r"\b(eval|exec|system|subprocess|__import__|os\.system)\s*\(",
]
_INJECTION_RE = [re.compile(p, re.IGNORECASE) for p in _INJECTION_PATTERNS]


def _is_injection(message: str) -> bool:
    for pat in _INJECTION_RE:
        if pat.search(message):
            return True
    return False


# ── 4. NỘI DUNG NHẠY CẢM ────────────────────────────────────────────────────
_SENSITIVE_PATTERNS = [
    # Nội dung 18+
    r"(sach|truyen|noi dung|tim|mua).{0,20}(khieu dam|18\+|nguoi lon|\bsex\b|porn|erotic|hen ho nguoi lon)",
    # Bạo lực / nguy hiểm
    r"(huong dan|cach|chi toi|day toi).{0,30}(giet nguoi|lam bom|pha huy|tan cong|hack he thong|danh nguoi)",
    r"(lam bom|che tao bom|bomb making|how to (make|build) (bomb|weapon))",  # pattern bổ sung
    r"(cach giet|how to kill)",
    # Dò thông tin nội bộ
    r"(mat khau|password|passwd|api.?key|secret.?key|token).{0,20}(he thong|server|database|admin)",
    r"(thong tin ca nhan|personal (data|info)).{0,20}(cua user|cua khach|nguoi dung khac)",
    # Kích động chính trị (nhẹ — chỉ cảnh báo)
    r"(xuong duong|bieu tinh|lat do|phan dong|chong chinh phu|pha hoai)",
]
_SENSITIVE_RE = [re.compile(p, re.IGNORECASE) for p in _SENSITIVE_PATTERNS]


def _is_sensitive(norm_msg: str) -> bool:
    for pat in _SENSITIVE_RE:
        if pat.search(norm_msg):
            return True
    return False


# ── 5. SPAM / FLOOD (dùng context session) ───────────────────────────────────
_SPAM_MAX_SAME = 3        # Block khi gửi ý 3 lần trước đó giống hệt (lần thứ 4+)
_SPAM_MIN_LEN  = 5        # Tin < 5 ký tự lặp lại cũng tính


def _is_spam(message: str, context: dict) -> bool:
    """Kiểm tra tin lặp lại trong session."""
    _hist = context.get("_guard_msg_history", [])
    norm  = _norm(message)[:80]   # Normalize + truncate cho so sánh
    same_count = sum(1 for h in _hist[-(_SPAM_MAX_SAME + 5):] if h == norm)
    is_sp = same_count >= _SPAM_MAX_SAME
    # Lưu lại dù có spam hay không
    _hist.append(norm)
    context["_guard_msg_history"] = _hist[-20:]   # giữ 20 tin gần nhất
    return is_sp


# ── Response templates ────────────────────────────────────────────────────────
_RESPONSES = {
    "jailbreak": {
        "guest":    "Mình chỉ hỗ trợ tìm sách và thông tin tại BookStore thôi bạn nhé! 😊\n"
                    "Nếu cần hỗ trợ thêm, liên hệ: **0353260721**",
        "customer": "Mình chỉ hỗ trợ các nghiệp vụ BookStore (tìm sách, đơn hàng, khuyến mãi). "
                    "Không thể thực hiện yêu cầu này nhé! 😊",
        "staff":    "⚠️ Yêu cầu này vi phạm chính sách sử dụng chatbot.\n"
                    "Chatbot chỉ hỗ trợ các nghiệp vụ BookStore. Vui lòng dùng đúng mục đích.",
        "admin":    "🚫 **[Security Block]** Hành động này vi phạm chính sách sử dụng hệ thống.\n"
                    "Chatbot Admin chỉ hỗ trợ quản trị BookStore — không có chế độ override.",
    },
    "profanity_warn": {
        "guest":    "😊 Bạn ơi, hãy dùng ngôn ngữ lịch sự để mình hỗ trợ tốt hơn nhé!",
        "customer": "🙏 Mình rất muốn hỗ trợ bạn, nhưng hãy dùng ngôn ngữ lịch sự nhé!",
        "staff":    "⚠️ Vui lòng sử dụng ngôn ngữ phù hợp khi làm việc với hệ thống.",
        "admin":    "⚠️ Ngôn ngữ không phù hợp. Vui lòng sử dụng ngôn ngữ chuyên nghiệp.",
    },
    "profanity_block": {
        "guest":    "🙅 Mình không thể hỗ trợ khi bạn dùng ngôn ngữ không phù hợp.\n"
                    "Hãy thử lại với ngôn ngữ lịch sự, mình luôn sẵn sàng giúp! 😊",
        "customer": "🙅 Mình sẽ không thể tiếp tục hỗ trợ với ngôn ngữ như vậy.\n"
                    "Hãy liên hệ lại khi bạn sẵn sàng, mình vẫn ở đây! 😊",
        "staff":    "🚫 Yêu cầu bị từ chối do ngôn ngữ không phù hợp. Vui lòng thử lại.",
        "admin":    "🚫 [Warning logged] Ngôn ngữ không phù hợp. Vui lòng tuân thủ quy tắc.",
    },
    "injection": {
        "guest":    "🚫 Tin nhắn của bạn chứa nội dung không được phép.\n"
                    "Tôi chỉ hỗ trợ tìm sách, kiểm tra đơn hàng và thông tin BookStore thôi nhé! 😊",
        "customer": "🚫 Tin nhắn của bạn chứa nội dung không được phép.\n"
                    "Tôi chỉ hỗ trợ tìm sách, kiểm tra đơn hàng và thông tin BookStore thôi nhé! 😊",
        "staff":    "🚫 Yêu cầu bị từ chối: nội dung không phù hợp với phạm vi chatbot.",
        "admin":    "🚫 **[Security Block]** Yêu cầu bị chặn: phát hiện pattern injection.",
    },
    "sensitive": {
        "guest":    "Mình không thể hỗ trợ nội dung này. Hãy hỏi về sách hoặc đơn hàng nhé! 😊",
        "customer": "Mình không thể hỗ trợ nội dung này. Nếu cần tìm sách, mình luôn sẵn sàng! 📚",
        "staff":    "⚠️ Nội dung không phù hợp với phạm vi hỗ trợ của chatbot.",
        "admin":    "🚫 Nội dung không phù hợp. Chatbot chỉ hỗ trợ nghiệp vụ quản trị BookStore.",
    },
    "spam": {
        "guest":    "😊 Bạn có vẻ đang gặp khó khăn với câu hỏi này. "
                    "Thử diễn đạt theo cách khác, mình sẽ cố hỗ trợ tốt hơn!",
        "customer": "😊 Bạn đã gửi tin này nhiều lần. Hãy thử hỏi theo cách khác "
                    "hoặc liên hệ hotline **0353260721** để được hỗ trợ nhanh hơn nhé!",
        "staff":    "⚠️ Phát hiện tin nhắn lặp lại nhiều lần. Vui lòng thử lại với nội dung khác.",
        "admin":    "⚠️ Phát hiện tin nhắn lặp lại. Nếu cần hỗ trợ, vui lòng mô tả cụ thể hơn.",
    },
    "too_long": {
        "guest":    "😊 Tin nhắn của bạn quá dài! Hãy rút gọn câu hỏi để mình hiểu rõ hơn nhé.",
        "customer": "Tin nhắn quá dài (tối đa 1000 ký tự). Vui lòng rút gọn câu hỏi.",
        "staff":    "⚠️ Tin nhắn vượt giới hạn 1000 ký tự. Vui lòng rút gọn.",
        "admin":    "⚠️ Tin nhắn vượt giới hạn 1000 ký tự. Vui lòng rút gọn.",
    },
}


def _get_resp(key: str, role: str) -> str:
    role = role.lower()
    if role not in ("guest", "customer", "staff", "admin"):
        role = "guest"
    return _RESPONSES.get(key, {}).get(role, "Yêu cầu không hợp lệ.")


# ── Main entry point ──────────────────────────────────────────────────────────
MAX_MESSAGE_LEN = 1000


def check_security(
    message: str,
    role: str = "guest",
    context: Optional[dict] = None,
) -> Optional[dict]:
    """
    Kiểm tra bảo mật trước khi xử lý tin nhắn.

    Args:
        message: Tin nhắn gốc của user.
        role: "guest" | "customer" | "staff" | "admin".
        context: Session context dict (dùng để detect spam).

    Returns:
        None nếu tin nhắn an toàn.
        dict với keys {"blocked": True, "reason": str, "response": str} nếu vi phạm.
    """
    if context is None:
        context = {}

    # ── 0. Kiểm tra độ dài ────────────────────────────────────────────
    if len(message) > MAX_MESSAGE_LEN:
        print(f"[SecurityGuard] TOO_LONG | role={role} | len={len(message)}")
        return {
            "blocked": True,
            "reason":  "too_long",
            "response": _get_resp("too_long", role),
        }

    norm_msg = _norm(message)

    # ── 1. Injection (ưu tiên cao — check trước khi normalize xóa syntax) ─
    if _is_injection(message):
        print(f"[SecurityGuard] INJECTION_BLOCK | role={role} | msg={message[:80]!r}")
        return {
            "blocked": True,
            "reason":  "injection",
            "response": _get_resp("injection", role),
        }

    # ── 2. Jailbreak ──────────────────────────────────────────────────
    if _is_jailbreak(norm_msg):
        print(f"[SecurityGuard] JAILBREAK_BLOCK | role={role} | msg={message[:80]!r}")
        return {
            "blocked": True,
            "reason":  "jailbreak",
            "response": _get_resp("jailbreak", role),
        }

    # ── 3. Nội dung nhạy cảm ─────────────────────────────────────────
    if _is_sensitive(norm_msg):
        print(f"[SecurityGuard] SENSITIVE_BLOCK | role={role} | msg={message[:80]!r}")
        return {
            "blocked": True,
            "reason":  "sensitive",
            "response": _get_resp("sensitive", role),
        }

    # ── 4. Profanity ──────────────────────────────────────────────────
    if _is_profanity(norm_msg, message):
        profanity_count = context.get("_guard_profanity_count", 0) + 1
        context["_guard_profanity_count"] = profanity_count
        print(f"[SecurityGuard] PROFANITY | role={role} | count={profanity_count} | msg={message[:80]!r}")

        if profanity_count >= 2:
            # Lần 2 trở đi: BLOCK
            return {
                "blocked": True,
                "reason":  "profanity_block",
                "response": _get_resp("profanity_block", role),
            }
        else:
            # Lần 1: WARN — trả về response cảnh báo nhưng đánh dấu blocked
            # để router trả về luôn (không đưa vào NLU)
            return {
                "blocked": True,
                "reason":  "profanity_warn",
                "response": _get_resp("profanity_warn", role),
            }

    # ── 5. Spam / Flood ────────────────────────────────────────────────
    if _is_spam(message, context):
        print(f"[SecurityGuard] SPAM_BLOCK | role={role} | msg={message[:80]!r}")
        return {
            "blocked": True,
            "reason":  "spam",
            "response": _get_resp("spam", role),
        }

    return None   # ✅ An toàn
