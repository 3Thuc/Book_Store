"""
models.py – Pydantic models cho ChatBot API.

UserRole:
  - customer : khách hàng thông thường, chỉ nhận answer
  - staff    : nhân viên, nhận thêm intent/confidence/sentiment debug
  - admin    : admin, nhận full debug gồm cả session metadata

v2 – Thêm NavigateButton:
  - Chatbot trả về danh sách nút điều hướng (link sách, đơn hàng...)
  - FE render thành card/button có thể click → điều hướng trong app
  - KHÔNG gọi API backend từ chatbot → đơn giản, an toàn, đúng kiến trúc
"""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from enum import Enum


class MessageRole(str, Enum):
    user      = "user"
    assistant = "assistant"


class UserRole(str, Enum):
    customer = "customer"   # khách hàng
    staff    = "staff"      # nhân viên CSKH
    admin    = "admin"      # quản trị viên


class ChatMessage(BaseModel):
    role: MessageRole
    content: str


class ChatRequest(BaseModel):
    session_id: str
    user_id:    Optional[int]  = None
    message:    str
    history:    List[ChatMessage] = []
    role:       UserRole = UserRole.customer


# ── Navigate Button ───────────────────────────────────────────────────────────
class NavigateButton(BaseModel):
    """
    Nút điều hướng được render trong ChatWidget.
    FE dùng url để navigate (React Router / window.location).

    type:
      book        – Link đến trang chi tiết sách  (/books/detail/{id})
      order       – Link đến trang đơn hàng       (/account?tab=orders)
      page        – Link đến trang bất kỳ
      confirm_yes – Nút xác nhận "Đúng rồi" (Clarify-First system)
      confirm_no  – Nút phủ nhận "Không phải" (Clarify-First system)
      quick_reply – Gợi ý nhanh cho slot-filling (Clarify-First system)
    """
    label:    str            # Text hiển thị, VD: "📚 Đắc Nhân Tâm – 89,000đ"
    url:      str            # FE route, VD: "/books/detail/42" (rỗng với confirm/quick_reply)
    type:     str = "page"   # "book" | "order" | "page" | "confirm_yes" | "confirm_no" | "quick_reply"
    metadata: Optional[Dict[str, Any]] = None  # Thêm data cho FE (giá, rating...)



class ChatResponse(BaseModel):
    session_id:       str
    answer:           str
    intent:           str
    confidence:       float = 1.0
    sentiment:        str   = "NEUTRAL"
    sources:          List[str] = []
    # Navigate buttons – FE render thành nút/card có thể click
    navigate_buttons: List[NavigateButton] = []
    # Debug fields – chỉ có khi role = staff | admin
    debug:            Optional[Dict[str, Any]] = None


class HistoryMessage(BaseModel):
    """Tin nhắn trong lịch sử hội thoại (trả về cho UI)."""
    id:         int
    role:       str
    content:    str
    intent:     Optional[str]   = None
    confidence: Optional[float] = None
    sentiment:  Optional[str]   = None
    created_at: Optional[str]   = None


class SessionHistoryResponse(BaseModel):
    session_id:  str
    messages:    List[HistoryMessage]
    turn_count:  int
    last_active: Optional[str] = None

