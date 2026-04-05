from pydantic import BaseModel
from typing import Optional, List
from enum import Enum


class MessageRole(str, Enum):
    user      = "user"
    assistant = "assistant"


class ChatMessage(BaseModel):
    role: MessageRole
    content: str


class ChatRequest(BaseModel):
    session_id: str
    user_id:    Optional[int] = None    # None nếu khách vãng lai
    message:    str
    history:    List[ChatMessage] = []  # Tối đa 10 tin gần nhất


class ChatResponse(BaseModel):
    session_id: str
    answer:     str
    intent:     str
    confidence: float = 1.0
    sentiment:  str   = "NEUTRAL"
    sources:    List[str] = []
