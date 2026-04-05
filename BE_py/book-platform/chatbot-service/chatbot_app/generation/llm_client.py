"""
llm_client.py – Gọi Ollama REST API (LLM chạy local, hoàn toàn miễn phí).

Ollama API endpoint: http://localhost:11434 (local) hoặc http://ollama:11434 (Docker)
Model: qwen2.5:7b (hiểu tiếng Việt tốt nhất trong dòng open-source)

Không dùng SDK Ollama riêng – gọi thẳng qua httpx để đơn giản nhất.
"""
import httpx
from chatbot_app.config import OLLAMA_HOST, OLLAMA_MODEL

SYSTEM_PROMPT = """Bạn là trợ lý ảo của website bán sách BookStore.

Vai trò của bạn:
- Tư vấn sách phù hợp với nhu cầu khách hàng
- Hỗ trợ tra cứu đơn hàng và giải thích chính sách
- Trả lời bằng tiếng Việt, thân thiện và chuyên nghiệp

Nguyên tắc bắt buộc:
- Chỉ trả lời dựa trên thông tin được cung cấp trong [CONTEXT]
- Nếu không có thông tin, nói rõ: "Tôi không có thông tin về vấn đề này. Vui lòng liên hệ hotline để được hỗ trợ."
- KHÔNG bịa đặt giá, tên sách, thông tin đơn hàng
- Trả lời ngắn gọn (dưới 200 từ), rõ ràng, có cấu trúc"""


async def generate(
    user_message: str,
    context: str,
    history: list[dict],
    tone: str = "thân thiện, ngắn gọn",
) -> str:
    """
    Gọi Ollama /api/chat để generate câu trả lời.

    Args:
        user_message: Câu hỏi người dùng
        context:      Thông tin lấy từ RAG / MySQL / Recommend
        history:      Lịch sử hội thoại [{role, content}]
        tone:         Phong cách trả lời theo user cluster

    Returns:
        Câu trả lời dạng string
    """
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Lịch sử hội thoại (tối đa 6 tin)
    messages.extend(history[-6:])

    # Tin nhắn hiện tại kèm context
    user_content = f"""[CONTEXT]
{context}
[/CONTEXT]

Phong cách trả lời: {tone}

Câu hỏi: {user_message}"""

    messages.append({"role": "user", "content": user_content})

    async with httpx.AsyncClient(timeout=90.0) as client:
        resp = await client.post(
            f"{OLLAMA_HOST}/api/chat",
            json={
                "model":   OLLAMA_MODEL,
                "messages": messages,
                "stream":  False,
                "options": {
                    "temperature": 0.3,   # thấp → chính xác, ít sáng tạo
                    "top_p":       0.9,
                    "num_predict": 512,
                },
            },
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"]


async def check_ollama_health() -> bool:
    """Kiểm tra Ollama đang chạy và model đã sẵn sàng."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{OLLAMA_HOST}/api/tags")
            models = [m["name"] for m in r.json().get("models", [])]
            return any(OLLAMA_MODEL.split(":")[0] in m for m in models)
    except Exception:
        return False
