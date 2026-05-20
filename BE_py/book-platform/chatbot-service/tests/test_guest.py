import asyncio, httpx
async def test():
    async with httpx.AsyncClient() as c:
        payload = {"session_id": "test_guest_123", "message": "kiểm tra đơn hàng", "role": "customer"}
        resp = await c.post("http://127.0.0.1:8000/api/chat/message", json=payload)
        print(resp.json())
asyncio.run(test())
