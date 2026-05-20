import asyncio, json, uuid, time, os
import httpx
from colorama import init, Fore, Style

init(autoreset=True)

ENDPOINT = "http://127.0.0.1:8000/api/chat/message"

SCENARIOS = {
    "customer": [
        {"msg": "Cho mình xem các thể loại sách", "expected_intent": "recommend_category"},
        {"msg": "Gợi ý sách kỹ năng sống cho mình đi", "expected_intent": "recommend_category"},
        {"msg": "Còn sách nào khác cùng thể loại không?", "expected_intent": "recommend_category", "note": "Test Genre Coref (v8)"},
        {"msg": "Muốn đặt mua cuốn đó luôn", "expected_intent": "cart_help", "note": "Test Title Coref"},
        {"msg": "Thanh toán bằng thẻ được không?", "expected_intent": "payment_method"}
    ],
    "staff": [
        {"msg": "Kiểm tra tình trạng đơn hàng 9988", "expected_intent": "staff_order_lookup"},
        {"msg": "Cập nhật trạng thái đơn đó thành đang giao", "expected_intent": "staff_order_status_update", "note": "Test Order Coref"},
        {"msg": "Hủy luôn đơn đó đi khách đổi ý", "expected_intent": "staff_order_status_update", "note": "Test Order Coref"},
        {"msg": "Kiểm tra tồn kho sách Đắc Nhân Tâm", "expected_intent": "staff_inventory_check"},
        {"msg": "Cập nhật tồn kho nó lên 150 cuốn", "expected_intent": "staff_inventory_update", "note": "Test Title Coref"}
    ],
    "admin": [
        {"msg": "Báo cáo doanh thu tháng này", "expected_intent": "admin_revenue_stats"},
        {"msg": "Lập báo cáo doanh thu tuần vừa rồi", "expected_intent": "admin_revenue_report", "note": "Test Date Context"},
        {"msg": "Xem danh sách mã khuyến mãi đang chạy", "expected_intent": "admin_promotion_list"},
        {"msg": "Tạo mã khuyến mãi mới FREESHIP100", "expected_intent": "admin_promotion_create_guide"},
        {"msg": "Top sách bán chạy nhất là những cuốn nào", "expected_intent": "admin_top_books"}
    ]
}

async def run_scenario(role: str, turns: list, client: httpx.AsyncClient):
    session_id = f"test_mem_{role}_{uuid.uuid4().hex[:6]}"
    
    if role == "staff":
        endpoint_url = "http://127.0.0.1:8000/api/staff/chat/message"
    elif role == "admin":
        endpoint_url = "http://127.0.0.1:8000/api/admin/chat/message"
    else:
        endpoint_url = "http://127.0.0.1:8000/api/chat/message"

    print(f"\n{Fore.CYAN}{'='*60}")
    print(f"{Fore.CYAN}🚀 RUN MẠCH HỘI THOẠI: {role.upper()}")
    print(f"{Fore.CYAN}SESSION_ID: {session_id}")
    print(f"{Fore.CYAN}{'='*60}")

    for idx, turn in enumerate(turns, 1):
        payload = {
            "session_id": session_id,
            "message": turn["msg"],
            "user_id": 1,
            "role": role,
        }
        
        note = f" ({turn['note']})" if "note" in turn else ""
        print(f"\n{Fore.YELLOW}Turn {idx}:{note} {Fore.WHITE}User: {turn['msg']}")
        print(f"Expect Intent: {Fore.BLUE}{turn['expected_intent']}")
        
        t0 = time.perf_counter()
        try:
            r = await client.post(endpoint_url, json=payload, timeout=60.0)
            if r.status_code == 200:
                data = r.json()
                actual_intent = data.get("intent", "")
                conf = data.get("confidence", 0.0)
                answer = data.get("answer", "").strip().replace("\n", " ")
                
                lat = (time.perf_counter() - t0) * 1000
                
                if actual_intent == turn["expected_intent"]:
                    status = f"{Fore.GREEN}✅ PASS"
                else:
                    status = f"{Fore.RED}❌ FAIL (Got: {actual_intent})"
                    
                print(f"{status} {Fore.WHITE}| {lat:.0f}ms | conf={conf:.2f}")
                
                if actual_intent == turn["expected_intent"] and "confirmation" in actual_intent:
                    # Nếu dính clarify-first, bỏ qua answer
                    pass
                print(f"{Fore.MAGENTA}Bot: {answer[:150]}{'...' if len(answer)>150 else ''}")
                
            else:
                print(f"{Fore.RED}❌ HTTP ERROR {r.status_code}: {r.text}")
        except Exception as e:
            print(f"{Fore.RED}❌ EXCEPTION: {e}")
            
        await asyncio.sleep(0.5)

async def main():
    async with httpx.AsyncClient() as client:
        # Check health
        try:
            r = await client.get("http://127.0.0.1:8000/api/chat/health")
            if r.status_code != 200:
                raise Exception("Server not ready")
        except Exception as e:
            print(f"{Fore.RED}❌ Lỗi kết nối tới Server. Đảm bảo uvicorn đang chạy ở port 8000.")
            return

        for role, turns in SCENARIOS.items():
            await run_scenario(role, turns, client)
            
    print("\n" + "="*60)
    print("🏁 HOÀN THÀNH BÀI TEST BỘ NHỚ ĐA LƯỢT.")

if __name__ == "__main__":
    asyncio.run(main())
