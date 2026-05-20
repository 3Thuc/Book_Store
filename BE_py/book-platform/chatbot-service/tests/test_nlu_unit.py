"""
test_nlu_unit.py – Kiểm tra NLU thuần không cần server
========================================================
Test trực tiếp hàm detect_intent() của customer, staff, admin.

Ưu điểm:
  - Không cần uvicorn, không cần Ollama, không cần MySQL
  - Chạy trong 30-60 giây (chỉ mất thời gian load SBERT)
  - Dùng để debug regex nhanh nhất

Cách chạy:
  cd D:\\...\\chatbot-service
  python tests/test_nlu_unit.py
"""

import sys
import os
# Luôn trỏ về thư mục chatbot-service (parent của tests/)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)


# ═══════════════════════════════════════════════════════════════════
# SECTION 1 – CUSTOMER NLU CASES (50 cases)
# ═══════════════════════════════════════════════════════════════════
CUSTOMER_CASES = [
    # ── chitchat (8) ──────────────────────────────────────────────
    ("Xin chào bot nha",                          "chitchat"),
    ("Hello shop ơi",                             "chitchat"),
    ("Hi there",                                  "chitchat"),
    ("Cảm ơn bạn nhiều lắm",                      "chitchat"),
    ("Tạm biệt nhé bot",                          "chitchat"),
    ("Bạn là robot hay người thật",               "chitchat"),
    ("Bạn có thể làm gì cho tôi",                 "chitchat"),
    ("Are you a bot or a human",                  "chitchat"),

    # ── book_search (8) ───────────────────────────────────────────
    ("Tìm sách kỹ năng giao tiếp",               "book_search"),
    ("Có bán cuốn Đắc nhân tâm không",           "book_search"),
    ("tim sach day nau an",                       "book_search"),
    ("Cho xem sách về tâm lý học",               "book_search"),
    ("i'm looking for a novel",                   "book_search"),
    ("Sách lịch sử Việt Nam cận đại",            "book_search"),
    ("find me some books about leadership",       "book_search"),
    ("Mình muốn tìm sách đầu tư chứng khoán",   "book_search"),

    # ── book_detail (5) ───────────────────────────────────────────
    ("Cuốn này giá bao nhiêu vậy",              "book_detail"),
    ("Tác giả cuốn sách đó là ai",              "book_detail"),
    ("Sách này bao nhiêu trang",                "book_detail"),
    ("Nhà xuất bản nào in cuốn này",            "book_detail"),
    ("Thể loại sách này là gì",                 "book_detail"),

    # ── order_status (6) ──────────────────────────────────────────
    ("Đơn hàng số 12345 đang ở đâu rồi",       "order_status"),
    ("Bao giờ nhận được sách đã đặt",           "order_status"),
    ("Track my order 778899",                    "order_status"),
    ("Kiểm tra đơn hàng giúp mình",             "order_status"),
    ("Đơn của tôi giao bao giờ",                "order_status"),
    ("where is my package",                      "order_status"),

    # ── order_cancel (4) ──────────────────────────────────────────
    ("Hủy đơn hàng 12345 giúp mình",            "order_cancel"),
    ("Mình đổi ý không mua nữa hủy giúp",       "order_cancel"),
    ("Cancel my order please",                   "order_cancel"),
    ("Bỏ đơn vừa đặt đi",                       "order_cancel"),

    # ── return_policy (4) ─────────────────────────────────────────
    ("Chính sách đổi trả như thế nào",           "return_policy"),
    ("Bao nhiêu ngày được đổi hàng",             "return_policy"),
    ("Điều kiện để được đổi sách",               "return_policy"),
    ("Can i return a book",                       "return_policy"),

    # ── return_request (4) ────────────────────────────────────────
    ("Mình muốn đổi cuốn sách vừa mua",         "return_request"),
    ("Cho tôi trả lại hàng",                     "return_request"),
    ("Yêu cầu hoàn tiền đơn hàng 123",          "return_request"),
    ("i want to return the book i received",     "return_request"),

    # ── recommend_personal (4) ────────────────────────────────────
    ("Gợi ý sách hay cho mình với",             "recommend_personal"),
    ("Tư vấn nên mua sách gì",                  "recommend_personal"),
    ("Nên đọc sách gì bây giờ",                 "recommend_personal"),
    ("Can you recommend a good book",            "recommend_personal"),

    # ── payment_issue (4) ─────────────────────────────────────────
    ("Momo bị lỗi thanh toán",                  "payment_issue"),
    ("Bị trừ tiền 2 lần cho 1 đơn",             "payment_issue"),
    ("Thanh toán thất bại mãi",                 "payment_issue"),
    ("my payment keeps failing",                 "payment_issue"),

    # ── out_of_scope (3) ──────────────────────────────────────────
    ("Thời tiết Hà Nội hôm nay thế nào",        "out_of_scope"),
    ("Giá vàng 9999 bao nhiêu",                  "out_of_scope"),
    ("Tuyển Việt Nam đá mấy giờ",               "out_of_scope"),
]


# ═══════════════════════════════════════════════════════════════════
# SECTION 2 – STAFF NLU CASES (25 cases)
# ═══════════════════════════════════════════════════════════════════
STAFF_CASES = [
    # order_lookup (4)
    ("Tra cứu đơn hàng số 987654",              "staff_order_lookup"),
    ("Tìm đơn hàng của khách theo mã 11223",    "staff_order_lookup"),
    ("Xem chi tiết đơn #55667",                  "staff_order_lookup"),
    ("Lookup order 44556",                       "staff_order_lookup"),

    # order_status_update (4)
    ("Cập nhật trạng thái đơn sang delivered",   "staff_order_status_update"),
    ("Đánh dấu đơn 123 đã giao",                "staff_order_status_update"),
    ("Update order status to shipped",           "staff_order_status_update"),
    ("Chuyển đơn sang processing",               "staff_order_status_update"),

    # order_list_pending (3)
    ("Danh sách đơn đang chờ xử lý",            "staff_order_list_pending"),
    ("Bao nhiêu đơn pending hôm nay",           "staff_order_list_pending"),
    ("List pending orders",                      "staff_order_list_pending"),

    # inventory_check (4)
    ("Kiểm tra tồn kho sách này",               "staff_inventory_check"),
    ("Còn bao nhiêu cuốn trong kho",            "staff_inventory_check"),
    ("Check inventory của sách id 99",           "staff_inventory_check"),
    ("Stock còn bao nhiêu",                      "staff_inventory_check"),

    # inventory_low (3)
    ("Sách sắp hết hàng danh sách",             "staff_inventory_low"),
    ("Cảnh báo tồn kho thấp",                   "staff_inventory_low"),
    ("Low stock alert",                          "staff_inventory_low"),

    # customer_lookup (3)
    ("Tra cứu thông tin khách hàng email abc@gmail.com", "staff_customer_lookup"),
    ("Xem profile khách hàng id 101",            "staff_customer_lookup"),
    ("Lookup customer info",                     "staff_customer_lookup"),

    # revenue_today (2)
    ("Doanh thu hôm nay bao nhiêu",             "staff_revenue_today"),
    ("Revenue today",                            "staff_revenue_today"),

    # top_selling (2)
    ("Sách bán chạy nhất trong hệ thống",        "staff_top_selling"),
    ("Top selling books",                        "staff_top_selling"),
]


# ═══════════════════════════════════════════════════════════════════
# SECTION 3 – ADMIN NLU CASES (25 cases)
# ═══════════════════════════════════════════════════════════════════
ADMIN_CASES = [
    # dashboard_summary (3)
    ("Xem tổng quan hệ thống hôm nay",           "admin_dashboard_summary"),
    ("Dashboard overview",                        "admin_dashboard_summary"),
    ("Tóm tắt tình hình hôm nay",               "admin_dashboard_summary"),

    # revenue_report (3)
    ("Báo cáo doanh thu tuần này",               "admin_revenue_report"),
    ("Revenue report month",                      "admin_revenue_report"),
    ("Thống kê doanh thu tháng 3",               "admin_revenue_report"),

    # user_update_role (3)
    ("Nâng cấp user lên staff",                  "admin_user_update_role"),
    ("Đổi quyền tài khoản thành admin",         "admin_user_update_role"),
    ("Set role staff cho user 101",              "admin_user_update_role"),

    # user_lock_unlock (3)
    ("Khóa tài khoản user 202",                  "admin_user_lock_unlock"),
    ("Mở khóa tài khoản bị ban",                "admin_user_lock_unlock"),
    ("Lock account user id 55",                  "admin_user_lock_unlock"),

    # user_reset_password (3)
    ("Reset mật khẩu cho user 303",             "admin_user_reset_password"),
    ("Đặt lại password cho tài khoản",          "admin_user_reset_password"),
    ("Force reset password user 44",             "admin_user_reset_password"),

    # list_staff (2)
    ("Danh sách nhân viên staff hiện có",        "admin_list_staff"),
    ("List all staff accounts",                  "admin_list_staff"),

    # promotion_list (2)
    ("Danh sách khuyến mãi đang active",         "admin_promotion_list"),
    ("List all promotions",                      "admin_promotion_list"),

    # promotion_expiring (2)
    ("Khuyến mãi sắp hết hạn",                  "admin_promotion_expiring"),
    ("Voucher nào sắp hết hạn trong tuần",      "admin_promotion_expiring"),

    # system_health (2)
    ("Kiểm tra trạng thái các service",          "admin_system_health"),
    ("System health check",                      "admin_system_health"),

    # order_stats (2)
    ("Thống kê đơn hàng theo trạng thái",       "admin_order_stats"),
    ("Bao nhiêu đơn bị hủy hôm nay",           "admin_order_stats"),
]


# ═══════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════
def run_section(label: str, cases: list, detect_fn) -> tuple:
    correct = 0
    failed = []

    print(f"\n  {'─'*80}")
    print(f"  🔍 {label} – {len(cases)} cases")
    print(f"  {'─'*80}")
    print(f"  {'Message':<45} {'Expected':<25} {'Got':<25} OK")
    print(f"  {'-'*44} {'-'*24} {'-'*24} ---")

    for msg, expected in cases:
        result = detect_fn(msg)
        ok = result.intent == expected
        if ok:
            correct += 1
        else:
            failed.append((msg, expected, result.intent, result.confidence))
        icon = "✅" if ok else "❌"
        display = msg[:43] + "…" if len(msg) > 43 else msg
        print(f"  {display:<45} {expected:<25} {result.intent:<25} {icon}")

    pct = correct / len(cases) * 100
    print(f"\n  📊 {label}: {correct}/{len(cases)} = {pct:.1f}%")
    return correct, len(cases), failed


def main():
    print(f"\n{'='*82}")
    print(f"  🧠 NLU UNIT TEST – Offline (không cần server)")
    print(f"  Đang load SBERT model... (mất 20-40 giây lần đầu)")
    print(f"{'='*82}")

    # Import các classifier
    try:
        from chatbot_app.nlu.customer_intent_classifier import detect_intent as customer_detect
        print("  ✅ Customer intent classifier loaded")
    except Exception as e:
        print(f"  ❌ Lỗi load customer classifier: {e}")
        return

    try:
        from chatbot_app.nlu.staff_intent_classifier import detect_staff_intent as staff_detect
        print("  ✅ Staff intent classifier loaded")
    except Exception as e:
        print(f"  ❌ Lỗi load staff classifier: {e}")
        staff_detect = None

    try:
        from chatbot_app.nlu.admin_intent_classifier import detect_admin_intent as admin_detect
        print("  ✅ Admin intent classifier loaded")
    except Exception as e:
        print(f"  ❌ Lỗi load admin classifier: {e}")
        admin_detect = None

    all_correct = 0
    all_total = 0
    all_failed = []

    # Run Customer
    c, t, f = run_section("CUSTOMER NLU", CUSTOMER_CASES, customer_detect)
    all_correct += c; all_total += t; all_failed.extend(f)

    # Run Staff
    if staff_detect:
        c, t, f = run_section("STAFF NLU", STAFF_CASES, staff_detect)
        all_correct += c; all_total += t; all_failed.extend(f)

    # Run Admin
    if admin_detect:
        c, t, f = run_section("ADMIN NLU", ADMIN_CASES, admin_detect)
        all_correct += c; all_total += t; all_failed.extend(f)

    # Final report
    pct = all_correct / all_total * 100
    print(f"\n{'='*82}")
    print(f"  📊 KẾT QUẢ TỔNG HỢP: {all_correct}/{all_total} = {pct:.1f}%")
    if pct >= 85:
        print(f"  ✅ ĐẠT ngưỡng luận văn (≥ 85%) – NLU đáng tin cậy")
    elif pct >= 75:
        print(f"  ⚠️  GẦN ĐẠT – Cần tune thêm một số regex edge case")
    else:
        print(f"  ❌ CHƯA ĐẠT – Cần rà soát lại QUICK_RULES")

    if all_failed:
        print(f"\n  ❌ {len(all_failed)} case thất bại:")
        print(f"  {'─'*80}")
        for msg, exp, got, conf in all_failed:
            print(f"  \"{msg}\"")
            print(f"    ↳ expected: {exp}   got: {got}   conf: {conf:.2f}")
    print(f"{'='*82}\n")


if __name__ == "__main__":
    main()
