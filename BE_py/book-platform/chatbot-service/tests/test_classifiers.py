import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from chatbot_app.nlu.admin_intent_classifier import classify as admin_classify
from chatbot_app.nlu.staff_intent_classifier import classify as staff_classify

ADMIN_TESTS = [
    ("thong ke so luong user moi dang ky", "admin_user_stats"),
    ("tao them chuong trinh promotion SALE50", "admin_promotion_create_guide"),
    ("xem thong tin user email admin@test.com", "admin_user_details"),
    ("nang tai khoan id 43 len staff di", "admin_user_update_role"),
    ("danh sach ma giam gia dang kich hoat", "admin_promotion_list"),
    ("ma nao sap het han trong tuan nay", "admin_promotion_expiring"),
    ("khoa tai khoan nguoi dung id 42 lai", "admin_user_lock_unlock"),
    ("doanh thu thang nay bi mat bao nhieu", "admin_revenue_report"),
    ("check tinh trang system health ngay", "admin_system_health"),
    ("xem dashboard tong quan he thong", "admin_dashboard_summary"),
    ("chao tro ly admin hom nay the nao", "admin_chitchat"),
    ("gia xang hom nay tang hay giam vay", "admin_out_of_scope"),
]

STAFF_TESTS = [
    ("cam on het ca viec lam hom nay", "staff_chitchat"),
    ("chao buoi sang dau tuan", "staff_chitchat"),
    ("loc cac don hang dang cho duyet hom nay", "staff_order_list_pending"),
    ("tra cuu thong tin chi tiet don so 5589", "staff_order_lookup"),
    ("con bao nhieu cuon sach id 201 trong kho", "staff_inventory_check"),
    ("chuyen don 5589 sang trang thai shipped", "staff_order_status_update"),
    ("doanh thu hom nay dat bao nhieu roi", "staff_revenue_today"),
]

print("=" * 60)
print("  ADMIN CLASSIFIER TESTS")
print("=" * 60)
admin_ok = 0
for msg, expected in ADMIN_TESTS:
    result = admin_classify(msg)
    got = result.intent
    ok = "✅" if got == expected else "❌"
    if got == expected: admin_ok += 1
    print(f"{ok} [{got}] expected=[{expected}]")
    print(f"   MSG: {msg}")
print(f"\nAdmin: {admin_ok}/{len(ADMIN_TESTS)} = {admin_ok/len(ADMIN_TESTS)*100:.0f}%")

print("\n" + "=" * 60)
print("  STAFF CLASSIFIER TESTS")
print("=" * 60)
staff_ok = 0
for msg, expected in STAFF_TESTS:
    result = staff_classify(msg)
    got = result.intent
    ok = "✅" if got == expected else "❌"
    if got == expected: staff_ok += 1
    print(f"{ok} [{got}] expected=[{expected}]")
    print(f"   MSG: {msg}")
print(f"\nStaff: {staff_ok}/{len(STAFF_TESTS)} = {staff_ok/len(STAFF_TESTS)*100:.0f}%")
