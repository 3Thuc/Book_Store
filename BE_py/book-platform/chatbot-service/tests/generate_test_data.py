import random
import os

def generate_customer_guest():
    intents = [
        ("Cho mình tìm sách doanh nhân", "book_search"),
        ("Xem chi tiết cuốn sách đắc nhân tâm", "book_detail"),
        ("Giá cuốn đó bao nhiêu vậy", "book_detail"),
        ("Kiểm tra đơn hàng của tôi", "order_status"),   # Bị chặn
        ("Giỏ hàng của tôi có gì", "cart_help"),       # Bị chặn
        ("Tôi muốn hủy đơn hàng", "order_cancel"),    # Bị chặn
        ("Sách văn học bán chạy nhất", "recommend_trending"),
        ("Mã giảm giá cho người mới", "voucher_apply") # Bị chặn
    ]
    return intents

def generate_customer_auth():
    intents = [
        ("Gợi ý cho mình sách chứng khoán", "recommend_category"),
        ("Bạn còn sách đó không", "book_availability"),
        ("Cho hỏi sách đó có phù hợp làm quà tặng không", "recommend_gift"),
        ("Kiểm tra trạng thái giao hàng của đơn 8812", "order_status"),
        ("Tôi muốn xem lịch sử mua sắm của mình", "order_history"),
        ("Hỗ trợ tôi trả hàng cuốn bị rách", "return_request"),
        ("Điểm tích lũy hiện tại của tôi", "loyalty_points"),
        ("Làm sao để đổi tài khoản email", "account_help")
    ]
    return intents

def generate_staff():
    intents = [
        ("Lấy danh sách các đơn đang chờ xử lý", "staff_order_list_pending"),
        ("Kiểm tra tồn kho sách id 99", "staff_inventory_check"),
        ("Cập nhật số lượng lên 150", "staff_inventory_update"),
        ("Duyệt đơn hàng số 5555", "staff_order_status_update"),
        ("Tìm thông tin khách có số điện thoại 090", "staff_customer_lookup"),
        ("Bạn còn tồn kho cuốn tiểu thuyết đó không", "staff_inventory_check"),
        ("Xem doanh thu ca sáng nay", "staff_revenue_today"),
        ("Sách nào đang bán chạy nhất tháng này", "staff_top_selling")
    ]
    return intents

def generate_admin():
    intents = [
        ("Thống kê doanh thu tháng 8", "admin_revenue_stats"),
        ("Lập báo cáo số lượng đơn hàng", "admin_order_stats"),
        ("Khóa tài khoản của user nguyenvana mang id 15", "admin_user_lock_unlock"),
        ("Nâng quyền cho nhân viên đó", "admin_user_update_role"),
        ("Xem danh sách người dùng vi phạm", "admin_banned_users"),
        ("Thêm một chương trình khuyến mãi mới", "admin_promotion_create_guide"),
        ("Top những sách bị đánh giá thấp", "admin_book_low_rating"),
        ("Kiểm tra health của hệ thống server", "admin_system_health")
    ]
    return intents

print(generate_customer_guest())
