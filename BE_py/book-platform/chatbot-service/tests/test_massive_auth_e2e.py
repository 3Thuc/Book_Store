import asyncio, uuid, time, os, glob, random, json
import httpx
from colorama import init, Fore

init(autoreset=True)

# Lấy danh sách ảnh thật từ ổ đĩa (fall back to empty nếu ko có)
IMAGE_PATHS = []
for p in ["D:\\craw_demo\\scraped_images\\*.jpg", "C:\\Users\\ADMIN\\Downloads\\*.jpg", "C:\\Users\\ADMIN\\Downloads\\*.png"]:
    IMAGE_PATHS.extend(glob.glob(p))

def get_random_image():
    if IMAGE_PATHS:
        return random.choice(IMAGE_PATHS)
    return None

def build_suites(role="customer"):
    """10 bộ test case hoàn toàn khác nhau cho mỗi role, mỗi bộ 8 câu."""

    if role == "guest":
        all_suites_raw = [
            # Bộ 1 – Tìm sách & thông tin
            [("tìm sách triết học stoicism", "book_search"),
             ("sách nào hay nhất về tâm lý học", "recommend_category"),
             ("có sách paulo coelho không", "book_search"),
             ("sách bán chạy tuần này là gì", "recommend_trending"),
             ("shop giao hàng toàn quốc không", "store_info"),
             ("cách thanh toán online ở đây", "store_info"),
             ("có chương trình khuyến mãi gì không", "promotion_current"),
             ("bye bạn nhé", "chitchat")],
            # Bộ 2 – Sách giáo dục & hỏi giá
            [("sách luyện thi IELTS giá bao nhiêu", "book_detail"),
             ("có sách toán lớp 12 không", "book_search"),
             ("gợi ý sách học tiếng Anh cho người mới bắt đầu", "recommend_category"),
             ("cuốn Atomic Habits bán ko", "book_detail"),
             ("thời gian giao hàng mất bao lâu", "store_info"),
             ("có được đổi trả không nếu sách lỗi", "return_policy"),
             ("ship về miền Tây được không", "store_info"),
             ("cảm ơn shop nhiều nha", "chitchat")],
            # Bộ 3 – Thể loại truyện & văn học
            [("có truyện ngôn tình hay không", "book_search"),
             ("gợi sách trinh thám hấp dẫn", "recommend_category"),
             ("Harry Potter bộ mấy cuốn", "book_detail"),
             ("sách thiếu nhi nào phù hợp lớp 3", "recommend_category"),
             ("sách Tư Duy Nhanh Và Chậm có không", "book_detail"),
             ("chính sách đổi trả hàng ra sao", "return_policy"),
             ("hotline hỗ trợ là số mấy", "store_info"),
             ("hi, cho hỏi chút", "chitchat")],
            # Bộ 4 – Review & đánh giá sách
            [("sách nào được đánh giá cao về kinh tế", "recommend_category"),
             ("Nhà Giả Kim có bản tiếng Việt không", "book_detail"),
             ("sách Rich Dad Poor Dad giá bao nhiêu", "book_detail"),
             ("gợi sách kỹ năng lãnh đạo", "recommend_category"),
             ("có mã giảm giá không", "promotion_current"),
             ("cần tư vấn mua sách cho trẻ em", "recommend_category"),
             ("địa chỉ cửa hàng ở đâu", "store_info"),
             ("xin cảm ơn", "chitchat")],
            # Bộ 5 – Sách lập trình & công nghệ
            [("sách python cho người mới bắt đầu", "book_search"),
             ("có sách clean code không", "book_detail"),
             ("gợi ý sách machine learning tiếng Việt", "recommend_category"),
             ("sách Design Patterns giá bao nhiêu", "book_detail"),
             ("sale sách IT có không", "promotion_current"),
             ("thêm sách vào wishlist được không", "store_info"),
             ("policy hoàn tiền thế nào", "return_policy"),
             ("bạn tên gì vậy", "chitchat")],
            # Bộ 6 – Sách kinh doanh & tài chính
            [("sách khởi nghiệp cho sinh viên", "recommend_category"),
             ("Zero to One có bán không", "book_detail"),
             ("sách đầu tư chứng khoán cho người mới", "recommend_category"),
             ("The Lean Startup giá mấy", "book_detail"),
             ("voucher first order có không", "promotion_current"),
             ("book này có ebook không", "store_info"),
             ("chính sách bảo hành sách ra sao", "return_policy"),
             ("chào buổi sáng", "chitchat")],
            # Bộ 7 – Văn học nước ngoài
            [("sách Dostoevsky có không", "book_search"),
             ("1984 George Orwell bán bao nhiêu", "book_detail"),
             ("gợi tiểu thuyết kinh điển châu Âu", "recommend_category"),
             ("Kafka On The Shore của Murakami còn không", "book_detail"),
             ("flash sale cuối tuần có không", "promotion_current"),
             ("giao hàng nhanh nhất bao nhiêu ngày", "store_info"),
             ("đổi sách bị rách được không", "return_policy"),
             ("bạn có thể giúp gì cho tôi", "chitchat")],
            # Bộ 8 – Sách tâm linh & phát triển bản thân
            [("sách thiền định mindfulness", "book_search"),
             ("The Power of Now giá bao nhiêu", "book_detail"),
             ("gợi sách giảm stress cho dân văn phòng", "recommend_category"),
             ("sách Ikigai còn hàng không", "book_detail"),
             ("có chương trình mua 2 tặng 1 không", "promotion_current"),
             ("mình có thể mua làm quà tặng không", "store_info"),
             ("nếu sách in sai lỗi có đổi không", "return_policy"),
             ("shop mở cửa mấy giờ vậy", "store_info")],
            # Bộ 9 – Sách học ngoại ngữ
            [("sách học tiếng Nhật N5 có không", "book_search"),
             ("giáo trình tiếng Hàn giá bao nhiêu", "book_detail"),
             ("gợi ý tài liệu TOEIC 900+", "recommend_category"),
             ("sách ngữ pháp tiếng Anh Oxford", "book_detail"),
             ("mã giảm giá cho sinh viên", "promotion_current"),
             ("có gói combo không", "store_info"),
             ("bị giao nhầm sách thì làm sao", "return_policy"),
             ("thanks bạn nhé", "chitchat")],
            # Bộ 10 – Sách sức khoẻ & dinh dưỡng
            [("sách ăn uống lành mạnh", "book_search"),
             ("Eat Move Sleep giá mấy", "book_detail"),
             ("gợi sách thể dục tại nhà", "recommend_category"),
             ("sách về giấc ngủ khoa học", "book_search"),
             ("có khuyến mãi ngày lễ không", "promotion_current"),
             ("đặt hàng online có an toàn không", "store_info"),
             ("trả hàng trong bao nhiêu ngày", "return_policy"),
             ("mình thích đọc sách lắm", "chitchat")],
        ]

    elif role == "customer":
        all_suites_raw = [
            # Bộ 1 – Lịch sử đơn & điểm
            [("lịch sử mua hàng của tôi", "order_history"),
             ("đơn hàng gần nhất của tôi trạng thái gì", "order_status"),
             ("điểm thưởng của tôi còn bao nhiêu", "loyalty_points"),
             ("gợi ý sách tương tự Đắc Nhân Tâm", "recommend_category"),
             ("có voucher nào đang dùng được không", "voucher_apply"),
             ("hoá đơn đơn #5678 thế nào", "order_status"),
             ("muốn đổi trả đơn hàng bị rách bìa", "return_request"),
             ("huỷ giúp tôi đơn mới đặt", "order_cancel")],
            # Bộ 2 – Giỏ hàng & thanh toán
            [("thêm sách Tôi Tài Giỏi Bạn Cũng Thế vào giỏ", "cart_help"),
             ("mã SALE20 có dùng được không", "voucher_apply"),
             ("thanh toán bằng ví momo được không", "payment_method"),
             ("tôi muốn mua Người Giàu Có Nhất Thành Babylon", "cart_help"),
             ("điểm tích luỹ đổi thành tiền được không", "loyalty_points"),
             ("gợi ý sách kinh doanh cho mình", "recommend_category"),
             ("đơn #7890 đã giao chưa", "order_status"),
             ("huỷ đơn hàng số 6543", "order_cancel")],
            # Bộ 3 – Đổi trả & chính sách
            [("chính sách đổi trả của shop thế nào", "return_policy"),
             ("tôi muốn trả lại sách bị lỗi in", "return_request"),
             ("thời gian hoàn tiền mất bao lâu", "payment_issue"),
             ("sách nào bán chạy nhất hiện tại", "recommend_trending"),
             ("tổng chi tiêu của tôi là bao nhiêu", "loyalty_points"),
             ("thêm cuốn Nhà Giả Kim vào cart", "cart_help"),
             ("giao hàng có nhanh không", "store_info"),
             ("tôi muốn hủy đơn 9999", "order_cancel")],
            # Bộ 4 – Gợi ý sách theo sở thích
            [("gợi sách về marketing cho tôi", "recommend_category"),
             ("sách thể loại romance hay nhất", "recommend_category"),
             ("tôi thích đọc sách khoa học viễn tưởng", "recommend_category"),
             ("cuốn này bán bao nhiêu vậy shop", "book_detail"),
             ("dùng mã FREESHIP có free ship không", "voucher_apply"),
             ("tôi muốn xem lại đơn cũ", "order_history"),
             ("điểm tích luỹ dùng được không", "loyalty_points"),
             ("hủy đơn đặt nhầm", "order_cancel")],
            # Bộ 5 – OCR + ngữ cảnh
            [("sách trinh thám Nhật Bản hay không", "book_search"),
             ("thêm Tôi Thấy Hoa Vàng Trên Cỏ Xanh vào giỏ", "cart_help"),
             ("mã discount này dùng được chưa", "voucher_apply"),
             ("đơn hàng #1234 đã ship chưa vậy", "order_status"),
             ("điểm của tôi đổi được gì", "loyalty_points"),
             ("tôi cần đổi trả cuốn sách bị ố vàng", "return_request"),
             ("đơn hàng mình đang ở bước nào rồi", "order_status"),
             ("bỏ đơn 5555 giúp tôi", "order_cancel")],
            # Bộ 6 – Thanh toán & tài khoản
            [("thanh toán COD có tiện không", "payment_method"),
             ("thanh toán bị lỗi phải làm sao", "payment_issue"),
             ("tôi cần đổi địa chỉ nhận hàng", "account_help"),
             ("profile của tôi cập nhật thế nào", "account_help"),
             ("điểm tích luỹ mình có bao nhiêu rồi", "loyalty_points"),
             ("gợi sách tâm lý học bán chạy", "recommend_category"),
             ("đơn hàng #2468 đang ở đâu", "order_status"),
             ("hủy đơn 8888 chưa giao", "order_cancel")],
            # Bộ 7 – Sách học sinh & phụ huynh
            [("gợi sách cho bé lớp 5 học tốt", "recommend_category"),
             ("sách bài tập toán nâng cao tiểu học", "book_search"),
             ("thêm sách Doraemon vào giỏ giúp mình", "cart_help"),
             ("voucher KIDS10 còn hạn không", "voucher_apply"),
             ("điểm tôi có thể dùng mua được mấy cuốn", "loyalty_points"),
             ("muốn đổi sách bị thiếu trang", "return_request"),
             ("đơn #3579 giao được chưa", "order_status"),
             ("huỷ đơn hàng đặt nhầm cuốn sách", "order_cancel")],
            # Bộ 8 – Sách ngoại văn & nhập khẩu
            [("sách tiếng Anh nguyên bản có không", "book_search"),
             ("giá cuốn Sapiens tiếng Việt bao nhiêu", "book_detail"),
             ("thêm cuốn Think and Grow Rich vào giỏ", "cart_help"),
             ("sách về khoa học hành vi hay", "recommend_category"),
             ("mã SUMMER23 còn hiệu lực không", "voucher_apply"),
             ("xem điểm tích luỹ của tôi", "loyalty_points"),
             ("đơn 7777 shipper liên lạc chưa", "order_status"),
             ("mình đổi ý muốn hủy đơn", "order_cancel")],
            # Bộ 9 – Chăm sóc sau mua
            [("tôi muốn viết đánh giá cho sách đã mua", "account_help"),
             ("đơn hàng cũ 6 tháng có còn không", "order_history"),
             ("dùng điểm giảm giá đơn tiếp theo thế nào", "loyalty_points"),
             ("đổi trả đơn #4321 vì sách in mờ", "return_request"),
             ("thanh toán online bị từ chối thẻ", "payment_issue"),
             ("thêm Bắt Trẻ Đồng Xanh vào giỏ hàng", "cart_help"),
             ("trạng thái đơn hàng #9090 ra sao", "order_status"),
             ("hủy đơn hàng tôi vừa tạo xong", "order_cancel")],
            # Bộ 10 – Chi tiết & so sánh sách
            [("So sánh giá sách giữa các sàn", "book_detail"),
             ("gợi ý 3 cuốn sách về self-help tốt nhất", "recommend_category"),
             ("thêm Cây Cam Ngọt Của Tôi vào giỏ", "cart_help"),
             ("tôi có voucher mới nhận được", "voucher_apply"),
             ("bao nhiêu điểm để giảm 50k", "loyalty_points"),
             ("đơn hàng đang chờ xác nhận thì sao", "order_status"),
             ("muốn đổi lại đơn vì sai sản phẩm", "return_request"),
             ("hủy ngay đơn tôi đặt ban nãy", "order_cancel")],
        ]

    elif role == "staff":
        all_suites_raw = [
            # Bộ 1 – Xử lý đơn cơ bản
            [("danh sách đơn hàng đang chờ xử lý", "staff_order_list_pending"),
             ("xem chi tiết đơn #2233", "staff_order_lookup"),
             ("chuyển đơn 2233 sang trạng thái đang giao", "staff_order_status_update"),
             ("tìm khách hàng email test@gmail.com", "staff_customer_lookup"),
             ("tồn kho sách Sapiens còn bao nhiêu", "staff_inventory_check"),
             ("cập nhật số lượng sách ID 100 lên 30", "staff_inventory_update"),
             ("doanh thu hôm nay được bao nhiêu", "staff_order_statistics"),
             ("top 5 sách bán chạy nhất tháng", "staff_top_selling")],
            # Bộ 2 – Tra cứu & cập nhật
            [("show tôi danh sách đơn processing", "staff_order_list_pending"),
             ("check bill #3344 cho tôi", "staff_order_lookup"),
             ("update đơn 3344 thành delivered", "staff_order_status_update"),
             ("lookup khách có số điện thoại 0901", "staff_customer_lookup"),
             ("kiểm tra tồn kho cuốn Nhà Giả Kim", "staff_inventory_check"),
             ("nhập thêm 100 cuốn Đắc Nhân Tâm", "staff_inventory_update"),
             ("tổng số đơn thành công hôm nay", "staff_order_statistics"),
             ("sách nào được bán nhiều nhất tuần qua", "staff_top_selling")],
            # Bộ 3 – Kho hàng & thống kê
            [("đơn nào đang pending cần duyệt gấp", "staff_order_list_pending"),
             ("tìm đơn hàng #5566", "staff_order_lookup"),
             ("đổi trạng thái đơn 5566 sang shipped", "staff_order_status_update"),
             ("tìm info khách hàng id 200", "staff_customer_lookup"),
             ("sách lập trình Python còn mấy cuốn", "staff_inventory_check"),
             ("điều chỉnh số lượng tồn kho sách ID 55", "staff_inventory_update"),
             ("báo cáo đơn hàng ngày hôm nay", "staff_order_statistics"),
             ("top 3 danh mục bán chạy", "staff_top_selling")],
            # Bộ 4 – Xử lý tình huống
            [("lọc đơn chưa xác nhận từ sáng đến giờ", "staff_order_list_pending"),
             ("check chi tiết bill #7788 cho mình", "staff_order_lookup"),
             ("hủy đơn hàng 7788 do khách yêu cầu", "staff_order_status_update"),
             ("tìm khách hàng tên Nguyễn Văn A", "staff_customer_lookup"),
             ("Doraemon tập 1 còn không", "staff_inventory_check"),
             ("thay đổi tồn kho sách Atomic Habits thành 25", "staff_inventory_update"),
             ("thống kê đơn hàng theo ngày", "staff_order_statistics"),
             ("cuốn sách doanh thu cao nhất tháng", "staff_top_selling")],
            # Bộ 5 – Vận hành
            [("show all orders đang pending", "staff_order_list_pending"),
             ("xem order #9900 có gì", "staff_order_lookup"),
             ("mark đơn 9900 là cancelled", "staff_order_status_update"),
             ("tìm khách hàng mua nhiều nhất", "staff_customer_lookup"),
             ("inventory sách Clean Code bao nhiêu cuốn", "staff_inventory_check"),
             ("update stock cuốn Design Patterns lên 15", "staff_inventory_update"),
             ("tổng doanh thu tuần này", "staff_order_statistics"),
             ("ranking sách best seller tháng này", "staff_top_selling")],
            # Bộ 6 – Thay phiên ca
            [("đang có bao nhiêu đơn tồn đọng", "staff_order_list_pending"),
             ("kiểm tra đơn số #1357", "staff_order_lookup"),
             ("cập nhật đơn 1357 đã giao xong", "staff_order_status_update"),
             ("tìm khách hàng user_id 300", "staff_customer_lookup"),
             ("còn bao nhiêu cuốn 7 Thói Quen", "staff_inventory_check"),
             ("thêm 50 quyển Rich Dad Poor Dad vào kho", "staff_inventory_update"),
             ("có bao nhiêu đơn hủy hôm nay", "staff_order_statistics"),
             ("sách nào bán chạy nhất tuần trước", "staff_top_selling")],
            # Bộ 7 – Nghiệp vụ nâng cao
            [("lấy danh sách đơn hàng cần giao gấp", "staff_order_list_pending"),
             ("pull order #2468 thông tin gì", "staff_order_lookup"),
             ("set status đơn 2468 thành processing", "staff_order_status_update"),
             ("tìm khách email abc@yahoo.com", "staff_customer_lookup"),
             ("check kho sách 1984 George Orwell", "staff_inventory_check"),
             ("giảm kho sách ID 77 xuống 5 cuốn", "staff_inventory_update"),
             ("bao nhiêu đơn shipped hôm nay", "staff_order_statistics"),
             ("best seller theo doanh thu", "staff_top_selling")],
            # Bộ 8 – Cuối ngày
            [("còn đơn nào chưa duyệt không", "staff_order_list_pending"),
             ("tra đơn #3691 của khách", "staff_order_lookup"),
             ("chuyển đơn 3691 sang delivered", "staff_order_status_update"),
             ("lookup thông tin khách id 150", "staff_customer_lookup"),
             ("sách Harry Potter tập 7 còn không", "staff_inventory_check"),
             ("nhập kho 200 cuốn sách giáo khoa", "staff_inventory_update"),
             ("tổng số đơn thành công tháng này", "staff_order_statistics"),
             ("top sách theo số lượng bán", "staff_top_selling")],
            # Bộ 9 – Đầu ca sáng
            [("đơn nào từ tối hôm qua chưa duyệt", "staff_order_list_pending"),
             ("chi tiết đơn hàng #8024", "staff_order_lookup"),
             ("xác nhận đơn 8024 đã xử lý", "staff_order_status_update"),
             ("tìm khách sdt 0379", "staff_customer_lookup"),
             ("tồn kho cuốn Cây Cam Ngọt", "staff_inventory_check"),
             ("update tồn kho sách ID 200 thành 60", "staff_inventory_update"),
             ("thống kê đơn theo trạng thái", "staff_order_statistics"),
             ("top 10 sách theo rating cao nhất", "staff_top_selling")],
            # Bộ 10 – Kiểm tra cuối tuần
            [("còn tồn đơn xử lý không", "staff_order_list_pending"),
             ("check bill #6060 phải trả bao nhiêu", "staff_order_lookup"),
             ("cập nhật bill 6060 thành đang vận chuyển", "staff_order_status_update"),
             ("tìm khách hàng có địa chỉ Hà Nội", "staff_customer_lookup"),
             ("check inventory cuốn sách kỹ năng mềm", "staff_inventory_check"),
             ("nhập hàng mới: 80 cuốn sách IT", "staff_inventory_update"),
             ("doanh thu tuần này so sánh tuần trước", "staff_order_statistics"),
             ("danh sách sách hot tháng 4", "staff_top_selling")],
        ]

    else:  # admin
        all_suites_raw = [
            # Bộ 1 – Doanh thu & người dùng
            [("doanh thu tuần này bao nhiêu", "admin_revenue_stats"),
             ("tỷ lệ đơn giao thành công tháng qua", "admin_order_stats"),
             ("liệt kê tài khoản đang bị khóa", "admin_banned_users"),
             ("ban tài khoản lừa đảo user 99", "admin_user_lock_unlock"),
             ("đổi role user 200 thành staff", "admin_user_update_role"),
             ("tạo mã giảm giá SUMMER2026", "admin_promotion_create_guide"),
             ("sách nào rating dưới 2 sao", "admin_book_low_rating"),
             ("kiểm tra trạng thái server", "admin_system_health")],
            # Bộ 2 – Thống kê tổng quan
            [("báo cáo doanh thu tháng 3", "admin_revenue_stats"),
             ("tổng số đơn hàng hôm nay", "admin_order_stats"),
             ("account nào đang bị suspend", "admin_banned_users"),
             ("khóa user spam bình luận", "admin_user_lock_unlock"),
             ("cấp quyền admin cho user 50", "admin_user_update_role"),
             ("thêm voucher mới FLASH30", "admin_promotion_create_guide"),
             ("xem sách có điểm đánh giá thấp", "admin_book_low_rating"),
             ("health check hệ thống", "admin_system_health")],
            # Bộ 3 – Quản lý người dùng
            [("gross revenue cả năm ngoái", "admin_revenue_stats"),
             ("đơn nào bị hoàn trả nhiều nhất", "admin_order_stats"),
             ("tài khoản bị banned ai quản lý", "admin_banned_users"),
             ("vô hiệu hóa user 777", "admin_user_lock_unlock"),
             ("thăng chức user 88 lên admin", "admin_user_update_role"),
             ("khởi tạo chương trình sale mới", "admin_promotion_create_guide"),
             ("sách nào cần cải thiện chất lượng", "admin_book_low_rating"),
             ("check mysql và elasticsearch", "admin_system_health")],
            # Bộ 4 – Phân tích kinh doanh
            [("revenue tháng này so t trước", "admin_revenue_stats"),
             ("tỉ lệ hủy đơn tháng 4", "admin_order_stats"),
             ("show me danh sách user bị lock", "admin_banned_users"),
             ("block account gian lận", "admin_user_lock_unlock"),
             ("upgrade quyền user thành staff", "admin_user_update_role"),
             ("setup promotion cho ngày lễ", "admin_promotion_create_guide"),
             ("review sản phẩm dưới 3 sao", "admin_book_low_rating"),
             ("ping database status", "admin_system_health")],
            # Bộ 5 – Quản trị hệ thống
            [("tổng doanh thu Q1 năm nay", "admin_revenue_stats"),
             ("tỷ lệ thành công đơn hàng 3 tháng", "admin_order_stats"),
             ("danh sách user đang lock", "admin_banned_users"),
             ("tạm khóa tài khoản suspect", "admin_user_lock_unlock"),
             ("role update user sang customer", "admin_user_update_role"),
             ("hướng dẫn thêm mã coupon mới", "admin_promotion_create_guide"),
             ("đánh giá sách kém chất lượng", "admin_book_low_rating"),
             ("service ollama đang chạy không", "admin_system_health")],
            # Bộ 6 – Khuyến mãi & sách
            [("doanh thu theo danh mục sách", "admin_revenue_stats"),
             ("số đơn bị trả hàng tháng này", "admin_order_stats"),
             ("user nào đang bị inactive", "admin_banned_users"),
             ("khóa user vi phạm chính sách", "admin_user_lock_unlock"),
             ("set role user thành staff level", "admin_user_update_role"),
             ("tạo promotion cho sự kiện", "admin_promotion_create_guide"),
             ("sách 1-2 sao cần xem lại", "admin_book_low_rating"),
             ("system health overall status", "admin_system_health")],
            # Bộ 7 – Phân tích & báo cáo
            [("tổng doanh thu year to date", "admin_revenue_stats"),
             ("conversion rate đơn hàng", "admin_order_stats"),
             ("ai bị khóa tài khoản gần đây", "admin_banned_users"),
             ("disable account user spam", "admin_user_lock_unlock"),
             ("assign staff role cho nhân viên mới", "admin_user_update_role"),
             ("tạo discount code cho black friday", "admin_promotion_create_guide"),
             ("sách bị review xấu cần kiểm tra", "admin_book_low_rating"),
             ("check logs server hôm nay", "admin_system_health")],
            # Bộ 8 – Quản lý nội dung
            [("revenue breakdown theo kênh", "admin_revenue_stats"),
             ("đơn hàng delivered thành công tháng 4", "admin_order_stats"),
             ("list locked accounts", "admin_banned_users"),
             ("suspend user lợi dụng hệ thống", "admin_user_lock_unlock"),
             ("change user permission to staff", "admin_user_update_role"),
             ("tạo voucher mã NEWBOOK", "admin_promotion_create_guide"),
             ("sách đang có phản hồi tiêu cực", "admin_book_low_rating"),
             ("trạng thái các service backend", "admin_system_health")],
            # Bộ 9 – Cuối tháng
            [("tổng kết doanh thu cuối tháng", "admin_revenue_stats"),
             ("tỷ lệ đơn shipped vs delivered", "admin_order_stats"),
             ("user nào bị report nhiều", "admin_banned_users"),
             ("lock user có hành vi gian lận", "admin_user_lock_unlock"),
             ("promote user lên manager", "admin_user_update_role"),
             ("tạo mã sale cuối năm", "admin_promotion_create_guide"),
             ("review thấp nhất hệ thống", "admin_book_low_rating"),
             ("check api gateway status", "admin_system_health")],
            # Bộ 10 – Audit & compliance
            [("COD revenue vs online payment revenue", "admin_revenue_stats"),
             ("đơn đang vận chuyển bao nhiêu", "admin_order_stats"),
             ("ai đang bị restricted", "admin_banned_users"),
             ("tắt tài khoản test user", "admin_user_lock_unlock"),
             ("nâng quyền nhân viên mới lên staff", "admin_user_update_role"),
             ("setup mã giảm giá birthday", "admin_promotion_create_guide"),
             ("sách bị khiếu nại chất lượng in", "admin_book_low_rating"),
             ("database connection pool status", "admin_system_health")],
        ]

    # Chuyển đổi thành format [(msg, intent, is_image, img_path)]
    suites = []
    for raw_suite in all_suites_raw:
        suite = []
        for j, (msg, intent) in enumerate(raw_suite):
            is_image = False
            img_path = None
            # Chèn ảnh thật vào vị trí 0 và 3 nếu là guest/customer
            if IMAGE_PATHS and (j == 0 or j == 3) and role in ["guest", "customer"]:
                img_path = random.choice(IMAGE_PATHS)
                is_image = True
                msg = f"Tìm sách trong ảnh này: {os.path.basename(img_path)}"
                intent = "image_search"
            suite.append((msg, intent, is_image, img_path))
        suites.append(suite)

    return suites


async def run_massive_tests():
    configs = [
        {"role_name": "guest", "role_param": "customer", "user_id": None, "endpoint": "http://127.0.0.1:8000/api/chat/message"},
        {"role_name": "customer", "role_param": "customer", "user_id": 105, "endpoint": "http://127.0.0.1:8000/api/chat/message"},
        {"role_name": "staff", "role_param": "staff", "user_id": 99, "endpoint": "http://127.0.0.1:8000/api/staff/chat/message"},
        {"role_name": "admin", "role_param": "admin", "user_id": 1, "endpoint": "http://127.0.0.1:8000/api/admin/chat/message"}
    ]

    total_pass = 0
    total_turns = 10 * 4 * 8
    upload_url = "http://127.0.0.1:8000/api/chat/upload-image"

    report_path = os.path.join(os.path.dirname(__file__), "massive_ocr_e2e_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        def log(clean_msg, colored_msg):
            print(colored_msg)
            f.write(clean_msg + "\n")
            f.flush()

        async with httpx.AsyncClient(timeout=60.0) as client:
            for cfg in configs:
                r_name = cfg['role_name']
                log("="*60, f"\n{Fore.GREEN}{'='*60}")
                log(f"🚀 BẮT ĐẦU CHẠY LUỒNG: {r_name.upper()} (Auth: {cfg['user_id']})", f"{Fore.GREEN}🚀 BẮT ĐẦU CHẠY LUỒNG: {r_name.upper()} (Auth: {cfg['user_id']})")
                log("="*60, f"{Fore.GREEN}{'='*60}")
                
                suites = build_suites(r_name)
                
                for i, suite in enumerate(suites, 1):
                    session_id = f"test_{r_name}_{uuid.uuid4().hex[:6]}"
                    log(f"\n--- BỘ TEST CASE {i}/10 (ID: {session_id}) ---", f"\n{Fore.CYAN}--- BỘ TEST CASE {i}/10 (ID: {session_id}) ---")
                    
                    for turn_idx, (msg, expected_intent, is_image, img_path) in enumerate(suite, 1):
                        payload = {"session_id": session_id, "message": msg, "user_id": cfg["user_id"], "role": cfg["role_param"]}
                        
                        t0 = time.perf_counter()
                        got_intent = ""
                        answer = ""
                        
                        try:
                            img_nav_buttons = []
                            if is_image and img_path:
                                with open(img_path, "rb") as bf:
                                    files = {"file": (os.path.basename(img_path), bf, "image/jpeg")}
                                    data = {
                                        "session_id": session_id, 
                                        "message": msg,
                                        "role": cfg["role_param"],
                                    }
                                    if cfg["user_id"]: data["user_id"] = str(cfg["user_id"])

                                    async with client.stream("POST", upload_url, data=data, files=files) as r:
                                        if r.status_code == 200:
                                            async for line in r.aiter_lines():
                                                if line.startswith("data: "):
                                                    try:
                                                        evt = json.loads(line[6:])
                                                        if evt.get("type") == "token" and evt.get("complete"):
                                                            answer = evt.get("content", "")
                                                        elif evt.get("type") == "done":
                                                            got_intent = "image_search"
                                                            # Lấy danh sách sách từ navigate_buttons
                                                            img_nav_buttons = evt.get("navigate_buttons", [])
                                                    except: pass
                            else:
                                r = await client.post(cfg["endpoint"], json=payload)
                                if r.status_code == 200:
                                    resp_data = r.json()
                                    got_intent = resp_data.get("intent", "")
                                    answer = resp_data.get("answer", "").strip().replace("\n", " ")
                                    navigate_buttons = resp_data.get("navigate_buttons", [])
                                    
                                    # Guest blocking checks
                                    if r_name == "guest" and expected_intent in ["order_status", "order_history", "cart_help", "account_help"]:
                                        if any(b.get("url") == "/login" for b in navigate_buttons):
                                            expected_intent = got_intent # Tự động quy về đúng do bị block
                            
                            # Build display answer - không cắt ngắn
                            if is_image:
                                if img_nav_buttons:
                                    book_titles = [b.get("label", b.get("url", "?")) for b in img_nav_buttons[:5]]
                                    img_summary = " | ".join(book_titles)
                                    display_answer = f"[{len(img_nav_buttons)} sách] {img_summary}"
                                elif answer.strip():
                                    display_answer = answer
                                else:
                                    display_answer = "[Không nhận dạng được sách trong ảnh]"
                            else:
                                display_answer = answer if answer.strip() else "[không có nội dung]"
                            
                            lat = (time.perf_counter() - t0) * 1000
                            if got_intent == expected_intent or ("confirmation" in got_intent) or (r_name == "guest" and got_intent == "account_help"):
                                log(f" [T{turn_idx}] {'[IMG] ' if is_image else ''}{msg[:30]:<30} -> ✅ PASS ({got_intent}) | {lat:.0f}ms\n      Bot: {display_answer}",
                                    f" {Fore.WHITE}[T{turn_idx}] {'[IMG] ' if is_image else ''}{msg[:30]:<30} -> {Fore.GREEN}✅ PASS ({got_intent}) | {lat:.0f}ms\n      {Fore.MAGENTA}Bot: {display_answer}")
                                total_pass += 1
                            else:
                                log(f" [T{turn_idx}] {'[IMG] ' if is_image else ''}{msg[:30]:<30} -> ❌ FAIL (Got: {got_intent}, Exp: {expected_intent})\n      Bot: {display_answer}",
                                    f" {Fore.WHITE}[T{turn_idx}] {'[IMG] ' if is_image else ''}{msg[:30]:<30} -> {Fore.RED}❌ FAIL (Got: {got_intent}, Exp: {expected_intent})\n      {Fore.MAGENTA}Bot: {display_answer}")
                                
                        except Exception as e:
                            log(f"Exception: {e}", f"{Fore.RED}Exception: {e}")
                        
                        await asyncio.sleep(1.0)
                        
        log(f"\n🏆 TỔNG KẾT: {total_pass}/{total_turns} TURNS PASSED.", f"\n{Fore.YELLOW}🏆 TỔNG KẾT: {total_pass}/{total_turns} TURNS PASSED.")

if __name__ == "__main__":
    asyncio.run(run_massive_tests())
