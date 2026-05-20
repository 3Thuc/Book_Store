from tests.evaluate_chatbot_multiturn import Flow, Turn

CUSTOMER_FLOWS: list[Flow] = [
    Flow("C-F01-Greeting-Search-Detail-Cart-Pay", "customer", [
        Turn("xin chao shop oi", "chitchat"),
        Turn("tim sach ky nang lãnh đạo hay cho minh", "book_search"),
        Turn("tac gia cuon sach do la ai vay shop", "book_detail"),
        Turn("them cuon do vao gio hang giup minh voi", "cart_help"),
        Turn("toi muon thanh toan bang the tin dung", "payment_method")
    ]),
    Flow("C-F02-Trending-Combo-OOS-Cancel-History", "customer", [
        Turn("sach ban chay nhat hien nay la cuon nao", "recommend_trending"),
        Turn("nen doc gi tiep sau khi doc xong cuon Sapiens", "recommend_combo"),
        Turn("thoi nilon cua shop co tai che duoc khong", "out_of_scope"),
        Turn("thoi e huy don vua dat nhe", "order_cancel"),
        Turn("cho e xem lai lich su mua hang cua e", "order_history")
    ]),
    Flow("C-F03-Gift-Compare-Availability-Review-Recommend", "customer", [
        Turn("mua sach tang ban gai dip sinh nhat", "recommend_gift"),
        Turn("so sanh hai cuon Atomic Habits va Mindset", "book_compare"),
        Turn("cuon Atomic Habits con hang khong shop", "book_availability"),
        Turn("review sach do di", "book_review"),
        Turn("goi y sach tam ly hoc khac", "recommend_category")
    ]),
    Flow("C-F04-Payment-Issue-OOS-Yes-Confirm", "customer", [
        Turn("hinh thuc thanh toan cua shop la gi vay", "payment_method"),
        Turn("Momo bi loi thanh toan khong qua duoc", "payment_issue"),
        Turn("an uong gi ngon nhat Ha Noi", "out_of_scope"),
        Turn("xac nhan don hang qua email chua", "confirmation_yes"),
        Turn("don hang so 99887 dang o dau roi vay", "order_status")
    ]),
    Flow("C-F05-Damage-Return-No-Policy-Voucher", "customer", [
        Turn("sach nhan ve bi rach bia nang qua shop", "complaint_damaged"),
        Turn("muon tra lai hang lay lai tien cho toi", "return_request"),
        Turn("thoi khong can nua cam on shop", "confirmation_no"),
        Turn("bao nhieu ngay duoc doi tra hang vay shop", "return_policy"),
        Turn("ma giam gia BOOK10 co dung duoc khong", "voucher_apply")
    ]),
    Flow("C-F06-Promo-Store-Account-Points-Chitchat", "customer", [
        Turn("hien co chuong trinh khuyen mai gi khong", "promotion_current"),
        Turn("so hotline ho tro cua shop la so may", "store_info"),
        Turn("toi quen mat khau dang nhap vao tai khoan", "account_help"),
        Turn("diem thuong cua toi con bao nhieu vay", "loyalty_points"),
        Turn("hi ban bot lam duoc gi", "chitchat")
    ]),
    Flow("C-F07-WrongItem-Return-No-Detail-Search", "customer", [
        Turn("giao sai sach hoan toan khong dung cuon dat", "complaint_wrong"),
        Turn("yeu cau hoan tien don hang 123456", "return_request"),
        Turn("huy bo thao tac", "confirmation_no"),
        Turn("cuon Harry Potter gia bao nhieu vay", "book_detail"),
        Turn("tim theo ten sach Đắc Nhân Tâm", "book_search")
    ]),
    Flow("C-F08-Personal-Trending-OOS-Return-Contact", "customer", [
        Turn("goi y sach hay cho nguoi moi di lam", "recommend_personal"),
        Turn("sach dang ban chay nhat thang nay la gi", "recommend_trending"),
        Turn("tivi nha toi bi hu roi", "out_of_scope"),
        Turn("chinh sach doi tra cua shop nhu the nao", "return_policy"),
        Turn("shop o dau co dia chi khong", "store_info")
    ]),
    Flow("C-F09-Order-Search-OOS-Yes-No", "customer", [
        Turn("don hang so 778899 khi nao giao den nha", "order_status"),
        Turn("tim kiem ma sach 12345", "book_search"),
        Turn("cho xin tin tuc nong nhat hom nay", "out_of_scope"),
        Turn("ok tks shop", "confirmation_yes"),
        Turn("khong dung bot nua doc sach thoi", "confirmation_no")
    ]),
    Flow("C-F10-Voucher-Promo-Points-Pay-Cart", "customer", [
        Turn("nhap voucher SUMMER2025 bi loi khong ap", "voucher_apply"),
        Turn("sale gi khong shop oi hom nay", "promotion_current"),
        Turn("tich diem nhu the nao trong chuong trinh", "loyalty_points"),
        Turn("co the chuyen khoan ngan hang khong", "payment_method"),
        Turn("gio hang minh co gi vay", "cart_help")
    ]),
]

STAFF_FLOWS: list[Flow] = [
    Flow("S-F01-OrderLookup-Update-Stats-OOS-Chit", "staff", [
        Turn("tra cuu don hang 12345 cho khach", "staff_order_lookup"),
        Turn("cap nhat trang thai don 12345 sang delivered", "staff_order_status_update"),
        Turn("thong ke don hang hom nay co bao nhieu", "staff_order_statistics"),
        Turn("gia vang hom nay", "staff_out_of_scope"),
        Turn("chao vao h lam", "staff_chitchat")
    ]),
    Flow("S-F02-Pending-Return-Resolve-Lookup-OOS", "staff", [
        Turn("bao nhieu don cho xu ly hom nay the bot", "staff_order_list_pending"),
        Turn("danh sach don doi tra chua duoc xu ly", "staff_return_handle"),
        Turn("danh dau khieu nai don 33221 da xu ly xong", "staff_complaint_resolve"),
        Turn("xem chi tiet sach trong he thong id 1001", "staff_book_lookup"),
        Turn("the gioi chung khoan ra sao", "staff_out_of_scope")
    ]),
    Flow("S-F03-Inventory-LowStock-Update-Revenue-Top", "staff", [
        Turn("kiem tra ton kho sach Sapiens con bao nhieu", "staff_inventory_check"),
        Turn("sach nao sap het hang can nhap gap", "staff_inventory_low"),
        Turn("tang stock book 2002 them 30 quyen luon", "staff_inventory_update"),
        Turn("doanh thu hom nay cua cua hang la bn", "staff_revenue_today"),
        Turn("sach ban chay nhat he thong la gi", "staff_top_selling")
    ]),
    Flow("S-F04-CustLookup-Escalated-Resolve-Chit-Check", "staff", [
        Turn("tra cuu thong tin khach hang theo email", "staff_customer_lookup"),
        Turn("ticket can xu ly cap toc co bao nhieu ca", "staff_escalated_issues"),
        Turn("resolve complaint for order 33221 please", "staff_complaint_resolve"),
        Turn("toi can tro giup tu bot", "staff_chitchat"),
        Turn("con bn quyen clean code trg kho", "staff_inventory_check")
    ]),
    Flow("S-F05-LowStock-Update-Pending-Escalate-OOS", "staff", [
        Turn("canh bao ton kho thap danh sach day", "staff_inventory_low"),
        Turn("nhap them hang sach Sapiens vao kho 100", "staff_inventory_update"),
        Turn("don dang cho duyet hien tai", "staff_order_list_pending"),
        Turn("van de khach chua duoc xu ly", "staff_escalated_issues"),
        Turn("an tiec cong ty", "staff_out_of_scope")
    ]),
]

ADMIN_FLOWS: list[Flow] = [
    Flow("A-F01-Dashboard-Stats-Users-Promo-Revenue", "admin", [
        Turn("cho xem dashboard tong quan", "admin_dashboard_summary"),
        Turn("thong ke don hang thang nay", "admin_order_stats"),
        Turn("thong ke so luong nguoi dung xai web", "admin_user_stats"),
        Turn("check danh sach ma giam gia he thong", "admin_promotion_list"),
        Turn("bao cao doanh thu trong chi tiet hom nay", "admin_revenue_report")
    ]),
    Flow("A-F02-Users-Update-OOS-PromoAdd-Top", "admin", [
        Turn("hien tai co bao nhieu account moi dang ky", "admin_user_stats"),
        Turn("nang cap tai khoan 112 sang admin luon", "admin_user_update_role"),
        Turn("troi mua lam ngap lut web", "admin_out_of_scope"),
        Turn("tao them ma khuyen mai moi dang SALE20", "admin_promotion_create"),
        Turn("bxh sach doanh thu thang nay do", "admin_top_books")
    ]),
    Flow("A-F03-Revenue-Top-Dashboard-Check-List", "admin", [
        Turn("doanh thu thang nay tong hop", "admin_revenue_report"),
        Turn("top ban chay nhat la nhung cuon nao", "admin_top_books"),
        Turn("tom tat he thong bay gio the nao", "admin_dashboard_summary"),
        Turn("kiem tra hieu luc cua voucher HELLO", "admin_promotion_check"),
        Turn("ma nao da bi expire list luon", "admin_promotion_list")
    ]),
    Flow("A-F04-Update-Promo-Del-OOS-Stats", "admin", [
        Turn("ha bac role user 54 thanh customer", "admin_user_update_role"),
        Turn("thong tin ma SUMMER he thong con hieu luc", "admin_promotion_check"),
        Turn("huy bo ma giam gia xau XOA di", "admin_promotion_delete"),
        Turn("thay mat kinh cua dien thoai", "admin_out_of_scope"),
        Turn("ti le don hang thanh cong thang nay", "admin_order_stats")
    ]),
    Flow("A-F05-Create-List-Exp-Sys-Chit", "admin", [
        Turn("them ma promo moi di NEW20", "admin_promotion_create"),
        Turn("ma nao trong he thong chua tung xai", "admin_promotion_list"),
        Turn("voucher code nao sap dead can ra soat", "admin_promotion_expiring"),
        Turn("xem luong cpu he thong the nao roi", "admin_system_health"),
        Turn("hello bot admin ne", "admin_chitchat")
    ]),
]
