package com.be.book.BookStorage.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;

@Getter
public enum ErrorCode {

    // ========== NHÓM 1xxx: CHUNG, XÁC THỰC & NGƯỜI DÙNG ==========
    INVALID_KEY(1001, "Khóa dữ liệu không hợp lệ", HttpStatus.BAD_REQUEST),
    USER_EXISTED(1002, "Người dùng đã tồn tại", HttpStatus.CONFLICT),
    USERNAME_INVALID(1003, "Tên đăng nhập phải có ít nhất {min} ký tự", HttpStatus.BAD_REQUEST),
    PASSWORD_INVALID(1004, "Mật khẩu phải có ít nhất {min} ký tự", HttpStatus.BAD_REQUEST),
    USER_NOT_FOUND(1005, "Người dùng không tồn tại", HttpStatus.NOT_FOUND),
    UNAUTHENTICATED(1006, "Bạn cần đăng nhập để thực hiện", HttpStatus.UNAUTHORIZED), // 401
    UNAUTHORIZED(1007, "Bạn không có quyền truy cập", HttpStatus.FORBIDDEN), // 403
    INVALID_DOB(1008, "Bạn phải từ {min} tuổi trở lên", HttpStatus.BAD_REQUEST),
    USER_INACTIVE(1009, "Tài khoản người dùng chưa kích hoạt", HttpStatus.FORBIDDEN),
    EMAIL_INVALID(1010, "Định dạng email không hợp lệ", HttpStatus.BAD_REQUEST),
    EMAIL_NOT_VERIFIED(1011, "Email chưa được xác thực", HttpStatus.FORBIDDEN),

    // --- Token ---
    TOKEN_EXPIRED(1012, "Token đã hết hạn", HttpStatus.UNAUTHORIZED),
    TOKEN_INVALID(1013, "Token không hợp lệ", HttpStatus.UNAUTHORIZED),
    REFRESH_TOKEN_INVALID(1014, "Refresh token không hợp lệ", HttpStatus.UNAUTHORIZED),

    // --- Yêu cầu & Tập tin ---
    INVALID_REQUEST(1015, "Dữ liệu yêu cầu không hợp lệ", HttpStatus.BAD_REQUEST),
    FILE_TOO_LARGE(1016, "Tệp tải lên quá lớn", HttpStatus.BAD_REQUEST),
    FILE_TYPE_NOT_ALLOWED(1017, "Loại tệp không được phép", HttpStatus.BAD_REQUEST),
    OPERATION_NOT_ALLOWED(1020, "Thao tác không được phép", HttpStatus.FORBIDDEN),
    INVALID_CREDENTIALS(1024, "Sai email hoặc mật khẩu", HttpStatus.UNAUTHORIZED),
    INCORRECT_PASSWORD(1025, "Sai mật khẩu", HttpStatus.BAD_REQUEST),

    // ========== NHÓM 2xxx: SẢN PHẨM, TÁC GIẢ, THỂ LOẠI ==========
    BOOK_NOT_FOUND(2001, "Sách không tồn tại", HttpStatus.NOT_FOUND),
    BOOK_ALREADY_DELETED(2012, "Sách đã được xóa", HttpStatus.CONFLICT),
    AUTHOR_NOT_FOUND(2002, "Không tìm thấy tác giả", HttpStatus.NOT_FOUND),
    CATEGORY_NOT_FOUND(2003, "Không tìm thấy thể loại", HttpStatus.NOT_FOUND),
    PUBLISHER_NOT_FOUND(2004,"Không tìm thấy nhà xuất bản", HttpStatus.NOT_FOUND),

    // --- Lỗi nghiệp vụ (Admin) ---
    AUTHOR_ALREADY_EXISTED(2005, "Tên tác giả này đã tồn tại", HttpStatus.CONFLICT), // Sửa lỗi trùng 1003
    CATEGORY_ALREADY_EXISTED(2006, "Tên thể loại này đã tồn tại", HttpStatus.CONFLICT),
    SKU_ALREADY_EXISTED(2007, "Mã SKU của sách đã tồn tại", HttpStatus.CONFLICT),
    CANNOT_DELETE_ASSOCIATED_RESOURCE(2008, "Không thể xoá (tác giả/thể loại) vì còn sách liên quan", HttpStatus.CONFLICT),
    PUBLISHER_ALREADY_EXISTED(2009,"Tên nhà xuất bản này đã tồn tại", HttpStatus.CONFLICT),
    // --- Lỗi kho hàng (Inventory) ---
    PRODUCT_OUT_OF_STOCK(2010, "Sản phẩm đã hết hàng", HttpStatus.BAD_REQUEST),
    INSUFFICIENT_STOCK(2011, "Số lượng trong kho không đủ", HttpStatus.BAD_REQUEST),


    // ========== NHÓM 3xxx: GIỎ HÀNG & THANH TOÁN (CART & CHECKOUT) ==========
    CART_NOT_FOUND(3001, "Không tìm thấy giỏ hàng", HttpStatus.NOT_FOUND),
    CART_ITEM_NOT_FOUND(3002, "Sản phẩm không có trong giỏ hàng", HttpStatus.NOT_FOUND),
    CART_IS_EMPTY(3003, "Giỏ hàng rỗng, không thể thanh toán", HttpStatus.BAD_REQUEST),
    QUANTITY_LIMIT_EXCEEDED(3004, "Vượt quá số lượng cho phép mua", HttpStatus.BAD_REQUEST),
    PRODUCT_NOT_AVAILABLE_FOR_CART(3005, "Sản phẩm này tạm thời không thể thêm vào giỏ", HttpStatus.BAD_REQUEST),
    PAYMENT_FAILED(3006, "Thanh toán thất bại", HttpStatus.INTERNAL_SERVER_ERROR),

    // ========== NHÓM 4xxx: ĐƠN HÀNG & VẬN CHUYỂN (ORDER & SHIPPING) ==========
    ORDER_NOT_FOUND(4001, "Không tìm thấy đơn hàng", HttpStatus.NOT_FOUND),
    INVALID_ORDER_STATUS(4002, "Trạng thái đơn hàng không hợp lệ", HttpStatus.BAD_REQUEST),
    CANNOT_CANCEL_ORDER(4003, "Không thể huỷ đơn hàng ở trạng thái này", HttpStatus.FORBIDDEN),
    INVALID_STATUS_TRANSITION(4004, "Không thể chuyển trạng thái đơn hàng", HttpStatus.BAD_REQUEST),
    ADDRESS_NOT_FOUND(4005, "Không tìm thấy địa chỉ", HttpStatus.NOT_FOUND),
    DEFAULT_ADDRESS_NOT_SET(4006, "Chưa thiết lập địa chỉ mặc định", HttpStatus.BAD_REQUEST),
    SHIPPING_NOT_AVAILABLE(4007, "Không hỗ trợ vận chuyển đến địa chỉ này", HttpStatus.BAD_REQUEST),
    INVALID_DATE_RANGE(4008, "Ngày kết thúc không được nhỏ hơn ngày bắt đầu", HttpStatus.BAD_REQUEST),
    PROMOTION_CODE_EXISTED(4009, "Mã khuyến mãi đã tồn tại", HttpStatus.CONFLICT),
    INVALID_DISCOUNT_PERCENT(4010, "Phần trăm giảm giá phải nằm trong khoảng 0 đến 100", HttpStatus.BAD_REQUEST),

    // Delete errors (4xxx)
    PROMOTION_ALREADY_DELETED(4011, "Khuyến mãi này đã bị xóa trước đó", HttpStatus.BAD_REQUEST),
    PROMOTION_NOT_DELETED(4012, "Khuyến mãi này hiện chưa bị xóa", HttpStatus.BAD_REQUEST),
    CANNOT_UPDATE_DELETED_PROMOTION(4013, "Không thể cập nhật một khuyến mãi đã bị xóa", HttpStatus.BAD_REQUEST),
    PROMOTION_IN_USE_CANNOT_DELETE(4014, "Không thể xóa khuyến mãi đang được sử dụng", HttpStatus.CONFLICT),

    CANNOT_ACTIVATE_FUTURE_PROMOTION(4020, "Không thể kích hoạt khuyến mãi chưa bắt đầu", HttpStatus.BAD_REQUEST),
    CANNOT_ACTIVATE_EXPIRED_PROMOTION(4021, "Không thể kích hoạt khuyến mãi đã hết hạn", HttpStatus.BAD_REQUEST),
    ADMIN_ONLY_OPERATION(4030, "Chỉ quản trị viên mới được phép thực hiện thao tác này", HttpStatus.FORBIDDEN),


    // ========== NHÓM 5xxx: KHUYẾN MÃI & ĐÁNH GIÁ (PROMOTION & REVIEW) ==========
    VOUCHER_NOT_FOUND(5001, "Mã giảm giá không tồn tại", HttpStatus.NOT_FOUND),
    VOUCHER_EXPIRED(5002, "Mã giảm giá đã hết hạn", HttpStatus.BAD_REQUEST),
    VOUCHER_LIMIT_USED(5003, "Đã hết lượt sử dụng mã giảm giá này", HttpStatus.BAD_REQUEST),
    VOUCHER_NOT_APPLICABLE(5004, "Mã giảm giá không áp dụng cho đơn hàng này", HttpStatus.BAD_REQUEST),

    REVIEW_ALREADY_EXISTS(5010, "Bạn đã đánh giá sản phẩm này rồi", HttpStatus.CONFLICT),
    CANNOT_REVIEW_NOT_PURCHASED(5011, "Chỉ có thể đánh giá sản phẩm đã mua", HttpStatus.FORBIDDEN),

    // ========== NHÓM 9xxx: LỖI HỆ THỐNG (SYSTEM) ==========
    DATABASE_ERROR(9001, "Lỗi cơ sở dữ liệu", HttpStatus.INTERNAL_SERVER_ERROR),
    EXTERNAL_SERVICE_ERROR(9002, "Lỗi từ dịch vụ bên ngoài", HttpStatus.INTERNAL_SERVER_ERROR),
    UPDATE_FAILED(9003, "Cập nhật thất bại", HttpStatus.INTERNAL_SERVER_ERROR),
    RESOURCE_CONFLICT(9004, "Xung đột tài nguyên", HttpStatus.CONFLICT),
    UNCATEGORIZED_EXCEPTION(9999, "Lỗi chưa xác định", HttpStatus.INTERNAL_SERVER_ERROR);


    // --- Phần mã boilerplate ---
    private final int code;
    private final String message;
    private final HttpStatusCode httpStatusCode;

    ErrorCode(int code, String message, HttpStatusCode httpStatusCode) {
        this.code = code;
        this.message = message;
        this.httpStatusCode = httpStatusCode;
    }
}