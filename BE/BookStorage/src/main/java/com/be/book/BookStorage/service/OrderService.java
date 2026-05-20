package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Order.CreateOrderReq;
import com.be.book.BookStorage.dto.Request.Order.OrderDetailReq;
import com.be.book.BookStorage.dto.Request.Order.UpdateOrderStatusReq;
import com.be.book.BookStorage.dto.Response.Book.PageRes;
import com.be.book.BookStorage.dto.Response.Order.OrderItemRes;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import com.be.book.BookStorage.dto.Response.Order.PaymentRes;
import com.be.book.BookStorage.entity.*;
import com.be.book.BookStorage.entity.Key.OrderDetailKey;
import com.be.book.BookStorage.enums.Oder.OrderStatus;
import com.be.book.BookStorage.enums.Oder.PaymentMethod;
import com.be.book.BookStorage.enums.Oder.PaymentStatus;
import com.be.book.BookStorage.enums.VoucherStatus;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final BookRepository bookRepository;
    private final AddressRepository addressRepository;
    private final PromotionsRepository promoRepository;
    private final OrderDetailRepository orderDetailRepository;
    private final UserRepository userRepository;
    private final MinioService minioService;
    private final RatingRepository ratingRepository;
    private final NotificationService notificationService;
    private final UserActionService userActionService;
    private final OrderEmailService orderEmailService;

    private UserEntity getCurrentUser(String email) {
        return userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));
    }

    @Transactional
    @CacheEvict(value = {"adminDashboard", "adminOrders"}, allEntries = true)
    public PaymentRes createOrder(String email, CreateOrderReq request) {
        UserEntity user = getCurrentUser(email);

        AddressEntity address = addressRepository.findById(request.getAddressId())
                .orElseThrow(() -> new AppException(ErrorCode.ADDRESS_NOT_FOUND));

        if (!address.getUser().getUserId().equals(user.getUserId())) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        List<OrderDetailEntity> orderDetails = new ArrayList<>();
        Double totalAmount = 0.0;

        for (OrderDetailReq detailReq : request.getOrderDetails()) {
            BookEntity book = bookRepository.findById(detailReq.getBookId())
                    .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

            Integer availableQuantity = bookRepository.getAvailableQuantity(detailReq.getBookId());
            if (availableQuantity == null) {
                availableQuantity = book.getStockQuantity();
            }

            if (availableQuantity < detailReq.getQuantity()) {
                throw new AppException(ErrorCode.INSUFFICIENT_STOCK);
            }

            Double unitPrice = book.getPrice();
            Double itemTotal = unitPrice * detailReq.getQuantity();
            totalAmount += itemTotal;

            OrderDetailEntity detail = new OrderDetailEntity();
            OrderDetailKey key = new OrderDetailKey();
            key.setBookId(book.getBookId());
            detail.setId(key);

            detail.setBook(book);
            detail.setBookName(book.getTitle());
            detail.setQuantity(detailReq.getQuantity());
            detail.setUnitPrice(unitPrice);
            detail.setTotalPrice(itemTotal);

            orderDetails.add(detail);
        }

        PromotionEntity promo = null;
        Double discountAmount = 0.0;

        if (request.getPromoCode() != null) {
            promo = promoRepository.findByCode(request.getPromoCode())
                    .orElseThrow(() -> new AppException(ErrorCode.VOUCHER_NOT_FOUND));

            if (promo.getStatus() != VoucherStatus.active) {
                throw new AppException(ErrorCode.VOUCHER_EXPIRED);
            }

            if (promo.getIsDeleted()) {
                throw new AppException(ErrorCode.PROMOTION_ALREADY_DELETED);
            }

            LocalDateTime now = LocalDateTime.now();
            if (promo.getStartDate() != null && now.toLocalDate().isBefore(promo.getStartDate())) {
                throw new AppException(ErrorCode.CANNOT_ACTIVATE_FUTURE_PROMOTION);
            }

            if (promo.getEndDate() != null && now.toLocalDate().isAfter(promo.getEndDate())) {
                throw new AppException(ErrorCode.VOUCHER_EXPIRED);
            }

            if (promo.getDiscountPercent() != null) {
                discountAmount = totalAmount * (promo.getDiscountPercent() / 100.0);
            }
        }

        Double finalAmount = totalAmount - discountAmount;

        OrderEntity order = new OrderEntity();
        order.setUser(user);
        order.setAddress(address);
        order.setPromo(promo);
        order.setOrderDate(LocalDateTime.now());
        order.setStatus(OrderStatus.pending);
        try {
            order.setPaymentMethod(PaymentMethod.valueOf(request.getPaymentMethod()));
        } catch (IllegalArgumentException e) {
            log.error("Invalid paymentMethod value received: '{}'", request.getPaymentMethod());
            throw new AppException(ErrorCode.INVALID_KEY);
        }
        order.setPaymentStatus(PaymentStatus.unpaid);
        order.setNote(request.getNote());
        order.setCreatedAt(LocalDateTime.now());
        order.setUpdatedAt(LocalDateTime.now());
        order.setSubtotal(totalAmount);
        order.setDiscountAmount(discountAmount);
        order.setTotalAmount(finalAmount);

        OrderEntity savedOrder = orderRepository.save(order);
        log.info("Order created with ID: {}", savedOrder.getOrderId());

        for (OrderDetailEntity detail : orderDetails) {
            detail.setOrder(savedOrder);
        }
        orderDetailRepository.saveAll(orderDetails);

        for (OrderDetailEntity detail : orderDetails) {
            try {
                userActionService.logPurchase(
                    user.getEmail(), 
                    detail.getBook().getBookId(),
                    detail.getQuantity(),
                    detail.getTotalPrice()
                );
            } catch (Exception e) {
                log.error("Failed to log purchase action: {}", e.getMessage());
            }
        }

        try {
            String userName = user.getUsername() != null ? user.getFullName() : user.getEmail();
            notificationService.sendNewOrderNotification(
                savedOrder.getOrderId().toString(), 
                userName
            );
        } catch (Exception e) {
            log.error("Failed to send notification for new order: {}", e.getMessage());
        }

        return mapToPaymentRes(savedOrder, totalAmount, discountAmount, finalAmount);
    }

    private PaymentRes mapToPaymentRes(OrderEntity order, Double subtotal,
                                       Double discount, Double total) {
        PaymentRes response = new PaymentRes();
        response.setId(order.getOrderId());
        response.setTotalAmount(total.longValue());
        response.setStatus(order.getStatus().name());
        response.setPaymentMethod(order.getPaymentMethod().name());
        response.setNote(order.getNote());
        response.setCreatedAt(order.getCreatedAt());
        return response;
    }

    @Transactional
    @CacheEvict(value = {"adminDashboard", "adminOrders"}, allEntries = true)
    public void updateOrderStatus(String orderCode, String status) {
        OrderEntity order = orderRepository.findById(Integer.parseInt(orderCode))
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderCode));

        // Dùng equalsIgnoreCase để tránh bug case mismatch ("cancelled" vs "CANCELLED")
        if ("PAID".equalsIgnoreCase(status)) {
            order.setPaymentStatus(PaymentStatus.paid);
            order.setStatus(OrderStatus.processing);
        } else if ("CANCELLED".equalsIgnoreCase(status)) {
            order.setPaymentStatus(PaymentStatus.unpaid);
            order.setStatus(OrderStatus.cancelled);
        } else if ("FAILED".equalsIgnoreCase(status)) {
            order.setPaymentStatus(PaymentStatus.unpaid);
            order.setStatus(OrderStatus.failed);
        }

        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    @Cacheable(value = "adminOrders", key = "#page + '-' + #size + '-' + #status + '-' + #search")
    public PageRes<OrderRes> getAllOrders(int page, int size, String status, String search) {
        Pageable pageable = PageRequest.of(page - 1, size); // ORDER BY \u0111\u00e3 trong query

        // B\u01b0\u1edbc 1: Lay c\u00e1c order ID m\u00e0 kh\u00f4ng JOIN collection \u2014 DB-level LIMIT/OFFSET
        org.springframework.data.domain.Page<Integer> idsPage =
                orderRepository.findOrderIdsPaged(
                        (status != null && !status.isBlank()) ? status : null,
                        (search != null && !search.isBlank()) ? search : null,
                        pageable
                );

        if (idsPage.isEmpty()) {
            return new PageRes<>(List.of(), page, size, 0L, 0);
        }

        // B\u01b0\u1edbc 2: Fetch \u0111\u1ea7y \u0111\u1ee7 chi ti\u1ebft ch\u1ec9 cho nh\u1eefng ID v\u1eeba l\u1ea5y
        List<Integer> ids = idsPage.getContent();
        List<OrderEntity> orderEntities = orderRepository.findByOrderIdsWithDetails(ids);

        // Cache reviewed book IDs per user (kh\u00f4ng c\u00f2n N+1 gi\u1eefa c\u00e1c \u0111\u01a1n h\u00e0ng)
        Map<Integer, Set<Integer>> userReviewedBooks = new HashMap<>();

        // Gi\u1eef \u0111\u00fang th\u1ee9 t\u1ef1 theo orderDate DESC (order c\u1ee7a ids t\u1eeb b\u01b0\u1edbc 1)
        Map<Integer, OrderEntity> orderMap = orderEntities.stream()
                .collect(Collectors.toMap(OrderEntity::getOrderId, e -> e));

        List<OrderRes> orders = ids.stream()
                .map(orderMap::get)
                .filter(java.util.Objects::nonNull)
                .map(order -> {
                    List<OrderDetailEntity> orderDetails = order.getDetails();
                    Double subtotal = orderDetails.stream()
                            .mapToDouble(OrderDetailEntity::getTotalPrice).sum();
                    Double discount = 0.0;
                    if (order.getPromo() != null && order.getPromo().getDiscountPercent() != null) {
                        discount = subtotal * (order.getPromo().getDiscountPercent() / 100.0);
                    }
                    Double total = subtotal - discount;

                    Integer userId = order.getUser() != null ? order.getUser().getUserId() : null;
                    Set<Integer> reviewedBookIds = userId != null
                            ? userReviewedBooks.computeIfAbsent(
                                    userId,
                                    id -> new HashSet<>(ratingRepository.findReviewedBookIds(id))
                            ) : new HashSet<>();

                    return mapToFullOrderRes(order, orderDetails, subtotal, discount, total, reviewedBookIds);
                })
                .collect(Collectors.toList());

        return new PageRes<>(
                orders,
                idsPage.getNumber() + 1,
                idsPage.getSize(),
                idsPage.getTotalElements(),
                idsPage.getTotalPages()
        );
    }

    /**
     * Load toàn bộ orders cho dashboard — KHÔNG sinh presigned URL cho item images.
     * Dashboard chỉ cần: totalAmount, status, orderDate, items.bookId, items.quantity.
     * Nhanh hơn getAllOrders ~10-30x với dataset lớn (tránh O(N*M) MinIO calls).
     * @Cacheable: lần 2 trở đi trả tức thì từ RAM (~50ms).
     */
    @Cacheable(value = "adminDashboard", key = "'ordersSlim'")
    @Transactional(readOnly = true)
    public PageRes<OrderRes> getAllOrdersSlim(int page, int size) {
        Pageable pageable = PageRequest.of(page - 1, size, Sort.by("orderDate").descending());
        Page<OrderEntity> orderPage = orderRepository.findAllWithDetailsFiltered(null, null, pageable);

        List<OrderRes> orders = orderPage.getContent().stream()
                .map(order -> {
                    List<OrderDetailEntity> orderDetails = order.getDetails();

                    Double subtotal = orderDetails.stream()
                            .mapToDouble(OrderDetailEntity::getTotalPrice)
                            .sum();
                    Double discount = 0.0;
                    if (order.getPromo() != null && order.getPromo().getDiscountPercent() != null) {
                        discount = subtotal * (order.getPromo().getDiscountPercent() / 100.0);
                    }
                    Double total = subtotal - discount;

                    // Slim items: chỉ cần bookId + quantity cho stats, KHÔNG cần imageUrl
                    List<com.be.book.BookStorage.dto.Response.Order.OrderItemRes> items = orderDetails.stream()
                            .map(detail -> com.be.book.BookStorage.dto.Response.Order.OrderItemRes.builder()
                                    .id(detail.getId().getOrderId() + "-" + detail.getId().getBookId())
                                    .bookId(String.valueOf(detail.getBook().getBookId()))
                                    .title(detail.getBook() != null ? detail.getBook().getTitle() : "")
                                    .quantity(detail.getQuantity())
                                    .price(detail.getTotalPrice())
                                    .imageUrl(null) // intentionally omitted for speed
                                    .isReviewed(false)
                                    .build())
                            .collect(Collectors.toList());

                    AddressEntity address = order.getAddress();
                    return OrderRes.builder()
                            .id(String.valueOf(order.getOrderId()))
                            .userId(order.getUser() != null ? String.valueOf(order.getUser().getUserId()) : null)
                            .items(items)
                            .totalAmount(total)
                            .orderDate(order.getOrderDate())
                            .status(order.getStatus())
                            .deliveryDate(order.getStatus() == OrderStatus.delivered ? order.getUpdatedAt() : (order.getOrderDate() != null ? order.getOrderDate().plusDays(3) : LocalDateTime.now().plusDays(3)))
                            .paymentMethod(order.getPaymentMethod())
                            .shippingAddress(address != null ? address.getAddressText() : null)
                            .customerName((address != null && address.getRecipientName() != null)
                                    ? address.getRecipientName()
                                    : (order.getUser() != null ? order.getUser().getFullName() : null))
                            .customerPhone((address != null && address.getRecipientPhone() != null)
                                    ? address.getRecipientPhone()
                                    : (order.getUser() != null ? order.getUser().getPhone() : null))
                            .isPaid(order.getPaymentStatus() == com.be.book.BookStorage.enums.Oder.PaymentStatus.paid)
                            .build();
                })
                .collect(Collectors.toList());

        return new PageRes<>(
                orders,
                orderPage.getNumber() + 1,
                orderPage.getSize(),
                orderPage.getTotalElements(),
                orderPage.getTotalPages()
        );
    }


    @Transactional(readOnly = true)
    public com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes getOrderStatistics() {
        return com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes.builder()
                .totalOrders(orderRepository.countTotalOrders())
                .pendingOrders(orderRepository.countPendingOrders())
                .processingOrders(orderRepository.countProcessingOrders())
                .shippedOrders(orderRepository.countShippedOrders())
                .deliveredOrders(orderRepository.countDeliveredOrders())
                .cancelRequestedOrders(orderRepository.countCancelRequestedOrders())
                .cancelledOrders(orderRepository.countCancelledOrders())
                .returnRequestedOrders(orderRepository.countReturnRequestedOrders())
                .returnedOrders(orderRepository.countReturnedOrders())
                .failedOrders(orderRepository.countFailedOrders())
                .totalRevenue(orderRepository.calculateTotalRevenue())
                .build();
    }

    private OrderRes mapToFullOrderRes(
            OrderEntity order,
            List<OrderDetailEntity> orderDetails,
            Double subtotal,
            Double discount,
            Double total,
            Set<Integer> reviewedBookIds
    ) {
        List<OrderItemRes> items = orderDetails.stream()
                .map(detail -> {
                    boolean isReviewed = reviewedBookIds.contains(detail.getBook().getBookId());

                    String rawImage = detail.getBook().getImage() != null
                            ? detail.getBook().getImage().getImageUrl()
                            : null;

                    // D\u00f9ng proxy path thay v\u00ec presigned URL \u2192 0 MinIO SDK calls
                    // FE proxy /minio/bookstore/{path} serve tr\u1ef1c ti\u1ebfp t\u1eeb MinIO public bucket
                    String imageUrlFinal = rawImage != null && !rawImage.isEmpty()
                            ? "/minio/bookstore/" + rawImage.split(",")[0].trim()
                            : null;

                    String authorName = detail.getBook().getAuthor() != null
                            ? detail.getBook().getAuthor().getAuthorName()
                            : "Unknown";

                    return OrderItemRes.builder()
                            .id(detail.getId().getOrderId() + "-" + detail.getId().getBookId())
                            .bookId(String.valueOf(detail.getBook().getBookId()))
                            .title(detail.getBook().getTitle())
                            .author(authorName)
                            .price(detail.getTotalPrice())
                            .quantity(detail.getQuantity())
                            .imageUrl(imageUrlFinal)
                            .isReviewed(isReviewed)
                            .build();
                })
                .collect(Collectors.toList());

        AddressEntity address = order.getAddress();
        String shippingAddress = formatAddress(address);

        // ưu tiên recipientName của địa chỉ; fallback về tên user nếu address không có
        String customerName = (address != null && address.getRecipientName() != null)
                ? address.getRecipientName()
                : (order.getUser() != null ? order.getUser().getFullName() : null);
        String customerPhone = (address != null && address.getRecipientPhone() != null)
                ? address.getRecipientPhone()
                : (order.getUser() != null ? order.getUser().getPhone() : null);

        boolean isPaid = order.getPaymentStatus() == PaymentStatus.paid;

        return OrderRes.builder()
                .id(String.valueOf(order.getOrderId()))
                .userId(order.getUser() != null ? String.valueOf(order.getUser().getUserId()) : null)
                .items(items)
                .totalAmount(total)
                .orderDate(order.getOrderDate())
                .status(order.getStatus())
                .deliveryDate(order.getStatus() == OrderStatus.delivered ? order.getUpdatedAt() : (order.getOrderDate() != null ? order.getOrderDate().plusDays(3) : LocalDateTime.now().plusDays(3)))
                .paymentMethod(order.getPaymentMethod())
                .shippingAddress(shippingAddress)
                .customerName(customerName)
                .customerPhone(customerPhone)
                .note(order.getNote())
                .promoCode(order.getPromo() != null ? order.getPromo().getCode() : null)
                .isPaid(isPaid)
                .build();
    }

    private String formatAddress(AddressEntity address) {
        if (address == null) return null;

        StringBuilder sb = new StringBuilder();

        if (address.getAddressText() != null) {
            sb.append(address.getAddressText());
        }

        if (address.getDistrict() != null) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(address.getDistrict());
        }

        if (address.getCity() != null) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(address.getCity());
        }

        return sb.toString();
    }

    @Transactional
    @CacheEvict(value = {"adminDashboard", "adminOrders"}, allEntries = true)
    public OrderRes cancelOrder(String email, Integer orderId) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new AppException(ErrorCode.ORDER_NOT_FOUND));

        if (!order.getUser().getUserId().equals(user.getUserId())) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        if (!(order.getStatus() == OrderStatus.pending ||
                order.getStatus() == OrderStatus.processing)) {
            throw new AppException(ErrorCode.INVALID_ORDER_STATUS);
        }

        order.setStatus(OrderStatus.cancel_requested);
        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);

        notificationService.sendOrderCancelledNotification(String.valueOf(orderId), user.getFullName());

        return buildOrderResponse(order, user.getUserId());
    }

    @Transactional
    @CacheEvict(value = {"adminDashboard", "adminOrders"}, allEntries = true)
    public OrderRes returnOrder(String email, Integer orderId, String reason) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new AppException(ErrorCode.ORDER_NOT_FOUND));

        if (!order.getUser().getUserId().equals(user.getUserId())) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        if (order.getStatus() != OrderStatus.delivered
                && order.getStatus() != OrderStatus.shipped) {
            throw new AppException(ErrorCode.INVALID_ORDER_STATUS);
        }

        // If order was already delivered, restore stock because customer returns delivered items
        if (order.getStatus() == OrderStatus.delivered) {
            restoreStockForOrder(order);
        }

        order.setStatus(OrderStatus.return_requested);
        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);

        // Send notification to admin about return request
        notificationService.sendReturnRequestedNotification(String.valueOf(orderId), user.getFullName(), reason);

        return buildOrderResponse(order, user.getUserId());
    }

    @Transactional
    @CacheEvict(value = {"adminDashboard", "adminOrders"}, allEntries = true)
    public OrderRes confirmDelivery(String email, Integer orderId) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new AppException(ErrorCode.ORDER_NOT_FOUND));

        if (!order.getUser().getUserId().equals(user.getUserId())) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        if (order.getStatus() != OrderStatus.shipped) {
            throw new AppException(ErrorCode.INVALID_ORDER_STATUS);
        }

        subtractStockForOrder(order);

        order.setStatus(OrderStatus.delivered);
        order.setPaymentStatus(PaymentStatus.paid);
        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);

        // Send notification to admin about delivery completion
        notificationService.sendDeliveryCompletedNotification(String.valueOf(orderId));

        return buildOrderResponse(order, user.getUserId());
    }

    // Helper method để tái sử dụng logic build response
    private OrderRes buildOrderResponse(OrderEntity order, Integer userId) {
        List<OrderDetailEntity> orderDetails = orderDetailRepository.findByOrderOrderId(order.getOrderId());

        Double subtotal = orderDetails.stream()
                .mapToDouble(OrderDetailEntity::getTotalPrice)
                .sum();

        Double discount = 0.0;
        if (order.getPromo() != null && order.getPromo().getDiscountPercent() != null) {
            discount = subtotal * (order.getPromo().getDiscountPercent() / 100.0);
        }

        Double total = subtotal - discount;

        Set<Integer> reviewedBookIds = new HashSet<>(ratingRepository.findReviewedBookIds(userId));

        return mapToFullOrderRes(order, orderDetails, subtotal, discount, total, reviewedBookIds);
    }

    // OPTIMIZED: Return PaymentRes thay vì OrderRes khi không cần full details
    @Transactional
    @CacheEvict(value = {"adminDashboard", "adminOrders"}, allEntries = true)
    public PaymentRes updateAdminOrderStatus(String email, Integer orderId, UpdateOrderStatusReq request) {
        userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new AppException(ErrorCode.ORDER_NOT_FOUND));

        OrderStatus newStatus;
        try {
            newStatus = OrderStatus.valueOf(request.getStatus().toLowerCase());
        } catch (IllegalArgumentException e) {
            throw new AppException(ErrorCode.INVALID_ORDER_STATUS);
        }

        order.setStatus(newStatus);
        order.setUpdatedAt(LocalDateTime.now());

        if (newStatus == OrderStatus.delivered) {
            order.setPaymentStatus(PaymentStatus.paid);
        } else if (newStatus == OrderStatus.cancelled || newStatus == OrderStatus.failed) {
            order.setPaymentStatus(PaymentStatus.unpaid);
        } else if (newStatus == OrderStatus.processing && order.getPaymentMethod() == PaymentMethod.CreditCard) {
            order.setPaymentStatus(PaymentStatus.paid);
        }

        orderRepository.save(order);

        // Gửi email thông báo cho khách hàng (async — không block response)
        try {
            String customerEmail = order.getUser() != null ? order.getUser().getEmail() : null;
            String customerName  = order.getUser() != null ? order.getUser().getFullName() : null;
            String formattedTotal = NumberFormat.getNumberInstance(new Locale("vi", "VN"))
                    .format(order.getTotalAmount().longValue()) + " đ";
            orderEmailService.sendOrderStatusUpdateEmail(
                    customerEmail, customerName, orderId, newStatus, formattedTotal
            );
        } catch (Exception e) {
            // Lỗi email không ảnh hưởng đến kết quả API
            log.error("Không thể gửi email cập nhật trạng thái cho đơn #{}: {}", orderId, e.getMessage());
        }

        PaymentRes response = new PaymentRes();
        response.setId(order.getOrderId());
        response.setStatus(order.getStatus().name());
        response.setPaymentMethod(order.getPaymentMethod().name());
        response.setNote(order.getNote());
        response.setCreatedAt(order.getCreatedAt());
        response.setTotalAmount(0L);

        return response;
    }


    private void subtractStockForOrder(OrderEntity order) {
        List<OrderDetailEntity> details = orderDetailRepository.findByOrderOrderId(order.getOrderId());
        for (OrderDetailEntity d : details) {
            BookEntity book = bookRepository.findById(d.getBook().getBookId())
                    .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

            int newStock = book.getStockQuantity() - d.getQuantity();
            if (newStock < 0) {
                log.warn("Stock for book ID {} went below 0 during confirmDelivery. Setting to 0.", book.getBookId());
                newStock = 0;
            }

            book.setStockQuantity(newStock);
            book.setUpdatedAt(LocalDateTime.now());

            // AUTO-DEACTIVATE: Nếu hết hàng thì ẩn khỏi catalog (status → inactive)
            if (newStock == 0 && book.getStatus() == com.be.book.BookStorage.enums.Status.active) {
                book.setStatus(com.be.book.BookStorage.enums.Status.inactive);
                log.info("Book ID {} is now out of stock → status set to inactive.", book.getBookId());
            }

            bookRepository.save(book);
        }
    }

    private void restoreStockForOrder(OrderEntity order) {
        List<OrderDetailEntity> details = orderDetailRepository.findByOrderOrderId(order.getOrderId());
        for (OrderDetailEntity d : details) {
            BookEntity book = bookRepository.findById(d.getBook().getBookId())
                    .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

            int restoredStock = book.getStockQuantity() + d.getQuantity();
            book.setStockQuantity(restoredStock);
            book.setUpdatedAt(LocalDateTime.now());

            // AUTO-REACTIVATE: Nếu trước đó bị inactive do hết hàng → tự bật lại
            if (restoredStock > 0 && book.getStatus() == com.be.book.BookStorage.enums.Status.inactive) {
                book.setStatus(com.be.book.BookStorage.enums.Status.active);
                log.info("Book ID {} stock restored to {} → status set back to active.", book.getBookId(), restoredStock);
            }

            bookRepository.save(book);
        }
    }
}