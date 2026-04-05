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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

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

    private UserEntity getCurrentUser(String email) {
        return userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));
    }

    @Transactional
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
        order.setPaymentMethod(PaymentMethod.valueOf(request.getPaymentMethod()));
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
    public void updateOrderStatus(String orderCode, String status) {
        OrderEntity order = orderRepository.findById(Integer.parseInt(orderCode))
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderCode));

        if ("PAID".equals(status)) {
            order.setPaymentStatus(PaymentStatus.paid);
            order.setStatus(OrderStatus.processing);
        } else if ("CANCELLED".equals(status)) {
            order.setPaymentStatus(PaymentStatus.unpaid);
            order.setStatus(OrderStatus.cancelled);
        } else if ("FAILED".equals(status)) {
            order.setPaymentStatus(PaymentStatus.unpaid);
            order.setStatus(OrderStatus.failed);
        }

        order.setUpdatedAt(LocalDateTime.now());
        orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public PageRes<OrderRes> getAllOrders(int page, int size, String status, String search) {
        Pageable pageable = PageRequest.of(page - 1, size, Sort.by("orderDate").descending());
        Page<OrderEntity> orderPage = orderRepository.findAllWithDetailsFiltered(status, search, pageable);

        // CRITICAL FIX: Fetch reviewed book IDs for ALL users at once
        Map<Integer, Set<Integer>> userReviewedBooks = new HashMap<>();

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

                    // Cache reviewed books per user - handle null user
                    Integer userId = order.getUser() != null ? order.getUser().getUserId() : null;
                    Set<Integer> reviewedBookIds = userId != null
                            ? userReviewedBooks.computeIfAbsent(
                                    userId,
                                    id -> new HashSet<>(ratingRepository.findReviewedBookIds(id))
                            )
                            : new HashSet<>();

                    return mapToFullOrderRes(order, orderDetails, subtotal, discount, total, reviewedBookIds);
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
        Map<String, String> presignCache = new HashMap<>();

        List<OrderItemRes> items = orderDetails.stream()
                .map(detail -> {
                    boolean isReviewed = reviewedBookIds.contains(detail.getBook().getBookId());

                    String rawImage = detail.getBook().getImage() != null
                            ? detail.getBook().getImage().getImageUrl()
                            : null;

                    String imageUrl = (rawImage != null && !rawImage.isEmpty())
                            ? rawImage.split(",")[0].trim()
                            : null;

                    String imageUrlPresign = null;
                    if (imageUrl != null) {
                        imageUrlPresign = presignCache.computeIfAbsent(
                                imageUrl,
                                url -> {
                                    try {
                                        return minioService.getPresignedUrl(url);
                                    } catch (Exception e) {
                                        return null;
                                    }
                                }
                        );
                    }

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
                            .imageUrl(imageUrlPresign)
                            .isReviewed(isReviewed)
                            .build();
                })
                .collect(Collectors.toList());

        AddressEntity address = order.getAddress();
        String shippingAddress = formatAddress(address);

        String customerName = address != null ? address.getRecipientName() : null;
        String customerPhone = address != null ? address.getRecipientPhone() : null;

        boolean isPaid = order.getPaymentStatus() == PaymentStatus.paid;

        return OrderRes.builder()
                .id(String.valueOf(order.getOrderId()))
                .userId(order.getUser() != null ? String.valueOf(order.getUser().getUserId()) : null)
                .items(items)
                .totalAmount(total)
                .orderDate(order.getOrderDate())
                .status(order.getStatus())
                .deliveryDate(order.getUpdatedAt())
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
                throw new AppException(ErrorCode.INSUFFICIENT_STOCK);
            }

            book.setStockQuantity(newStock);
            book.setUpdatedAt(LocalDateTime.now());
            bookRepository.save(book);
        }
    }

    private void restoreStockForOrder(OrderEntity order) {
        List<OrderDetailEntity> details = orderDetailRepository.findByOrderOrderId(order.getOrderId());
        for (OrderDetailEntity d : details) {
            BookEntity book = bookRepository.findById(d.getBook().getBookId())
                    .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

            book.setStockQuantity(book.getStockQuantity() + d.getQuantity());
            book.setUpdatedAt(LocalDateTime.now());
            bookRepository.save(book);
        }
    }
}