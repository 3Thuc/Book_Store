package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.dto.Request.Order.CreateOrderReq;
import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Order.PaymentRes;
import com.be.book.BookStorage.enums.Oder.PaymentMethod;
import com.be.book.BookStorage.service.OrderService;
import com.be.book.BookStorage.service.PayOSService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderService orderService;
    private final PayOSService payOSService;

    public OrderController(OrderService orderService, PayOSService payOSService) {
        this.orderService = orderService;
        this.payOSService = payOSService;
    }

    @PostMapping
    public ResponseEntity<ApiResponse<PaymentRes>> createOrder(@RequestBody CreateOrderReq request,
                                                               Authentication authentication) {
        String email = authentication.getName();

        log.info("=== CREATE ORDER REQUEST ===");
        log.info("User email: {}", email);
        log.info("AddressId: {}", request.getAddressId());
        log.info("PaymentMethod: {}", request.getPaymentMethod());
        log.info("PromoCode: {}", request.getPromoCode());
        log.info("Note: {}", request.getNote());
        log.info("OrderDetails: {}", request.getOrderDetails());
        if (request.getOrderDetails() != null) {
            request.getOrderDetails().forEach(d ->
                log.info("  -> bookId={}, quantity={}", d.getBookId(), d.getQuantity())
            );
        }
        log.info("===========================");

        PaymentRes res = orderService.createOrder(email, request);

        if (PaymentMethod.CreditCard.name().equalsIgnoreCase(request.getPaymentMethod())) {
            try {
                String paymentUrl = payOSService.createPaymentLink(
                        res.getId(),
                        res.getTotalAmount(),
                        "Thanh toan don hang #" + res.getId()
                );
                res.setPaymentUrl(paymentUrl);
            } catch (Exception e) {
                log.error("Failed to create PayOS payment link: {}", e.getMessage());
                throw new RuntimeException("Không thể tạo link thanh toán: " + e.getMessage());
            }
        }

        ApiResponse<PaymentRes> responseBody = ApiResponse.<PaymentRes>builder()
                .result(res)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
}
