package com.be.book.BookStorage.controller;


import com.be.book.BookStorage.dto.Request.Order.CreateOrderReq;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Order.PaymentRes;
import com.be.book.BookStorage.enums.Oder.PaymentMethod;
import com.be.book.BookStorage.service.OrderService;
import com.be.book.BookStorage.service.PayOSService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final PayOSService payOSService;
    @PostMapping
    public ResponseEntity<ApiResponse<PaymentRes>> createOrder(@RequestBody CreateOrderReq request,
                                                               Authentication authentication) {
        String email = authentication.getName();

        PaymentRes res = orderService.createOrder(email, request);

        if(PaymentMethod.CreditCard.name().equalsIgnoreCase(request.getPaymentMethod())) {
            try {
                String paymentUrl = payOSService.createPaymentLink(
                        res.getId(),
                        res.getTotalAmount(),
                        "Thanh toan don hang #" + res.getId()
                );
                res.setPaymentUrl(paymentUrl);
            } catch (Exception e) {
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

