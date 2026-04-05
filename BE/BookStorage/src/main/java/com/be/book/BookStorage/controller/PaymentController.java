package com.be.book.BookStorage.controller;



import com.be.book.BookStorage.service.OrderService;
import com.be.book.BookStorage.service.PayOSService;
import com.be.book.BookStorage.service.PaymentService;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;


@RestController
@RequestMapping("/api/payment")
@RequiredArgsConstructor
public class PaymentController {

    private final OrderService orderService;
    private final PayOSService payOSService;
    private final PaymentService paymentService;
    /**
     * PayOS redirect về đây sau khi thanh toán THÀNH CÔNG
     * URL: http://localhost:8080/api/payment/return?orderCode=123&status=PAID&signature=xxx
     */
    @GetMapping("/return")
    public void handlePaymentReturn(@RequestParam Map<String, String> params,
                                    HttpServletResponse response) throws IOException {
        try {
            String orderCode = params.get("orderCode");
            String status = params.get("status");


            if ("PAID".equalsIgnoreCase(status)) {
                paymentService.savePayment(orderCode, "success");

                orderService.updateOrderStatus(orderCode, "PAID");

                response.sendRedirect("http://localhost:3000/payment/return?orderId=" + orderCode + "&status=paid");
            } else {
                paymentService.savePayment(orderCode, "failed");
                orderService.updateOrderStatus(orderCode, "FAILED");
                response.sendRedirect("http://localhost:3000/payment/failed?orderId=" + orderCode);
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.sendRedirect("http://localhost:3000/payment/error");
        }
    }

    /**
     * PayOS redirect về đây khi user HỦY thanh toán
     * URL: http://localhost:8080/api/payment/cancel?orderCode=123
     */
    @GetMapping("/cancel")
    public void handlePaymentCancel(@RequestParam String orderCode,
                                    HttpServletResponse response) throws IOException {
        try {
            // Cập nhật trạng thái order thành CANCELLED
            orderService.updateOrderStatus(orderCode, "cancelled");

            // Redirect về frontend cancel page
            response.sendRedirect("http://localhost:3000/payment/cancel?orderId=" + orderCode);

        } catch (Exception e) {
            e.printStackTrace();
            response.sendRedirect("http://localhost:3000/payment/error");
        }
    }

    /**
     * Webhook từ PayOS (optional - để cập nhật real-time)
     * PayOS sẽ POST về endpoint này khi có thay đổi trạng thái thanh toán
     */
    @PostMapping("/webhook")
    public ResponseEntity<Map<String, Object>> handleWebhook(@RequestBody Map<String, Object> payload) {
        try {
            // Verify webhook signature
            boolean isValid = payOSService.verifyWebhook(payload);

            if (isValid) {
                String orderCode = payload.get("orderCode").toString();
                String status = payload.get("status").toString();

                // Cập nhật order status
                orderService.updateOrderStatus(orderCode, status);

                return ResponseEntity.ok(Map.of("success", true));
            }

            return ResponseEntity.badRequest()
                    .body(Map.of("success", false, "message", "Invalid signature"));

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }
}