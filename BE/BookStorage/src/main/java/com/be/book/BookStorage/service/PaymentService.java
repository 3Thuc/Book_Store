package com.be.book.BookStorage.service;

import com.be.book.BookStorage.entity.OrderDetailEntity;
import com.be.book.BookStorage.entity.OrderEntity;
import com.be.book.BookStorage.entity.PaymentEntity;
import com.be.book.BookStorage.enums.Oder.PaymentStatus;
import com.be.book.BookStorage.enums.PaymentResult;
import com.be.book.BookStorage.repository.OrderRepository;
import com.be.book.BookStorage.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final OrderRepository orderRepository;

    @Transactional
    public void savePayment(String orderIdStr, String status) {
        Integer orderId = Integer.parseInt(orderIdStr);

        String safeTransactionId = UUID.randomUUID().toString();

        OrderEntity order = orderRepository.findById(orderId)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderId));

        double subtotal = order.getDetails()
                .stream()
                .mapToDouble(OrderDetailEntity::getTotalPrice)
                .sum();

        double discount = 0.0;
        if (order.getPromo() != null && order.getPromo().getDiscountPercent() != null) {
            discount = subtotal * (order.getPromo().getDiscountPercent() / 100.0);
        }

        double totalAmount = subtotal - discount;

        Optional<PaymentEntity> existingPayment = paymentRepository.findByOrder_OrderId(orderId);

        PaymentEntity payment;
        if (existingPayment.isPresent()) {
            payment = existingPayment.get();
        } else {
            payment = new PaymentEntity();
            payment.setOrder(order);
        }

        payment.setAmount(totalAmount);
        payment.setProvider("PayOS");
        payment.setTransactionId(safeTransactionId);
        payment.setStatus(PaymentResult.valueOf(status));
        payment.setCreatedAt(LocalDateTime.now());

        paymentRepository.save(payment);
    }

}