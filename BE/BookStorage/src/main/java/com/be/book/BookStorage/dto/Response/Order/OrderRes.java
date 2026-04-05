package com.be.book.BookStorage.dto.Response.Order;

import com.be.book.BookStorage.enums.Oder.OrderStatus;
import com.be.book.BookStorage.enums.Oder.PaymentMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderRes {
    private String id;
    private String userId;
    private List<OrderItemRes> items;
    private Double totalAmount;
    private LocalDateTime orderDate;
    private OrderStatus status;
    private LocalDateTime deliveryDate;
    private PaymentMethod paymentMethod;
    private String shippingAddress;
    private String customerName;
    private String customerPhone;
    private String note;
    private String promoCode;
    private Boolean isPaid;
}
