package com.be.book.BookStorage.dto.Response.Order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentRes {
    private Integer id;
    private Long totalAmount;
    private String status;
    private String paymentMethod;
    private String note;
    private LocalDateTime createdAt;
    private String paymentUrl;
}
