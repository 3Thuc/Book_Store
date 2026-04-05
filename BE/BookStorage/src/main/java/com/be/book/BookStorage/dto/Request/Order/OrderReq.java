package com.be.book.BookStorage.dto.Request.Order;

import com.be.book.BookStorage.enums.Oder.PaymentStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderReq {
    private PaymentStatus paymentStatus;
}
