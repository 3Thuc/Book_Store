package com.be.book.BookStorage.dto.Request.Order;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderReq {
    private Integer addressId;
    private String  promoCode;
    private String paymentMethod;
    private String note;
    private List<OrderDetailReq> orderDetails;
}
