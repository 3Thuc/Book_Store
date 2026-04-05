package com.be.book.BookStorage.dto.Request.User;

import lombok.Data;

@Data
public class PaymentReq {
    private Long amount;
    private String orderInfo;
    private String bankCode;
    private String language;
}