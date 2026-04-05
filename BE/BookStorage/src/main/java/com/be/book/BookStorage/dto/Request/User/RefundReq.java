package com.be.book.BookStorage.dto.Request.User;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RefundReq {
    private String orderId;
    private long amount;
    private String transDate;
    private String user;
    private String tranType;
}
