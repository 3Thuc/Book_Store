package com.be.book.BookStorage.dto.Request.User;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryReq {
    private String orderId;
    private String transDate;
    private String ipAddr;
}
