package com.be.book.BookStorage.dto.Request.Order;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderDetailReq {
    private Integer bookId;
    private Integer quantity;
}
