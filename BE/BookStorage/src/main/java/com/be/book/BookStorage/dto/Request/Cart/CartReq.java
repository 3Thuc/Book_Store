package com.be.book.BookStorage.dto.Request.Cart;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CartReq {
    private Integer bookId;
    private Integer quantity;
}
