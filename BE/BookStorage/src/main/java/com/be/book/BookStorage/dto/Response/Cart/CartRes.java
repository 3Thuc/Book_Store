package com.be.book.BookStorage.dto.Response.Cart;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CartRes {
    private Integer cartId;
    private Integer userId;
    private String userName;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private List<CartItemRes> items;
    private Double totalPrice;
    private Integer totalItems;
}
