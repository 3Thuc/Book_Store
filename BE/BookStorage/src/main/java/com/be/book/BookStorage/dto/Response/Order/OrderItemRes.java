package com.be.book.BookStorage.dto.Response.Order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderItemRes {
    private String id;
    private String bookId;
    private String title;
    private String author;
    private Double price;
    private Integer quantity;
    private String imageUrl;
    private Boolean isReviewed;
}