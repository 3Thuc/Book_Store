package com.be.book.BookStorage.dto.Response.Cart;

import com.be.book.BookStorage.enums.BookFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CartItemRes {
    private Integer bookId;
    private String bookTitle;
    private String authorName;
    private String publisherName;
    private Double price;
    private Integer quantity;
    private Double subtotal;
    private String images;
    private BookFormat format;
    private Double avgRating;
    private Integer stockQuantity;
}
