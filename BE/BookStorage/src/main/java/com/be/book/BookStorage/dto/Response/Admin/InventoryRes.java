package com.be.book.BookStorage.dto.Response.Admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryRes {
    private Integer bookId;
    private String title;
    private Integer stockQuantity;
    private Integer orderedQuantity;
    private Integer availableQuantity;
    private Integer threshold;
    private String status;
    private String imageUrl;    // proxy path: /minio/bookstore/covers/books/...
}