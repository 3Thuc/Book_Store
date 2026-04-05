package com.be.book.BookStorage.dto.Response.Admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class InventoryRes {
    private Integer bookId;
    private String title;
    private Integer stockQuantity;
    private Integer orderedQuantity;      // Changed back to Integer
    private Integer availableQuantity;    // Changed back to Integer
    private Integer threshold;
    private String status;
}