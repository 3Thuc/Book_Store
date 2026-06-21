package com.be.book.BookStorage.dto.Request.Book;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RatingReq {
    private Integer rating;
    private String review;
    // Accept as String to handle both Integer and String formats from FE
    // Will be converted to Integer in service layer
    private String orderId;
}
