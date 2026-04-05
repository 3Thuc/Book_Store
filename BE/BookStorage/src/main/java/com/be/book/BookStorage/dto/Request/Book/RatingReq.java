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
}
