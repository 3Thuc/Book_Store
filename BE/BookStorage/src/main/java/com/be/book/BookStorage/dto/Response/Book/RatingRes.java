package com.be.book.BookStorage.dto.Response.Book;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Builder
@Data
@AllArgsConstructor
public class RatingRes {
    private Integer ratingId;
    private Integer userId;
    private String userName;
    private Integer bookId;
    private Integer rating;
    private String review;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
