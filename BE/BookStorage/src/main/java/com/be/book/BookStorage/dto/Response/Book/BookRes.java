package com.be.book.BookStorage.dto.Response.Book;

import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Book.PublisherRes;
import com.be.book.BookStorage.enums.BookFormat;
import com.be.book.BookStorage.enums.Status;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BookRes {
    private Integer bookId;
    private String title;
    private String authorName;
    private Double price;
    private Integer publicationYear;
    private String description;
    private Double avgRating;
    private Integer ratingCount;
    private BookFormat format;
    private String language;
    private Integer stockQuantity;
    private Integer availableQuantity;
    private Status status;
    private String imageUrl;

    private PublisherRes publisher;

    private List<CategoryRes> categories;
}
