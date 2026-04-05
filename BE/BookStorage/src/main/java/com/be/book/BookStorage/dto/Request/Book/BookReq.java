package com.be.book.BookStorage.dto.Request.Book;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BookReq {
    private String title;
    private Integer authorId;
    private Integer publisherId;
    private Integer category;
    private String description;
    private Double price;
    private String image;
    private Integer publishedYear;
    private Integer stock;
    private List<Integer> categoryIds;
}