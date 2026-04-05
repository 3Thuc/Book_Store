package com.be.book.BookStorage.dto.Request.Admin;

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
    private String description;
    private Double price;
    private Integer stock;
    private String image;
    private Integer publishedYear;
    private String format;
    private String language;
    private String status;
    private List<Integer> categoryIds;
}
