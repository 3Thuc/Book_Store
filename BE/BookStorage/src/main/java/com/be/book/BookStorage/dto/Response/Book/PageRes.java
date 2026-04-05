package com.be.book.BookStorage.dto.Response.Book;
import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;

import java.util.List;

@Builder
@Data
@AllArgsConstructor
public class PageRes<T> {
    private List<T> books;
    private int page;
    private int size;
    private long totalElements;
    private int totalPages;
}
