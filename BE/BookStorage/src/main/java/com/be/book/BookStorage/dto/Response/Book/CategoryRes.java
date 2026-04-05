package com.be.book.BookStorage.dto.Response.Book;

import com.be.book.BookStorage.enums.Status;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.LocalDateTime;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CategoryRes {
    private Integer categoryId;
    private String categoryName;
    private Status status;
}
