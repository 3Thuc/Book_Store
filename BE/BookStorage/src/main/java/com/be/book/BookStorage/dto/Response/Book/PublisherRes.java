package com.be.book.BookStorage.dto.Response.Book;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PublisherRes {
    private Integer publisherId;
    private String publisherName;
}
