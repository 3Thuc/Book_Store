package com.be.book.BookStorage.dto.Response.Admin;


import com.be.book.BookStorage.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PublishersRes {
    private  Integer publisherId;
    private  String publisherName;
    private Status status;
}
