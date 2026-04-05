package com.be.book.BookStorage.dto.Request.Admin;

import com.be.book.BookStorage.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuthorReq {
    private  String authorName;
    private  String bio;
    private Status status;

}
