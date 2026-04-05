package com.be.book.BookStorage.dto.Response.Admin;


import com.be.book.BookStorage.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Stack;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuthorRes {
    private Integer authorId;
    private String authorName;
    private String bio;
    private Status status;
}
