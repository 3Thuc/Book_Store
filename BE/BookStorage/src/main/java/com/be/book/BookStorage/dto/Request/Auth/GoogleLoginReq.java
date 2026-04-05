package com.be.book.BookStorage.dto.Request.Auth;

import lombok.Data;

@Data
public class GoogleLoginReq {
    private String code;
}
