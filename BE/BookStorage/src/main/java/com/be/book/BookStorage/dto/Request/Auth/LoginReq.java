package com.be.book.BookStorage.dto.Request.Auth;

import lombok.Data;

@Data
public class LoginReq {
    private String email;
    private String password;
}
