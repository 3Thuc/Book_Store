package com.be.book.BookStorage.dto.Request.Auth;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class RegisterReq {
    private String email;
    private String password;
    private String fullName;
}
