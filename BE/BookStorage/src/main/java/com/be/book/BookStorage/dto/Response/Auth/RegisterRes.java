package com.be.book.BookStorage.dto.Response.Auth;

import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
public class RegisterRes {
    private String token;
    private Integer id;
    private String username;
    private String fullName;
    private String email;
    private Status status;
    private Role role;
}
