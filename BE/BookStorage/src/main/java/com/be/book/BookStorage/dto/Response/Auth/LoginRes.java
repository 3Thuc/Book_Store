package com.be.book.BookStorage.dto.Response.Auth;

import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.springframework.http.ResponseCookie;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
public class LoginRes {
    private String token;
    private Integer id;
    private String userName;
    private String fullName;
    private String email;
    private Status status;
    private String phoneNumber;
    private Role role;

    @JsonIgnore
    private ResponseCookie refreshCookie;
}
