package com.be.book.BookStorage.dto.Response.User;

import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
public class UserRes {
    private Integer id;
    private String fullName;
    private String phoneNumber;
    private String userName;
    private String email;
    private Role role;
    private Status status;
    private LocalDateTime createdAt;
}
