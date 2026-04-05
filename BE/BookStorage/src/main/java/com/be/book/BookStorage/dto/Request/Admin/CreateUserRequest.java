package com.be.book.BookStorage.dto.Request.Admin;

import com.be.book.BookStorage.enums.Role;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class CreateUserRequest {
    private String email;
    private String fullName;
    private Role role;
}
