package com.be.book.BookStorage.dto.Request.Admin;

import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import jakarta.validation.Valid;
import lombok.Data;

@Data
public class UpdateUserRequest {
    @Valid
    private Role role;

    @Valid
    private Status status;

}
