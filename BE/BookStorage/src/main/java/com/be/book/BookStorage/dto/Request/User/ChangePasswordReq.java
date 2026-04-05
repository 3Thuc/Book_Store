package com.be.book.BookStorage.dto.Request.User;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ChangePasswordReq {
    private String newPassword;
    private String currentPassword;
}
