package com.be.book.BookStorage.dto.Request.User;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class UserReq {
    private String userName;
    private String fullName;
    private String phoneNumber;
}
