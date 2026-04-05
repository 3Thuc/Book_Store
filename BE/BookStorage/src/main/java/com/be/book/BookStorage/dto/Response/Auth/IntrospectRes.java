package com.be.book.BookStorage.dto.Response.Auth;


import lombok.*;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
public class IntrospectRes {
    boolean valid;
}
