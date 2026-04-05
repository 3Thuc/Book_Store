package com.be.book.BookStorage.dto.Response.Auth;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.http.ResponseCookie;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
public class AuthenticationResponse {
    String token;
    boolean authenticated;

    @JsonIgnore
    ResponseCookie refreshCookie;
}


