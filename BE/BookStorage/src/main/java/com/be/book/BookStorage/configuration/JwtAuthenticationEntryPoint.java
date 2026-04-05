package com.be.book.BookStorage.configuration;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.exception.ErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class JwtAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void commence(
            HttpServletRequest request,
            HttpServletResponse response,
            AuthenticationException authException
    ) throws IOException, ServletException {

        ErrorCode errorCode = mapToErrorCode(authException);

        response.setStatus(errorCode.getHttpStatusCode().value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);

        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(errorCode.getMessage())
                .build();

        response.getWriter().write(mapper.writeValueAsString(apiResponse));
        response.flushBuffer();
    }


    private ErrorCode mapToErrorCode(AuthenticationException ex) {
        if (ex instanceof BadCredentialsException) {
            return ErrorCode.INVALID_CREDENTIALS;
        }

        if (ex.getCause() instanceof JwtException jwtEx) {
            String msg = jwtEx.getMessage() != null ? jwtEx.getMessage().toLowerCase() : "";
            if (msg.contains("expired")) return ErrorCode.TOKEN_EXPIRED;
            if (msg.contains("invalid")) return ErrorCode.TOKEN_INVALID;
            if (msg.contains("refresh")) return ErrorCode.REFRESH_TOKEN_INVALID;
        }

        return ErrorCode.UNAUTHENTICATED;
    }
}
