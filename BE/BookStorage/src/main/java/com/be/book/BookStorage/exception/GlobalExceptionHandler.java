package com.be.book.BookStorage.exception;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import jakarta.validation.ConstraintViolation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.Map;
import java.util.Objects;

@Slf4j
@ControllerAdvice
public class GlobalExceptionHandler {

    private static final String MIN_ATTRIBUTE = "min";

    @ExceptionHandler(AppException.class)
    public ResponseEntity<ApiResponse<?>> handleAppException(AppException e) {
        ErrorCode errorCode = e.getErrorCode();
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(e.getMessage())
                .build();
        return ResponseEntity
                .status(errorCode.getHttpStatusCode().value())
                .body(apiResponse);
    }

    @ExceptionHandler(BadCredentialsException.class)
    public ResponseEntity<ApiResponse<?>> handleBadCredentials(BadCredentialsException ex) {
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(ErrorCode.UNAUTHENTICATED.getCode())
                .message("Sai email hoặc mật khẩu")
                .build();
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(apiResponse);
    }

    @ExceptionHandler(org.springframework.security.authentication.InternalAuthenticationServiceException.class)
    public ResponseEntity<ApiResponse<?>> handleInternalAuthenticationServiceException(
            org.springframework.security.authentication.InternalAuthenticationServiceException ex) {
        
        Throwable cause = ex.getCause();
        if (cause instanceof AppException) {
            AppException appException = (AppException) cause;
            ErrorCode errorCode = appException.getErrorCode();
            
            if (errorCode == ErrorCode.USER_NOT_FOUND) {
                ApiResponse<?> apiResponse = ApiResponse.builder()
                        .code(ErrorCode.UNAUTHENTICATED.getCode())
                        .message("Sai email hoặc mật khẩu")
                        .build();
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(apiResponse);
            }
            
            ApiResponse<?> apiResponse = ApiResponse.builder()
                    .code(errorCode.getCode())
                    .message(appException.getMessage())
                    .build();
            return ResponseEntity.status(errorCode.getHttpStatusCode()).body(apiResponse);
        }
        
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(ErrorCode.UNAUTHENTICATED.getCode())
                .message("Xác thực thất bại")
                .build();
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(apiResponse);
    }

    // Bắt AccessDeniedException (403)
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ApiResponse<?>> handleAccessDeniedException(AccessDeniedException e) {
        ErrorCode errorCode = ErrorCode.UNAUTHORIZED;
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(errorCode.getMessage())
                .build();
        return ResponseEntity.status(errorCode.getHttpStatusCode().value()).body(apiResponse);
    }

    // Bắt lỗi validation từ @Valid / @Validated
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<?>> handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {

        String enumKey = e.getFieldError().getDefaultMessage();
        ErrorCode errorCode = ErrorCode.INVALID_KEY;
        Map<String, Object> attributes = null;

        try {
            // Map defaultMessage về ErrorCode
            errorCode = ErrorCode.valueOf(enumKey);

            ConstraintViolation<?> violation = e.getBindingResult().getAllErrors().get(0)
                    .unwrap(ConstraintViolation.class);
            attributes = violation.getConstraintDescriptor().getAttributes();
            log.info("Validation attributes: {}", attributes);

        } catch (IllegalArgumentException ex) {
            // Không map được, giữ mặc định INVALID_KEY
        }

        String message = Objects.nonNull(attributes) ?
                mapAttributes(errorCode.getMessage(), attributes) :
                errorCode.getMessage();

        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(message)
                .build();

        return ResponseEntity.badRequest().body(apiResponse);
    }

    // Bắt tất cả lỗi khác chưa được xử lý
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<?>> handleException(Exception e) {
        log.error("Unhandled exception: ", e);
        ErrorCode errorCode = ErrorCode.UNCATEGORIZED_EXCEPTION;
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(errorCode.getMessage())
                .build();
        return ResponseEntity.status(errorCode.getHttpStatusCode().value()).body(apiResponse);
    }

    private String mapAttributes(String message, Map<String, Object> attributes) {
        if (attributes.containsKey(MIN_ATTRIBUTE)) {
            return message.replace("{" + MIN_ATTRIBUTE + "}", attributes.get(MIN_ATTRIBUTE).toString());
        }
        return message;
    }
}
