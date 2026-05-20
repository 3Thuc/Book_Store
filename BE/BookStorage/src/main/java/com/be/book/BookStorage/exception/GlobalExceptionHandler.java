package com.be.book.BookStorage.exception;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.async.AsyncRequestNotUsableException;

import java.util.Map;
import java.util.Objects;

@ControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final String MIN_ATTRIBUTE = "min";

    @ExceptionHandler(AppException.class)
    public ResponseEntity<ApiResponse<?>> handleAppException(AppException e) {
        ErrorCode errorCode = e.getErrorCode();
        log.warn("AppException: code={}, message={}", errorCode.getCode(), e.getMessage());
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(e.getMessage())
                .build();
        return ResponseEntity
                .status(errorCode.getHttpStatusCode().value())
                .body(apiResponse);
    }

    // Bắt lỗi khi JSON không đọc được (sai kiểu, enum không hợp lệ, NaN, thiếu field bắt buộc)
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse<?>> handleHttpMessageNotReadable(HttpMessageNotReadableException e) {
        log.error("JSON parse error (HttpMessageNotReadableException): {}", e.getMessage());
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(ErrorCode.INVALID_KEY.getCode())
                .message("Dữ liệu request không hợp lệ: " + e.getMostSpecificCause().getMessage())
                .build();
        return ResponseEntity.badRequest().body(apiResponse);
    }

    // Bắt lỗi khi Spring cố ghi đè Error JSON (LinkedHashMap) lên một response đã bị set Content-Type là application/javascript (thường do WebSocket/SockJS ngắt kết nối đột ngột)
    @ExceptionHandler(org.springframework.http.converter.HttpMessageNotWritableException.class)
    public void handleHttpMessageNotWritable(org.springframework.http.converter.HttpMessageNotWritableException e) {
        log.debug("Client connection dropped / Content-Type mismatch (HttpMessageNotWritableException): {}", e.getMessage());
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

    // Bắt 405 Method Not Allowed – log rõ URL + method để debug FE gọi sai endpoint
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiResponse<?>> handleMethodNotSupported(
            HttpRequestMethodNotSupportedException ex,
            HttpServletRequest request) {
        log.warn("405 Method Not Allowed: {} {} (allowed: {})",
                ex.getMethod(),
                request.getRequestURI(),
                ex.getSupportedHttpMethods());
        ApiResponse<?> apiResponse = ApiResponse.builder()
                .code(405)
                .message(String.format(
                        "Phương thức %s không được hỗ trợ tại %s. Phương thức được phép: %s",
                        ex.getMethod(), request.getRequestURI(), ex.getSupportedHttpMethods()))
                .build();
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(apiResponse);
    }

    // Bắt lỗi validation từ @Valid / @Validated
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<?>> handleMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        log.warn("Validation error: {}", e.getMessage());

        ErrorCode errorCode = ErrorCode.INVALID_KEY;
        Map<String, Object> attributes = null;

        try {
            // getFieldError() có thể null nếu chỉ có global error
            var fieldError = e.getFieldError();
            if (fieldError != null) {
                String enumKey = fieldError.getDefaultMessage();
                // Map defaultMessage về ErrorCode
                errorCode = ErrorCode.valueOf(enumKey);

                ConstraintViolation<?> violation = e.getBindingResult().getAllErrors().get(0)
                        .unwrap(ConstraintViolation.class);
                attributes = violation.getConstraintDescriptor().getAttributes();
                log.info("Validation attributes: {}", attributes);
            }
        } catch (IllegalArgumentException ex) {
            // Không map được, giữ mặc định INVALID_KEY
        } catch (Exception ex) {
            log.warn("Unexpected error processing validation exception: {}", ex.getMessage());
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

    // Client ngắt kết nối trước khi server ghi xong response (vd: tab đóng, refresh nhanh)
    // Đây là hành vi BÌNH THƯỜNG khi response nặng – log DEBUG thay vì ERROR
    @ExceptionHandler(AsyncRequestNotUsableException.class)
    public void handleAsyncRequestNotUsable(AsyncRequestNotUsableException e) {
        log.debug("Client disconnected before response completed (AsyncRequestNotUsableException): {}",
                e.getMessage());
        // Không trả response vì socket đã đóng
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
        // FIX: Force Content-Type → application/json
        // Ngăn lỗi "No converter for LinkedHashMap with preset Content-Type application/javascript"
        // khi Tomcat async-dispatch kế thừa Content-Type từ PayOS webhook request
        return ResponseEntity.status(errorCode.getHttpStatusCode().value())
                .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .body(apiResponse);
    }

    private String mapAttributes(String message, Map<String, Object> attributes) {
        if (attributes.containsKey(MIN_ATTRIBUTE)) {
            return message.replace("{" + MIN_ATTRIBUTE + "}", attributes.get(MIN_ATTRIBUTE).toString());
        }
        return message;
    }
}
