package com.be.book.BookStorage.exception;

import lombok.Getter;

@Getter
public class AppException extends RuntimeException {

    private final ErrorCode errorCode;

    // Message mặc định từ ErrorCode
    public AppException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
    }

    // Message dynamic / custom
    public AppException(ErrorCode errorCode, String customMessage) {
        super(customMessage);
        this.errorCode = errorCode;
    }

    // Message với placeholder {min}, {max}, ...
    public AppException(ErrorCode errorCode, java.util.Map<String, Object> placeholders) {
        super(replacePlaceholders(errorCode.getMessage(), placeholders));
        this.errorCode = errorCode;
    }

    private static String replacePlaceholders(String message, java.util.Map<String, Object> placeholders) {
        String result = message;
        if (placeholders != null) {
            for (var entry : placeholders.entrySet()) {
                result = result.replace("{" + entry.getKey() + "}", entry.getValue().toString());
            }
        }
        return result;
    }
}
