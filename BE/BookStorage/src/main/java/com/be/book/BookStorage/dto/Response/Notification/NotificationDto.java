package com.be.book.BookStorage.dto.Response.Notification;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationDto {
    private String id;
    private String type; 
    private String title;
    private String message;
    private LocalDateTime time;
    private boolean isRead;
    private String orderId;
    private String userName;
}
