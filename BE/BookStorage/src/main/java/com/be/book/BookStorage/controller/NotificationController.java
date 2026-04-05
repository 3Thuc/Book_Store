package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Notification.NotificationDto;
import com.be.book.BookStorage.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;

    /**
     * Get all notifications with pagination (Admin only)
     */
    @GetMapping
    public ResponseEntity<ApiResponse<Page<NotificationDto>>> getAllNotifications(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            Authentication authentication
    ) {
        String email = authentication.getName();
        Pageable pageable = PageRequest.of(page, size);
        Page<NotificationDto> notifications = notificationService.getAllNotifications(pageable);
        
        return ResponseEntity.ok(ApiResponse.<Page<NotificationDto>>builder()
                .result(notifications)
                .build());
    }

    /**
     * Get unread notifications (Admin only)
     */
    @GetMapping("/unread")
    public ResponseEntity<ApiResponse<List<NotificationDto>>> getUnreadNotifications(Authentication authentication) {
        String email = authentication.getName();
        List<NotificationDto> notifications = notificationService.getUnreadNotifications();
        
        return ResponseEntity.ok(ApiResponse.<List<NotificationDto>>builder()
                .result(notifications)
                .build());
    }

    /**
     * Get unread count (Admin only)
     */
    @GetMapping("/unread/count")
    public ResponseEntity<ApiResponse<Long>> getUnreadCount(Authentication authentication) {
        String email = authentication.getName();
        long count = notificationService.getUnreadCount();
        
        return ResponseEntity.ok(ApiResponse.<Long>builder()
                .result(count)
                .build());
    }

    /**
     * Mark notification as read (Admin only)
     */
    @PutMapping("/{id}/read")
    public ResponseEntity<ApiResponse<String>> markAsRead(@PathVariable Integer id, Authentication authentication) {
        String email = authentication.getName();
        notificationService.markAsRead(id);
        
        return ResponseEntity.ok(ApiResponse.<String>builder()
                .result("Notification marked as read")
                .build());
    }

    /**
     * Mark all notifications as read (Admin only)
     */
    @PutMapping("/read-all")
    public ResponseEntity<ApiResponse<String>> markAllAsRead(Authentication authentication) {
        String email = authentication.getName();
        notificationService.markAllAsRead();
        
        return ResponseEntity.ok(ApiResponse.<String>builder()
                .result("All notifications marked as read")
                .build());
    }

    /**
     * Delete notification (Admin only)
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<ApiResponse<String>> deleteNotification(@PathVariable Integer id, Authentication authentication) {
        String email = authentication.getName();
        notificationService.deleteNotification(id);
        
        return ResponseEntity.ok(ApiResponse.<String>builder()
                .result("Notification deleted")
                .build());
    }
}
