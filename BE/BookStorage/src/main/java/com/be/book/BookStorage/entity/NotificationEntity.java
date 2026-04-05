package com.be.book.BookStorage.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "notifications")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEntity {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "notification_id")
    private Integer notificationId;
    
    @Column(name = "type", nullable = false, length = 50)
    private String type; // new_order, low_stock, order_completed, etc.
    
    @Column(name = "title", nullable = false, length = 255)
    private String title;
    
    @Column(name = "message", columnDefinition = "TEXT")
    private String message;
    
    @Column(name = "time", nullable = false)
    private LocalDateTime time;
    
    @Column(name = "is_read", nullable = false)
    private Boolean isRead = false;
    
    @Column(name = "order_id")
    private Integer orderId;
    
    @Column(name = "user_name", length = 255)
    private String userName;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (time == null) {
            time = LocalDateTime.now();
        }
    }
    
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
