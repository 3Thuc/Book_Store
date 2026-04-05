package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Response.Notification.NotificationDto;
import com.be.book.BookStorage.entity.NotificationEntity;
import com.be.book.BookStorage.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final SimpMessagingTemplate messagingTemplate;
    private final NotificationRepository notificationRepository;

    /**
     * Send notification to all admin users via WebSocket and save to DB
     */
    @Transactional
    public NotificationEntity sendNotificationToAdmins(NotificationDto notification) {
        // Save to database first
        NotificationEntity entity = NotificationEntity.builder()
                .type(notification.getType())
                .title(notification.getTitle())
                .message(notification.getMessage())
                .time(notification.getTime())
                .isRead(notification.isRead())
                .orderId(notification.getOrderId() != null ? Integer.parseInt(notification.getOrderId()) : null)
                .userName(notification.getUserName())
                .build();
        
        NotificationEntity savedEntity = notificationRepository.save(entity);
        log.info("Notification saved to DB with ID: {}", savedEntity.getNotificationId());
        
        // Then send via WebSocket
        try {
            // Update notification DTO with DB ID
            notification.setId(savedEntity.getNotificationId().toString());
            messagingTemplate.convertAndSend("/topic/admin-notifications", notification);
            log.info("Notification sent to admins via WebSocket: {}", notification.getTitle());
        } catch (Exception e) {
            log.error("Failed to send notification via WebSocket: {}", e.getMessage(), e);
        }
        
        return savedEntity;
    }

    /**
     * Create and send new order notification
     */
    public void sendNewOrderNotification(String orderId, String userName) {
        NotificationDto notification = NotificationDto.builder()
                .id(UUID.randomUUID().toString())
                .type("new_order")
                .title("Đơn hàng mới")
                .message("Bạn có 1 đơn hàng mới từ " + userName)
                .time(LocalDateTime.now())
                .isRead(false)
                .orderId(orderId)
                .userName(userName)
                .build();

        sendNotificationToAdmins(notification);
    }

    /**
     * Create and send low stock notification
     */
    public void sendLowStockNotification(String bookTitle, Integer remainingQuantity) {
        NotificationDto notification = NotificationDto.builder()
                .id(UUID.randomUUID().toString())
                .type("low_stock")
                .title("Sắp hết hàng")
                .message(String.format("Sách \"%s\" sắp hết hàng (còn %d cuốn)", bookTitle, remainingQuantity))
                .time(LocalDateTime.now())
                .isRead(false)
                .build();

        sendNotificationToAdmins(notification);
    }

    /**
     * Create and send order completed notification
     */
    public void sendOrderCompletedNotification(String orderId) {
        NotificationDto notification = NotificationDto.builder()
                .id(UUID.randomUUID().toString())
                .type("order_completed")
                .title("Đơn hàng hoàn thành")
                .message("Đơn hàng #" + orderId + " đã được giao thành công")
                .time(LocalDateTime.now())
                .isRead(false)
                .orderId(orderId)
                .build();

        sendNotificationToAdmins(notification);
    }

    /**
     * Create and send order cancelled notification
     */
    public void sendOrderCancelledNotification(String orderId, String userName) {
        NotificationDto notification = NotificationDto.builder()
                .id(UUID.randomUUID().toString())
                .type("order_cancelled")
                .title("Đơn hàng bị hủy")
                .message("Khách hàng " + userName + " yêu cầu hủy đơn #" + orderId)
                .time(LocalDateTime.now())
                .isRead(false)
                .orderId(orderId)
                .userName(userName)
                .build();

        sendNotificationToAdmins(notification);
    }

    /**
     * Create and send return requested notification
     */
    public void sendReturnRequestedNotification(String orderId, String userName, String reason) {
        NotificationDto notification = NotificationDto.builder()
                .id(UUID.randomUUID().toString())
                .type("return_requested")
                .title("Yêu cầu trả hàng")
                .message("Khách hàng " + userName + " yêu cầu trả hàng đơn #" + orderId + 
                        (reason != null && !reason.isEmpty() ? " - Lý do: " + reason : ""))
                .time(LocalDateTime.now())
                .isRead(false)
                .orderId(orderId)
                .userName(userName)
                .build();

        sendNotificationToAdmins(notification);
    }

    /**
     * Create and send delivery completed notification
     */
    public void sendDeliveryCompletedNotification(String orderId) {
        NotificationDto notification = NotificationDto.builder()
                .id(UUID.randomUUID().toString())
                .type("delivery_completed")
                .title("Giao hàng thành công")
                .message("Khách hàng đã xác nhận nhận hàng đơn #" + orderId)
                .time(LocalDateTime.now())
                .isRead(false)
                .orderId(orderId)
                .build();

        sendNotificationToAdmins(notification);
    }
    
    /**
     * Get all notifications with pagination
     */
    @Transactional(readOnly = true)
    public Page<NotificationDto> getAllNotifications(Pageable pageable) {
        Page<NotificationEntity> entities = notificationRepository.findAllByOrderByTimeDesc(pageable);
        return entities.map(this::mapToDto);
    }
    
    /**
     * Get unread notifications
     */
    @Transactional(readOnly = true)
    public List<NotificationDto> getUnreadNotifications() {
        List<NotificationEntity> entities = notificationRepository.findUnreadNotifications();
        return entities.stream()
                .map(this::mapToDto)
                .collect(Collectors.toList());
    }
    
    /**
     * Get unread count
     */
    @Transactional(readOnly = true)
    public long getUnreadCount() {
        return notificationRepository.countByIsReadFalse();
    }
    
    /**
     * Mark notification as read
     */
    @Transactional
    public void markAsRead(Integer notificationId) {
        notificationRepository.markAsRead(notificationId, LocalDateTime.now());
        log.info("Notification {} marked as read", notificationId);
    }
    
    /**
     * Mark all notifications as read
     */
    @Transactional
    public void markAllAsRead() {
        notificationRepository.markAllAsRead(LocalDateTime.now());
        log.info("All notifications marked as read");
    }
    
    /**
     * Delete notification
     */
    @Transactional
    public void deleteNotification(Integer notificationId) {
        notificationRepository.deleteById(notificationId);
        log.info("Notification {} deleted", notificationId);
    }
    
    /**
     * Delete old notifications (older than 30 days)
     */
    @Transactional
    public void deleteOldNotifications(int daysOld) {
        LocalDateTime beforeDate = LocalDateTime.now().minusDays(daysOld);
        notificationRepository.deleteOldNotifications(beforeDate);
        log.info("Deleted notifications older than {} days", daysOld);
    }
    
    /**
     * Map entity to DTO
     */
    private NotificationDto mapToDto(NotificationEntity entity) {
        return NotificationDto.builder()
                .id(entity.getNotificationId().toString())
                .type(entity.getType())
                .title(entity.getTitle())
                .message(entity.getMessage())
                .time(entity.getTime())
                .isRead(entity.getIsRead())
                .orderId(entity.getOrderId() != null ? entity.getOrderId().toString() : null)
                .userName(entity.getUserName())
                .build();
    }
}
