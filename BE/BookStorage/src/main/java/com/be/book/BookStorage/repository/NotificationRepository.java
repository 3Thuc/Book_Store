package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.NotificationEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface NotificationRepository extends JpaRepository<NotificationEntity, Integer> {
    
    // Find all notifications ordered by time descending
    Page<NotificationEntity> findAllByOrderByTimeDesc(Pageable pageable);
    
    // Find unread notifications
    @Query("SELECT n FROM NotificationEntity n WHERE n.isRead = false ORDER BY n.time DESC")
    List<NotificationEntity> findUnreadNotifications();
    
    // Count unread notifications
    long countByIsReadFalse();
    
    // Find notifications by type
    Page<NotificationEntity> findByTypeOrderByTimeDesc(String type, Pageable pageable);
    
    // Mark notification as read
    @Modifying
    @Query("UPDATE NotificationEntity n SET n.isRead = true, n.updatedAt = :now WHERE n.notificationId = :id")
    void markAsRead(@Param("id") Integer id, @Param("now") LocalDateTime now);
    
    // Mark all notifications as read
    @Modifying
    @Query("UPDATE NotificationEntity n SET n.isRead = true, n.updatedAt = :now WHERE n.isRead = false")
    void markAllAsRead(@Param("now") LocalDateTime now);
    
    // Delete old notifications (older than X days)
    @Modifying
    @Query("DELETE FROM NotificationEntity n WHERE n.time < :beforeDate")
    void deleteOldNotifications(@Param("beforeDate") LocalDateTime beforeDate);
    
    // Find recent notifications (last 7 days)
    @Query("SELECT n FROM NotificationEntity n WHERE n.time >= :fromDate ORDER BY n.time DESC")
    List<NotificationEntity> findRecentNotifications(@Param("fromDate") LocalDateTime fromDate);
}
