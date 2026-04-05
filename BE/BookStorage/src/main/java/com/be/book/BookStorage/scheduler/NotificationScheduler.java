package com.be.book.BookStorage.scheduler;

import com.be.book.BookStorage.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationScheduler {

    private final NotificationService notificationService;

    /**
     * Delete notifications older than 30 days
     * Runs daily at 2:00 AM
     */
    @Scheduled(cron = "0 0 2 * * *")
    public void deleteOldNotifications() {
        log.info("Starting scheduled task: Delete old notifications");
        try {
            notificationService.deleteOldNotifications(30);
            log.info("Successfully deleted old notifications");
        } catch (Exception e) {
            log.error("Failed to delete old notifications: {}", e.getMessage(), e);
        }
    }
}
