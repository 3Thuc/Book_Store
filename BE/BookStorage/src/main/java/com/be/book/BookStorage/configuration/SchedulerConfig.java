package com.be.book.BookStorage.configuration;

import com.be.book.BookStorage.service.PromotionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
@Configuration
@EnableScheduling
@RequiredArgsConstructor
public class SchedulerConfig {
    private final PromotionService promotionService;


//    @Scheduled(cron = "0 0 0 * * *", zone = "Asia/Ho_Chi_Minh")
    @Scheduled(fixedDelay = 43200000)
    public void autoUpdatePromotionStatuses() {
        log.info("Starting scheduled task: Auto-update promotion statuses");

        try {
            promotionService.autoUpdatePromotionStatuses();
            log.info("Completed scheduled task: Auto-update promotion statuses");
        } catch (Exception e) {
            log.error("Error in scheduled task: Auto-update promotion statuses", e);
        }
    }

}
