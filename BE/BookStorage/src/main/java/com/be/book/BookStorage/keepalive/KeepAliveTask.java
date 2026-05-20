package com.be.book.BookStorage.keepalive;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KeepAliveTask {
    private final RestTemplate restTemplate = new RestTemplate();

    // Gửi request mỗi 3 phút (180000 ms)
    @Scheduled(fixedRate = 180000)
    public void keepAlive() {
        try {
            // Đổi URL này thành URL public của BE trên Render
            restTemplate.getForObject("https://your-app-url.onrender.com/", String.class);
        } catch (Exception ignored) {
            // Không làm gì nếu lỗi
        }
    }
}
