package com.be.book.BookStorage.configuration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;

/**
 * Cấu hình ThreadPool riêng cho @Async.
 * Giải quyết warning: "More than one TaskExecutor bean found, none named 'taskExecutor'"
 * — do WebSocket cũng register các executor (clientInboundChannelExecutor, v.v.)
 *
 * Bean này được đặt tên "taskExecutor" (tên mặc định Spring tìm kiếm cho @Async),
 * đảm bảo email không chạy trên WebSocket thread.
 */
@Slf4j
@Configuration
public class AsyncConfig implements AsyncConfigurer {

    @Bean(name = "taskExecutor")
    @Override
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);          // 2 thread thường trực
        executor.setMaxPoolSize(5);           // Tối đa 5 thread khi bận
        executor.setQueueCapacity(100);       // Hàng chờ tối đa 100 task
        executor.setThreadNamePrefix("email-async-"); // Dễ nhận diện trong log
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        log.info("✅ AsyncConfig: email-async ThreadPool khởi tạo (core=2, max=5, queue=100)");
        return executor;
    }

    /**
     * Xử lý exception từ @Async method — log thay vì nuốt im lặng.
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (Throwable ex, Method method, Object... params) ->
                log.error("❌ @Async exception trong method '{}': {}", method.getName(), ex.getMessage(), ex);
    }
}
