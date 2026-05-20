package com.be.book.BookStorage.service;

import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.http.Method;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class MinioService {

    private final MinioClient minioClient;

    @Value("${minio.bucket}")
    private String bucketName;

    public String getPresignedUrl(String objectName) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .method(Method.GET)
                        .bucket(bucketName)
                        .object(objectName)
                        .expiry(7, TimeUnit.DAYS)
                        .build()
        );
    }

    /**
     * Presigned URL được CACHE để tránh gọi MinIO SDK lặp lại.
     * Lần đầu gọi: chậm (SDK call) → cache kết quả.
     * Các lần sau: instant từ cache.
     * Cache tên: "imageUrls", key = objectName.
     */
    @Cacheable(value = "imageUrls", key = "#objectName")
    public String getCachedPresignedUrl(String objectName) {
        if (objectName == null || objectName.isBlank()) return null;
        if (objectName.startsWith("http")) return objectName;
        try {
            return getPresignedUrl(objectName);
        } catch (Exception e) {
            // Fallback: direct URL qua Vite proxy /minio/*
            return getPublicUrl(objectName);
        }
    }

    /**
     * URL trực tiếp qua FE proxy (/minio/bucket/objectName) — không cần presigning.
     * Chỉ hoạt động nếu MinIO bucket có public-read policy.
     */
    public String getPublicUrl(String objectName) {
        if (objectName == null || objectName.isBlank()) return null;
        if (objectName.startsWith("http")) return objectName;
        return "/minio/" + bucketName + "/" + objectName;
    }

    public String uploadFile(MultipartFile file, String objectPath) throws Exception {
        try (InputStream inputStream = file.getInputStream()) {
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectPath)
                    .stream(inputStream, file.getSize(), -1)
                    .contentType(file.getContentType())
                    .build()
            );
        }
        return objectPath;
    }

    public void deleteFile(String objectPath) throws Exception {
        minioClient.removeObject(
            RemoveObjectArgs.builder()
                .bucket(bucketName)
                .object(objectPath)
                .build()
        );
    }
}
