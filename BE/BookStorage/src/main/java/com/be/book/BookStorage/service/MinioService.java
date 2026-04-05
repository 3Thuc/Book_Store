package com.be.book.BookStorage.service;

import io.minio.GetPresignedObjectUrlArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.http.Method;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
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
