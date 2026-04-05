package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.service.MinioService;
import io.minio.*;
import io.minio.http.Method;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/img")
public class ImgController {

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private MinioService minioService;

    private final String bucketName = "bookstore";

    @PostMapping("/upload")
    public String uploadImage(@RequestParam("file") MultipartFile file) {
        try {
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }

            try (InputStream is = file.getInputStream()) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(file.getOriginalFilename())
                                .stream(is, is.available(), -1)
                                .contentType(file.getContentType())
                                .build()
                );
            }
            return "Upload thành công: " + file.getOriginalFilename();

        } catch (Exception e) {
            e.printStackTrace();

            return "Upload lỗi: " + e.getMessage();
        }
    }

    /**
     * Get presigned URL for a single image path
     */
    @GetMapping("/presign")
    public Map<String, String> getPresignedUrl(@RequestParam("path") String imagePath) {
        try {
            String presignedUrl = minioService.getPresignedUrl(imagePath);
            Map<String, String> response = new HashMap<>();
            response.put("url", presignedUrl);
            return response;
        } catch (Exception e) {
            Map<String, String> response = new HashMap<>();
            response.put("error", e.getMessage());
            return response;
        }
    }

    /**
     * Get presigned URLs for multiple image paths (batch)
     */
    @PostMapping("/presign-batch")
    public Map<String, String> getPresignedUrls(@RequestBody Map<String, List<String>> request) {
        List<String> paths = request.get("paths");
        Map<String, String> presignedUrls = new HashMap<>();
        
        for (String path : paths) {
            try {
                String presignedUrl = minioService.getPresignedUrl(path);
                presignedUrls.put(path, presignedUrl);
            } catch (Exception e) {
                presignedUrls.put(path, null);
            }
        }
        
        return presignedUrls;
    }

//    @GetMapping("/{bookId}/image-url")
//    public ImageUrlResponse refreshImage(@PathVariable Long bookId) {
//        String key = bookService.getImageKey(bookId);
//        String url = minioService.getPresignedUrl(key);
//        return new ImageUrlResponse(url);
//    }

}
