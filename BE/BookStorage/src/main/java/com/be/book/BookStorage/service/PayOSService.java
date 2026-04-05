package com.be.book.BookStorage.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


@RequiredArgsConstructor
@Slf4j
@Service
public class PayOSService {

    @Value("${payos.client-id}")
    private String clientId;

    @Value("${payos.api-key}")
    private String apiKey;

    @Value("${payos.checksum-key}")
    private String checksumKey;

    private final RestTemplate restTemplate = new RestTemplate();

    public String createPaymentLink(Integer orderCode, Long amount, String description) {
        try {
            String url = "https://api-merchant.payos.vn/v2/payment-requests";

            // Tạo request body
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("orderCode", orderCode);
            requestBody.put("amount", amount);
            requestBody.put("description", description);
            requestBody.put("returnUrl", "https://localhost:8443/bookdb/api/payment/return");
            requestBody.put("cancelUrl", "https://localhost:8443/bookdb/api/payment/cancel");


            // Tạo signature cho request
            String signature = createSignature(requestBody);
            requestBody.put("signature", signature);

            // Set headers
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.set("x-client-id", clientId);
            headers.set("x-api-key", apiKey);

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);

            // Call API
            ResponseEntity<Map> response = restTemplate.postForEntity(url, entity, Map.class);

            if (response.getStatusCode() == HttpStatus.OK) {
                Map<String, Object> data = (Map<String, Object>) response.getBody().get("data");
                return (String) data.get("checkoutUrl");
            }

            throw new RuntimeException("Failed to create payment link");

        } catch (Exception e) {
            throw new RuntimeException("Error creating payment link: " + e.getMessage());
        }
    }

    /**
     * Tạo signature dùng checksumKey
     */
    private String createSignature(Map<String, Object> data) {
        try {
            // Sắp xếp data theo key
            String dataString = data.entrySet().stream()
                    .filter(e -> !"signature".equals(e.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&"));

            return calculateHmacSHA256(dataString, checksumKey);
        } catch (Exception e) {
            throw new RuntimeException("Error creating signature: " + e.getMessage());
        }
    }

    /**
     * Verify callback từ PayOS dùng checksumKey
     */
    public boolean verifyPaymentCallback(Map<String, String> params) {
        try {
            String receivedSignature = params.get("signature");
            if (receivedSignature == null) {
                return false;
            }

            String dataToSign = params.entrySet().stream()
                    .filter(e -> !"signature".equals(e.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&"));

            String calculatedSignature = calculateHmacSHA256(dataToSign, checksumKey);

            return receivedSignature.equals(calculatedSignature);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Verify webhook từ PayOS dùng checksumKey
     */
    public boolean verifyWebhook(Map<String, Object> payload) {
        try {
            String receivedSignature = (String) payload.get("signature");
            if (receivedSignature == null) {
                return false;
            }

            String dataToSign = payload.entrySet().stream()
                    .filter(e -> !"signature".equals(e.getKey()))
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("&"));

            String calculatedSignature = calculateHmacSHA256(dataToSign, checksumKey);

            return receivedSignature.equals(calculatedSignature);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Tính HMAC SHA256 dùng checksumKey
     */
    private String calculateHmacSHA256(String data, String key) throws Exception {
        Mac sha256Hmac = Mac.getInstance("HmacSHA256");
        SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        sha256Hmac.init(secretKey);
        byte[] hash = sha256Hmac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(hash);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
}