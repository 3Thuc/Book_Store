package com.be.book.BookStorage.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class VNPayService {

    @Value("${vnp.tmnCode}")
    private String vnpTmnCode;

    @Value("${vnp.hashSecret}")
    private String vnpHashSecret;

    @Value("${vnp.payUrl}")
    private String vnpPayUrl;

    @Value("${vnp.apiUrl}")
    private String vnpApiUrl;

    @Value("${vnp.returnUrl}")
    private String vnpReturnUrl;

    private final RestTemplate restTemplate = new RestTemplate();
    private final Gson gson = new Gson();

    // 1. Tạo URL thanh toán
    public String createPayment(Long amount, String orderInfo, String bankCode, String language) throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("vnp_Version", "2.1.0");
        params.put("vnp_Command", "pay");
        params.put("vnp_TmnCode", vnpTmnCode);
        params.put("vnp_Amount", String.valueOf(amount * 100));
        params.put("vnp_CurrCode", "VND");
        if (bankCode != null && !bankCode.isEmpty()) {
            params.put("vnp_BankCode", bankCode);
        }
        params.put("vnp_TxnRef", String.valueOf(System.currentTimeMillis()));
        params.put("vnp_OrderInfo", orderInfo);
        params.put("vnp_OrderType", "other");
        params.put("vnp_Locale", language != null ? language : "vn");
        params.put("vnp_ReturnUrl", vnpReturnUrl);
        params.put("vnp_IpAddr", "127.0.0.1");

        Calendar cld = Calendar.getInstance(TimeZone.getTimeZone("Etc/GMT+7"));
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        params.put("vnp_CreateDate", formatter.format(cld.getTime()));
        cld.add(Calendar.MINUTE, 15);
        params.put("vnp_ExpireDate", formatter.format(cld.getTime()));

        List<String> fieldNames = new ArrayList<>(params.keySet());
        Collections.sort(fieldNames);
        StringBuilder hashData = new StringBuilder();
        StringBuilder query = new StringBuilder();

        for (String name : fieldNames) {
            String value = params.get(name);
            if (value != null && value.length() > 0) {
                if (hashData.length() > 0) hashData.append("&");
                hashData.append(name).append("=").append(URLEncoder.encode(value, StandardCharsets.US_ASCII.toString()));

                if (query.length() > 0) query.append("&");
                query.append(name).append("=").append(URLEncoder.encode(value, StandardCharsets.US_ASCII.toString()));
            }
        }

        String secureHash = hmacSHA512(vnpHashSecret, hashData.toString());
        query.append("&vnp_SecureHash=").append(secureHash);
        return vnpPayUrl + "?" + query;
    }

    // 2. Tra cứu giao dịch
    public String queryTransaction(String orderId, String transDate, String ipAddr) throws Exception {
        String requestId = String.valueOf(System.currentTimeMillis());
        String createDate = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        // Tạo hashData theo đúng thứ tự trong tài liệu VNPay
        String hashData = String.join("|",
                requestId,
                "2.1.0",
                "querydr",
                vnpTmnCode,
                orderId,
                transDate,
                createDate,
                ipAddr,
                "Truy van GD:" + orderId
        );

        String secureHash = hmacSHA512(vnpHashSecret, hashData);

        // Tạo JSON theo đúng format
        Map<String, String> params = new LinkedHashMap<>();
        params.put("vnp_RequestId", requestId);
        params.put("vnp_Version", "2.1.0");
        params.put("vnp_Command", "querydr");
        params.put("vnp_TmnCode", vnpTmnCode);
        params.put("vnp_TxnRef", orderId);
        params.put("vnp_OrderInfo", "Truy van GD:" + orderId);
        params.put("vnp_TransactionDate", transDate);
        params.put("vnp_CreateDate", createDate);
        params.put("vnp_IpAddr", ipAddr);
        params.put("vnp_SecureHash", secureHash);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String jsonBody = gson.toJson(params);
        HttpEntity<String> entity = new HttpEntity<>(jsonBody, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(vnpApiUrl, entity, String.class);
        return response.getBody();
    }

    // 3. Hoàn tiền
    public String refundTransaction(String orderId, long amount, String transDate, String user, String tranType) throws Exception {
        String requestId = String.valueOf(System.currentTimeMillis());
        String createDate = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String ipAddr = "127.0.0.1";
        String transactionNo = ""; // Để trống nếu không có

        // Tạo hashData theo đúng thứ tự trong tài liệu VNPay cho refund
        String hashData = String.join("|",
                requestId,
                "2.1.0",
                "refund",
                vnpTmnCode,
                tranType,
                orderId,
                String.valueOf(amount * 100),
                transactionNo,
                transDate,
                user,
                createDate,
                ipAddr,
                "Hoan tien GD:" + orderId
        );

        String secureHash = hmacSHA512(vnpHashSecret, hashData);

        // Tạo JSON theo đúng format
        Map<String, String> params = new LinkedHashMap<>();
        params.put("vnp_RequestId", requestId);
        params.put("vnp_Version", "2.1.0");
        params.put("vnp_Command", "refund");
        params.put("vnp_TmnCode", vnpTmnCode);
        params.put("vnp_TransactionType", tranType);
        params.put("vnp_TxnRef", orderId);
        params.put("vnp_Amount", String.valueOf(amount * 100));
        params.put("vnp_OrderInfo", "Hoan tien GD:" + orderId);
        params.put("vnp_TransactionNo", transactionNo);
        params.put("vnp_TransactionDate", transDate);
        params.put("vnp_CreateBy", user);
        params.put("vnp_CreateDate", createDate);
        params.put("vnp_IpAddr", ipAddr);
        params.put("vnp_SecureHash", secureHash);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String jsonBody = gson.toJson(params);
        HttpEntity<String> entity = new HttpEntity<>(jsonBody, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(vnpApiUrl, entity, String.class);
        return response.getBody();
    }

    // HMAC SHA512
    private String hmacSHA512(String key, String data) throws Exception {
        javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA512");
        javax.crypto.spec.SecretKeySpec secretKey = new javax.crypto.spec.SecretKeySpec(
                key.getBytes(StandardCharsets.UTF_8),
                "HmacSHA512"
        );
        mac.init(secretKey);
        byte[] hashBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder hash = new StringBuilder();
        for (byte b : hashBytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hash.append('0');
            hash.append(hex);
        }
        return hash.toString();
    }
}