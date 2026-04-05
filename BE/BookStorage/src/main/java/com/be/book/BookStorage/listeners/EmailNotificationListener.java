package com.be.book.BookStorage.listeners;


import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.events.GoogleUserCreatedEvent;
import com.be.book.BookStorage.events.GoogleUserForgotPasswordEvent;
import com.be.book.BookStorage.events.UserRegisteredEvent;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;


@Component
@Slf4j
public class EmailNotificationListener {
    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;
    private final String baseUrl;
    private final String signerKey;
    private final String verifyDuration;

    public EmailNotificationListener(JavaMailSender mailSender,
                                     TemplateEngine templateEngine,
                                     @Value("${app.base-url}") String baseUrl,
                                     @Value("${jwt.signerKey}") String signerKey,
                                     @Value("${jwt.verify-token-expiration}") String verifyDuration) {
        this.mailSender = mailSender;
        this.templateEngine = templateEngine;
        this.baseUrl = baseUrl;
        this.signerKey = signerKey;
        this.verifyDuration = verifyDuration;
    }

    @Async
    @EventListener
    public void handleUserRegistration(UserRegisteredEvent event) {
        log.info("Nhận được sự kiện đăng ký người dùng, chuẩn bị gửi email tới: {}", event.getUser().getEmail());
        sendVerificationEmail(event.getUser());
    }

    private void sendVerificationEmail(UserEntity user) {
        String token = generateVerifyEmailToken(user);
        String verifyLink = baseUrl + "/bookdb/auth/verify-email?token=" + token;

        Context context = new Context();
        context.setVariable("fullName", user.getFullName());
        context.setVariable("verifyLink", verifyLink);
        context.setVariable("expirationHours", 1);
        String emailContent = templateEngine.process("emails/verification-email", context);

        String subject = "Xác thực tài khoản của bạn tại BookStorage ✔";

        try {
            sendHtmlMail(user.getEmail(), subject, emailContent);
            log.info("Đã gửi email xác thực thành công tới: {}", user.getEmail());
        } catch (Exception e) {
            log.error("Gửi email xác thực thất bại cho {}: {}", user.getEmail(), e.getMessage());
        }
    }

    private void sendHtmlMail(String to, String subject, String htmlContent) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(htmlContent, true);
            mailSender.send(message);
        } catch (Exception e) {
            throw new RuntimeException("Không thể gửi email: " + e.getMessage());
        }
    }

    private String generateVerifyEmailToken(UserEntity user) {
        try {
            long expireMinutes = Long.parseLong(verifyDuration);
            Instant now = Instant.now();
            Date expiry = Date.from(now.plus(expireMinutes, ChronoUnit.MINUTES));

            JWSHeader header = new JWSHeader(JWSAlgorithm.HS512);
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .subject(user.getEmail())
                    .issueTime(Date.from(now))
                    .expirationTime(expiry)
                    .claim("purpose", "EMAIL_VERIFICATION")
                    .build();

            JWSObject jwsObject = new JWSObject(header, new Payload(claims.toJSONObject()));
            jwsObject.sign(new MACSigner(signerKey.getBytes()));

            return jwsObject.serialize();
        } catch (Exception e) {
            log.error("Không thể tạo token xác thực email", e);
            throw new RuntimeException("Không thể tạo token xác thực", e);
        }
    }
    @Async
    @EventListener
    public void handleGoogleUserCreated(GoogleUserCreatedEvent event) {
        UserEntity user = event.getUser();
        String rawPassword = event.getRawPassword();

        try {
            sendGooglePasswordEmail(user, rawPassword);
        } catch (Exception ignored) {
        }
    }

    private void sendGooglePasswordEmail(UserEntity user, String rawPassword) {
        Context context = new Context();
        context.setVariable("fullName", user.getFullName());
        context.setVariable("password", rawPassword);
        context.setVariable("email", user.getEmail());

        String htmlContent = templateEngine.process("emails/google-welcome-email", context); 
        String subject = "BookStorage — Mật khẩu đăng nhập (tài khoản Google)";

        sendHtmlMail(user.getEmail(), subject, htmlContent);
    }

    @Async
    @EventListener
    public void handleGoogleUserForgotPassword(GoogleUserForgotPasswordEvent event) {
        UserEntity user = event.getUser();
        String newPassword = event.getNewPassword();
        String requestTime = event.getRequestTime();

        try {
            sendGoogleForgotPasswordEmail(user, newPassword, requestTime);
        } catch (Exception e) {
            log.error("Lỗi khi gửi email quên mật khẩu cho {}: {}", user.getEmail(), e.getMessage());
        }
    }

    private void sendGoogleForgotPasswordEmail(UserEntity user, String newPassword, String requestTime) {
        Context context = new Context();
        context.setVariable("fullName", user.getFullName());
        context.setVariable("email", user.getEmail());
        context.setVariable("newPassword", newPassword);
        context.setVariable("requestTime", requestTime);

        String htmlContent = templateEngine.process("emails/forgotpass-email", context);
        String subject = "BookStorage — Cấp lại mật khẩu đăng nhập (Tài khoản Google)";

        sendHtmlMail(user.getEmail(), subject, htmlContent);
    }
}
