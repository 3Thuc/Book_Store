package com.be.book.BookStorage.service;

import com.be.book.BookStorage.enums.Oder.OrderStatus;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Service gửi email thông báo trạng thái đơn hàng đến khách hàng.
 * Được gọi bất đồng bộ (@Async) để không làm chậm API response.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEmailService {

    private final JavaMailSender mailSender;
    private final TemplateEngine templateEngine;

    @Value("${app.frontend-url:http://localhost:3000}")
    private String frontendUrl;

    private static final DateTimeFormatter VN_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm - dd/MM/yyyy", new Locale("vi", "VN"));

    /**
     * Gửi email thông báo cập nhật trạng thái đơn hàng cho khách hàng.
     * Được gọi async — không block thread xử lý chính.
     *
     * @param customerEmail email của khách hàng
     * @param customerName  tên hiển thị của khách hàng
     * @param orderId       ID đơn hàng
     * @param newStatus     trạng thái mới của đơn hàng
     * @param totalAmount   tổng tiền đơn hàng (đã format)
     */
    @Async
    public void sendOrderStatusUpdateEmail(
            String customerEmail,
            String customerName,
            Integer orderId,
            OrderStatus newStatus,
            String totalAmount
    ) {
        if (customerEmail == null || customerEmail.isBlank()) {
            log.warn("Bỏ qua gửi email: email khách hàng trống cho đơn hàng #{}", orderId);
            return;
        }

        // Chỉ gửi email cho các trạng thái quan trọng với khách hàng
        if (!isNotifiableStatus(newStatus)) {
            log.info("📧 Bỏ qua gửi email cho trạng thái nội bộ: {} - Đơn #{}", newStatus, orderId);
            return;
        }

        try {
            StatusEmailInfo info = buildStatusInfo(newStatus);
            String updatedAt = LocalDateTime.now().format(VN_FORMATTER);
            String orderDetailUrl = frontendUrl + "/account?tab=orders&orderId=" + orderId;

            Context context = new Context();
            context.setVariable("customerName",  customerName != null ? customerName : "Quý khách");
            context.setVariable("orderId",        orderId.toString());
            context.setVariable("statusLabel",    info.label);
            context.setVariable("statusEmoji",    info.emoji);
            context.setVariable("statusMessage",  info.message);
            context.setVariable("statusNote",     info.note);
            context.setVariable("badgeBg",        info.badgeBg);
            context.setVariable("badgeColor",     info.badgeColor);
            context.setVariable("totalAmount",    totalAmount);
            context.setVariable("updatedAt",      updatedAt);
            context.setVariable("orderDetailUrl", orderDetailUrl);

            String htmlContent = templateEngine.process("emails/order-status-update", context);
            String subject     = info.subjectPrefix + " Đơn hàng #" + orderId + " — BookStore";

            sendHtmlMail(customerEmail, subject, htmlContent);
            log.info("✅ Đã gửi email [{}] tới {} cho đơn hàng #{}", newStatus, customerEmail, orderId);

        } catch (Exception e) {
            // Lỗi gửi email không nên crash hệ thống — chỉ log warning
            log.error("❌ Gửi email thất bại cho đơn hàng #{} tới {}: {}", orderId, customerEmail, e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────

    /**
     * Chỉ gửi email cho các trạng thái cần thông báo KH.
     * Khớp đúng với enum OrderStatus (không có 'confirmed', 'packing').
     */
    private boolean isNotifiableStatus(OrderStatus status) {
        boolean notifiable = switch (status) {
            case shipped, delivered, cancelled, failed, returned -> true;
            default -> false;
        };
        log.info("📧 isNotifiableStatus({}) = {}", status, notifiable);
        return notifiable;
    }

    /** Thông tin nội dung email tương ứng từng trạng thái */
    private StatusEmailInfo buildStatusInfo(OrderStatus status) {
        return switch (status) {
            case shipped -> new StatusEmailInfo(
                    "🚚 Đang giao hàng",
                    "🚚",
                    "#e8f0fe", "#1a56db",
                    "[Đơn hàng đang giao]",
                    "Đơn hàng của bạn đã được bàn giao cho đơn vị vận chuyển và đang trên đường đến bạn!",
                    "Vui lòng đảm bảo có người nhận hàng tại địa chỉ đã đăng ký."
            );
            case delivered -> new StatusEmailInfo(
                    "📦 Giao thành công",
                    "📦",
                    "#e6f4ea", "#2d7a47",
                    "[Giao hàng thành công]",
                    "Đơn hàng của bạn đã được giao thành công! Cảm ơn bạn đã mua sắm tại BookStore. "
                    + "Đừng quên để lại đánh giá để giúp người mua khác nhé!",
                    null
            );
            case cancelled -> new StatusEmailInfo(
                    "❌ Đã hủy đơn hàng",
                    "❌",
                    "#fde8e8", "#c53030",
                    "[Đơn hàng bị hủy]",
                    "Rất tiếc, đơn hàng của bạn đã bị hủy. Nếu bạn đã thanh toán, "
                    + "chúng tôi sẽ hoàn tiền trong vòng 3–5 ngày làm việc.",
                    "Nếu đây là nhầm lẫn, vui lòng liên hệ support@bookstore.com ngay."
            );
            case failed -> new StatusEmailInfo(
                    "⚠️ Xử lý thất bại",
                    "⚠️",
                    "#fff3e0", "#e65100",
                    "[Đơn hàng thất bại]",
                    "Đơn hàng của bạn không thể xử lý do sự cố kỹ thuật. "
                    + "Vui lòng đặt lại đơn hàng hoặc liên hệ chúng tôi để được hỗ trợ.",
                    null
            );
            case returned -> new StatusEmailInfo(
                    "↩️ Đã hoàn hàng",
                    "↩️",
                    "#f3e8ff", "#6b21a8",
                    "[Hoàn hàng thành công]",
                    "Yêu cầu hoàn hàng của bạn đã được xử lý thành công. "
                    + "Chúng tôi sẽ hoàn tiền trong vòng 3–5 ngày làm việc.",
                    null
            );
            default -> new StatusEmailInfo(
                    "📋 Cập nhật đơn hàng",
                    "📋",
                    "#f0f0f0", "#333333",
                    "[Cập nhật đơn hàng]",
                    "Đơn hàng của bạn vừa được cập nhật trạng thái mới.",
                    null
            );
        };
    }


    private void sendHtmlMail(String to, String subject, String htmlContent) throws Exception {
        MimeMessage message = mailSender.createMimeMessage();
        MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");
        helper.setTo(to);
        helper.setSubject(subject);
        helper.setText(htmlContent, true);
        mailSender.send(message);
    }

    /** DTO nội bộ chứa thông tin hiển thị cho từng trạng thái */
    private record StatusEmailInfo(
            String label,
            String emoji,
            String badgeBg,
            String badgeColor,
            String subjectPrefix,
            String message,
            String note
    ) {}
}
