package com.be.book.BookStorage.dto.Request.Admin;

import com.be.book.BookStorage.enums.VoucherStatus;
import lombok.Data;

import java.time.LocalDate;

@Data
public class PromotionReq {
    private String promotionCode;
    private Double discount_percent;
    private LocalDate start_date;
    private LocalDate end_date;
    private VoucherStatus status;
}
