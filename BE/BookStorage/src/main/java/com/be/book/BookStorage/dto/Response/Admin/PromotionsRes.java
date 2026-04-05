package com.be.book.BookStorage.dto.Response.Admin;


import com.be.book.BookStorage.enums.VoucherStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PromotionsRes {
    private Integer promotionId;
    private String promotionCode;
    private Double discount_percent;
    private LocalDate start_date;
    private LocalDate end_date;
    private VoucherStatus status;
    private Boolean isDelete;
}
