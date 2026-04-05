package com.be.book.BookStorage.dto.Response.Order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderStatisticsRes {
    private Long totalOrders;
    private Long pendingOrders;
    private Long processingOrders;
    private Long shippedOrders;
    private Long deliveredOrders;
    private Long cancelledOrders;
    private Long returnRequestedOrders;
    private Long returnedOrders;
    private Long failedOrders;
    private Double totalRevenue;
}
