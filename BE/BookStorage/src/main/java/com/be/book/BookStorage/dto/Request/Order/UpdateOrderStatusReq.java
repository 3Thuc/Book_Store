package com.be.book.BookStorage.dto.Request.Order;

import com.be.book.BookStorage.enums.Oder.OrderStatus;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateOrderStatusReq {
    private String status;

}
