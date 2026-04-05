package com.be.book.BookStorage.entity.Key;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;

@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderDetailKey implements Serializable {
    private Integer orderId;
    private Integer bookId;
}

