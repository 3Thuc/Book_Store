package com.be.book.BookStorage.entity.Key;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;

@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartItemKey implements Serializable {
    private Integer cartId;
    private Integer bookId;
}

