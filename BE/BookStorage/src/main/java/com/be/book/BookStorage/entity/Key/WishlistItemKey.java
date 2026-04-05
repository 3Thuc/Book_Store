package com.be.book.BookStorage.entity.Key;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;

@Embeddable
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WishlistItemKey implements Serializable {
    private Integer wishlistId;
    private Integer bookId;
}

