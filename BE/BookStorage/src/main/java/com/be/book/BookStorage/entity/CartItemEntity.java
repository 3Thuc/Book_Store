package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.entity.Key.CartItemKey;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "cart_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CartItemEntity {

    @EmbeddedId
    private CartItemKey id;

    @ManyToOne
    @MapsId("cartId")
    @JoinColumn(name = "cart_id")
    private CartEntity cart;

    @ManyToOne
    @MapsId("bookId")
    @JoinColumn(name = "book_id")
    private BookEntity book;

    private Integer quantity;
}

