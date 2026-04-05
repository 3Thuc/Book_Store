package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.entity.Key.WishlistItemKey;
import jakarta.persistence.*;
import lombok.*;


@Entity
@Table(name = "wishlist_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WishlistItemEntity {

    @EmbeddedId
    private WishlistItemKey id;

    @ManyToOne
    @MapsId("wishlistId")
    @JoinColumn(name = "wishlist_id")
    private WishlistEntity wishlist;

    @ManyToOne
    @MapsId("bookId")
    @JoinColumn(name = "book_id")
    private BookEntity book;
}

