package com.be.book.BookStorage.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "wishlists")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WishlistEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer wishlistId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "wishlist", cascade = CascadeType.ALL)
    private List<WishlistItemEntity> items;
}

