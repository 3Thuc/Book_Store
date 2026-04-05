package com.be.book.BookStorage.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "carts")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CartEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer cartId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "cart", cascade = CascadeType.ALL)
    private List<CartItemEntity> items;
}

