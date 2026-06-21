package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.RatingStatus;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;


@Entity
@Table(name = "ratings", indexes = {
    @Index(name = "idx_rating_user_book_order", columnList = "user_id,book_id,order_id", unique = true)
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RatingEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer ratingId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    @ManyToOne
    @JoinColumn(name = "book_id")
    private BookEntity book;

    @ManyToOne
    @JoinColumn(name = "order_id")
    private com.be.book.BookStorage.entity.OrderEntity order;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private RatingStatus status = RatingStatus.pending;

    private Integer rating;
    private String review;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}

