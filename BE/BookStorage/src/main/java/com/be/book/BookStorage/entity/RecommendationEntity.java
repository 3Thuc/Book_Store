package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Oder.AlgoType;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "recommendations")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RecommendationEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer recommendationId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    @ManyToOne
    @JoinColumn(name = "book_id")
    private BookEntity book;

    private Double score;
    private Integer rankRcm;

    @Enumerated(EnumType.STRING)
    private AlgoType algoType;

    private String explanation;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}

