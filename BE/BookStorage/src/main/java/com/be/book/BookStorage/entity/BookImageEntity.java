package com.be.book.BookStorage.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "book_images")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BookImageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer imageId;

    @OneToOne
    @JoinColumn(name = "book_id", unique = true)
    private BookEntity book;

    @Column(nullable = false)
    private String imageUrl;

    private Boolean isMain = false;
}

