package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.BookFormat;
import com.be.book.BookStorage.enums.Status;
import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;


@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "books")
public class BookEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer bookId;

    @Column(nullable = false)
    private String title;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "author_id")
    private AuthorEntity author;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "publisher_id")
    private PublisherEntity publisher;

    private Double price;
    private Integer stockQuantity;

    @Column(name = "description", columnDefinition = "TEXT")
    private String description;
    private Integer publicationYear;

    private Double avgRating = 0.0;
    private Integer ratingCount = 0;
    private String language;

    @Enumerated(EnumType.STRING)
    private BookFormat format;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(name = "book_categories",
            joinColumns = @JoinColumn(name = "book_id"),
            inverseJoinColumns = @JoinColumn(name = "category_id"))
    private Set<CategoryEntity> categories;


    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('active', 'inactive', 'deleted') DEFAULT 'active'")
    private Status status = Status.active;

    @Column(name = "deleted_by")
    private Integer deletedBy;

    @Column(name = "created_by")
    private Integer createdBy;

    @Transient
    private Integer availableQuantity;

    @OneToOne(mappedBy = "book", fetch = FetchType.LAZY)
    private BookImageEntity image;
}



