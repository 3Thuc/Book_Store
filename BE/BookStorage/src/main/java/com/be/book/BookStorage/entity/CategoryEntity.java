package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Status;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "categories")
@ToString(exclude = "books")
public class CategoryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer categoryId;

    @Column(nullable = false, unique = true)
    private String categoryName;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @ManyToMany(mappedBy = "categories", fetch = FetchType.LAZY)
    private Set<BookEntity> books;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('active', 'inactive', 'deleted') DEFAULT 'active'")
    private Status status = Status.active;

    @Column(name = "deleted_by")
    private Integer deletedBy;

    @Column(name = "created_by")
    private Integer createdBy;

}

