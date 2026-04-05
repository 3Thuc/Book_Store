package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Status;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "authors")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuthorEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer authorId;

    @Column(nullable = false, unique = true)
    private String authorName;

    @Column(columnDefinition = "TEXT")
    private String bio;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('active', 'inactive', 'deleted') DEFAULT 'active'")
    private Status status = Status.active;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @Column(name = "deleted_by")
    private Integer deletedBy;

    @Column(name = "created_by")
    private Integer createdBy;
}
