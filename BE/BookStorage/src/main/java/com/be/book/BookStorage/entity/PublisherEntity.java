package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Status;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "publishers")
@ToString(exclude = "books")
public class PublisherEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer publisherId;

    @Column(nullable = false, unique = true)
    private String publisherName;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "publisher", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    private List<BookEntity> books;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('active', 'inactive', 'deleted') DEFAULT 'active'")
    private Status status = Status.active;

    @Column(name = "deleted_by")
    private Integer deletedBy;

    @Column(name = "created_by")
    private Integer createdBy;
}


