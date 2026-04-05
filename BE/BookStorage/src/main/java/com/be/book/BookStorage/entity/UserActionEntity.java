package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.ActionType;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "user_actions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserActionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer actionId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    @ManyToOne
    @JoinColumn(name = "book_id")
    private BookEntity book;

    @Enumerated(EnumType.STRING)
    private ActionType actionType;

    @Column(columnDefinition = "JSON")
    private String metadata;

    private LocalDateTime actionDate;
}

