package com.be.book.BookStorage.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;


@Entity
@Table(name = "access_keys")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AccessKeyEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer accessKeyId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    @Column(nullable = false)
    private String accessToken;

    private String refreshToken;
    private String deviceInfo;
    private String ipAddress;
    private LocalDateTime expiresAt;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}

