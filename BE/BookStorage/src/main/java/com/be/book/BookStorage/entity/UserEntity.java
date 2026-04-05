package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "users")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "user_id", columnDefinition = "INT NOT NULL")
    private Integer userId;

    @Column(nullable = false, length = 50)
    private String username;

    @Column(nullable = false, unique = true, length = 100)
    private String email;

    @Column(name = "password_hash", nullable = false, length = 255)
    private String passwordHash;

    @Column(name = "full_name", length = 100)
    private String fullName;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('unverified', 'active', 'locked') DEFAULT 'active'")
    private Status status = Status.active;

    @Column(length = 20)
    private String phone;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('customer', 'admin', 'staff') DEFAULT 'customer'")
    private Role role = Role.customer;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Relations
    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL)
    private List<AddressEntity> addresses;

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL)
    private List<CartEntity> carts;

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL)
    private List<WishlistEntity> wishlists;

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL)
    private List<RatingEntity> ratings;

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL)
    private List<RecommendationEntity> recommendations;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}