package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Status;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "addresses")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AddressEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "address_id")
    private Integer addressId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    @Column(name = "recipient_name", nullable = false, length = 255)
    private String recipientName;

    @Column(name = "phone_number", nullable = false, length = 20)
    private String recipientPhone;

    @Column(name = "district", length = 255)
    private String district;

    @Column(name = "city", length = 255)
    private String city;

    @Column(name = "address_text", nullable = false, columnDefinition = "TEXT")
    private String addressText;

    @Column(name = "is_default")
    private Boolean isDefault = false;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM('active', 'deleted') DEFAULT 'active'")
    private Status status = Status.active;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

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
