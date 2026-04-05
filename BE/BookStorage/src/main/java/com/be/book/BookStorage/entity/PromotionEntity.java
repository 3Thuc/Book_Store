package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.VoucherStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "promotions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PromotionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer promoId;

    @Column(nullable = false, unique = true)
    private String code;


    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "ENUM( 'active', 'inactive', 'deleted') DEFAULT 'active'")
    private VoucherStatus status = VoucherStatus.active;

    @Column(name = "discount_percent", nullable = false)
    private Double discountPercent;

    @Column(name = "start_date")
    private LocalDate startDate;

    @Column(name = "end_date")
    private LocalDate endDate;

    @Column(name = "is_deleted", nullable = false)
    @Builder.Default
    private Boolean isDeleted = false;


    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @Column(name = "deleted_by")
    private Integer deletedBy;

    @Column(name = "created_by")
    private Integer createdBy;

    @Column(name = "deleted_at")
    private LocalDateTime deletedAt;

    @OneToMany(mappedBy = "promo")
    private List<OrderEntity> orders;

}
