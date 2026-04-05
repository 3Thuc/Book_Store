package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.PromotionEntity;
import com.be.book.BookStorage.enums.VoucherStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface PromotionsRepository extends JpaRepository<PromotionEntity, Integer> {
    boolean existsByCode(String code);

    Optional<PromotionEntity> findByCode(String code);


    boolean existsByCodeAndPromoIdNot(String code, Integer promoId);


    Optional<PromotionEntity> findByCodeAndIsDeletedFalse(String code);


    List<PromotionEntity> findByIsDeletedFalse();


    List<PromotionEntity> findByIsDeletedTrueAndDeletedAtBefore(LocalDateTime date);


    @Query("SELECT p FROM PromotionEntity p WHERE p.isDeleted = false AND p.status = :status")
    List<PromotionEntity> findActivePromotions(@Param("status") VoucherStatus status);


    @Query("SELECT p FROM PromotionEntity p WHERE p.isDeleted = false AND " +
            "(p.endDate < :today OR (p.startDate > :today AND p.status != 'disabled'))")
    List<PromotionEntity> findPromotionsNeedingStatusUpdate(@Param("today") LocalDate today);


    @Query("SELECT COUNT(p) FROM PromotionEntity p WHERE p.isDeleted = false AND p.status = 'active'")
    long countActivePromotions();
}
