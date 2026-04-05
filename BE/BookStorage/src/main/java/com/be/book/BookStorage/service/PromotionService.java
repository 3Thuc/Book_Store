package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.PromotionReq;
import com.be.book.BookStorage.dto.Response.Admin.PromotionsRes;
import com.be.book.BookStorage.entity.PromotionEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.enums.VoucherStatus;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.PromotionsRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PromotionService {

    private final UserRepository userRepository;
    private final PromotionsRepository promotionsRepository;


    private UserEntity validateAndGetUser(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        if (user.getStatus() != Status.active) {
            throw new AppException(ErrorCode.USER_INACTIVE);
        }

        return user;
    }

    // ============== STATUS CALCULATION ==============


    private VoucherStatus calculateStatus(PromotionEntity promo, LocalDate referenceDate) {
        if (promo.getIsDeleted()) {
            return VoucherStatus.deleted;
        }

        if (promo.getEndDate() != null && promo.getEndDate().isBefore(referenceDate)) {
            return VoucherStatus.inactive;
        }

        if (promo.getStartDate() != null && promo.getStartDate().isAfter(referenceDate)) {
            return VoucherStatus.deleted;
        }

        return promo.getStatus();
    }


    private VoucherStatus determineInitialStatus(LocalDate startDate, LocalDate endDate, LocalDate today) {
        if (startDate != null && startDate.isAfter(today)) {
            return VoucherStatus.deleted; // Scheduled
        }

        if (endDate != null && endDate.isBefore(today)) {
            return VoucherStatus.inactive;
        }

        return VoucherStatus.active;
    }

    // ============== VALIDATION ==============

    private void validateDateRange(LocalDate startDate, LocalDate endDate) {
        if (startDate != null && endDate != null && endDate.isBefore(startDate)) {
            throw new AppException(ErrorCode.INVALID_DATE_RANGE);
        }
    }

    private void validatePromotionCode(String code, Integer excludePromoId) {
        boolean exists = excludePromoId != null
                ? promotionsRepository.existsByCodeAndPromoIdNot(code, excludePromoId)
                : promotionsRepository.existsByCode(code);

        if (exists) {
            throw new AppException(ErrorCode.PROMOTION_CODE_EXISTED);
        }
    }

    private void validateDiscountPercent(Double percent) {
        if (percent == null || percent <= 0 || percent > 100) {
            throw new AppException(ErrorCode.INVALID_DISCOUNT_PERCENT);
        }
    }

    // ============== BUSINESS LOGIC - QUERY ==============


    @Transactional(readOnly = true)
    public List<PromotionsRes> getListPromotions(String email, boolean includeDeleted) {
        validateAndGetUser(email);
        LocalDate today = LocalDate.now();

        List<PromotionEntity> promotions = includeDeleted
                ? promotionsRepository.findAll()
                : promotionsRepository.findByIsDeletedFalse();

        return promotions.stream()
                .map(promo -> mapToResponse(promo, today))
                .toList();
    }


    @Transactional(readOnly = true)
    public List<PromotionsRes> getListPromotions(String email) {
        return getListPromotions(email, false);
    }

    @Transactional(readOnly = true)
    public List<PromotionsRes> getActivePromotions(String email) {
        validateAndGetUser(email);
        LocalDate today = LocalDate.now();

        return promotionsRepository.findAll().stream()
                .filter(promo -> !promo.getIsDeleted())
                .filter(promo -> {
                    VoucherStatus status = calculateStatus(promo, today);
                    return status == VoucherStatus.active;
                })
                .map(promo -> mapToResponse(promo, today))
                .toList();
    }

    @Transactional(readOnly = true)
    public List<PromotionsRes> getAvailablePromotions() {
        // Public method for users to get active promotions
        // No authentication required
        LocalDate today = LocalDate.now();

        return promotionsRepository.findAll().stream()
                .filter(promo -> !promo.getIsDeleted())
                .filter(promo -> {
                    VoucherStatus status = calculateStatus(promo, today);
                    return status == VoucherStatus.active;
                })
                .map(promo -> mapToResponse(promo, today))
                .toList();
    }

    // ============== BUSINESS LOGIC - CREATE/UPDATE ==============

    @Transactional
    public PromotionsRes createPromotion(String email, PromotionReq req) {
        validateAndGetUser(email);

        validateDateRange(req.getStart_date(), req.getEnd_date());
        validatePromotionCode(req.getPromotionCode(), null);
        validateDiscountPercent(req.getDiscount_percent());

        LocalDate today = LocalDate.now();
        VoucherStatus initialStatus = req.getStatus() != null
                ? req.getStatus()
                : determineInitialStatus(req.getStart_date(), req.getEnd_date(), today);

        PromotionEntity promo = PromotionEntity.builder()
                .code(req.getPromotionCode())
                .discountPercent(req.getDiscount_percent())
                .startDate(req.getStart_date())
                .endDate(req.getEnd_date())
                .status(initialStatus)
                .isDeleted(false)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        promo = promotionsRepository.save(promo);


        return mapToResponse(promo, today);
    }

    @Transactional
    public PromotionsRes updatePromotion(String email, Integer id, PromotionReq req) {
        validateAndGetUser(email);

        PromotionEntity promo = promotionsRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.VOUCHER_NOT_FOUND));

        if (promo.getIsDeleted()) {
            throw new AppException(ErrorCode.CANNOT_UPDATE_DELETED_PROMOTION);
        }

        validateDateRange(req.getStart_date(), req.getEnd_date());
        if (!promo.getCode().equals(req.getPromotionCode())) {
            validatePromotionCode(req.getPromotionCode(), id);
        }
        validateDiscountPercent(req.getDiscount_percent());

        promo.setCode(req.getPromotionCode());
        promo.setDiscountPercent(req.getDiscount_percent());
        promo.setStartDate(req.getStart_date());
        promo.setEndDate(req.getEnd_date());

        if (req.getStatus() != null) {
            LocalDate today = LocalDate.now();
            if (req.getStatus() == VoucherStatus.active) {
                if (promo.getStartDate() != null && promo.getStartDate().isAfter(today)) {
                    throw new AppException(ErrorCode.CANNOT_ACTIVATE_FUTURE_PROMOTION);
                }
                if (promo.getEndDate() != null && promo.getEndDate().isBefore(today)) {
                    throw new AppException(ErrorCode.CANNOT_ACTIVATE_EXPIRED_PROMOTION);
                }
            }
            promo.setStatus(req.getStatus());
        }

        promo.setUpdatedAt(LocalDateTime.now());
        promo = promotionsRepository.save(promo);


        return mapToResponse(promo, LocalDate.now());
    }

    // ============== BUSINESS LOGIC - DELETE ==============

    @Transactional
    public void deletePromotion(String email, Integer id) {
        UserEntity user = validateAndGetUser(email);

        PromotionEntity promo = promotionsRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.VOUCHER_NOT_FOUND));

        if (promo.getIsDeleted()) {
            throw new AppException(ErrorCode.PROMOTION_ALREADY_DELETED);
        }

        promo.setIsDeleted(true);
        promo.setStatus(VoucherStatus.deleted);
        promo.setDeletedBy(user.getUserId());
        promo.setDeletedAt(LocalDateTime.now());
        promo.setUpdatedAt(LocalDateTime.now());

        promotionsRepository.save(promo);

    }


    @Transactional
    public void permanentDeletePromotion(String email, Integer id) {
        UserEntity user = validateAndGetUser(email);

        if (user.getRole() != Role.admin) {
            throw new AppException(ErrorCode.ADMIN_ONLY_OPERATION);
        }

        PromotionEntity promo = promotionsRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.VOUCHER_NOT_FOUND));

        if (isPromotionInUse(promo)) {
            throw new AppException(ErrorCode.PROMOTION_IN_USE_CANNOT_DELETE);
        }

        promotionsRepository.delete(promo);

    }


    @Transactional
    public int cleanupOldDeletedPromotions(String email) {
        UserEntity user = validateAndGetUser(email);

        if (user.getRole() != Role.admin) {
            throw new AppException(ErrorCode.ADMIN_ONLY_OPERATION);
        }

        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(90);
        List<PromotionEntity> oldDeletedPromos = promotionsRepository
                .findByIsDeletedTrueAndDeletedAtBefore(cutoffDate);

        int count = 0;
        for (PromotionEntity promo : oldDeletedPromos) {
            if (!isPromotionInUse(promo)) {
                promotionsRepository.delete(promo);
                count++;
            }
        }

        return count;
    }

    @Transactional
    public PromotionsRes restorePromotion(String email, Integer id) {
        validateAndGetUser(email);

        PromotionEntity promo = promotionsRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.VOUCHER_NOT_FOUND));

        if (!promo.getIsDeleted()) {
            throw new AppException(ErrorCode.PROMOTION_NOT_DELETED);
        }

        promo.setIsDeleted(false);
        promo.setDeletedBy(null);
        promo.setDeletedAt(null);

        LocalDate today = LocalDate.now();
        VoucherStatus restoredStatus = determineInitialStatus(
                promo.getStartDate(),
                promo.getEndDate(),
                today
        );

        promo.setStatus(restoredStatus);
        promo.setUpdatedAt(LocalDateTime.now());

        promo = promotionsRepository.save(promo);


        return mapToResponse(promo, today);
    }


    @Transactional
    public void autoUpdatePromotionStatuses() {
        LocalDate today = LocalDate.now();
        List<PromotionEntity> promotions = promotionsRepository.findByIsDeletedFalse();

        int updated = 0;
        for (PromotionEntity promo : promotions) {
            VoucherStatus newStatus = calculateStatus(promo, today);

            if (promo.getStatus() != newStatus) {
                promo.setStatus(newStatus);
                promo.setUpdatedAt(LocalDateTime.now());
                promotionsRepository.save(promo);
                updated++;
            }
        }

    }

    private boolean isPromotionInUse(PromotionEntity promo) {
        // TODO: Implement check với các bảng liên quan
        // - Có orders đang dùng promotion này không?
        // - Có user_promotions đang active không?
        // Example:
        // return orderRepository.existsByPromoId(promo.getPromoId()) ||
        //        userPromotionRepository.existsByPromoIdAndUsedAtIsNotNull(promo.getPromoId());
        return false;
    }
    private PromotionsRes mapToResponse(PromotionEntity promo, LocalDate referenceDate) {
        return new PromotionsRes(
                promo.getPromoId(),
                promo.getCode(),
                promo.getDiscountPercent(),
                promo.getStartDate(),
                promo.getEndDate(),
                calculateStatus(promo, referenceDate),
                promo.getIsDeleted()
                );
    }

}
