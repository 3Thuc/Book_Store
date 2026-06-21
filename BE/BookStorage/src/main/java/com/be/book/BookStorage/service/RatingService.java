package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Book.RatingReq;
import com.be.book.BookStorage.dto.Response.Book.RatingRes;
import com.be.book.BookStorage.entity.BookEntity;
import com.be.book.BookStorage.entity.RatingEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.RatingStatus;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.BookRepository;
import com.be.book.BookStorage.repository.RatingRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class RatingService {
    private final RatingRepository ratingRepository;
    private final UserRepository userRepository;
    private final BookRepository bookRepository;
        private final com.be.book.BookStorage.repository.OrderRepository orderRepository;

    private RatingRes convertToDTO(RatingEntity entity) {
        return RatingRes.builder()
                .ratingId(entity.getRatingId())
                .userId(entity.getUser().getUserId())
                .userName(entity.getUser().getFullName())
                .bookId(entity.getBook().getBookId())
                .orderId(entity.getOrder() != null ? entity.getOrder().getOrderId() : null)
                .rating(entity.getRating())
                .review(entity.getReview())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .build();
    }

    // Helper to fetch entity with eager-loaded relationships
    private RatingEntity fetchRatingWithDetails(Integer ratingId) {
        return ratingRepository.findById(ratingId)
                .orElseThrow(() -> new AppException(ErrorCode.DATABASE_ERROR));
    }

    @Transactional(readOnly = true)
    public List<RatingRes> getAllBookRatings(Integer bookId) {
        List<RatingEntity> ratings = ratingRepository.findByBookIdAndStatusWithDetails(
                bookId, RatingStatus.approved);

        return ratings.stream()
                .map(this::convertToDTO)
                .collect(Collectors.toList());
    }

    @Transactional
    public RatingRes createRating(String email, Integer bookId, RatingReq req) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        BookEntity book = bookRepository.findById(bookId)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        RatingEntity.RatingEntityBuilder builder = RatingEntity.builder()
                                .user(user)
                                .book(book)
                                .rating(req.getRating())
                                .review(req.getReview())
                                .status(RatingStatus.approved)
                                .createdAt(LocalDateTime.now())
                                .updatedAt(LocalDateTime.now());

        // Handle orderId - can be String (from FE) or Integer format
        Integer orderId = null;
        if (req.getOrderId() != null && !req.getOrderId().trim().isEmpty()) {
            try {
                // Try to parse as Integer first
                orderId = Integer.parseInt(req.getOrderId());
            } catch (NumberFormatException e) {
                // If it's in format "ORD-123" or timestamp string, try to extract number part
                String orderIdStr = req.getOrderId().replaceAll("[^0-9]", "");
                if (!orderIdStr.isEmpty()) {
                    try {
                        // Try to parse extracted number as Integer
                        orderId = Integer.parseInt(orderIdStr);
                    } catch (NumberFormatException ex) {
                        // If still fails (e.g., number too large), log and continue without order
                        // This allows reviews without specific order association
                        orderId = null;
                    }
                }
            }
        }

        if (orderId != null) {
            try {
                com.be.book.BookStorage.entity.OrderEntity order = orderRepository.findById(orderId)
                        .orElseThrow(() -> new AppException(ErrorCode.DATABASE_ERROR));
                builder.order(order);
                // Prevent duplicate review for same user+book+order
                if (ratingRepository.existsByUser_UserIdAndBook_BookIdAndOrder_OrderId(user.getUserId(), book.getBookId(), orderId)) {
                    throw new AppException(ErrorCode.REVIEW_ALREADY_EXISTS);
                }
            } catch (Exception ex) {
                // If order lookup fails, fall back to user+book check
                orderId = null;
            }
        }

        if (orderId == null) {
            // If orderId not provided or invalid, fall back to existing uniqueness check
            if (ratingRepository.existsByUser_UserIdAndBook_BookId(user.getUserId(), book.getBookId())) {
                throw new AppException(ErrorCode.REVIEW_ALREADY_EXISTS);
            }
        }

        RatingEntity entity = builder.build();

        RatingEntity saved = ratingRepository.save(entity);
        // Fetch with eager-loaded relationships before converting to DTO
        RatingEntity savedWithDetails = fetchRatingWithDetails(saved.getRatingId());
        return convertToDTO(savedWithDetails);
    }

    @Transactional
    public RatingRes updateRating(String email, Integer bookId, Integer reviewId, RatingReq req) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        BookEntity book = bookRepository.findById(bookId)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        RatingEntity rating = ratingRepository.findById(reviewId)
                .orElseThrow(() -> new AppException(ErrorCode.DATABASE_ERROR));

        if (!rating.getUser().getUserId().equals(user.getUserId())) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        rating.setRating(req.getRating());
        rating.setReview(req.getReview());
        rating.setUpdatedAt(LocalDateTime.now());

        RatingEntity saved = ratingRepository.save(rating);
        // Fetch with eager-loaded relationships before converting to DTO
        RatingEntity savedWithDetails = fetchRatingWithDetails(saved.getRatingId());
        return convertToDTO(savedWithDetails);
    }
}