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

    private RatingRes convertToDTO(RatingEntity entity) {
        return RatingRes.builder()
                .ratingId(entity.getRatingId())
                .userId(entity.getUser().getUserId())
                .userName(entity.getUser().getFullName())
                .bookId(entity.getBook().getBookId())
                .rating(entity.getRating())
                .review(entity.getReview())
                .createdAt(entity.getCreatedAt())
                .updatedAt(entity.getUpdatedAt())
                .build();
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

        RatingEntity entity = RatingEntity.builder()
                .user(user)
                .book(book)
                .rating(req.getRating())
                .review(req.getReview())
                .status(RatingStatus.approved)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        RatingEntity saved = ratingRepository.save(entity);
        return RatingRes.builder()
                .ratingId(saved.getRatingId())
                .review(saved.getReview())
                .build();
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
        return RatingRes.builder()
                .ratingId(saved.getRatingId())
                .review(saved.getReview())
                .build();
    }
}