package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.RatingEntity;
import com.be.book.BookStorage.enums.RatingStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface RatingRepository extends JpaRepository<RatingEntity, Integer> {

    @Query("""
        SELECT r.book.bookId 
        FROM RatingEntity r 
        WHERE r.user.userId = :userId 
        AND r.review IS NOT NULL
    """)
    List<Integer> findReviewedBookIds(@Param("userId") Integer userId);

    @Query("""
        SELECT r.user.userId, r.book.bookId 
        FROM RatingEntity r 
        WHERE r.user.userId IN :userIds 
        AND r.review IS NOT NULL
    """)
    List<Object[]> findReviewedBookIdsByUsers(@Param("userIds") List<Integer> userIds);

    List<RatingEntity> findByBook_BookIdAndStatus(Integer bookId, RatingStatus status);

    Page<RatingEntity> findByBook_BookIdAndStatus(Integer bookId, RatingStatus status, Pageable pageable);

    Optional<RatingEntity> findByUser_UserIdAndBook_BookId(Integer userId, Integer bookId);

    List<RatingEntity> findByBook_BookId(Integer bookId);

    List<RatingEntity> findByUser_UserId(Integer userId);

    boolean existsByUser_UserIdAndBook_BookId(Integer userId, Integer bookId);

    @Query("""
        SELECT AVG(r.rating) 
        FROM RatingEntity r 
        WHERE r.book.bookId = :bookId 
        AND r.status = :status
    """)
    Double getAverageRatingByBookAndStatus(@Param("bookId") Integer bookId, @Param("status") RatingStatus status);

    Long countByBook_BookIdAndStatus(Integer bookId, RatingStatus status);

    @Query("""
        SELECT r FROM RatingEntity r 
        LEFT JOIN FETCH r.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        LEFT JOIN FETCH r.user 
        WHERE r.book.bookId = :bookId 
        AND r.status = :status
        ORDER BY r.createdAt DESC
    """)
    List<RatingEntity> findByBookIdAndStatusWithDetails(
            @Param("bookId") Integer bookId,
            @Param("status") RatingStatus status
    );
}