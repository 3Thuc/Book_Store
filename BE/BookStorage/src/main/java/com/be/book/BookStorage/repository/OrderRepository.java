package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OrderRepository extends JpaRepository<OrderEntity, Integer> {

    @Query("""
        SELECT DISTINCT o FROM OrderEntity o
        LEFT JOIN FETCH o.details od
        LEFT JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        LEFT JOIN FETCH o.user u
        LEFT JOIN FETCH o.address a
        ORDER BY o.orderDate DESC
    """)
    List<OrderEntity> findAllWithDetails();

    @Query(value = """
        SELECT DISTINCT o FROM OrderEntity o
        LEFT JOIN FETCH o.details od
        LEFT JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        LEFT JOIN FETCH o.user u
        LEFT JOIN FETCH o.address a
    """,
    countQuery = """
        SELECT COUNT(DISTINCT o) FROM OrderEntity o
    """)
    org.springframework.data.domain.Page<OrderEntity> findAllWithDetails(org.springframework.data.domain.Pageable pageable);

    @Query("""
        SELECT CASE WHEN COUNT(r) > 0 THEN true ELSE false END
        FROM RatingEntity r
        WHERE r.user.userId = :userId
        AND r.book.bookId = :bookId
        AND r.review IS NOT NULL
    """)
    boolean hasUserReviewedBook(@Param("userId") Integer userId, @Param("bookId") Integer bookId);

    @Query(value = """
        SELECT DISTINCT o FROM OrderEntity o
        LEFT JOIN FETCH o.details od
        LEFT JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        LEFT JOIN FETCH o.user u
        LEFT JOIN FETCH o.address a
        WHERE (:status IS NULL OR UPPER(o.status) = UPPER(:status))
        AND (:search IS NULL OR CAST(o.orderId AS string) LIKE %:search%)
    """,
    countQuery = """
        SELECT COUNT(DISTINCT o) FROM OrderEntity o
        WHERE (:status IS NULL OR UPPER(o.status) = UPPER(:status))
        AND (:search IS NULL OR CAST(o.orderId AS string) LIKE %:search%)
    """)
    org.springframework.data.domain.Page<OrderEntity> findAllWithDetailsFiltered(
        @Param("status") String status,
        @Param("search") String search,
        org.springframework.data.domain.Pageable pageable
    );

    // Statistics queries
    @Query("SELECT COUNT(o) FROM OrderEntity o")
    Long countTotalOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'PENDING'")
    Long countPendingOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'PROCESSING'")
    Long countProcessingOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'SHIPPED'")
    Long countShippedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'DELIVERED'")
    Long countDeliveredOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'CANCELLED'")
    Long countCancelledOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'RETURN_REQUESTED'")
    Long countReturnRequestedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'RETURNED'")
    Long countReturnedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'FAILED'")
    Long countFailedOrders();

    @Query("SELECT COALESCE(SUM(od.totalPrice), 0.0) FROM OrderDetailEntity od JOIN od.order o WHERE o.status NOT IN ('CANCELLED', 'RETURNED')")
    Double calculateTotalRevenue();
}