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

    @Query("""
        SELECT CASE WHEN COUNT(r) > 0 THEN true ELSE false END
        FROM RatingEntity r
        WHERE r.user.userId = :userId
        AND r.book.bookId = :bookId
        AND r.order.orderId = :orderId
        AND r.review IS NOT NULL
    """)
    boolean hasUserReviewedBookInOrder(@Param("userId") Integer userId, @Param("bookId") Integer bookId, @Param("orderId") Integer orderId);

    /**
     * B\u01b0\u1edbc 1/2: Paginate ch\u1ec9 tr\u00ean order IDs \u2014 kh\u00f4ng JOIN FETCH collection.
     * LIMIT/OFFSET \u0111\u01b0\u1ee3c \u00e1p d\u1ee5ng \u0111\u00fang t\u1ea1i D B, kh\u00f4ng ph\u1ea3i in-memory.
     * S\u1eeda l\u1ed7i Hibernate HHH90003004 g\u00e2y t\u1ea3i to\u00e0n b\u1ed9 orders v\u00e0o heap.
     */
    @Query(value = """
        SELECT o.orderId FROM OrderEntity o
        WHERE (:status IS NULL OR UPPER(CAST(o.status AS string)) = UPPER(:status))
          AND (:search IS NULL OR CAST(o.orderId AS string) LIKE %:search%)
        ORDER BY o.orderDate DESC
    """,
    countQuery = """
        SELECT COUNT(o) FROM OrderEntity o
        WHERE (:status IS NULL OR UPPER(CAST(o.status AS string)) = UPPER(:status))
          AND (:search IS NULL OR CAST(o.orderId AS string) LIKE %:search%)
    """)
    org.springframework.data.domain.Page<Integer> findOrderIdsPaged(
        @Param("status") String status,
        @Param("search") String search,
        org.springframework.data.domain.Pageable pageable
    );

    /**
     * B\u01b0\u1edbc 2/2: Fetch chi ti\u1ebft ch\u1ec9 cho nh\u1eefng order ID v\u1eeba l\u1ea5y.
     * An to\u00e0n v\u1edbi JOIN FETCH v\u00ec kh\u00f4ng c\u00f3 Pageable.
     */
    @Query("""
        SELECT DISTINCT o FROM OrderEntity o
        LEFT JOIN FETCH o.details od
        LEFT JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        LEFT JOIN FETCH o.user u
        LEFT JOIN FETCH o.address a
        LEFT JOIN FETCH o.promo
        WHERE o.orderId IN :ids
        ORDER BY o.orderDate DESC
    """)
    List<OrderEntity> findByOrderIdsWithDetails(@Param("ids") List<Integer> ids);

    /**
     * Legacy: gây in-memory pagination, gi\u1eef l\u1ea1i cho c\u00e1c ch\u1ee9c n\u0103ng kh\u00f4ng pagination.
     */
    @Query(value = """
        SELECT DISTINCT o FROM OrderEntity o
        LEFT JOIN FETCH o.details od
        LEFT JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        LEFT JOIN FETCH o.user u
        LEFT JOIN FETCH o.address a
        WHERE (:status IS NULL OR UPPER(CAST(o.status AS string)) = UPPER(:status))
        AND (:search IS NULL OR CAST(o.orderId AS string) LIKE %:search%)
    """,
    countQuery = """
        SELECT COUNT(DISTINCT o) FROM OrderEntity o
        WHERE (:status IS NULL OR UPPER(CAST(o.status AS string)) = UPPER(:status))
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

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'pending'")
    Long countPendingOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'processing'")
    Long countProcessingOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'shipped'")
    Long countShippedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'delivered'")
    Long countDeliveredOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'cancelled'")
    Long countCancelledOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'cancel_requested'")
    Long countCancelRequestedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'return_requested'")
    Long countReturnRequestedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'returned'")
    Long countReturnedOrders();

    @Query("SELECT COUNT(o) FROM OrderEntity o WHERE o.status = 'failed'")
    Long countFailedOrders();

    @Query("SELECT COALESCE(SUM(o.totalAmount), 0.0) FROM OrderEntity o WHERE o.status NOT IN ('cancelled', 'returned', 'failed')")
    Double calculateTotalRevenue();

    @Query("SELECT COALESCE(SUM(o.totalAmount), 0.0) FROM OrderEntity o WHERE o.status = 'delivered'")
    Double calculateDeliveredRevenue();
}