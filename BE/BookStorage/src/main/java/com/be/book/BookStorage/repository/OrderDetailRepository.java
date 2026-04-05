package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.OrderDetailEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderDetailRepository extends JpaRepository<OrderDetailEntity, Integer> {

    @Query("""
        SELECT od
        FROM OrderDetailEntity od
        JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        WHERE od.order.orderId = :orderId
    """)
    List<OrderDetailEntity> findFullByOrderId(@Param("orderId") Integer orderId);

    @Query("""
        SELECT od
        FROM OrderDetailEntity od
        JOIN FETCH od.book b
        LEFT JOIN FETCH b.image
        LEFT JOIN FETCH b.author
        WHERE od.order.orderId = :orderId
    """)
    List<OrderDetailEntity> findByOrderOrderId(@Param("orderId") Integer orderId);
}