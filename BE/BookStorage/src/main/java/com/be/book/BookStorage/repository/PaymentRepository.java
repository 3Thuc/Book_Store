package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.OrderEntity;
import com.be.book.BookStorage.entity.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<PaymentEntity, Integer> {
    Optional<PaymentEntity> findByTransactionId(String transactionId);

    Optional<PaymentEntity> findByOrder_OrderId(Integer orderCode);
}
