package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Oder.PaymentStatus;
import com.be.book.BookStorage.enums.PaymentResult;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer paymentId;

    @ManyToOne
    @JoinColumn(name = "order_id")
    private OrderEntity order;

    private Double amount;
    private String provider;
    private String transactionId;

    @Enumerated(EnumType.STRING)
    private PaymentResult status;

    private LocalDateTime createdAt;
}

