package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.enums.Oder.OrderStatus;
import com.be.book.BookStorage.enums.Oder.PaymentMethod;
import com.be.book.BookStorage.enums.Oder.PaymentStatus;
import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.List;


@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer orderId;

    @ManyToOne
    @JoinColumn(name = "user_id")
    private UserEntity user;

    @ManyToOne
    @JoinColumn(name = "address_id")
    private AddressEntity address;

    @ManyToOne
    @JoinColumn(name = "promo_id")
    private PromotionEntity promo;

    private LocalDateTime orderDate;

    @Enumerated(EnumType.STRING)
    private OrderStatus status = OrderStatus.pending;

    @Enumerated(EnumType.STRING)
    private PaymentMethod paymentMethod = PaymentMethod.COD;

    @Enumerated(EnumType.STRING)
    private PaymentStatus paymentStatus = PaymentStatus.unpaid;

    @Column(columnDefinition = "TEXT", nullable = true)
    private String note;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<OrderDetailEntity> details;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<PaymentEntity> payments;


    @Column(nullable = false)
    private Double subtotal;

    @Column(nullable = false)
    private Double discountAmount;

    @Column(nullable = false)
    private Double totalAmount;

}

