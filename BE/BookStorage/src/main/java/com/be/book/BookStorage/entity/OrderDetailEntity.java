package com.be.book.BookStorage.entity;

import com.be.book.BookStorage.entity.Key.OrderDetailKey;
import jakarta.persistence.*;
import lombok.*;



@Entity
@Table(name = "order_details")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderDetailEntity {

    @EmbeddedId
    private OrderDetailKey id;

    @ManyToOne
    @MapsId("orderId")
    @JoinColumn(name = "order_id")
    private OrderEntity order;

    @ManyToOne
    @MapsId("bookId")
    @JoinColumn(name = "book_id")
    private BookEntity book;


    @Column(name = "book_name")
    private String bookName;
    private Integer quantity;
    private Double unitPrice;
    private Double totalPrice;
}

