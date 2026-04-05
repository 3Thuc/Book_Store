package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.CartItemEntity;
import com.be.book.BookStorage.entity.Key.CartItemKey;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CartItemRepository extends JpaRepository<CartItemEntity, CartItemKey> {
    void deleteByCart_CartId(Integer cartId);
    List<CartItemEntity> findByCart_CartId(Integer cartId);

}