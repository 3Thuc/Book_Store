package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.CartEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface CartRepository extends JpaRepository<CartEntity, Integer> {
    Optional<CartEntity> findByUser_UserId(Integer userId);

}
