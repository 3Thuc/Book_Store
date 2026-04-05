package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.BookImageEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface BookImageRepository extends JpaRepository<BookImageEntity, Integer> {
    Optional<BookImageEntity> findByBook_BookId(Integer bookId);
}
