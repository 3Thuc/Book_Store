package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.CategoryEntity;
import com.be.book.BookStorage.entity.PublisherEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface CategoryRepository extends JpaRepository<CategoryEntity, Integer> {
    boolean existsByCategoryName(String categoryName);

    Optional<CategoryEntity> findByCategoryName(String categoryName);
    @Query("""
    SELECT c FROM CategoryEntity c
    WHERE c.status <> 'deleted'
    """)
    List<CategoryEntity> findAllActive();
}
