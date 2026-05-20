package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.CategoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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

    /** Kiểm tra trùng tên chỉ trong danh mục ACTIVE (bỏ qua deleted) */
    @Query("""
    SELECT COUNT(c) > 0 FROM CategoryEntity c
    WHERE c.categoryName = :name AND c.status <> 'deleted'
    """)
    boolean existsByActiveCategoryName(@Param("name") String name);

    /** Đếm số sách thuộc danh mục — dùng để quyết định hard-delete hay không */
    @Query("""
    SELECT COUNT(b) FROM BookEntity b
    JOIN b.categories c
    WHERE c.categoryId = :categoryId
    """)
    long countBooksByCategoryId(@Param("categoryId") Integer categoryId);
}
