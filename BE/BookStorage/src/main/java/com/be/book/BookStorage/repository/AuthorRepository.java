package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.AuthorEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface AuthorRepository extends JpaRepository<AuthorEntity, Integer> {
    boolean existsByAuthorName(String authorName);

    Optional<AuthorEntity> findByAuthorName(String name);

    /** Chỉ check trùng tên với tác giả ACTIVE (bỏ qua deleted) */
    @Query("""
    SELECT COUNT(a) > 0 FROM AuthorEntity a
    WHERE a.authorName = :name AND a.status <> 'deleted'
    """)
    boolean existsByActiveAuthorName(@Param("name") String name);

    /** Đếm số sách thuộc tác giả — dùng để quyết định hard-delete */
    @Query("""
    SELECT COUNT(b) FROM BookEntity b
    WHERE b.author.authorId = :authorId
    """)
    long countBooksByAuthorId(@Param("authorId") Integer authorId);
}
