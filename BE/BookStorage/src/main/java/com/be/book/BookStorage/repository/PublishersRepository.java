package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.PublisherEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface PublishersRepository extends JpaRepository<PublisherEntity, Integer> {
    boolean existsByPublisherName(String publisherName);

    Optional<PublisherEntity> findByPublisherName(String publisherName);

    /** Kiểm tra trùng tên chỉ trong NXB ACTIVE (bỏ qua deleted) */
    @Query("""
    SELECT COUNT(p) > 0 FROM PublisherEntity p
    WHERE p.publisherName = :name AND p.status <> 'deleted'
    """)
    boolean existsByActivePublisherName(@Param("name") String name);

    /** Đếm số sách thuộc NXB — dùng để quyết định hard-delete hay không */
    @Query("""
    SELECT COUNT(b) FROM BookEntity b
    WHERE b.publisher.publisherId = :publisherId
    """)
    long countBooksByPublisherId(@Param("publisherId") Integer publisherId);
}
