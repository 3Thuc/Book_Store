package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.PublisherEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PublishersRepository extends JpaRepository<PublisherEntity, Integer> {
    boolean existsByPublisherName(String publisherName);

    Optional<PublisherEntity> findByPublisherName(String publisherName);
}
