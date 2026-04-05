package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.AuthorEntity;
import com.be.book.BookStorage.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AuthorRepository extends JpaRepository<AuthorEntity, Integer> {
    boolean existsByAuthorName(String authorName);
    Optional<AuthorEntity> findByAuthorName(String name);

}
