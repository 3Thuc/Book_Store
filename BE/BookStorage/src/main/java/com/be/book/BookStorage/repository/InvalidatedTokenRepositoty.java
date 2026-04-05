package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.InvalidatedToken;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InvalidatedTokenRepositoty extends JpaRepository<InvalidatedToken, String>{

}
