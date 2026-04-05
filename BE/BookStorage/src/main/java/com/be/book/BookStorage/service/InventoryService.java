package com.be.book.BookStorage.service;


import com.be.book.BookStorage.dto.Request.Admin.InventoryReq;
import com.be.book.BookStorage.dto.Response.Admin.InventoryRes;
import com.be.book.BookStorage.dto.Response.Admin.PublishersRes;
import com.be.book.BookStorage.entity.BookEntity;
import com.be.book.BookStorage.entity.PublisherEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.BookRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class InventoryService {
    private final BookRepository bookRepository;
    private final UserRepository userRepository;

    private void checkPermission(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff ) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }
        if (user.getStatus() != Status.active) {
            throw new AppException(ErrorCode.USER_INACTIVE);
        }
    }

    public List<InventoryRes> getInventoryList() {
        return bookRepository.getInventoryList();
    }
    public InventoryRes updateInventory(String email, Integer id, InventoryReq inventoryReq) {
        checkPermission(email);
        BookEntity book = bookRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        book.setStockQuantity(inventoryReq.getStockQuantity());
        book.setUpdatedAt(LocalDateTime.now());
        BookEntity saved = bookRepository.save(book);
        return InventoryRes.builder()
                .bookId(saved.getBookId())
                .title(saved.getTitle())
                .stockQuantity(saved.getStockQuantity())
                .build();
    }
}
