package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.PublishersReq;
import com.be.book.BookStorage.dto.Response.Admin.PublishersRes;
import com.be.book.BookStorage.entity.PublisherEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.PublishersRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PublishersService {
    private final PublishersRepository publishersRepository;
    private final UserRepository userRepository;

    private void checkPermission(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }
        if (user.getStatus() != Status.active) {
            throw new AppException(ErrorCode.USER_INACTIVE);
        }
    }

    public List<PublishersRes> getAllPublishers(String email) {
        checkPermission(email);

        return publishersRepository.findAll()
                .stream()
                .map(publisher -> PublishersRes.builder()
                        .publisherId(publisher.getPublisherId())
                        .publisherName(publisher.getPublisherName())
                        .status(publisher.getStatus())
                        .bookCount(publishersRepository.countBooksByPublisherId(publisher.getPublisherId()))
                        .build()
                )
                .toList();
    }

    public PublishersRes addPublishers(String email, PublishersReq publishersReq) {
        checkPermission(email);

        String name = publishersReq.getPublisherName().trim();

        // Chỉ chặn trùng tên với NXB ACTIVE (không chặn trùng với deleted)
        if (publishersRepository.existsByActivePublisherName(name)) {
            throw new AppException(ErrorCode.PUBLISHER_ALREADY_EXISTED);
        }

        PublisherEntity publisher = new PublisherEntity();
        publisher.setPublisherName(name);
        publisher.setStatus(publishersReq.getStatus() != null ? publishersReq.getStatus() : Status.active);
        publisher.setCreatedAt(LocalDateTime.now());
        publisher.setUpdatedAt(LocalDateTime.now());
        PublisherEntity saved = publishersRepository.save(publisher);

        return PublishersRes.builder()
                .publisherId(saved.getPublisherId())
                .publisherName(saved.getPublisherName())
                .status(saved.getStatus())
                .build();
    }

    public PublishersRes updatePublishers(String email, PublishersReq publishersReq, Integer id) {
        checkPermission(email);

        PublisherEntity publisher = publishersRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.PUBLISHER_NOT_FOUND));

        String newName = publishersReq.getPublisherName().trim();

        Optional<PublisherEntity> existingPublisherWithNewName = publishersRepository.findByPublisherName(newName);

        if (existingPublisherWithNewName.isPresent() &&
                !existingPublisherWithNewName.get().getPublisherId().equals(id)) {
            throw new AppException(ErrorCode.PUBLISHER_ALREADY_EXISTED);
        }
        publisher.setPublisherName(publishersReq.getPublisherName());
        if (publishersReq.getStatus() != null) {
            publisher.setStatus(publishersReq.getStatus());
        }
        publisher.setUpdatedAt(LocalDateTime.now());
        PublisherEntity saved = publishersRepository.save(publisher);
        return PublishersRes.builder()
                .publisherId(saved.getPublisherId())
                .publisherName(saved.getPublisherName())
                .status(saved.getStatus())
                .build();
    }

    /** Soft delete: đổi status → deleted, giữ record trong DB */
    public void deletePublishers(String email, Integer id) {
        checkPermission(email);

        PublisherEntity publisher = publishersRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.PUBLISHER_NOT_FOUND));

        publisher.setStatus(Status.deleted);
        publisher.setUpdatedAt(LocalDateTime.now());

        publishersRepository.save(publisher);
    }

    /** Hard delete: xóa vĩnh viễn — chỉ cho phép khi NXB không có sách nào */
    public void hardDeletePublisher(String email, Integer id) {
        checkPermission(email);

        PublisherEntity publisher = publishersRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.PUBLISHER_NOT_FOUND));

        long bookCount = publishersRepository.countBooksByPublisherId(id);
        if (bookCount > 0) {
            throw new AppException(ErrorCode.PUBLISHER_HAS_BOOKS);
        }

        publishersRepository.delete(publisher);
    }
}
