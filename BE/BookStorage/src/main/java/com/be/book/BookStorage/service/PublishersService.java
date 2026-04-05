package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.AuthorReq;
import com.be.book.BookStorage.dto.Request.Admin.PublishersReq;
import com.be.book.BookStorage.dto.Response.Admin.AuthorRes;
import com.be.book.BookStorage.dto.Response.Admin.PublishersRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.entity.AuthorEntity;
import com.be.book.BookStorage.entity.PublisherEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.CategoryRepository;
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

        if (user.getRole() != Role.admin && user.getRole() != Role.staff ) {
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
                .map(publisher -> new PublishersRes(
                        publisher.getPublisherId(),
                        publisher.getPublisherName(),
                        publisher.getStatus()
                ))
                .toList();
    }

    public PublishersRes addPublishers(String email, PublishersReq publishersReq) {
        checkPermission(email);

        String name = publishersReq.getPublisherName().trim();

        if (publishersRepository.existsByPublisherName(name)) {
            throw new AppException(ErrorCode.PUBLISHER_ALREADY_EXISTED);
        }

        PublisherEntity publisher = new PublisherEntity();
        publisher.setPublisherName(name);
        publisher.setCreatedAt(LocalDateTime.now());
        publisher.setUpdatedAt(LocalDateTime.now());
        PublisherEntity saved = publishersRepository.save(publisher);

        return PublishersRes.builder()
                .publisherId(saved.getPublisherId())
                .publisherName(saved.getPublisherName())
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
        publisher.setStatus(publishersReq.getStatus());
        publisher.setUpdatedAt(LocalDateTime.now());
        PublisherEntity saved = publishersRepository.save(publisher);
        return PublishersRes.builder()
                .publisherId(saved.getPublisherId())
                .publisherName(saved.getPublisherName())
                .status(saved.getStatus())
                .build();
    }

    public void deletePublishers(String email, Integer id) {
        checkPermission(email);

        PublisherEntity publisher = publishersRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.PUBLISHER_NOT_FOUND));

        publisher.setStatus(Status.deleted);
        publisher.setUpdatedAt(LocalDateTime.now());

        publishersRepository.save(publisher);
    }
}

