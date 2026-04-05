package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.AuthorReq;
import com.be.book.BookStorage.dto.Request.Admin.PublishersReq;
import com.be.book.BookStorage.dto.Response.Admin.AuthorRes;
import com.be.book.BookStorage.dto.Response.Admin.PublishersRes;
import com.be.book.BookStorage.entity.AuthorEntity;
import com.be.book.BookStorage.entity.PublisherEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.AuthorRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class AuthorService {
    private final AuthorRepository authorRepository;


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

    public List<AuthorRes> getAllAuthors(String email) {
        checkPermission(email);

        return authorRepository.findAll()
                .stream()
                .map(authors -> new AuthorRes(
                        authors.getAuthorId(),
                        authors.getAuthorName(),
                        authors.getBio(),
                        authors.getStatus()
                ))
                .toList();
    }

    public AuthorRes addAuthor(String email, AuthorReq authorReq) {
        checkPermission(email);

        String name = authorReq.getAuthorName().trim();

        if (authorRepository.existsByAuthorName(name)) {
            throw new AppException(ErrorCode.AUTHOR_ALREADY_EXISTED);
        }

        AuthorEntity author = new AuthorEntity();
        author.setAuthorName(name);
        author.setBio(authorReq.getBio());
        author.setCreatedAt(LocalDateTime.now());
        author.setUpdatedAt(LocalDateTime.now());
        author.setStatus(Status.active);
        AuthorEntity saved = authorRepository.save(author);

        return AuthorRes.builder()
                .authorId(saved.getAuthorId())
                .authorName(saved.getAuthorName())
                .bio(saved.getBio())
                .status(saved.getStatus())
                .build();
    }
    public AuthorRes updateAuthor(String email, AuthorReq authorReq, Integer id) {
        checkPermission(email);

        AuthorEntity author = authorRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.AUTHOR_NOT_FOUND));

        String newName = authorReq.getAuthorName().trim();

        Optional<AuthorEntity> existingAuthorWithNewName = authorRepository.findByAuthorName(newName);

        if (existingAuthorWithNewName.isPresent() &&
                !existingAuthorWithNewName.get().getAuthorId().equals(id)) {
            throw new AppException(ErrorCode.AUTHOR_ALREADY_EXISTED);
        }
        author.setAuthorName(authorReq.getAuthorName());
        author.setBio(authorReq.getBio());
        author.setStatus(authorReq.getStatus());
        author.setUpdatedAt(LocalDateTime.now());
        AuthorEntity saved = authorRepository.save(author);
        return AuthorRes.builder()
                .authorId(saved.getAuthorId())
                .authorName(saved.getAuthorName())
                .bio(saved.getBio())
                .status(saved.getStatus())
                .build();
    }

    public void deleteAuthor(String email, Integer id) {
        checkPermission(email);

        AuthorEntity author = authorRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.AUTHOR_NOT_FOUND));

        author.setStatus(Status.deleted);
        author.setUpdatedAt(LocalDateTime.now());

        authorRepository.save(author);
    }

}
