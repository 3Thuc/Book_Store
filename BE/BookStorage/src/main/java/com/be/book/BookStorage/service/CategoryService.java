package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.CategoryReq;
import com.be.book.BookStorage.dto.Response.Admin.PublishersRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.entity.CategoryEntity;
import com.be.book.BookStorage.entity.PublisherEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.CategoryRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class CategoryService {
    private final CategoryRepository categoryRepository;
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

    public List<CategoryRes> getAllCategories(String email) {
        checkPermission(email);
        return categoryRepository.findAll()
                .stream()
                .map(category -> CategoryRes.builder()
                        .categoryId(category.getCategoryId())
                        .categoryName(category.getCategoryName())
                        .build())
                .toList();
    }
    public List<CategoryRes> getUserCategories() {
        return categoryRepository.findAllActive()
                .stream()
                .map(category -> CategoryRes.builder()
                        .categoryId(category.getCategoryId())
                        .categoryName(category.getCategoryName())
                        .build())
                .toList();
    }

    public CategoryRes addCategory(String email, CategoryReq categoryReq){
        checkPermission(email);
        String name = categoryReq.getCategoryName().trim();

        if (categoryRepository.existsByCategoryName(name)) {
            throw new AppException(ErrorCode.CATEGORY_ALREADY_EXISTED);
        }

        CategoryEntity category = new CategoryEntity();
        category.setCategoryName(name);
        category.setStatus(categoryReq.getStatus());
        category.setCreatedAt(LocalDateTime.now());
        category.setUpdatedAt(LocalDateTime.now());
        CategoryEntity saved = categoryRepository.save(category);

        return CategoryRes.builder()
                .categoryId(saved.getCategoryId())
                .categoryName(saved.getCategoryName())
                .build();
    }
    public CategoryRes updateCategory(String email, CategoryReq categoryReq, Integer id){
        checkPermission(email);

        CategoryEntity category = categoryRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.CATEGORY_NOT_FOUND));

        String newName = categoryReq.getCategoryName().trim();
        Optional<CategoryEntity> existingCategoryWithNewName = categoryRepository.findByCategoryName(newName);

        if (existingCategoryWithNewName.isPresent() &&
                !existingCategoryWithNewName.get().getCategoryId().equals(id)) {
            throw new AppException(ErrorCode.CATEGORY_ALREADY_EXISTED);
        }
        category.setCategoryName(categoryReq.getCategoryName());
        category.setStatus(categoryReq.getStatus());
        category.setCreatedAt(LocalDateTime.now());
        category.setUpdatedAt(LocalDateTime.now());
        CategoryEntity saved = categoryRepository.save(category);
        return CategoryRes.builder()
                .categoryId(saved.getCategoryId())
                .categoryName(saved.getCategoryName())
                .status(saved.getStatus())
                .build();
    }

    public void deleteCategory(String email, Integer id){
        checkPermission(email);
        CategoryEntity category = categoryRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.CATEGORY_NOT_FOUND));
        category.setStatus(Status.deleted);
        category.setUpdatedAt(LocalDateTime.now());
        categoryRepository.save(category);
    }
}
