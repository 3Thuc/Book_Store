package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Admin.CategoryReq;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.entity.CategoryEntity;
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

        if (user.getRole() != Role.admin && user.getRole() != Role.staff) {
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
                        .status(category.getStatus())
                        .bookCount(categoryRepository.countBooksByCategoryId(category.getCategoryId()))
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

    public CategoryRes addCategory(String email, CategoryReq categoryReq) {
        checkPermission(email);
        String name = categoryReq.getCategoryName().trim();

        // Chỉ chặn trùng tên với danh mục ACTIVE (không chặn trùng với deleted)
        if (categoryRepository.existsByActiveCategoryName(name)) {
            throw new AppException(ErrorCode.CATEGORY_ALREADY_EXISTED);
        }

        CategoryEntity category = new CategoryEntity();
        category.setCategoryName(name);
        category.setStatus(categoryReq.getStatus() != null ? categoryReq.getStatus() : Status.active);
        category.setCreatedAt(LocalDateTime.now());
        category.setUpdatedAt(LocalDateTime.now());
        CategoryEntity saved = categoryRepository.save(category);

        return CategoryRes.builder()
                .categoryId(saved.getCategoryId())
                .categoryName(saved.getCategoryName())
                .status(saved.getStatus())
                .build();
    }

    public CategoryRes updateCategory(String email, CategoryReq categoryReq, Integer id) {
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
        if (categoryReq.getStatus() != null) {
            category.setStatus(categoryReq.getStatus());
        }
        category.setUpdatedAt(LocalDateTime.now());
        CategoryEntity saved = categoryRepository.save(category);
        return CategoryRes.builder()
                .categoryId(saved.getCategoryId())
                .categoryName(saved.getCategoryName())
                .status(saved.getStatus())
                .build();
    }

    /** Soft delete: đổi status → deleted, giữ record trong DB */
    public void deleteCategory(String email, Integer id) {
        checkPermission(email);
        CategoryEntity category = categoryRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.CATEGORY_NOT_FOUND));
        category.setStatus(Status.deleted);
        category.setUpdatedAt(LocalDateTime.now());
        categoryRepository.save(category);
    }

    /** Hard delete: xóa vĩnh viễn — chỉ cho phép khi danh mục không có sách nào */
    public void hardDeleteCategory(String email, Integer id) {
        checkPermission(email);
        CategoryEntity category = categoryRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.CATEGORY_NOT_FOUND));

        long bookCount = categoryRepository.countBooksByCategoryId(id);
        if (bookCount > 0) {
            throw new AppException(ErrorCode.CATEGORY_HAS_BOOKS);
        }

        categoryRepository.delete(category);
    }
}
