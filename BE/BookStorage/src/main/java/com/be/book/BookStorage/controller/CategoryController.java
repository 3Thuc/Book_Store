package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Book.BookRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Book.PageRes;
import com.be.book.BookStorage.service.CategoryService;
import lombok.RequiredArgsConstructor;
import java.util.List;

import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/categories")
@RequiredArgsConstructor
public class CategoryController {
    private final CategoryService categoryService;
    @GetMapping
    public ApiResponse<List<CategoryRes>> getCategories() {

        List<CategoryRes> data = categoryService.getUserCategories();
        return ApiResponse.<List<CategoryRes>>builder()
                .result(data)
                .build();
    }
}
