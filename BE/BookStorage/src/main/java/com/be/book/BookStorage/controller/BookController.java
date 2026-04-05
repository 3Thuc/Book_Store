package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Book.BookRes;
import com.be.book.BookStorage.dto.Response.Book.PageRes;

import com.be.book.BookStorage.service.BookService;
import com.be.book.BookStorage.service.UserActionService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable; // 1. Import Pageable
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/books")
@RequiredArgsConstructor
public class BookController {

    private final BookService bookService;
    private final UserActionService userActionService;

    @GetMapping
    public ApiResponse<PageRes<BookRes>> getBooks(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) Long category,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "newest") String sort
    ) {
        PageRes<BookRes> data = bookService.getBooks(page, limit, category, search, sort);

        return ApiResponse.<PageRes<BookRes>>builder()
                .result(data)
                .build();
    }



    @GetMapping("/bestsellers")
    public ApiResponse<List<BookRes>> getBestSellers() {
        List<BookRes> books = bookService.getBooksBestSellers();
        return ApiResponse.<List<BookRes>>builder()
                .result(books)
                .build();
    }

    @GetMapping("/detail/{id}")
    public ApiResponse<BookRes> getBookDetail(@PathVariable Integer id, Authentication authentication) {
        BookRes bookDetail = bookService.getBookDetail(id);
        
        // Log user view action (if authenticated)
        if (authentication != null && authentication.isAuthenticated()) {
            String email = authentication.getName();
            userActionService.logBookView(email, id);
        }
        
        return ApiResponse.<BookRes>builder()
                .result(bookDetail)
                .build();
    }
    @GetMapping("/category/{categoryId}")
    public ApiResponse<PageRes<BookRes>> getBooksByCategory(
            @PathVariable Integer categoryId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int limit
    ) {
        PageRes<BookRes> books = bookService.getBooksByCategory(categoryId, page, limit);
        return ApiResponse.<PageRes<BookRes>>builder()
                .result(books)
                .build();
    }

    @GetMapping("/search")
    public ApiResponse<PageRes<BookRes>> searchBooks(
            @RequestParam String q,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return ApiResponse.<PageRes<BookRes>>builder()
                .result(bookService.searchBooks(q, page, limit))
                .build();
    }

    @GetMapping("/featured")
    public ResponseEntity<Void> handleEmpty() {
        return ResponseEntity.noContent().build();
    }

}