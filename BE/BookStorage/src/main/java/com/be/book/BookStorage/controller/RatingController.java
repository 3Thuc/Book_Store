package com.be.book.BookStorage.controller;

//
///books/${bookId}/reviews

import com.be.book.BookStorage.dto.Request.Book.RatingReq;
import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Book.RatingRes;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import com.be.book.BookStorage.service.BookService;
import com.be.book.BookStorage.service.OrderService;
import com.be.book.BookStorage.service.RatingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.awt.print.Book;
import java.util.List;

@RestController
@RequestMapping("/books")
@RequiredArgsConstructor
public class RatingController {

    public final RatingService ratingService;

    @GetMapping("/{bookId}/reviews")
    public ResponseEntity<ApiResponse<List<RatingRes>>> getAllBookRatings(@PathVariable Integer bookId) {
        List<RatingRes> ratings = ratingService.getAllBookRatings(bookId);

        ApiResponse<List<RatingRes>> responseBody = ApiResponse.<List<RatingRes>>builder()
                .result(ratings)
                .build();
        return ResponseEntity.ok()
                .body(responseBody);
    }
//
//    CREATE: (bookId: string) => `/books/${bookId}/reviews`,
//    UPDATE: (bookId: string, reviewId: string) => `/books/${bookId}/reviews/${reviewId}`,
    @PostMapping("/{bookId}/reviews")
    public ResponseEntity<ApiResponse<RatingRes>> createRating(
            @PathVariable Integer bookId,
            @RequestBody RatingReq ratingReq,
            Authentication authentication
    )
    {
        String email = authentication.getName();

        RatingRes data = ratingService.createRating(email, bookId, ratingReq);

        ApiResponse<RatingRes> response = ApiResponse.<RatingRes>builder()
                .result(data)
                .build();

        return ResponseEntity.ok(response);
    }
    @PutMapping("/{bookId}/reviews/{reviewId}")
    public ResponseEntity<ApiResponse<RatingRes>> updateRating(
            @PathVariable Integer bookId,
            @PathVariable Integer reviewId,
            @RequestBody RatingReq ratingReq,
            Authentication authentication
    )
    {
        String email = authentication.getName();

        RatingRes data = ratingService.updateRating(email, bookId, reviewId, ratingReq);

        ApiResponse<RatingRes> response = ApiResponse.<RatingRes>builder()
                .result(data)
                .build();

        return ResponseEntity.ok(response);
    }

}
