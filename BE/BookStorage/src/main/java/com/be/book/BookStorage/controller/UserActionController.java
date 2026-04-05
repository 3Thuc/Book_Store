package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.entity.UserActionEntity;
import com.be.book.BookStorage.enums.ActionType;
import com.be.book.BookStorage.service.UserActionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/user-actions")
@RequiredArgsConstructor
public class UserActionController {

    private final UserActionService userActionService;

    /**
     * Get book action statistics (Admin only)
     */
    @GetMapping("/books/{bookId}/stats")
    public ResponseEntity<ApiResponse<Map<String, Long>>> getBookStats(
            @PathVariable Integer bookId,
            Authentication authentication
    ) {
        String email = authentication.getName();
        Map<String, Long> stats = userActionService.getBookActionStats(bookId);
        
        return ResponseEntity.ok(ApiResponse.<Map<String, Long>>builder()
                .result(stats)
                .build());
    }

    /**
     * Get popular books by views (Admin only)
     */
    @GetMapping("/books/popular")
    public ResponseEntity<ApiResponse<List<Object[]>>> getPopularBooks(Authentication authentication) {
        String email = authentication.getName();
        List<Object[]> popularBooks = userActionService.getPopularBooks();
        
        return ResponseEntity.ok(ApiResponse.<List<Object[]>>builder()
                .result(popularBooks)
                .build());
    }

    /**
     * Get user action history
     */
    @GetMapping("/my-history")
    public ResponseEntity<ApiResponse<List<UserActionEntity>>> getMyActionHistory(Authentication authentication) {
        String email = authentication.getName();
        // Need to get userId from email first
        // This is a simplified version, you may need to adjust based on your UserService
        
        return ResponseEntity.ok(ApiResponse.<List<UserActionEntity>>builder()
                .message("Use /users/me/actions endpoint instead")
                .build());
    }

    /**
     * Get user purchase history
     */
    @GetMapping("/my-purchases")
    public ResponseEntity<ApiResponse<List<UserActionEntity>>> getMyPurchaseHistory(Authentication authentication) {
        String email = authentication.getName();
        // Need to get userId from email first
        
        return ResponseEntity.ok(ApiResponse.<List<UserActionEntity>>builder()
                .message("Use /users/me/purchases endpoint instead")
                .build());
    }

    /**
     * Get recent user actions (last 7 days)
     */
    @GetMapping("/my-recent")
    public ResponseEntity<ApiResponse<List<UserActionEntity>>> getMyRecentActions(Authentication authentication) {
        String email = authentication.getName();
        
        return ResponseEntity.ok(ApiResponse.<List<UserActionEntity>>builder()
                .message("Use /users/me/recent-actions endpoint instead")
                .build());
    }
}
