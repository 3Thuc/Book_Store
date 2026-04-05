package com.be.book.BookStorage.service;

import com.be.book.BookStorage.entity.BookEntity;
import com.be.book.BookStorage.entity.UserActionEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.ActionType;
import com.be.book.BookStorage.repository.BookRepository;
import com.be.book.BookStorage.repository.UserActionRepository;
import com.be.book.BookStorage.repository.UserRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserActionService {

    private final UserActionRepository userActionRepository;
    private final UserRepository userRepository;
    private final BookRepository bookRepository;
    private final ObjectMapper objectMapper;

    /**
     * Log user action asynchronously
     */
    @Async
    @Transactional
    public void logAction(String email, Integer bookId, ActionType actionType, Map<String, Object> metadata) {
        try {
            UserEntity user = userRepository.findByEmail(email)
                    .orElseThrow(() -> new RuntimeException("User not found: " + email));
            
            BookEntity book = bookRepository.findById(bookId)
                    .orElseThrow(() -> new RuntimeException("Book not found: " + bookId));
            
            String metadataJson = null;
            if (metadata != null && !metadata.isEmpty()) {
                metadataJson = objectMapper.writeValueAsString(metadata);
            }
            
            UserActionEntity action = UserActionEntity.builder()
                    .user(user)
                    .book(book)
                    .actionType(actionType)
                    .metadata(metadataJson)
                    .actionDate(LocalDateTime.now())
                    .build();
            
            userActionRepository.save(action);
            log.info("User action logged: user={}, book={}, action={}", email, bookId, actionType);
            
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize metadata: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Failed to log user action: {}", e.getMessage(), e);
        }
    }

    /**
     * Log book view
     */
    public void logBookView(String email, Integer bookId) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp", LocalDateTime.now().toString());
        logAction(email, bookId, ActionType.view, metadata);
    }

    /**
     * Log add to cart
     */
    public void logAddToCart(String email, Integer bookId, Integer quantity) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("quantity", quantity);
        metadata.put("timestamp", LocalDateTime.now().toString());
        logAction(email, bookId, ActionType.add_to_cart, metadata);
    }

    /**
     * Log purchase
     */
    public void logPurchase(String email, Integer bookId, Integer quantity, Double price) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("quantity", quantity);
        metadata.put("price", price);
        metadata.put("timestamp", LocalDateTime.now().toString());
        logAction(email, bookId, ActionType.purchase, metadata);
    }

    /**
     * Log wishlist action
     */
    public void logWishlistAction(String email, Integer bookId) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("timestamp", LocalDateTime.now().toString());
        logAction(email, bookId, ActionType.wishlist, metadata);
    }

    /**
     * Get user action history
     */
    @Transactional(readOnly = true)
    public List<UserActionEntity> getUserActions(Integer userId) {
        return userActionRepository.findByUser_UserIdOrderByActionDateDesc(userId);
    }

    /**
     * Get user actions by type
     */
    @Transactional(readOnly = true)
    public List<UserActionEntity> getUserActionsByType(Integer userId, ActionType actionType) {
        return userActionRepository.findByUser_UserIdAndActionTypeOrderByActionDateDesc(userId, actionType);
    }

    /**
     * Get book action statistics
     */
    @Transactional(readOnly = true)
    public Map<String, Long> getBookActionStats(Integer bookId) {
        Map<String, Long> stats = new HashMap<>();
        stats.put("views", userActionRepository.countByBookAndActionType(bookId, ActionType.view));
        stats.put("addToCart", userActionRepository.countByBookAndActionType(bookId, ActionType.add_to_cart));
        stats.put("purchases", userActionRepository.countByBookAndActionType(bookId, ActionType.purchase));
        stats.put("wishlist", userActionRepository.countByBookAndActionType(bookId, ActionType.wishlist));
        return stats;
    }

    /**
     * Get user purchase history
     */
    @Transactional(readOnly = true)
    public List<UserActionEntity> getUserPurchaseHistory(Integer userId) {
        return userActionRepository.findUserPurchaseHistory(userId);
    }

    /**
     * Get popular books by views (last 30 days)
     */
    @Transactional(readOnly = true)
    public List<Object[]> getPopularBooks() {
        LocalDateTime thirtyDaysAgo = LocalDateTime.now().minusDays(30);
        return userActionRepository.findPopularBooksByViews(thirtyDaysAgo);
    }

    /**
     * Get recent user actions (last 7 days)
     */
    @Transactional(readOnly = true)
    public List<UserActionEntity> getRecentUserActions(Integer userId) {
        LocalDateTime sevenDaysAgo = LocalDateTime.now().minusDays(7);
        return userActionRepository.findRecentActionsByUser(userId, sevenDaysAgo);
    }
}
