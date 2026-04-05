package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.entity.UserActionEntity;
import com.be.book.BookStorage.enums.ActionType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface UserActionRepository extends JpaRepository<UserActionEntity, Integer> {
    
    /**
     * Find all actions by user
     */
    List<UserActionEntity> findByUser_UserIdOrderByActionDateDesc(Integer userId);
    
    /**
     * Find all actions by user and action type
     */
    List<UserActionEntity> findByUser_UserIdAndActionTypeOrderByActionDateDesc(Integer userId, ActionType actionType);
    
    /**
     * Find all actions by book
     */
    List<UserActionEntity> findByBook_BookIdOrderByActionDateDesc(Integer bookId);
    
    /**
     * Find recent actions by user
     */
    @Query("SELECT ua FROM UserActionEntity ua WHERE ua.user.userId = :userId " +
           "AND ua.actionDate >= :startDate ORDER BY ua.actionDate DESC")
    List<UserActionEntity> findRecentActionsByUser(
            @Param("userId") Integer userId, 
            @Param("startDate") LocalDateTime startDate);
    
    /**
     * Count actions by book and type
     */
    @Query("SELECT COUNT(ua) FROM UserActionEntity ua WHERE ua.book.bookId = :bookId " +
           "AND ua.actionType = :actionType")
    Long countByBookAndActionType(
            @Param("bookId") Integer bookId, 
            @Param("actionType") ActionType actionType);
    
    /**
     * Find popular books based on views
     */
    @Query("SELECT ua.book.bookId, COUNT(ua) as viewCount FROM UserActionEntity ua " +
           "WHERE ua.actionType = 'view' AND ua.actionDate >= :startDate " +
           "GROUP BY ua.book.bookId ORDER BY viewCount DESC")
    List<Object[]> findPopularBooksByViews(@Param("startDate") LocalDateTime startDate);
    
    /**
     * Find user purchase history
     */
    @Query("SELECT ua FROM UserActionEntity ua WHERE ua.user.userId = :userId " +
           "AND ua.actionType = 'purchase' ORDER BY ua.actionDate DESC")
    List<UserActionEntity> findUserPurchaseHistory(@Param("userId") Integer userId);
}
