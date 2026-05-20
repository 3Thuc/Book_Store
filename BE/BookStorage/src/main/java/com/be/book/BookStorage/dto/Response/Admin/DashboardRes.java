package com.be.book.BookStorage.dto.Response.Admin;

import com.be.book.BookStorage.dto.Response.Book.BookRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Combined dashboard response containing all data needed for admin statistics.
 * All fields fetched in parallel via CompletableFuture in AdminController.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DashboardRes {
    private List<BookRes> books;
    private List<CategoryRes> categories;
    private List<OrderRes> orders;
    private OrderStatisticsRes orderStatistics;
    private List<InventoryRes> inventory;
    private List<PublishersRes> publishers;
    private List<AuthorRes> authors;
    private List<PromotionsRes> promotions;
    // Users: thêm vào dashboard để tránh extra sequential call từ FE
    private List<UserRes> users;
}
