package com.be.book.BookStorage.dto.Response.Admin;

import com.be.book.BookStorage.dto.Response.Book.BookRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Combined dashboard response containing all data needed for admin statistics
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DashboardRes {
    // Books data
    private List<BookRes> books;
    
    // Categories data
    private List<CategoryRes> categories;
    
    // Orders data
    private List<OrderRes> orders;
    
    // Order statistics (counts by status, total revenue)
    private OrderStatisticsRes orderStatistics;
    
    // Inventory data
    private List<InventoryRes> inventory;
    
    // Publishers data
    private List<PublishersRes> publishers;
    
    // Authors data
    private List<AuthorRes> authors;
    
    // Promotions data
    private List<PromotionsRes> promotions;
}
