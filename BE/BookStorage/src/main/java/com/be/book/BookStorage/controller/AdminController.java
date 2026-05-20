package com.be.book.BookStorage.controller;



import com.be.book.BookStorage.dto.Request.Admin.*;

import com.be.book.BookStorage.dto.Request.Order.UpdateOrderStatusReq;
import com.be.book.BookStorage.dto.Response.Admin.AuthorRes;
import com.be.book.BookStorage.dto.Response.Admin.InventoryRes;
import com.be.book.BookStorage.dto.Response.Admin.PromotionsRes;
import com.be.book.BookStorage.dto.Response.Admin.PublishersRes;
import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Book.BookRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Book.PageRes;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import com.be.book.BookStorage.dto.Response.Order.PaymentRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import com.be.book.BookStorage.service.*;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.security.core.Authentication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final BookService bookService;
    private final CategoryService categoryService;
    private final PublishersService publishersService;
    private final AuthorService authorService;
    private final UserService userService;
    private final PromotionService promotionService;
    private final InventoryService inventoryService;
    private final OrderService orderService;
    private final CacheManager cacheManager;

    @Value("${ADMIN_API_KEY:}")
    private String adminApiKey;

    /**
     * Internal cache-eviction endpoint dùng bởi Python chatbot service.
     * Xác thực bằng X-Admin-Key header — không cần JWT.
     * Gọi sau khi chatbot UPDATE MySQL trực tiếp để đảm bảo Spring Cache bị reset.
     */
    @PostMapping("/cache/evict")
    public ResponseEntity<?> evictCaches(
            @RequestHeader(value = "X-Admin-Key", required = false) String key
    ) {
        // Fallback key if application.properties doesn't have it
        String expectedKey = (adminApiKey != null && !adminApiKey.isBlank()) 
                             ? adminApiKey : "bookstore-internal-key";

        if (!expectedKey.equals(key)) {
            return ResponseEntity.status(401).body(Map.of("error", "Unauthorized cache eviction"));
        }

        // Clear all caches
        cacheManager.getCacheNames().forEach(cacheName -> {
            var cache = cacheManager.getCache(cacheName);
            if (cache != null) cache.clear();
        });
        return ResponseEntity.ok(Map.of("evicted", cacheManager.getCacheNames(), "status", "ok"));
    }

    @GetMapping("/books")
    public ApiResponse<PageRes<BookRes>> getBooks(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) Long category,
            @RequestParam(required = false) String search
    ) {
        PageRes<BookRes> data = bookService.getAdminBooks(page, limit, category, search);

        return ApiResponse.<PageRes<BookRes>>builder()
                .result(data)
                .build();
    }
    @DeleteMapping("/books/{id}")
    public ResponseEntity<ApiResponse<Void>> deleteBook(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        bookService.deleteBook(email, id);

        return ResponseEntity.ok(
                ApiResponse.<Void>builder()
                        .build()
        );
    }
    @GetMapping("/categories")
    public ResponseEntity<ApiResponse<List<CategoryRes>>> getCategories(Authentication authentication) {
        String email = authentication.getName();
        List<CategoryRes> data = categoryService.getAllCategories(email);

        ApiResponse<List<CategoryRes>> responseBody = ApiResponse.<List<CategoryRes>>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

//    CREATE_CATEGORY: '/admin/categories',
    @PostMapping("/categories")
    public ResponseEntity<ApiResponse<CategoryRes>> addCategory(
            Authentication authentication,
            @RequestBody CategoryReq categoryReq
    ) {
        String email = authentication.getName();
        CategoryRes data = categoryService.addCategory(email,categoryReq);

        ApiResponse<CategoryRes> responseBody = ApiResponse.<CategoryRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);

    }



//    UPDATE_CATEGORY: (id: string) => `/admin/categories/${id}`,
    @PatchMapping("/categories/{id}")
    public ResponseEntity<ApiResponse<CategoryRes>> updateCategory(
            Authentication authentication,
            @RequestBody CategoryReq categoryReq,
            @PathVariable Integer id
    ){
        String email = authentication.getName();
        CategoryRes data = categoryService.updateCategory(email, categoryReq, id);

        ApiResponse<CategoryRes> responseBody = ApiResponse.<CategoryRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

//    DELETE_CATEGORY: (id: string) => `/admin/categories/${id}`,
    @DeleteMapping("/categories/{id}")
    public ResponseEntity<ApiResponse<Void>> deleteCategory(
            Authentication authentication,
            @PathVariable Integer id
    ){
        String email = authentication.getName();
        categoryService.deleteCategory(email, id);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Vô hiệu hóa danh mục thành công")
                .build();

        return ResponseEntity.ok(responseBody);
    }

    /** Xóa vĩnh viễn — chỉ khi danh mục không có sách nào */
    @DeleteMapping("/categories/{id}/permanent")
    public ResponseEntity<ApiResponse<Void>> hardDeleteCategory(
            Authentication authentication,
            @PathVariable Integer id
    ){
        String email = authentication.getName();
        categoryService.hardDeleteCategory(email, id);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Xóa vĩnh viễn danh mục thành công")
                .build();

        return ResponseEntity.ok(responseBody);
    }

    @PostMapping("/books")
    public ResponseEntity<ApiResponse<BookRes>> addBooks(
            @ModelAttribute BookReq request,
            @RequestParam(value = "imageFile", required = false) org.springframework.web.multipart.MultipartFile imageFile,
            Authentication authentication) {
        String email = authentication.getName();
        BookRes data = bookService.addBooks(email, request, imageFile);

        ApiResponse<BookRes> responseBody = ApiResponse.<BookRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @PatchMapping("/books/{id}")
    public ResponseEntity<ApiResponse<BookRes>> updateBook(
            @PathVariable Integer id,
            @ModelAttribute BookReq request,
            @RequestParam(value = "imageFile", required = false) org.springframework.web.multipart.MultipartFile imageFile,
            Authentication authentication) {
        String email = authentication.getName();
        BookRes data = bookService.updateBook(email, id, request, imageFile);

        ApiResponse<BookRes> responseBody = ApiResponse.<BookRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @GetMapping("/publishers")
    public ResponseEntity<ApiResponse<List<PublishersRes>>> getPublishers(Authentication authentication) {
        String email = authentication.getName();
        List<PublishersRes> data = publishersService.getAllPublishers(email);

        ApiResponse<List<PublishersRes>> responseBody = ApiResponse.<List<PublishersRes>>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @PostMapping("/publishers")
    public ResponseEntity<ApiResponse<PublishersRes>> addPublishers(Authentication authentication, @RequestBody PublishersReq publishersReq) {
        String email = authentication.getName();
        PublishersRes data = publishersService.addPublishers(email,publishersReq);

        ApiResponse<PublishersRes> responseBody = ApiResponse.<PublishersRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @PatchMapping("/publishers/{id}")
    public ResponseEntity<ApiResponse<PublishersRes>> updatePublishers(
            Authentication authentication,
            @RequestBody PublishersReq publishersReq,
            @PathVariable Integer id) {
        String email = authentication.getName();
        PublishersRes data = publishersService.updatePublishers(email,publishersReq,id);

        ApiResponse<PublishersRes> responseBody = ApiResponse.<PublishersRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @DeleteMapping("/publishers/{id}")
    public ResponseEntity<ApiResponse<Void>> deletePublishers(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        publishersService.deletePublishers(email, id);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Vô hiệu hóa nhà xuất bản thành công")
                .build();

        return ResponseEntity.ok(responseBody);
    }

    /** Xóa vĩnh viễn NXB — chỉ khi không có sách nào */
    @DeleteMapping("/publishers/{id}/permanent")
    public ResponseEntity<ApiResponse<Void>> hardDeletePublisher(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        publishersService.hardDeletePublisher(email, id);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Xóa vĩnh viễn nhà xuất bản thành công")
                .build();

        return ResponseEntity.ok(responseBody);
    }

    @GetMapping("/authors")
    public ResponseEntity<ApiResponse<List<AuthorRes>>> getAuthors(Authentication authentication) {
        String email = authentication.getName();
        List<AuthorRes> data = authorService.getAllAuthors(email);
        ApiResponse<List<AuthorRes>> responseBody = ApiResponse.<List<AuthorRes>>builder()
                .result(data)
                .build();
        return ResponseEntity
                .ok()
                .body(responseBody);
    }
    @PostMapping("/authors")
    public ResponseEntity<ApiResponse<AuthorRes>> addAuthors(Authentication authentication, @RequestBody AuthorReq authorReq) {
        String email = authentication.getName();
        AuthorRes data = authorService.addAuthor(email,authorReq);

        ApiResponse<AuthorRes> responseBody = ApiResponse.<AuthorRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
    @PatchMapping("/authors/{id}")
    public ResponseEntity<ApiResponse<AuthorRes>> updateAuthor(
            Authentication authentication,
            @RequestBody AuthorReq authorReq,
            @PathVariable Integer id) {
        String email = authentication.getName();
        AuthorRes data = authorService.updateAuthor(email,authorReq,id);

        ApiResponse<AuthorRes> responseBody = ApiResponse.<AuthorRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @DeleteMapping("/authors/{id}")
    public ResponseEntity<ApiResponse<Void>> deleteAuthor(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        authorService.deleteAuthor(email, id);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Vô hiệu hóa tác giả thành công")
                .build();

        return ResponseEntity.ok(responseBody);
    }

    /** Xóa vĩnh viễn tác giả — chỉ khi không có sách nào */
    @DeleteMapping("/authors/{id}/permanent")
    public ResponseEntity<ApiResponse<Void>> hardDeleteAuthor(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        authorService.hardDeleteAuthor(email, id);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Xóa vĩnh viễn tác giả thành công")
                .build();

        return ResponseEntity.ok(responseBody);
    }

    @GetMapping("/users")
    public ResponseEntity<ApiResponse<List<UserRes>>> getListUsers(Authentication authentication) {
        String email = authentication.getName();

        List<UserRes> data = userService.getAllUsers(email);

        return ResponseEntity.ok(
                ApiResponse.<List<UserRes>>builder()
                        .result(data)
                        .build()
        );
    }

    @PatchMapping("/users/{id}")
    public ResponseEntity<ApiResponse<UserRes>> updateUser(
            Authentication authentication,
            @RequestBody UpdateUserRequest request,
            @PathVariable Integer id) {
        String email = authentication.getName();
        UserRes data = userService.updateUser(email, id, request);
        ApiResponse<UserRes> responseBody = ApiResponse.<UserRes>builder()
                .result(data)
                .build();
        return ResponseEntity.ok().body(responseBody);
    }
    @PostMapping("/users")
    public ResponseEntity<ApiResponse<UserRes>> createUser(Authentication authentication, @RequestBody CreateUserRequest request) {
        String email = authentication.getName();

        UserRes data = userService.createUser(email, request);

        ApiResponse<UserRes> responseBody = ApiResponse.<UserRes>builder()
                .result(data)
                .build();

        return ResponseEntity.ok().body(responseBody);
    }

    @PostMapping("/users/{id}/reset-password")
    public ResponseEntity<ApiResponse<UserRes>> adminResetPassUser(
            Authentication authentication,
            @PathVariable Integer id) {
        String email = authentication.getName();
        UserRes data = userService.adminResetPassUser(email, id);
        ApiResponse<UserRes> responseBody = ApiResponse.<UserRes>builder()
                .result(data)
                .build();
        return ResponseEntity.ok().body(responseBody);
    }


    @GetMapping("/promotions")
    public ResponseEntity<ApiResponse<List<PromotionsRes>>> getActivePromotions(
            Authentication authentication) {

        String email = authentication.getName();
        List<PromotionsRes> promotions = promotionService.getListPromotions(email, false);

        return ResponseEntity.ok(
                ApiResponse.<List<PromotionsRes>>builder()
                        .message("Get active promotions successfully")
                        .result(promotions)
                        .build()
        );
    }

//    CREATE_PROMOTION: '/admin/promotions',
    @PostMapping("/promotions")
    public ResponseEntity<ApiResponse<PromotionsRes>> createPromotion(
            Authentication authentication,
            @Valid @RequestBody PromotionReq request) {

        String email = authentication.getName();
        PromotionsRes promotion = promotionService.createPromotion(email, request);

        return ResponseEntity.ok(
                ApiResponse.<PromotionsRes>builder()
                        .result(promotion)
                        .build()
        );
    }

    @PatchMapping("/promotions/{id}")
    public ResponseEntity<ApiResponse<PromotionsRes>> updatePromotion(
            Authentication authentication,
            @PathVariable Integer id,
            @Valid @RequestBody PromotionReq request) {

        String email = authentication.getName();
        PromotionsRes promotion = promotionService.updatePromotion(email, id, request);

        return ResponseEntity.ok(
                ApiResponse.<PromotionsRes>builder()
                        .result(promotion)
                        .build()
        );
    }

    @DeleteMapping("/promotions/{id}")
    public ResponseEntity<ApiResponse<Void>> deletePromotion(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        promotionService.deletePromotion(email, id);

        return ResponseEntity.ok(
                ApiResponse.<Void>builder()
                        .build()
        );
    }

    /** Xóa vĩnh viễn khuyến mãi đã bị vô hiệu hóa khỏi CSDL */
    @DeleteMapping("/promotions/{id}/permanent")
    public ResponseEntity<ApiResponse<Void>> permanentDeletePromotion(
            Authentication authentication,
            @PathVariable Integer id) {

        String email = authentication.getName();
        promotionService.permanentDeletePromotion(email, id);

        return ResponseEntity.ok(
                ApiResponse.<Void>builder()
                        .message("Xóa vĩnh viễn khuyến mãi thành công")
                        .build()
        );
    }

    @GetMapping("/inventory")
    public ResponseEntity<List<InventoryRes>> getInventoryList() {
        List<InventoryRes> inventory = inventoryService.getInventoryList();
        return ResponseEntity.ok(inventory);
    }

    @PatchMapping("/inventory/{id}")
    public ResponseEntity<ApiResponse<InventoryRes>> updateInventory(
            Authentication authentication,
            @PathVariable Integer id,
            @RequestBody InventoryReq request
    ){
        String email = authentication.getName();
        InventoryRes data = inventoryService.updateInventory(email, id, request);

        return ResponseEntity.ok(
                ApiResponse.<InventoryRes>builder()
                        .build()
        );
    }

    @GetMapping("/orders")
    public ResponseEntity<ApiResponse<PageRes<OrderRes>>> getAllOrders(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String search
    ) {
        PageRes<OrderRes> orders = orderService.getAllOrders(page, size, status, search);

        return ResponseEntity.ok(
                ApiResponse.<PageRes<OrderRes>>builder()
                        .result(orders)
                        .build()
        );
    }

    //UPDATE_ORDER_STATUS: (id: string | number) => `/admin/orders/${id}/status`,
    @PatchMapping("/orders/{id}/status")
    public ResponseEntity<ApiResponse<PaymentRes>> updateAdminOrderStatus(
            @PathVariable Integer id,
            Authentication authentication,
            @RequestBody UpdateOrderStatusReq request
    )
    {
        String email = authentication.getName();
        PaymentRes res = orderService.updateAdminOrderStatus(email, id, request);


        ApiResponse<PaymentRes> responseBody = ApiResponse.<PaymentRes>builder()
                .result(res)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);

    }

    @GetMapping("/order-statistics")
    public ResponseEntity<ApiResponse<com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes>> getOrderStatistics() {
        com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes statistics = orderService.getOrderStatistics();
        
        return ResponseEntity.ok(
                ApiResponse.<com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes>builder()
                        .result(statistics)
                        .build()
        );
    }

    /**
     * Dedicated dashboard endpoint that returns all data needed for the statistics dashboard
     * Mỗi service call chạy SONG SONG bằng CompletableFuture → tổng thời gian = slowest query,
     * không phải tổng cộng tất cả query. Một lỗi không làm crash toàn bộ endpoint.
     */
    @GetMapping("/dashboard")
    public ResponseEntity<ApiResponse<com.be.book.BookStorage.dto.Response.Admin.DashboardRes>> getDashboard(
            Authentication authentication) {
        String email = authentication.getName();

        // ── Truyền SecurityContext sang ForkJoinPool threads ──────────────────────
        // CompletableFuture.supplyAsync() dùng ForkJoinPool – thread đó KHÔNG có
        // SecurityContext nên @PreAuthorize sẽ throw Access Denied.
        // DelegatingSecurityContextExecutor tự copy SecurityContext vào mỗi task.
        java.util.concurrent.Executor secureExec =
            new org.springframework.security.concurrent.DelegatingSecurityContextExecutor(
                java.util.concurrent.ForkJoinPool.commonPool()
            );

        // ── Chạy song song toàn bộ queries ──────────────────────────────────────
        var booksFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            // getAdminBooksSlim: không sinh presigned URL → 0 MinIO calls → nhanh 20-50x
            try { return bookService.getAdminBooksSlim(1, Integer.MAX_VALUE, null, null).getBooks(); }
            catch (Exception e) { return java.util.Collections.<BookRes>emptyList(); }
        }, secureExec);

        var catFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try { return categoryService.getAllCategories(email); }
            catch (Exception e) { return java.util.Collections.<com.be.book.BookStorage.dto.Response.Book.CategoryRes>emptyList(); }
        }, secureExec);

        var ordersFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            // getAllOrdersSlim: không sinh presigned URL cho item images → nhanh 10-30x
            try { return orderService.getAllOrdersSlim(1, Integer.MAX_VALUE).getBooks(); }
            catch (Exception e) { return java.util.Collections.<com.be.book.BookStorage.dto.Response.Order.OrderRes>emptyList(); }
        }, secureExec);

        var statsFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try { return orderService.getOrderStatistics(); }
            catch (Exception e) { return (com.be.book.BookStorage.dto.Response.Order.OrderStatisticsRes) null; }
        }, secureExec);

        var pubFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try { return publishersService.getAllPublishers(email); }
            catch (Exception e) { return java.util.Collections.<com.be.book.BookStorage.dto.Response.Admin.PublishersRes>emptyList(); }
        }, secureExec);

        var authorFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try { return authorService.getAllAuthors(email); }
            catch (Exception e) { return java.util.Collections.<com.be.book.BookStorage.dto.Response.Admin.AuthorRes>emptyList(); }
        }, secureExec);

        var promoFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try { return promotionService.getListPromotions(email, false); }
            catch (Exception e) { return java.util.Collections.<com.be.book.BookStorage.dto.Response.Admin.PromotionsRes>emptyList(); }
        }, secureExec);

        var usersFuture = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try { return userService.getAllUsers(email); }
            catch (Exception e) { return java.util.Collections.<com.be.book.BookStorage.dto.Response.User.UserRes>emptyList(); }
        }, secureExec);

        // Inventory ĐƯỢC TÁCH RA khỏi dashboard – nó chạy 4 correlated subqueries × N sách.
        // FE sẽ lazy-load inventory chỉ khi mở tab Quản lý kho.

        // ── Chờ tất cả hoàn thành (tối đa 55 giây) ──
        try {
            java.util.concurrent.CompletableFuture.allOf(
                booksFuture, catFuture, ordersFuture, statsFuture,
                pubFuture, authorFuture, promoFuture, usersFuture
            ).get(55, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // Nếu timeout: lấy kết quả đã xong, còn lại là emptyList
        }

        var dashboard = com.be.book.BookStorage.dto.Response.Admin.DashboardRes.builder()
                .books(safeDone(booksFuture))
                .categories(safeDone(catFuture))
                .orders(safeDone(ordersFuture))
                .orderStatistics(statsFuture.getNow(null))
                .inventory(java.util.Collections.emptyList()) // lazy-loaded từ FE khi mở tab kho
                .publishers(safeDone(pubFuture))
                .authors(safeDone(authorFuture))
                .promotions(safeDone(promoFuture))
                .users(safeDone(usersFuture))
                .build();

        return ResponseEntity.ok(
                ApiResponse.<com.be.book.BookStorage.dto.Response.Admin.DashboardRes>builder()
                        .result(dashboard)
                        .build()
        );
    }

    /** Trả về kết quả nếu Future đã done, không thì emptyList. */
    @SuppressWarnings("unchecked")
    private <T> List<T> safeDone(java.util.concurrent.CompletableFuture<List<T>> future) {
        return future.getNow(java.util.Collections.emptyList());
    }

}
