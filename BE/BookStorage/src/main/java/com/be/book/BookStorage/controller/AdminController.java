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
import org.springframework.security.core.Authentication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;


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
                .message("Xóa thể loại thành công")
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
                .message("Xóa nhà xuất bản thành công")
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
                .message("Xóa tác giả thành công")
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
        List<PromotionsRes> promotions = promotionService.getListPromotions(email, true);

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
     * This includes: books, categories, orders, order statistics, inventory, publishers, authors, promotions
     */
    @GetMapping("/dashboard")
    public ResponseEntity<ApiResponse<com.be.book.BookStorage.dto.Response.Admin.DashboardRes>> getDashboard(
            Authentication authentication) {
        String email = authentication.getName();

        // Fetch all data in parallel streams for better performance
        var books = bookService.getAdminBooks(1, 5000, null, null).getBooks();
        var categories = categoryService.getAllCategories(email);
        var orders = orderService.getAllOrders(1, 10000, null, null).getBooks(); // reuse for all items
        var orderStats = orderService.getOrderStatistics();
        var inventory = inventoryService.getInventoryList();
        var publishers = publishersService.getAllPublishers(email);
        var authors = authorService.getAllAuthors(email);
        var promotions = promotionService.getListPromotions(email, false); // get all, not just active

        var dashboard = com.be.book.BookStorage.dto.Response.Admin.DashboardRes.builder()
                .books(books)
                .categories(categories)
                .orders(orders)
                .orderStatistics(orderStats)
                .inventory(inventory)
                .publishers(publishers)
                .authors(authors)
                .promotions(promotions)
                .build();

        return ResponseEntity.ok(
                ApiResponse.<com.be.book.BookStorage.dto.Response.Admin.DashboardRes>builder()
                        .result(dashboard)
                        .build()
        );
    }

}
