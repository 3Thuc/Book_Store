package com.be.book.BookStorage.service;


import com.be.book.BookStorage.dto.Request.Admin.InventoryReq;
import com.be.book.BookStorage.dto.Response.Admin.InventoryRes;
import com.be.book.BookStorage.entity.BookEntity;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.BookRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class InventoryService {
    private final BookRepository bookRepository;
    private final UserRepository userRepository;

    private void checkPermission(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff ) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }
        if (user.getStatus() != Status.active) {
            throw new AppException(ErrorCode.USER_INACTIVE);
        }
    }

    /**
     * Tải danh sách kho hiệu quả: 2 queries thay vì O(N×4) correlated subqueries.
     *
     *  Query 1: SELECT bookId, title, stockQuantity FROM books WHERE status = 'active'
     *  Query 2: SELECT book_id, SUM(qty) FROM order_details JOIN orders
     *           WHERE status IN ('pending','processing','shipped') GROUP BY book_id
     *  Merge trong Java với HashMap O(1) lookup.
     *
     * Lưu ý: @Cacheable đã bị gỡ bỏ vì xung đột AOP proxy với @EnableMethodSecurity
     * trong Spring Boot 3.x → AuthorizationDeniedException tại runtime.
     * 2 queries projection nhẹ, không cần cache.
     */
    private static final String MINIO_PROXY_PREFIX = "/minio/bookstore/";

    public List<InventoryRes> getInventoryList() {
        // Query 1: Chỉ lấy 4 cột cần thiết (bookId, title, stockQuantity, imageUrl) dưới dạng Object[]
        List<Object[]> bookRows = bookRepository.findActiveBooksForInventory();

        // Query 2: Tổng số lượng đã đặt theo book_id — 1 DB round-trip
        Map<Integer, Integer> orderedQtyMap = new HashMap<>();
        for (Object[] row : bookRepository.findOrderedQuantitiesByBookId()) {
            Integer bookId = ((Number) row[0]).intValue();
            Long    sumQty = (Long) row[1];
            orderedQtyMap.put(bookId, sumQty.intValue());
        }

        // Merge trong Java — 0 DB round-trips thêm
        return bookRows.stream()
                .map(row -> {
                    int bookId    = ((Number) row[0]).intValue();
                    String title  = (String) row[1];
                    int stock     = ((Number) row[2]).intValue();
                    // row[3] = imageUrl path từ book_images (có thể null nếu sách không có ảnh)
                    String rawImagePath = row.length > 3 ? (String) row[3] : null;
                    String imageUrl = rawImagePath != null ? MINIO_PROXY_PREFIX + rawImagePath : null;

                    int ordered   = orderedQtyMap.getOrDefault(bookId, 0);
                    int available = stock - ordered;
                    String status = available <= 0 ? "Hết hàng"
                                  : available <= 5 ? "Cần nhập thêm"
                                  : "Đầy đủ";
                    return InventoryRes.builder()
                            .bookId(bookId)
                            .title(title)
                            .stockQuantity(stock)
                            .orderedQuantity(ordered)
                            .availableQuantity(available)
                            .threshold(5)
                            .status(status)
                            .imageUrl(imageUrl)
                            .build();
                })
                .collect(Collectors.toList());
    }


    public InventoryRes updateInventory(String email, Integer id, InventoryReq inventoryReq) {
        checkPermission(email);
        BookEntity book = bookRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        book.setStockQuantity(inventoryReq.getStockQuantity());
        book.setUpdatedAt(LocalDateTime.now());
        BookEntity saved = bookRepository.save(book);
        return InventoryRes.builder()
                .bookId(saved.getBookId())
                .title(saved.getTitle())
                .stockQuantity(saved.getStockQuantity())
                .build();
    }
}
