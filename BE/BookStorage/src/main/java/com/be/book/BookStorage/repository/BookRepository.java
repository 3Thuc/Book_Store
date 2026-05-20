package com.be.book.BookStorage.repository;

import com.be.book.BookStorage.dto.Response.Admin.InventoryRes;
import com.be.book.BookStorage.entity.BookEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface BookRepository extends JpaRepository<BookEntity, Integer>, JpaSpecificationExecutor<BookEntity> {

    /**
     * Bước 1/2: Paginates chỉ trên book IDs — không có JOIN FETCH collection.
     * LIMIT/OFFSET được áp dụng đúng tại DB (không phải in-memory).
     * Sửa lỗi Hibernate HHH90003004: firstResult/maxResults with collection fetch.
     */
    @Query(value = """
    SELECT DISTINCT b.bookId FROM BookEntity b
    LEFT JOIN b.categories c
    LEFT JOIN b.author a
    WHERE b.status <> 'deleted'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')) OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :search, '%')))
    ORDER BY b.bookId ASC
    """,
            countQuery = """
    SELECT COUNT(DISTINCT b) FROM BookEntity b
    LEFT JOIN b.categories c
    LEFT JOIN b.author a
    WHERE b.status <> 'deleted'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')) OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :search, '%')))
    """)
    Page<Integer> findBookIdsPaged(
            @Param("category") Long category,
            @Param("search") String search,
            Pageable pageable
    );

    /**
     * Bước 2/2: Fetch chi tiết chỉ cho những ID vừa lấy.
     * An toàn với JOIN FETCH vì không có Pageable.
     */
    @Query("""
    SELECT DISTINCT b FROM BookEntity b
    LEFT JOIN FETCH b.author
    LEFT JOIN FETCH b.publisher
    LEFT JOIN FETCH b.categories
    LEFT JOIN FETCH b.image
    WHERE b.bookId IN :ids
    """)
    List<BookEntity> findByIdsWithDetails(@Param("ids") List<Integer> ids);

    /**
     * Legacy: chỉ dùng khi không cần pagination (v.d. bulk operations).
     * DEPRECATED cho paginated list vì gây in-memory pagination.
     */
    @Query(value = """
    SELECT DISTINCT b FROM BookEntity b
    LEFT JOIN FETCH b.author a
    LEFT JOIN FETCH b.publisher
    LEFT JOIN FETCH b.categories c
    LEFT JOIN FETCH b.image
    WHERE b.status <> 'deleted'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')) OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :search, '%')))
    """,
            countQuery = """
    SELECT COUNT(DISTINCT b) FROM BookEntity b
    LEFT JOIN b.categories c
    LEFT JOIN b.author a
    WHERE b.status <> 'deleted'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')) OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :search, '%')))
    """)
    Page<BookEntity> findAllWithDetails(
            @Param("category") Long category,
            @Param("search") String search,
            Pageable pageable
    );


    @Query(value = """
    SELECT DISTINCT b,
           bi.imageUrl,
           (b.stockQuantity -
            COALESCE((
                SELECT SUM(
                    CASE
                        WHEN o.status IN ('pending','processing','shipped')
                        THEN od.quantity
                        ELSE 0
                    END
                )
                FROM OrderDetailEntity od
                JOIN od.order o
                WHERE od.book.bookId = b.bookId
            ), 0)
           )
    FROM BookEntity b
    LEFT JOIN b.categories c  
    LEFT JOIN b.author a
    LEFT JOIN BookImageEntity bi ON bi.book.bookId = b.bookId AND bi.isMain = true
    WHERE b.status = 'active'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')) OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :search, '%')))
""",
            countQuery = """
    SELECT COUNT(DISTINCT b.bookId)
    FROM BookEntity b
    LEFT JOIN b.categories c
    LEFT JOIN b.author a
    WHERE b.status = 'active'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')) OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :search, '%')))
"""
    )
    Page<Object[]> findUserBooks(@Param("category") Long category,
                                 @Param("search") String search,
                                 Pageable pageable);




    @Query("SELECT b FROM BookEntity b " +
            "LEFT JOIN FETCH b.author " +
            "LEFT JOIN FETCH b.publisher " +
            "LEFT JOIN FETCH b.categories " +
            "WHERE b.bookId = :bookId")
    Optional<BookEntity> findByIdWithDetails(Integer bookId);

    @Query("""
    SELECT COALESCE(SUM(
        CASE
            WHEN od.order.status IN ('pending','processing','shipped')
            THEN od.quantity
            ELSE 0
        END
    ), 0)
    FROM OrderDetailEntity od
    WHERE od.book.bookId = :bookId
    """)
    Integer getReservedQuantity(Integer bookId);



    List<BookEntity> findTop10ByOrderByRatingCountDesc();

    Page<BookEntity> findByCategories_CategoryId(Integer categoryId, Pageable pageable);
    @Query("""
    SELECT DISTINCT b FROM BookEntity b
    LEFT JOIN b.author a
    LEFT JOIN b.categories c
    WHERE LOWER(b.title) LIKE LOWER(CONCAT('%', :keyword, '%'))
       OR LOWER(a.authorName) LIKE LOWER(CONCAT('%', :keyword, '%'))
       OR LOWER(c.categoryName) LIKE LOWER(CONCAT('%', :keyword, '%'))
""")
    List<BookEntity> searchRough(@Param("keyword") String keyword);


    /**
     * Inventory query: ch\u1ec9 l\u1ea5y 3 c\u1ed9t c\u1ea7n thi\u1ebft (bookId, title, stockQuantity).
     * KHÔNG load to\u00e0n b\u1ed9 BookEntity (proxy, lazy-collections, v.v.).
     * Nhanh h\u01a1n findAllActiveBooks ~5-10x v\u1edbi dataset l\u1edbn.
     * Tr\u1ea3 [bookId, title, stockQuantity] theo alphabet.
     */
    @Query("""
        SELECT b.bookId, b.title, COALESCE(b.stockQuantity, 0), i.imageUrl
        FROM BookEntity b
        LEFT JOIN b.image i
        WHERE b.status = 'active'
        ORDER BY b.title ASC
        """)
    List<Object[]> findActiveBooksForInventory();

    /**
     * @deprecated D\u00f9ng findActiveBooksForInventory thay th\u1ebf \u2014 hi\u1ec7u n\u0103ng ~5-10x t\u1ed1t h\u01a1n.
     */
    @Deprecated
    @Query("SELECT b FROM BookEntity b WHERE b.status = 'active' ORDER BY b.title ASC")
    List<BookEntity> findAllActiveBooks();

    /**
     * Bước 2/2: Tính tổng số lượng đã đặt (status pending/processing/shipped) theo từng sách.
     * Trả về [bookId, sumQty] gom theo GROUP BY — 1 query duy nhất cho toàn bộ dataset.
     * Thay thế 4 correlated subqueries × N sách = hạn chế từ O(N) → O(1) về số queries.
     */
    @Query("""
        SELECT od.book.bookId, SUM(od.quantity)
        FROM OrderDetailEntity od
        JOIN od.order o
        WHERE o.status IN ('pending', 'processing', 'shipped')
        GROUP BY od.book.bookId
    """)
    List<Object[]> findOrderedQuantitiesByBookId();


    @Query("""
    SELECT CAST(
        b.stockQuantity - COALESCE(SUM(
            CASE 
                WHEN o.status IN ('pending', 'processing', 'shipped')
                THEN od.quantity 
                ELSE 0 
            END
        ), 0)
    AS integer)
    FROM BookEntity b
    LEFT JOIN OrderDetailEntity od ON od.book = b
    LEFT JOIN od.order o
    WHERE b.bookId = :bookId
    GROUP BY b.bookId, b.stockQuantity
""")
    Integer getAvailableQuantity(@Param("bookId") Integer bookId);
}