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

    @Query(value = """
    SELECT DISTINCT b FROM BookEntity b
    LEFT JOIN FETCH b.author
    LEFT JOIN FETCH b.publisher
    LEFT JOIN FETCH b.categories c
    LEFT JOIN FETCH b.image
    WHERE (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')))
    """,
            countQuery = """
    SELECT COUNT(DISTINCT b) FROM BookEntity b
    LEFT JOIN b.categories c
    WHERE (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')))
    """)
    Page<BookEntity> findAllWithDetails(
            Long category,
            String search,
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
    LEFT JOIN BookImageEntity bi ON bi.book.bookId = b.bookId AND bi.isMain = true
    WHERE b.status = 'active'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')))
""",
            countQuery = """
    SELECT COUNT(DISTINCT b.bookId)
    FROM BookEntity b
    LEFT JOIN b.categories c
    WHERE b.status = 'active'
      AND (:category IS NULL OR c.categoryId = :category)
      AND (:search IS NULL OR LOWER(b.title) LIKE LOWER(CONCAT('%', :search, '%')))
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


    @Query("""
    SELECT new com.be.book.BookStorage.dto.Response.Admin.InventoryRes(
        b.bookId,
        b.title,
        b.stockQuantity,

        /* Đã đặt - dùng subquery dựa trên order.status (bao gồm COD) */
        CAST(
            COALESCE((
                SELECT SUM(od2.quantity)
                FROM OrderDetailEntity od2
                JOIN od2.order o2
                WHERE od2.book = b
                  AND o2.status IN ('pending', 'processing', 'shipped')
            ), 0) AS integer
        ),

        /* Có sẵn */
        CAST(
            b.stockQuantity - COALESCE((
                SELECT SUM(od2.quantity)
                FROM OrderDetailEntity od2
                JOIN od2.order o2
                WHERE od2.book = b
                  AND o2.status IN ('pending', 'processing', 'shipped')
            ), 0) AS integer
        ),

        5,

        /* Trạng thái */
        CASE
            WHEN (b.stockQuantity - COALESCE((
                SELECT SUM(od2.quantity)
                FROM OrderDetailEntity od2
                JOIN od2.order o2
                WHERE od2.book = b
                  AND o2.status IN ('pending', 'processing', 'shipped')
            ), 0)) <= 0
                THEN 'Hết hàng'
            WHEN (b.stockQuantity - COALESCE((
                SELECT SUM(od2.quantity)
                FROM OrderDetailEntity od2
                JOIN od2.order o2
                WHERE od2.book = b
                  AND o2.status IN ('pending', 'processing', 'shipped')
            ), 0)) <= 5
                THEN 'Cần nhập thêm'
            ELSE 'Đầy đủ'
        END
    )
    FROM BookEntity b
    ORDER BY b.title ASC
""")
    List<InventoryRes> getInventoryList();

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