package com.be.book.BookStorage.service;


import com.be.book.BookStorage.dto.Request.Admin.BookReq;
import com.be.book.BookStorage.dto.Response.Book.BookRes;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Book.PageRes;
import com.be.book.BookStorage.dto.Response.Book.PublisherRes;
import com.be.book.BookStorage.entity.*;
import com.be.book.BookStorage.enums.BookFormat;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.repository.*;
import lombok.RequiredArgsConstructor;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.data.domain.*;
import org.springframework.stereotype.Service;


import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class BookService {

    private final BookRepository bookRepository;
    private final UserRepository userRepository;
    private final MinioService minioService;
    private final AuthorRepository  authorRepository;
    private final PublishersRepository publishersRepository;
    private final CategoryRepository categoryRepository;
    private final BookImageRepository bookImageRepository;
    private final RestTemplate restTemplate;

    @Value("${python.search.api.url:http://localhost:8000}")
    private String pythonSearchApiUrl;

    @Value("${ADMIN_API_KEY:ed8fc15c4634bdbdf4fe16135f717ffad99afb49d6c7cd60b089797c7b04aac5}")
    private String adminApiKey;

    private void syncBookToSearch(Integer bookId) {
        syncBookToSearch(bookId, false);
    }

    private void syncBookToSearch(Integer bookId, boolean deleted) {
        CompletableFuture.runAsync(() -> {
            // 1. Đồng bộ vào OpenSearch (text/vector search)
            try {
                String url = pythonSearchApiUrl + "/admin/books/" + bookId + "/sync";
                HttpHeaders headers = new HttpHeaders();
                headers.set("X-Admin-Key", adminApiKey);
                HttpEntity<String> entity = new HttpEntity<>(headers);
                restTemplate.exchange(url, HttpMethod.POST, entity, String.class);
            } catch (Exception e) {
                System.err.println("OpenSearch sync failed for book ID " + bookId + ": " + e.getMessage());
            }

            // 2. Đồng bộ vào OCR visual pHash index (tìm kiếm bằng ảnh bìa)
            try {
                String ocrUrl = "http://localhost:8005/api/ocr/index-book/" + bookId;
                HttpHeaders ocrHeaders = new HttpHeaders();
                ocrHeaders.set("X-Admin-Key", adminApiKey);
                HttpEntity<String> ocrEntity = new HttpEntity<>(ocrHeaders);
                HttpMethod method = deleted ? HttpMethod.DELETE : HttpMethod.POST;
                restTemplate.exchange(ocrUrl, method, ocrEntity, String.class);
            } catch (Exception e) {
                System.err.println("OCR pHash index sync failed for book ID " + bookId + ": " + e.getMessage());
            }
        });
    }

    private UserEntity validateAndGetUser(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        if (user.getStatus() != Status.active) {
            throw new AppException(ErrorCode.USER_INACTIVE);
        }

        return user;
    }

    private BookRes mapToBookRes(BookEntity entity) {
        // getCachedPresignedUrl: first call hits MinIO SDK (slow), subsequent calls instant
        String imageUrl = null;
        if (entity.getImage() != null && entity.getImage().getImageUrl() != null) {
            imageUrl = minioService.getCachedPresignedUrl(entity.getImage().getImageUrl());
        }

        BookRes res = new BookRes(
                entity.getBookId(),
                entity.getTitle(),
                entity.getAuthor() != null ? entity.getAuthor().getAuthorName() : null,
                entity.getPrice(),
                entity.getPublicationYear(),
                entity.getDescription(),
                entity.getAvgRating(),
                entity.getRatingCount(),
                entity.getFormat(),
                entity.getLanguage(),
                entity.getStockQuantity(),
                entity.getAvailableQuantity(),
                entity.getStatus(),
                imageUrl,
                entity.getPublisher() != null ?
                        new PublisherRes(
                                entity.getPublisher().getPublisherId(),
                                entity.getPublisher().getPublisherName()
                        ) : null,
                entity.getCategories() != null ?
                        entity.getCategories().stream()
                                .map(c -> CategoryRes.builder()
                                        .categoryId(c.getCategoryId())
                                        .categoryName(c.getCategoryName())
                                        .status(c.getStatus())
                                        .build())
                                .toList() : List.of()
        );

        return res;
    }


    public PageRes<BookRes> getBooks(int page, int size, Long category, String search, String sort) {
        // Build sort order based on sort parameter
        Sort sortOrder;
        switch (sort) {
            case "rating":
                sortOrder = Sort.by("avgRating").descending();
                break;
            case "newest":
                sortOrder = Sort.by("publicationYear").descending();
                break;
            case "price-asc":
                sortOrder = Sort.by("price").ascending();
                break;
            case "price-desc":
                sortOrder = Sort.by("price").descending();
                break;
            case "name":
                sortOrder = Sort.by("title").ascending();
                break;
            case "popular":
            default:
                // Sort by average rating descending (popularity)
                sortOrder = Sort.by("avgRating").descending();
                break;
        }

        Pageable pageable = PageRequest.of(page - 1, size, sortOrder);

        Page<Object[]> rawPage = bookRepository.findUserBooks(category, search, pageable);

        List<BookRes> books = rawPage.getContent()
                .stream()
                .map(row -> {
                    BookEntity book = (BookEntity) row[0];
                    String imageUrl = (String) row[1];
                    Integer available = ((Number) row[2]).intValue();

                    book.setAvailableQuantity(available);
                    return mapToBookRes(book);
                })
                .toList();

        return new PageRes<>(
                books,
                rawPage.getNumber() + 1,
                rawPage.getSize(),
                rawPage.getTotalElements(),
                rawPage.getTotalPages()
        );
    }


    /**
     * Giống mapToBookRes nhưng BỎ QUA việc lấy presigned URL từ MinIO.
     * Dùng cho dashboard (không hiển thị ảnh trong stats cards).
     * 500 sách → 0 MinIO calls thay vì 500 calls tuần tự.
     */
    private BookRes mapToBookResNoImage(BookEntity entity) {
        BookRes res = new BookRes(
                entity.getBookId(),
                entity.getTitle(),
                entity.getAuthor() != null ? entity.getAuthor().getAuthorName() : null,
                entity.getPrice(),
                entity.getPublicationYear(),
                entity.getDescription(),
                entity.getAvgRating(),
                entity.getRatingCount(),
                entity.getFormat(),
                entity.getLanguage(),
                entity.getStockQuantity(),
                entity.getAvailableQuantity(),
                entity.getStatus(),
                null, // imageUrl = null → FE dùng icon placeholder, không ảnh hưởng stats
                entity.getPublisher() != null ?
                        new PublisherRes(
                                entity.getPublisher().getPublisherId(),
                                entity.getPublisher().getPublisherName()
                        ) : null,
                entity.getCategories() != null ?
                        entity.getCategories().stream()
                                .map(c -> CategoryRes.builder()
                                        .categoryId(c.getCategoryId())
                                        .categoryName(c.getCategoryName())
                                        .status(c.getStatus())
                                        .build())
                                .toList() : List.of()
        );
        return res;
    }

    /**
     * Load toàn bộ sách cho dashboard — KHÔNG sinh presigned URL.
     * Nhanh hơn getAdminBooks ~20-50x với dataset lớn.
     * @Cacheable: lần 2 trở đi trả tức thì từ RAM (~50ms).
     */
    @Cacheable(value = "adminDashboard", key = "'booksSlim'")
    public PageRes<BookRes> getAdminBooksSlim(int page, int size, Long category, String search) {
        Pageable pageable = PageRequest.of(page - 1, size, Sort.by("bookId").ascending());
        Page<BookEntity> bookPage = bookRepository.findAllWithDetails(category, search, pageable);

        List<BookRes> books = bookPage.getContent()
                .stream()
                .map(this::mapToBookResNoImage)
                .toList();

        return new PageRes<>(
                books,
                bookPage.getNumber() + 1,
                bookPage.getSize(),
                bookPage.getTotalElements(),
                bookPage.getTotalPages()
        );
    }

    /**
     * Tr\u1ea3 \u0111\u01b0\u1eddng d\u1eabn \u1ea3nh qua proxy /minio/bookstore/{path}.
     * MinIO bucket \u0111\u00e3 public-read \u2192 Nginx/Vite proxy serve tr\u1ef1c ti\u1ebfp.
     * 0 MinIO SDK calls \u2192 l\u1ea5y trang qu\u1ea3n l\u00fd s\u00e1ch t\u1ee9c th\u00ec.
     */
    private static final String MINIO_PROXY_PREFIX = "/minio/bookstore/";

    private BookRes mapToBookResWithProxyPath(BookEntity entity) {
        String imagePath = entity.getImage() != null && entity.getImage().getImageUrl() != null
                ? entity.getImage().getImageUrl()
                : null;
        String proxyImageUrl = imagePath != null ? MINIO_PROXY_PREFIX + imagePath : null;

        return new BookRes(
                entity.getBookId(),
                entity.getTitle(),
                entity.getAuthor() != null ? entity.getAuthor().getAuthorName() : null,
                entity.getPrice(),
                entity.getPublicationYear(),
                entity.getDescription(),
                entity.getAvgRating(),
                entity.getRatingCount(),
                entity.getFormat(),
                entity.getLanguage(),
                entity.getStockQuantity(),
                entity.getAvailableQuantity(),
                entity.getStatus(),
                proxyImageUrl,
                entity.getPublisher() != null ?
                        new PublisherRes(
                                entity.getPublisher().getPublisherId(),
                                entity.getPublisher().getPublisherName()
                        ) : null,
                entity.getCategories() != null ?
                        entity.getCategories().stream()
                                .map(c -> CategoryRes.builder()
                                        .categoryId(c.getCategoryId())
                                        .categoryName(c.getCategoryName())
                                        .status(c.getStatus())
                                        .build())
                                .toList() : List.of()
        );
    }

    /**
     * Danh s\u00e1ch s\u00e1ch cho admin \u2014 d\u00f9ng proxy URL thay v\u00ec presigned URL.
     * K\u1ebft qu\u1ea3 \u0111\u01b0\u1ee3c cache theo t\u1ed5 h\u1ee3p (page, size, category, search).
     * Hi\u1ec7u su\u1ea5t: 0 MinIO calls, t\u1ed1c \u0111\u1ed9 page load ~5-10x nhanh h\u01a1n.
     */
    @Cacheable(value = "adminBooks", key = "#page + '-' + #size + '-' + #category + '-' + #search")
    public PageRes<BookRes> getAdminBooks(int page, int size, Long category, String search) {
        Pageable pageable = PageRequest.of(page - 1, size); // không cần sort vì query đã ORDER BY b.bookId ASC

        // B\u01b0\u1edbc 1: Paginate ch\u1ec9 tr\u00ean IDs \u2014 DB-level LIMIT/OFFSET, kh\u00f4ng in-memory
        Page<Integer> idsPage = bookRepository.findBookIdsPaged(category, search, pageable);

        if (idsPage.isEmpty()) {
            return new PageRes<>(List.of(), page, size, 0L, 0);
        }

        // B\u01b0\u1edbc 2: Fetch chi ti\u1ebft ch\u1ec9 cho nh\u1eefng ID v\u1eeba l\u1ea5y
        List<Integer> ids = idsPage.getContent();
        List<BookEntity> entities = bookRepository.findByIdsWithDetails(ids);

        // Gi\u1eef \u0111\u00fang th\u1ee9 t\u1ef1 c\u1ee7a trang (sort theo th\u1ee9 t\u1ef1 ID)
        Map<Integer, BookEntity> entityMap = entities.stream()
                .collect(java.util.stream.Collectors.toMap(BookEntity::getBookId, e -> e));

        List<BookRes> books = ids.stream()
                .map(entityMap::get)
                .filter(java.util.Objects::nonNull)
                .map(this::mapToBookResWithProxyPath)
                .toList();

        return new PageRes<>(
                books,
                idsPage.getNumber() + 1,
                idsPage.getSize(),
                idsPage.getTotalElements(),
                idsPage.getTotalPages()
        );
    }


    public BookRes getBookDetail(Integer id) {
        BookEntity entity = bookRepository.findByIdWithDetails(id)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        // Dùng getAvailableQuantity (SQL COALESCE) để tránh NullPointerException khi chưa có đơn nào
        Integer available = bookRepository.getAvailableQuantity(id);
        if (available == null) {
            // Chưa có đơn nào pending/processing/shipped → tồn kho = stockQuantity
            available = entity.getStockQuantity() != null ? entity.getStockQuantity() : 0;
        }
        // Đảm bảo không âm
        available = Math.max(0, available);

        entity.setAvailableQuantity(available);

        BookRes res = mapToBookRes(entity);
        res.setAvailableQuantity(available);

        return res;
    }




    public List<BookRes> getBooksBestSellers() {
        List<BookEntity> entities = bookRepository.findTop10ByOrderByRatingCountDesc();

        return entities.stream()
                .map(this::mapToBookRes)
                .toList();
    }

    public PageRes<BookRes> getBooksByCategory(Integer categoryId, int page, int size) {
        Pageable pageable = PageRequest.of(page - 1, size, Sort.by("bookId").ascending());

        Page<BookEntity> bookPage = bookRepository.findByCategories_CategoryId(categoryId, pageable);

        List<BookRes> books = bookPage.getContent()
                .stream()
                .map(this::mapToBookRes)
                .toList();

        return new PageRes<>(
                books,
                bookPage.getNumber() + 1,
                bookPage.getSize(),
                bookPage.getTotalElements(),
                bookPage.getTotalPages()
        );
    }

    @Caching(evict = {
            @CacheEvict(value = "adminDashboard", key = "'booksSlim'"),
            @CacheEvict(value = "adminBooks", allEntries = true)
    })
    public BookRes addBooks(String email, BookReq bookReq, org.springframework.web.multipart.MultipartFile imageFile) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        AuthorEntity author = authorRepository.findById(bookReq.getAuthorId())
                .orElseThrow(() -> new AppException(ErrorCode.AUTHOR_NOT_FOUND));

        PublisherEntity publisher = null;
        if (bookReq.getPublisherId() != null) {
            publisher = publishersRepository.findById(bookReq.getPublisherId())
                    .orElseThrow(() -> new AppException(ErrorCode.PUBLISHER_NOT_FOUND));
        }

        BookEntity book = BookEntity.builder()
                .title(bookReq.getTitle())
                .author(author)
                .publisher(publisher)
                .description(bookReq.getDescription())
                .price(bookReq.getPrice())
                .stockQuantity(bookReq.getStock() != null ? bookReq.getStock() : 0)
                .publicationYear(bookReq.getPublishedYear())
                .language(bookReq.getLanguage() != null ? bookReq.getLanguage() : "vi")
                .format(bookReq.getFormat() != null ? BookFormat.valueOf(bookReq.getFormat()) : BookFormat.paperback)
                .status(bookReq.getStatus() != null && "active".equalsIgnoreCase(bookReq.getStatus()) ? Status.active : Status.active)
                .avgRating(0.0)
                .ratingCount(0)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        if (bookReq.getCategoryIds() != null && !bookReq.getCategoryIds().isEmpty()) {
            Set<CategoryEntity> categories = new HashSet<>();
            for (Integer categoryId : bookReq.getCategoryIds()) {
                CategoryEntity category = categoryRepository.findById(categoryId)
                        .orElseThrow(() -> new AppException(ErrorCode.CATEGORY_NOT_FOUND));
                categories.add(category);
            }
            book.setCategories(categories);
        }

        BookEntity savedBook = bookRepository.save(book);

        if (imageFile != null && !imageFile.isEmpty()) {
            try {
                String originalFilename = imageFile.getOriginalFilename();
                String extension = originalFilename != null && originalFilename.contains(".")
                        ? originalFilename.substring(originalFilename.lastIndexOf("."))
                        : ".jpg";
                String imagePath = "covers/books/" + savedBook.getBookId() + "/" + savedBook.getBookId() + extension;
                minioService.uploadFile(imageFile, imagePath);
                
                BookImageEntity bookImage = BookImageEntity.builder()
                        .book(savedBook)
                        .imageUrl(imagePath)
                        .isMain(true)
                        .build();
                bookImageRepository.save(bookImage);
            } catch (Exception e) {
                throw new AppException(ErrorCode.UNCATEGORIZED_EXCEPTION);
            }
        } else if (bookReq.getImage() != null) {
            // Create BookImageEntity with URL
            BookImageEntity bookImage = BookImageEntity.builder()
                    .book(savedBook)
                    .imageUrl(bookReq.getImage())
                    .isMain(true)
                    .build();
            bookImageRepository.save(bookImage);
        }

        syncBookToSearch(savedBook.getBookId());

        return mapToBookRes(savedBook);
    }

    @Caching(evict = {
            @CacheEvict(value = "adminDashboard", allEntries = true),
            @CacheEvict(value = "adminBooks", allEntries = true)
    })
    public BookRes updateBook(String email, Integer bookId, BookReq bookReq, org.springframework.web.multipart.MultipartFile imageFile) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getRole() != Role.admin && user.getRole() != Role.staff) {
            throw new AppException(ErrorCode.UNAUTHORIZED);
        }

        BookEntity book = bookRepository.findById(bookId)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        if (bookReq.getAuthorId() != null) {
            AuthorEntity author = authorRepository.findById(bookReq.getAuthorId())
                    .orElseThrow(() -> new AppException(ErrorCode.AUTHOR_NOT_FOUND));
            book.setAuthor(author);
        }

        if (bookReq.getPublisherId() != null) {
            PublisherEntity publisher = publishersRepository.findById(bookReq.getPublisherId())
                    .orElseThrow(() -> new AppException(ErrorCode.PUBLISHER_NOT_FOUND));
            book.setPublisher(publisher);
        }

        if (bookReq.getTitle() != null) book.setTitle(bookReq.getTitle());
        if (bookReq.getDescription() != null) book.setDescription(bookReq.getDescription());
        if (bookReq.getPrice() != null) book.setPrice(bookReq.getPrice());
        if (bookReq.getPublishedYear() != null) book.setPublicationYear(bookReq.getPublishedYear());
        if (bookReq.getLanguage() != null) book.setLanguage(bookReq.getLanguage());
        if (bookReq.getFormat() != null) book.setFormat(BookFormat.valueOf(bookReq.getFormat().toLowerCase()));
        if (bookReq.getStatus() != null) {
            book.setStatus("active".equalsIgnoreCase(bookReq.getStatus()) ? Status.active : Status.deleted);
        }

        if (imageFile != null && !imageFile.isEmpty()) {
            try {
                String originalFilename = imageFile.getOriginalFilename();
                String extension = originalFilename != null && originalFilename.contains(".")
                        ? originalFilename.substring(originalFilename.lastIndexOf("."))
                        : ".jpg";
                String imagePath = "covers/books/" + bookId + "/" + bookId + extension;
                
                System.out.println("Uploading image to MinIO: " + imagePath);
                System.out.println("File size: " + imageFile.getSize());
                System.out.println("Content type: " + imageFile.getContentType());
                
                minioService.uploadFile(imageFile, imagePath);
                
                Optional<BookImageEntity> existingImage = bookImageRepository.findByBook_BookId(bookId);
                if (existingImage.isPresent()) {
                    BookImageEntity bookImage = existingImage.get();
                    String oldImagePath = bookImage.getImageUrl();
                    bookImage.setImageUrl(imagePath);
                    bookImageRepository.save(bookImage);
                    
                    if (!oldImagePath.equals(imagePath)) {
                        try {
                            minioService.deleteFile(oldImagePath);
                        } catch (Exception e) {
                            System.err.println("Failed to delete old image from MinIO: " + e.getMessage());
                        }
                    }
                } else {
                    BookImageEntity newImage = BookImageEntity.builder()
                            .book(book)
                            .imageUrl(imagePath)
                            .isMain(true)
                            .build();
                    bookImageRepository.save(newImage);
                }
            } catch (Exception e) {
                System.err.println("Error updating book image: " + e.getMessage());
                e.printStackTrace();
                throw new AppException(ErrorCode.UNCATEGORIZED_EXCEPTION);
            }
        } else if (bookReq.getImage() != null) {
            Optional<BookImageEntity> existingImage = bookImageRepository.findByBook_BookId(bookId);
            if (existingImage.isPresent()) {
                BookImageEntity bookImage = existingImage.get();
                if (!bookReq.getImage().equals(bookImage.getImageUrl())) {
                    bookImage.setImageUrl(bookReq.getImage());
                    bookImageRepository.save(bookImage);
                }
            } else {
                BookImageEntity bookImage = BookImageEntity.builder()
                        .book(book)
                        .imageUrl(bookReq.getImage())
                        .isMain(true)
                        .build();
                bookImageRepository.save(bookImage);
            }
        }

        if (bookReq.getCategoryIds() != null && !bookReq.getCategoryIds().isEmpty()) {
            Set<CategoryEntity> categories = new HashSet<>();
            for (Integer categoryId : bookReq.getCategoryIds()) {
                CategoryEntity category = categoryRepository.findById(categoryId)
                        .orElseThrow(() -> new AppException(ErrorCode.CATEGORY_NOT_FOUND));
                categories.add(category);
            }
            book.setCategories(categories);
        }

        book.setUpdatedAt(LocalDateTime.now());
        BookEntity updatedBook = bookRepository.save(book);

        syncBookToSearch(updatedBook.getBookId());

        return mapToBookRes(updatedBook);
    }

    public PageRes<BookRes> searchBooks(String rawKeyword, int page, int limit) {
        String keyword = (rawKeyword == null) ? "" : rawKeyword.trim().toLowerCase();
        Pageable pageable = PageRequest.of(page - 1, limit);

        if (keyword.isBlank()) {
            Page<BookEntity> allBooks = bookRepository.findAll(pageable);
            List<BookRes> allData = allBooks.getContent()
                    .stream()
                    .map(this::mapToBookRes)
                    .toList();

            return new PageRes<>(
                    allData,
                    allBooks.getNumber() + 1,
                    allBooks.getSize(),
                    allBooks.getTotalElements(),
                    allBooks.getTotalPages()
            );
        }

        List<String> terms = Arrays.asList(keyword.split("\\s+"));
        LevenshteinDistance distance = new LevenshteinDistance();

        List<BookEntity> rough = bookRepository.searchRough(keyword);

        if (rough.isEmpty()) rough = bookRepository.findAll();

        List<AbstractMap.SimpleEntry<BookEntity, Double>> scored = rough.stream()
                .map(book -> {
                    String combined = String.join(" ",
                            Optional.ofNullable(book.getTitle()).orElse(""),
                            Optional.ofNullable(book.getAuthor())
                                    .map(a -> a.getAuthorName()).orElse(""),
                            book.getCategories() != null
                                    ? book.getCategories().stream()
                                    .map(CategoryEntity::getCategoryName)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.joining(" "))
                                    : ""
                    ).toLowerCase();

                    double score = terms.stream()
                            .mapToDouble(term -> getBestSimilarity(term, combined, distance))
                            .average()
                            .orElse(0);

                    return new AbstractMap.SimpleEntry<>(book, score);
                })
                .filter(e -> e.getValue() > 0.35)
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .toList();

        int start = Math.min((page - 1) * limit, scored.size());
        int end = Math.min(start + limit, scored.size());
        List<BookRes> paged = scored.subList(start, end)
                .stream()
                .map(e -> mapToBookRes(e.getKey()))
                .toList();

        return new PageRes<>(
                paged,
                page,
                limit,
                scored.size(),
                (int) Math.ceil((double) scored.size() / limit)
        );
    }

    private double getBestSimilarity(String term, String text, LevenshteinDistance distance) {
        String[] words = text.split("\\s+");
        return Arrays.stream(words)
                .mapToDouble(w -> computeSimilarity(term, w, distance))
                .max()
                .orElse(0);
    }

    private double computeSimilarity(String s1, String s2, LevenshteinDistance distance) {
        int maxLen = Math.max(s1.length(), s2.length());
        if (maxLen == 0) return 1.0;
        int diff = distance.apply(s1, s2);
        return 1.0 - (double) diff / maxLen;
    }
    @Caching(evict = {
            @CacheEvict(value = "adminDashboard", allEntries = true),
            @CacheEvict(value = "adminBooks", allEntries = true)
    })
    public void deleteBook(String email, Integer id) {
        UserEntity user = validateAndGetUser(email);

        BookEntity book = bookRepository.findById(id)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        if (book.getStatus().equals(Status.deleted)) {
            throw new AppException(ErrorCode.BOOK_ALREADY_DELETED);
        }

        book.setStatus(Status.deleted);
        book.setUpdatedAt(LocalDateTime.now());
        book.setDeletedBy(user.getUserId());
        bookRepository.save(book);

        syncBookToSearch(book.getBookId(), true);  // true = xóa khỏi OCR + đánh dấu deleted trong OpenSearch
    }

}


