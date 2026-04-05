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
import org.springframework.data.domain.*;
import org.springframework.stereotype.Service;


import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

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
        String imageUrl = null;
        if (entity.getImage() != null && entity.getImage().getImageUrl() != null) {
            try {
                String rawImageUrl = entity.getImage().getImageUrl();
                imageUrl = minioService.getPresignedUrl(rawImageUrl);
                System.out.println("[BookService] Book " + entity.getBookId() + " (" + entity.getTitle() + ") - Raw image: " + rawImageUrl + " -> Presigned: " + imageUrl);
            } catch (Exception e) {
                System.err.println("[BookService] Error getting presigned URL for book " + entity.getBookId() + " (" + entity.getTitle() + "): " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("[BookService] Book " + entity.getBookId() + " (" + entity.getTitle() + ") - NO IMAGE (entity.image=" + entity.getImage() + ")");
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
                                .map(c -> new CategoryRes(c.getCategoryId(), c.getCategoryName(), c.getStatus()))
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


    public PageRes<BookRes> getAdminBooks(int page, int size, Long category, String search) {

        Pageable pageable = PageRequest.of(page - 1, size, Sort.by("bookId").ascending());

        Page<BookEntity> bookPage = bookRepository.findAllWithDetails(category, search, pageable);

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


    public BookRes getBookDetail(Integer id) {
        BookEntity entity = bookRepository.findByIdWithDetails(id)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        Integer reserved = bookRepository.getReservedQuantity(id);
        Integer available = entity.getStockQuantity() - reserved;

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

        return mapToBookRes(savedBook);
    }

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
    }

}


