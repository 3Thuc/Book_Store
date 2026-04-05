package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Cart.CartReq;
import com.be.book.BookStorage.dto.Response.Cart.CartItemRes;
import com.be.book.BookStorage.dto.Response.Cart.CartRes;
import com.be.book.BookStorage.entity.BookEntity;
import com.be.book.BookStorage.entity.CartEntity;
import com.be.book.BookStorage.entity.CartItemEntity;
import com.be.book.BookStorage.entity.Key.CartItemKey;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.BookRepository;
import com.be.book.BookStorage.repository.CartItemRepository;
import com.be.book.BookStorage.repository.CartRepository;
import com.be.book.BookStorage.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CartService {

    private final CartRepository cartRepository;
    private final UserRepository userRepository;
    private final BookRepository bookRepository;
    private final CartItemRepository cartItemRepository;
    private final UserActionService userActionService;

    private UserEntity validateAndGetUser(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getStatus() != Status.active) {
            throw new AppException(ErrorCode.USER_INACTIVE);
        }

        return user;
    }

    @Transactional
    public CartRes getUserCart(String email) {
        UserEntity user = validateAndGetUser(email);

        CartEntity cart = cartRepository.findByUser_UserId(user.getUserId())
                .orElseGet(() -> createEmptyCart(user.getUserId()));

        List<CartItemRes> itemResponses = cart.getItems() != null ?
                cart.getItems().stream()
                        .map(this::mapToCartItemResponse)
                        .collect(Collectors.toList()) :
                new ArrayList<>();

        double totalPrice = itemResponses.stream()
                .mapToDouble(CartItemRes::getSubtotal)
                .sum();

        int totalItems = itemResponses.stream()
                .mapToInt(CartItemRes::getQuantity)
                .sum();

        return CartRes.builder()
                .cartId(cart.getCartId())
                .userId(cart.getUser().getUserId())
                .userName(cart.getUser().getUsername())
                .createdAt(cart.getCreatedAt())
                .updatedAt(cart.getUpdatedAt())
                .items(itemResponses)
                .totalPrice(totalPrice)
                .totalItems(totalItems)
                .build();
    }

    @Transactional
    public CartEntity createEmptyCart(Integer userId) {
        UserEntity user = userRepository.findById(userId)
                .orElseThrow(() -> new RuntimeException("User not found with id: " + userId));

        CartEntity cart = CartEntity.builder()
                .user(user)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .items(new ArrayList<>())
                .build();

        return cartRepository.save(cart);
    }

    private CartItemRes mapToCartItemResponse(CartItemEntity item) {
        double subtotal = item.getBook().getPrice() * item.getQuantity();

        return CartItemRes.builder()
                .bookId(item.getBook().getBookId())
                .bookTitle(item.getBook().getTitle())
                .authorName(item.getBook().getAuthor() != null ?
                        item.getBook().getAuthor().getAuthorName() : null)
                .publisherName(item.getBook().getPublisher() != null ?
                        item.getBook().getPublisher().getPublisherName() : null)
                .price(item.getBook().getPrice())
                .quantity(item.getQuantity())
                .subtotal(subtotal)
                .images(item.getBook().getImage().getImageUrl())
                .format(item.getBook().getFormat())
                .avgRating(item.getBook().getAvgRating())
                .stockQuantity(item.getBook().getStockQuantity())
                .build();
    }
//    PRODUCT_OUT_OF_STOCK
//    INSUFFICIENT_STOCK
//    BOOK_NOT_FOUND
    @Transactional
    public CartRes addCartItem(String email, CartReq request) {
        UserEntity user = validateAndGetUser(email);
        CartEntity cart = cartRepository.findByUser_UserId(user.getUserId())
                .orElseGet(() -> createEmptyCart(user.getUserId()));
        BookEntity book = bookRepository.findById(request.getBookId())
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        Integer availableQuantity = bookRepository.getAvailableQuantity(book.getBookId());
        if (availableQuantity == null) {
            availableQuantity = book.getStockQuantity();
        }

        if (availableQuantity < request.getQuantity()) {
            throw new AppException(ErrorCode.INSUFFICIENT_STOCK);
        }

        CartItemKey cartItemKey = new CartItemKey(cart.getCartId(), book.getBookId());
        Optional<CartItemEntity> existingItem = cartItemRepository.findById(cartItemKey);

        if (existingItem.isPresent()) {
            CartItemEntity item = existingItem.get();
            int newQuantity = item.getQuantity() + request.getQuantity();

            // Kiểm tra lại với số lượng có sẵn
            if (availableQuantity < newQuantity) {
                throw new AppException(ErrorCode.INSUFFICIENT_STOCK);
            }

            item.setQuantity(newQuantity);
            cartItemRepository.save(item);
        } else {
            CartItemEntity newItem = CartItemEntity.builder()
                    .id(cartItemKey)
                    .cart(cart)
                    .book(book)
                    .quantity(request.getQuantity())
                    .build();
            cartItemRepository.save(newItem);
        }

        cart.setUpdatedAt(LocalDateTime.now());
        cartRepository.save(cart);

        // Log add to cart action
        userActionService.logAddToCart(user.getEmail(), book.getBookId(), request.getQuantity());

        return getUserCart(user.getEmail());
    }

    @Transactional
    public CartRes updateCartItem(String email, Integer bookId, CartReq request) {
        UserEntity user = validateAndGetUser(email);

        CartEntity cart = cartRepository.findByUser_UserId(user.getUserId())
                .orElseThrow(() -> new AppException(ErrorCode.CART_NOT_FOUND));

        BookEntity book = bookRepository.findById(bookId)
                .orElseThrow(() -> new AppException(ErrorCode.BOOK_NOT_FOUND));

        Integer availableQuantity = bookRepository.getAvailableQuantity(bookId);
        if (availableQuantity == null) {
            availableQuantity = book.getStockQuantity();
        }

        if (availableQuantity < request.getQuantity()) {
            throw new AppException(ErrorCode.INSUFFICIENT_STOCK);
        }

        CartItemKey cartItemKey = new CartItemKey(cart.getCartId(), bookId);
        CartItemEntity cartItem = cartItemRepository.findById(cartItemKey)
                .orElseThrow(() -> new AppException(ErrorCode.CART_ITEM_NOT_FOUND));

        cartItem.setQuantity(request.getQuantity());
        cartItemRepository.save(cartItem);

        cart.setUpdatedAt(LocalDateTime.now());
        cartRepository.save(cart);

        return getUserCart(user.getEmail());
    }

    @Transactional
    public CartRes removeCartItem(String email, Integer bookId) {
        UserEntity user = validateAndGetUser(email);

        CartEntity cart = cartRepository.findByUser_UserId(user.getUserId())
                .orElseThrow(() -> new AppException(ErrorCode.CART_NOT_FOUND));

        CartItemKey cartItemKey = new CartItemKey(cart.getCartId(), bookId);
        CartItemEntity cartItem = cartItemRepository.findById(cartItemKey)
                .orElseThrow(() -> new AppException(ErrorCode.CART_ITEM_NOT_FOUND));

        cartItemRepository.delete(cartItem);

        cart.setUpdatedAt(LocalDateTime.now());
        cartRepository.save(cart);

        return getUserCart(user.getEmail());
    }

    @Transactional
    public CartRes clearCartItem(String email){
        UserEntity user = validateAndGetUser(email);

        CartEntity cart = cartRepository.findByUser_UserId(user.getUserId())
                .orElseThrow(() -> new AppException(ErrorCode.CART_NOT_FOUND));

        cartItemRepository.deleteByCart_CartId(cart.getCartId());

        cart.setUpdatedAt(LocalDateTime.now());
        cartRepository.save(cart);

        return getUserCart(user.getEmail());
    }


}