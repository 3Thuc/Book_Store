package com.be.book.BookStorage.controller;


import com.be.book.BookStorage.dto.Request.Admin.CategoryReq;
import com.be.book.BookStorage.dto.Request.Cart.CartReq;
import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Book.CategoryRes;
import com.be.book.BookStorage.dto.Response.Cart.CartRes;
import com.be.book.BookStorage.service.CartService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/cart")
@RequiredArgsConstructor
public class CartController {

    private final CartService cartService;

    @GetMapping
    public ResponseEntity<ApiResponse<CartRes>> getUserCart(Authentication authentication) {
        String email = authentication.getName();
        CartRes cart = cartService.getUserCart(email);
        ApiResponse<CartRes> responseBody = ApiResponse.<CartRes>builder()
                .result(cart)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
    @PostMapping("/items")
    public ResponseEntity<ApiResponse<CartRes>> addCartItem(
            Authentication authentication,
            @RequestBody CartReq request
            )
    {
        String email = authentication.getName();
        CartRes data = cartService.addCartItem(email,request);

        ApiResponse<CartRes> responseBody = ApiResponse.<CartRes>builder()
                .result(data)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);

    }
    @PutMapping("/items/{itemId}")
    public ResponseEntity<ApiResponse<CartRes>> updateCartItem(
            Authentication authentication,
            @PathVariable Integer itemId,
            @RequestBody CartReq request
    ){
        String email = authentication.getName();
        CartRes cart = cartService.updateCartItem(email, itemId, request);
        ApiResponse<CartRes> responseBody = ApiResponse.<CartRes>builder()
                .result(cart)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
    @DeleteMapping("/items/{itemId}")
    public ResponseEntity<ApiResponse<CartRes>> removeCartItem(
            Authentication authentication,
            @PathVariable Integer itemId
    ){
        String email = authentication.getName();
        CartRes cart = cartService.removeCartItem(email, itemId);
        ApiResponse<CartRes> responseBody = ApiResponse.<CartRes>builder()
                .result(cart)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
    @DeleteMapping("/clear")
    public ResponseEntity<ApiResponse<CartRes>> clearCartItem(
            Authentication authentication
    ){
        String email = authentication.getName();
        CartRes cart = cartService.clearCartItem(email);
        ApiResponse<CartRes> responseBody = ApiResponse.<CartRes>builder()
                .result(cart)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
}
