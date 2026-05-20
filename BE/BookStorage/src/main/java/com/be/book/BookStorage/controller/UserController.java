package com.be.book.BookStorage.controller;



import com.be.book.BookStorage.dto.Request.User.AddAddressReq;
import com.be.book.BookStorage.dto.Request.User.UpdateAddressReq;
import com.be.book.BookStorage.dto.Request.User.UserReq;
import com.be.book.BookStorage.dto.Request.User.ChangePasswordReq;
import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Order.OrderRes;
import com.be.book.BookStorage.dto.Response.User.AddressRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.service.AuthenticationService;
import com.be.book.BookStorage.service.OrderService;
import com.be.book.BookStorage.service.UserService;
import com.be.book.BookStorage.service.PromotionService;
import com.be.book.BookStorage.dto.Response.Admin.PromotionsRes;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/user")
@RequiredArgsConstructor
public class UserController {

    private final AuthenticationService authenticationService;
    private final UserService userService;
    private final OrderService orderService;
    private final PromotionService promotionService;

    @PutMapping("/profile")
    public ResponseEntity<ApiResponse<UserRes>> updateProfile(
            @RequestBody UserReq request,
            Authentication authentication
    ) {
        String email = authentication.getName();

        UserRes updatedUser = userService.updateProfile(email, request);

        ApiResponse<UserRes> responseBody = ApiResponse.<UserRes>builder()
                .result(updatedUser)
                .message("Update profile successfully")
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }
    @PutMapping("/password")
    public ResponseEntity<ApiResponse<?>> changePassword(
            @RequestBody ChangePasswordReq request,
            Authentication authentication
    ) {
        String email = authentication.getName();

        boolean isCurrentPasswordValid = userService.verifyPassword(email, request.getCurrentPassword());
        if (!isCurrentPasswordValid) {
            throw new AppException(ErrorCode.INCORRECT_PASSWORD);
        }
        boolean changePass = userService.changePass(email, request.getNewPassword());
        ApiResponse<?> responseBody = ApiResponse.builder()
                .result(changePass)
                .message("Đổi mật khẩu thành công")
                .build();
        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @GetMapping("/addresses")
    public ResponseEntity<ApiResponse<List<AddressRes>>> getAddresses(Authentication authentication) {
        String email = authentication.getName();


        List<AddressRes> addresses = userService.getListAddress(email);

        ApiResponse<List<AddressRes>> responseBody = ApiResponse.<List<AddressRes>>builder()
                .result(addresses)
                .message("Lấy địa chỉ thành công")
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @PostMapping("/addresses")
    public ResponseEntity<ApiResponse<?>> addAddress(
            @RequestBody AddAddressReq request,
            Authentication authentication
    ) {
        String email = authentication.getName();

        AddressRes newAddress = userService.addAddress(email, request);
        ApiResponse<?> responseBody = ApiResponse.builder()
                .result(newAddress)
                .message("Thêm địa chỉ thành công")
                .build();
        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @PutMapping("/addresses/{id}")
    public ResponseEntity<ApiResponse<?>> updateAddress(
            @PathVariable("id") Integer id,
            @RequestBody UpdateAddressReq request,
            Authentication authentication
    ){
        String email = authentication.getName();

        AddressRes updateAddress = userService.updateAddress(email, id, request);
        ApiResponse<?> response = ApiResponse.builder()
                .result(updateAddress)
                .message("Cập nhật địa chỉ thành công")
                .build();
        return ResponseEntity
                .ok()
                .body(response);
    }

    @DeleteMapping("/addresses/{id}")
    public ResponseEntity<ApiResponse<?>> deleteAddress(@PathVariable("id") Integer id, Authentication authentication) {
        String email = authentication.getName();
        AddressRes deletedAddress = userService.deleteAddressById(email, id);

        ApiResponse<?> response = ApiResponse.builder()
                .result(deletedAddress)
                .message("Xóa địa chỉ thành công")
                .build();

        return ResponseEntity.ok(response);
    }

    @PutMapping("/addresses/{id}/default")
    public ResponseEntity<ApiResponse<?>> updateDefaultAddress(
            @PathVariable("id") Integer id,
            Authentication authentication
    ){
        String email = authentication.getName();

        AddressRes updateAddress = userService.updateDefaultAddress(email, id);
        ApiResponse<?> response = ApiResponse.builder()
                .result(updateAddress)
                .message("Đặt địa chỉ mặc định thành công")
                .build();
        return ResponseEntity
                .ok()
                .body(response);
    }
    @GetMapping("/orders")
    public ResponseEntity<ApiResponse<List<OrderRes>>> getListOrders(Authentication authentication) {
        String email = authentication.getName();
        List<OrderRes> orderRes = userService.getListOrders(email);

        ApiResponse<List<OrderRes>> responseBody = ApiResponse.<List<OrderRes>>builder()
                .result(orderRes)
                .build();

        return ResponseEntity
                .ok()
                .body(responseBody);
    }

    @PostMapping("/orders/{orderId}/cancel")
    public ResponseEntity<ApiResponse<OrderRes>> cancelOrder(
            @PathVariable Integer orderId,
            Authentication authentication
    ) {
        String email = authentication.getName();

        OrderRes data = orderService.cancelOrder(email, orderId);

        ApiResponse<OrderRes> response = ApiResponse.<OrderRes>builder()
                .result(data)
                .build();

        return ResponseEntity.ok(response);
    }

    @PostMapping("/orders/{orderId}/return")
    public ResponseEntity<ApiResponse<OrderRes>> returnOrder(
            @PathVariable Integer orderId,
            @RequestBody Map<String, String> request,
            Authentication authentication
    ) {
        String email = authentication.getName();

        String reason = request.getOrDefault("reason", "");

        OrderRes data = orderService.returnOrder(email, orderId, reason);

        ApiResponse<OrderRes> response = ApiResponse.<OrderRes>builder()
                .result(data)
                .build();

        return ResponseEntity.ok(response);
    }

    @GetMapping("/promotions")
    public ResponseEntity<ApiResponse<List<PromotionsRes>>> getAvailablePromotions() {

        List<PromotionsRes> promotions = promotionService.getAvailablePromotions();
        
        ApiResponse<List<PromotionsRes>> response = ApiResponse.<List<PromotionsRes>>builder()
                .result(promotions)
                .build();
        
        return ResponseEntity.ok(response);
    }

    @PostMapping("/orders/{orderId}/confirm-delivery")
    public ResponseEntity<ApiResponse<OrderRes>> confirmDelivery(
            @PathVariable Integer orderId,
            Authentication authentication
    ) {
        String email = authentication.getName();

        OrderRes data = orderService.confirmDelivery(email, orderId);

        ApiResponse<OrderRes> response = ApiResponse.<OrderRes>builder()
                .result(data)
                .build();
        return ResponseEntity.ok(response);
    }
}