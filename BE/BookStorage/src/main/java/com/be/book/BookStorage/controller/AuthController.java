package com.be.book.BookStorage.controller;

import com.be.book.BookStorage.dto.Request.Auth.*;
import com.be.book.BookStorage.dto.Response.ApiResponse;
import com.be.book.BookStorage.dto.Response.Auth.AuthenticationResponse;
import com.be.book.BookStorage.dto.Response.Auth.LoginRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.repository.UserRepository;
import com.be.book.BookStorage.service.AuthenticationService;
import com.be.book.BookStorage.service.UserService;
import com.nimbusds.jose.JOSEException;
import org.springframework.security.core.Authentication;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.text.ParseException;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthenticationService authenticationService;
    private final UserRepository userRepository;
    private final UserService userService;


    @Value("${spring.security.oauth2.client.registration.google.client-id}")
    private String GOOGLE_CLIENT_ID;

    @Value("${app.frontend-url}")
    private String frontendUrl;


    @PostMapping("/login")
    public ResponseEntity<ApiResponse<LoginRes>> login(@RequestBody LoginReq request) {

        LoginRes res = authenticationService.login(request);

        ApiResponse<LoginRes> responseBody = ApiResponse.<LoginRes>builder()
                .result(res)
                .message("Login successfully")
                .build();

        return ResponseEntity
                .ok()
                .header(HttpHeaders.SET_COOKIE, res.getRefreshCookie().toString())
                .body(responseBody);
    }

    @PostMapping("/login/google")
    public ResponseEntity<ApiResponse<LoginRes>> loginWithGoogle(@RequestBody GoogleLoginReq request)
            throws ParseException, JOSEException {

        LoginRes res = authenticationService.loginWithGoogle(request.getCode());

        ApiResponse<LoginRes> responseBody = ApiResponse.<LoginRes>builder()
                .result(res)
                .message("Login with Google successfully")
                .build();

        return ResponseEntity
                .ok()
                .header(HttpHeaders.SET_COOKIE, res.getRefreshCookie().toString())
                .body(responseBody);
    }

    @PostMapping("/register")
    public ResponseEntity<ApiResponse<Void>> register(@RequestBody RegisterReq request) {
        authenticationService.register(request);

        ApiResponse<Void> responseBody = ApiResponse.<Void>builder()
                .message("Register successfully. Please check your email to verify your account.")
                .build();

        return ResponseEntity.ok(responseBody);
    }


    @GetMapping("/verify-email")
    public void verifyEmail(@RequestParam String token, HttpServletResponse response) throws IOException {
        try {
            String email = authenticationService.verifyEmailToken(token);
            UserEntity user = authenticationService.getUserByEmail(email);
            if (user == null) {
                response.sendRedirect(frontendUrl + "/verify?status=error&message=user_not_found");
                return;
            }

            if (user.getStatus() == Status.active) {
                response.sendRedirect(frontendUrl + "/verify?status=error&message=user_already_active");
                return;
            }

            user.setStatus(Status.active);
            userRepository.save(user);

            response.sendRedirect(frontendUrl + "/login?verified=true");

        } catch (AppException e) {
            switch (e.getErrorCode()) {
                case TOKEN_INVALID:
                    response.sendRedirect(frontendUrl + "/verify?status=error&message=token_invalid");
                    break;
                case TOKEN_EXPIRED:
                    response.sendRedirect(frontendUrl + "/verify?status=error&message=token_expired");
                    break;
                case USER_NOT_FOUND:
                    response.sendRedirect(frontendUrl + "/verify?status=error&message=user_not_found");
                    break;
                case USER_INACTIVE:
                    response.sendRedirect(frontendUrl + "/verify?status=error&message=user_inactive");
                    break;
                default:
                    response.sendRedirect(frontendUrl + "/verify?status=error&message=unknown_error");
            }
        } catch (Exception e) {
            response.sendRedirect(frontendUrl + "/verify?status=error&message=unknown_error");
        }
    }



    @GetMapping("/google")
    public void redirectToGoogle(HttpServletResponse response) throws IOException {
        String redirectUri = "http://localhost:3000/bookdb/auth/login/google";
        String scope = "openid profile email";

        String url = "https://accounts.google.com/o/oauth2/v2/auth" +
                "?client_id=" + GOOGLE_CLIENT_ID +
                "&redirect_uri=" + redirectUri +
                "&response_type=code" +
                "&scope=" + scope;

        response.sendRedirect(url);
    }


    @PostMapping("/logout")
    public ResponseEntity<ApiResponse<Void>> logout(HttpServletRequest request) throws ParseException, JOSEException {
        ResponseCookie cookie = authenticationService.logout(request);
        return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, cookie.toString())
                .body(ApiResponse.<Void>builder()
                        .message("Logout successfully")
                        .build());
    }
    @PostMapping("/refresh-token")
    public ResponseEntity<ApiResponse<AuthenticationResponse>> refreshToken(
            @CookieValue(name = "refresh_token", required = false) String refreshToken
    ) throws ParseException, JOSEException {

        if (refreshToken == null || refreshToken.isBlank()) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(ApiResponse.<AuthenticationResponse>builder()
                            .result(AuthenticationResponse.builder()
                                    .authenticated(false)
                                    .build())
                            .build());
        }

        AuthenticationResponse authResponse = authenticationService.refreshToken(refreshToken);

        ApiResponse<AuthenticationResponse> body = ApiResponse.<AuthenticationResponse>builder()
                .message("Refresh token successfully")
                .result(authResponse)
                .build();

        return ResponseEntity.ok()
                .header(HttpHeaders.SET_COOKIE, authResponse.getRefreshCookie().toString())
                .body(body);
    }


    @PostMapping("/forgot-password")
    public ApiResponse<String> forgotPassword(@RequestBody ForgotPassReq request) {
        authenticationService.resetPassword(request.getEmail());
        return ApiResponse.<String>builder()
                .message("Yêu cầu đặt lại mật khẩu thành công. Vui lòng kiểm tra email của bạn.")
                .build();
    }

    @GetMapping("/me")
    public ResponseEntity<ApiResponse<UserRes>> infoUser(Authentication authentication) {
        String email = authentication.getName();

        UserRes userInfo = authenticationService.getUserInfo(email);

        ApiResponse<UserRes> response = ApiResponse.<UserRes>builder()
                .result(userInfo)
                .message("Lấy thông tin người dùng thành công")
                .build();

        return ResponseEntity.ok(response);
    }
}
