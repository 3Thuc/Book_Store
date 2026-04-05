package com.be.book.BookStorage.service;

import com.be.book.BookStorage.dto.Request.Auth.*;
import com.be.book.BookStorage.dto.Response.Auth.AuthenticationResponse;
import com.be.book.BookStorage.dto.Response.Auth.IntrospectRes;
import com.be.book.BookStorage.dto.Response.Auth.LoginRes;
import com.be.book.BookStorage.dto.Response.User.UserRes;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.entity.InvalidatedToken;
import com.be.book.BookStorage.enums.Role;
import com.be.book.BookStorage.enums.Status;
import com.be.book.BookStorage.events.GoogleUserCreatedEvent;
import com.be.book.BookStorage.events.GoogleUserForgotPasswordEvent;
import com.be.book.BookStorage.exception.AppException;
import com.be.book.BookStorage.exception.ErrorCode;
import com.be.book.BookStorage.repository.InvalidatedTokenRepositoty;
import com.be.book.BookStorage.repository.UserRepository;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.be.book.BookStorage.events.UserRegisteredEvent;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Service
@RequiredArgsConstructor
@Slf4j
public class AuthenticationService {

    private final UserRepository userRepository;
    private final InvalidatedTokenRepositoty invalidatedTokenRepositoty;
    private final AuthenticationManager authenticationManager;
    private final PasswordEncoder passwordEncoder;
    private final ApplicationEventPublisher eventPublisher;
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${jwt.signerKey}")
    protected String SIGNER_KEY;

    @Value("${jwt.access-token.expiration}")
    protected long VALID_DURATION;

    @Value("${jwt.refresh-token.expiration}")
    protected long REFRESHABLE_DURATION;

    @Value("${spring.security.oauth2.client.registration.google.client-id}")
    private String googleClientId;

    @Value("${spring.security.oauth2.client.registration.google.client-secret}")
    private String googleClientSecret;

    @Value("${google.redirect-uri}")
    private String googleRedirectUri;



    public LoginRes login(LoginReq request) {
        try {
            authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(request.getEmail(), request.getPassword())
            );
        } catch (BadCredentialsException ex) {
            throw new AppException(ErrorCode.INVALID_CREDENTIALS);
        }

        UserEntity user = userRepository.findByEmail(request.getEmail())
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        if (user.getStatus() == Status.unverified) {
            throw new AppException(ErrorCode.UNAUTHENTICATED);
        }
        if (user.getStatus() == Status.deleted) {
            throw new AppException(ErrorCode.USER_NOT_FOUND);
        }

        return buildLoginResponse(user);
    }


    public LoginRes loginWithGoogle(String code) throws ParseException, JOSEException {

        String idToken = exchangeCodeForIdToken(code);
        GoogleIdToken.Payload payload = verifyGoogleToken(idToken);
        String email = payload.getEmail();
        String fullName = (String) payload.get("name");
        String passwordRandom = UUID.randomUUID().toString().substring(0, 10);

        Optional<UserEntity> optionalUser = userRepository.findByEmail(email);
        UserEntity user;

        if (optionalUser.isPresent()) {
            user = optionalUser.get();
        } else {
            String randomPassword = UUID.randomUUID().toString().substring(0, 10);
            user = UserEntity.builder()
                    .email(email)
                    .fullName(fullName)
                    .passwordHash(passwordEncoder.encode(randomPassword))
                    .role(Role.customer)
                    .status(Status.active)
                    .username(fullName)
                    .build();

            userRepository.save(user);

            eventPublisher.publishEvent(new GoogleUserCreatedEvent(this, user, randomPassword));
        }
        if (user.getStatus() == Status.unverified) {
            user.setStatus(Status.active);
            userRepository.save(user);
        }
        if (user.getStatus() == Status.deleted) {
            throw new AppException(ErrorCode.USER_NOT_FOUND);
        }

        return buildLoginResponse(user);
    }


    private LoginRes buildLoginResponse(UserEntity user) {
        String accessToken = generateAccessToken(user);
        String refreshToken = generateRefreshToken(user);
        ResponseCookie refreshCookie = createRefreshTokenCookie(refreshToken);

        return LoginRes.builder()
                .id(user.getUserId())
                .userName(user.getUsername())
                .phoneNumber(user.getPhone())
                .fullName(user.getFullName())
                .email(user.getEmail())
                .role(user.getRole())
                .status(user.getStatus())
                .token(accessToken)
                .refreshCookie(refreshCookie)
                .build();
    }

    public String generateAccessToken(UserEntity user) {
        return generateToken(user, false);
    }

    public String generateRefreshToken(UserEntity user) {
        return generateToken(user, true);
    }

    private String generateToken(UserEntity user, boolean isRefresh) {
        try {
            JWSHeader jwsHeader = new JWSHeader(JWSAlgorithm.HS512);
            Instant now = Instant.now();
            Date expiry = isRefresh
                    ? Date.from(now.plus(REFRESHABLE_DURATION, ChronoUnit.SECONDS))
                    : Date.from(now.plus(VALID_DURATION, ChronoUnit.SECONDS));
//            Date expiry = isRefresh
//                    ? Date.from(now.plus(1, ChronoUnit.HOURS))
//                    : Date.from(now.plusSeconds(3));
            JWTClaimsSet jwtClaimsSet = new JWTClaimsSet.Builder()
                    .subject(user.getEmail())
                    .issuer("OmoonO")
                    .issueTime(Date.from(now))
                    .expirationTime(expiry)
                    .jwtID(UUID.randomUUID().toString())
                    .claim("scope", isRefresh ? "ROLE_REFRESH" : "ROLE_USER")
                    .claim("userId", user.getUserId())
                    .build();

            Payload payload = new Payload(jwtClaimsSet.toJSONObject());
            JWSObject jwsObject = new JWSObject(jwsHeader, payload);
            jwsObject.sign(new MACSigner(SIGNER_KEY.getBytes()));

            return jwsObject.serialize();
        } catch (JOSEException e) {
            log.error("Cannot sign JWT object", e);
            throw new RuntimeException("Cannot generate JWT", e);
        }
    }

    public GoogleIdToken.Payload verifyGoogleToken(String token) throws ParseException, JOSEException {
        try {
            GoogleIdTokenVerifier verifier = new GoogleIdTokenVerifier.Builder(new NetHttpTransport(), JacksonFactory.getDefaultInstance())
                    .setAudience(Collections.singletonList(googleClientId))
                    .build();

            GoogleIdToken idToken = verifier.verify(token);
            if (idToken != null) {
                return idToken.getPayload();
            } else {
                throw new AppException(ErrorCode.UNAUTHENTICATED);
            }
        } catch (Exception e) {
            throw new AppException(ErrorCode.UNAUTHENTICATED);
        }
    }

    private String exchangeCodeForIdToken(String code) {
        String tokenUri = "https://oauth2.googleapis.com/token";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("code", code);
        body.add("client_id", googleClientId);
        body.add("client_secret", googleClientSecret);
        body.add("redirect_uri", googleRedirectUri);
        body.add("grant_type", "authorization_code");

        HttpEntity<MultiValueMap<String, String>> requestEntity = new HttpEntity<>(body, headers);

        try {
            log.info("Đang đổi code lấy token từ Google...");

            ResponseEntity<com.be.book.BookStorage.dto.Response.Auth.GoogleTokenResponse> response = restTemplate.postForEntity(
                    tokenUri,
                    requestEntity,
                    com.be.book.BookStorage.dto.Response.Auth.GoogleTokenResponse.class
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                log.info("Đổi code thành công, nhận được id_token.");
                return response.getBody().getIdToken();
            } else {
                log.error("Failed to exchange code for token. Response Status: {}", response.getStatusCode());
                throw new AppException(ErrorCode.UNAUTHENTICATED, "Failed to exchange Google code.");
            }
        } catch (Exception e) {
            log.error("Error exchanging Google code (có thể do lỗi deserialize): {}", e.getMessage());
            log.error("Chi tiết lỗi deserialize: ", e);
            throw new AppException(ErrorCode.UNAUTHENTICATED, e.getMessage());
        }
    }

    @Transactional
    public void register(RegisterReq req) {
        String username = (req.getFullName() != null && !req.getFullName().trim().isEmpty()) 
                ? req.getFullName() 
                : req.getEmail().split("@")[0];
        
        UserEntity user = userRepository.findByEmail(req.getEmail())
                .map(u -> {
                    if (u.getStatus() == Status.active) {
                        throw new AppException(ErrorCode.USER_EXISTED);
                    }
                    u.setFullName(req.getFullName());
                    u.setUsername(username);
                    u.setPasswordHash(passwordEncoder.encode(req.getPassword()));
                    u.setStatus(Status.unverified);
                    u.setRole(Role.customer);
                    u.setUpdatedAt(LocalDateTime.now());
                    return u;
                })
                .orElseGet(() -> UserEntity.builder()
                        .email(req.getEmail())
                        .fullName(req.getFullName())
                        .username(username)
                        .passwordHash(passwordEncoder.encode(req.getPassword()))
                        .status(Status.unverified)
                        .role(Role.customer)
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .build()
                );

        userRepository.save(user);

        eventPublisher.publishEvent(new UserRegisteredEvent(this, user));
    }

    public String verifyEmailToken(String token) {
        try {
            JWSObject jwsObject = JWSObject.parse(token);

            JWSVerifier verifier = new MACVerifier(SIGNER_KEY.getBytes());
            if (!jwsObject.verify(verifier)) {
                throw new AppException(ErrorCode.TOKEN_INVALID);
            }

            JWTClaimsSet claims = JWTClaimsSet.parse(jwsObject.getPayload().toJSONObject());

            if (!"EMAIL_VERIFICATION".equals(claims.getStringClaim("purpose"))) {
                throw new AppException(ErrorCode.TOKEN_INVALID);
            }

            if (claims.getExpirationTime().before(new Date())) {
                throw new AppException(ErrorCode.TOKEN_EXPIRED);
            }

            return claims.getSubject();

        } catch (ParseException | JOSEException e) {
            throw new AppException(ErrorCode.TOKEN_INVALID);
        } catch (AppException e) {
            throw e;
        } catch (Exception e) {
            throw new AppException(ErrorCode.UNCATEGORIZED_EXCEPTION);
        }
    }

    public UserEntity getUserByToken(String token) {
        try {
            SignedJWT verified = verifyToken(token, false);
            String email = verified.getJWTClaimsSet().getSubject();
            return userRepository.findByEmail(email).orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));
        } catch (Exception e) {
            log.error("Cannot parse token to get user: {}", e.getMessage());
            throw new AppException(ErrorCode.UNAUTHENTICATED);
        }
    }

    public ResponseCookie createRefreshTokenCookie(String refreshToken) {
        return ResponseCookie.from("refresh_token", refreshToken)
                .httpOnly(true)
                .path("/")
                .sameSite("None")
                .secure(true)
                .maxAge(7 * 24 * 60 * 60)
                .build();
    }

    public ResponseCookie clearRefreshTokenCookie() {
        return ResponseCookie.from("refresh_token", "")
                .httpOnly(true)
                .path("/")
                .maxAge(0)
                .sameSite("None")
                .secure(true)
                .build();
    }

    public IntrospectRes introspect(IntrospectReq request) throws JOSEException, ParseException {
        var token = request.getToken();
        boolean isValid = true;
        try {
            verifyToken(token, false);
        } catch (AppException e) {
            isValid = false;
        }
        return IntrospectRes.builder()
                .valid(isValid)
                .build();
    }

    @Transactional
    public ResponseCookie logout(HttpServletRequest request) throws ParseException, JOSEException {
        String refreshToken = Arrays.stream(Optional.ofNullable(request.getCookies()).orElse(new Cookie[0]))
                .filter(c -> "refresh_token".equals(c.getName()))
                .map(Cookie::getValue)
                .findFirst()
                .orElse(null);
        log.info("Giá trị refreshToken sau khi lọc với tên 'refreshToken': {}", refreshToken);
        if (refreshToken != null) {
            try {
                var signToken = verifyToken(refreshToken, true);
                String jwtId = signToken.getJWTClaimsSet().getJWTID();
                Date expiryTime = signToken.getJWTClaimsSet().getExpirationTime();

                InvalidatedToken invalidatedToken = InvalidatedToken.builder()
                        .id(jwtId)
                        .expiryTime(expiryTime)
                        .build();

                invalidatedTokenRepositoty.save(invalidatedToken);

            } catch (AppException e) {
                log.warn("Refresh token already expired or invalid", e);
            }
        }
        return clearRefreshTokenCookie();
    }


    public UserRes getUserInfo(String email) {
        UserEntity user = userRepository.findByEmail(email)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        return UserRes.builder()
                .id(user.getUserId())
                .userName(user.getUsername())
                .email(user.getEmail())
                .fullName(user.getFullName())
                .phoneNumber(user.getPhone())
                .role(user.getRole())
                .status(user.getStatus())
                .createdAt(user.getCreatedAt())
                .build();
    }

    private SignedJWT verifyToken(String token, boolean isRefresh) throws ParseException, JOSEException, AppException {
        SignedJWT signedJWT = SignedJWT.parse(token);

        JWSVerifier verifier = new MACVerifier(SIGNER_KEY.getBytes());

        boolean isVerified = signedJWT.verify(verifier);
        if (!isVerified) {
            throw new AppException(ErrorCode.UNAUTHENTICATED);
        }

        Date expiryTime = signedJWT.getJWTClaimsSet().getExpirationTime();
        if (expiryTime.before(new Date())) {
            throw new AppException(ErrorCode.UNAUTHENTICATED);
        }

        String expectedScope = isRefresh ? "ROLE_REFRESH" : "ROLE_USER";
        String actualScope = signedJWT.getJWTClaimsSet().getStringClaim("scope");

        if (actualScope == null || !actualScope.equals(expectedScope)) {
            throw new AppException(ErrorCode.UNAUTHENTICATED);
        }

        if (isRefresh) {
            String jwtId = signedJWT.getJWTClaimsSet().getJWTID();
            if (jwtId != null && invalidatedTokenRepositoty.existsById(jwtId)) {
                throw new AppException(ErrorCode.UNAUTHENTICATED);
            }
        }

        return signedJWT;
    }



    public AuthenticationResponse refreshToken(String refreshToken) throws ParseException, JOSEException {
        var signToken = verifyToken(refreshToken, true);

        var username = signToken.getJWTClaimsSet().getSubject();
        var user = userRepository.findByEmail(username)
                .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));

        var newAccessToken = generateToken(user, false);
        var newRefreshToken = generateToken(user, true);
        ResponseCookie refreshCookie = createRefreshTokenCookie(newRefreshToken);

        String jwtId = signToken.getJWTClaimsSet().getJWTID();
        Date expiryTime = signToken.getJWTClaimsSet().getExpirationTime();
        if (jwtId != null && !invalidatedTokenRepositoty.existsById(jwtId)) {
            invalidatedTokenRepositoty.save(
                    InvalidatedToken.builder()
                            .id(jwtId)
                            .expiryTime(expiryTime)
                            .build()
            );
        }

        return AuthenticationResponse.builder()
                .token(newAccessToken)
                .authenticated(true)
                .refreshCookie(refreshCookie)
                .build();
    }

    public UserEntity getUserByEmail(String email) {
        try {
            return userRepository.findByEmail(email)
                    .orElseThrow(() -> new AppException(ErrorCode.USER_NOT_FOUND));
        } catch (AppException e) {
            log.error("Error in getUserByEmail: {}", e.getMessage());
            return null;
        }
    }

    @Transactional
    public void resetPassword(String email) {
        String cleanEmail = email.trim().toLowerCase();
        Optional<UserEntity> optionalUser = userRepository.findByEmail(cleanEmail);
        if (optionalUser.isPresent()) {
            UserEntity user = optionalUser.get();
            String newPassword = UUID.randomUUID().toString().substring(0, 10);
            user.setPasswordHash(passwordEncoder.encode(newPassword));
            userRepository.save(user);
            String requestTime = LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));
            eventPublisher.publishEvent(
                    new GoogleUserForgotPasswordEvent(this, user, newPassword, requestTime)
            );
        }
    }


}
