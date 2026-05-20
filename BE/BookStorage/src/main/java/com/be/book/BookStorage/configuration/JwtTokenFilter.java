package com.be.book.BookStorage.configuration;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.lang.NonNull;
import com.be.book.BookStorage.repository.UserRepository;
import com.be.book.BookStorage.entity.UserEntity;
import com.be.book.BookStorage.enums.Status;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Component
public class JwtTokenFilter extends OncePerRequestFilter {

    private final CustomJwtDecoder customJwtDecoder;
    private final UserRepository userRepository;

    public JwtTokenFilter(CustomJwtDecoder customJwtDecoder, UserRepository userRepository) {
        this.customJwtDecoder = customJwtDecoder;
        this.userRepository = userRepository;
    }

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain filterChain)
            throws ServletException, IOException {

        String header = request.getHeader("Authorization");

        if (header != null && header.startsWith("Bearer ")) {
            String token = header.substring(7);
            try {
                Jwt jwt = customJwtDecoder.decode(token);
                
                // Extract role from JWT claims
                String role = jwt.getClaimAsString("scope");
                if (role == null) {
                    role = jwt.getClaimAsString("role");
                }
                
                // Create authentication with authorities
                List<SimpleGrantedAuthority> authorities = Collections.emptyList();
                if (role != null) {
                    authorities = Collections.singletonList(new SimpleGrantedAuthority(role));
                }
                
                String email = jwt.getSubject();
                
                UserEntity user = userRepository.findByEmail(email).orElse(null);
                if (user != null && user.getStatus() == Status.locked) {
                    response.setStatus(403);
                    response.setContentType("application/json;charset=UTF-8");
                    response.getWriter().write("{\"code\": 1026, \"message\": \"Tài khoản của bạn đã bị khóa\"}");
                    return;
                }

                UsernamePasswordAuthenticationToken authentication = 
                    new UsernamePasswordAuthenticationToken(email, null, authorities);
                
                SecurityContextHolder.getContext().setAuthentication(authentication);
                
            } catch (JwtException ex) {
                if (ex.getMessage().toLowerCase().contains("expired")) {
                    request.setAttribute("tokenExpired", true);
                } else {
                    request.setAttribute("tokenInvalid", true);
                }
            }
        }

        filterChain.doFilter(request, response);
    }
}
