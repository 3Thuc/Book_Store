package com.be.book.BookStorage.configuration;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.stereotype.Component;

import javax.crypto.spec.SecretKeySpec;

@Component
public class CustomJwtDecoder implements JwtDecoder {

    @Value("${jwt.signerKey}")
    private String SIGNER_KEY;

    private NimbusJwtDecoder nimbusJwtDecoder;

    @PostConstruct
    public void init() {
        SecretKeySpec secretKey = new SecretKeySpec(SIGNER_KEY.getBytes(), "HmacSHA512");
        nimbusJwtDecoder = NimbusJwtDecoder.withSecretKey(secretKey)
                .macAlgorithm(MacAlgorithm.HS512)
                .build();
    }

    @Override
    public Jwt decode(String token) throws JwtException {
        return nimbusJwtDecoder.decode(token);
    }
}
