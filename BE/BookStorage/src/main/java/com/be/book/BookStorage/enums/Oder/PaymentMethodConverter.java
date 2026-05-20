package com.be.book.BookStorage.enums.Oder;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class PaymentMethodConverter implements AttributeConverter<PaymentMethod, String> {

    @Override
    public String convertToDatabaseColumn(PaymentMethod attribute) {
        if (attribute == null) {
            return null;
        }
        if (attribute == PaymentMethod.E_Wallet) {
            return "E-Wallet";
        }
        return attribute.name();
    }

    @Override
    public PaymentMethod convertToEntityAttribute(String dbData) {
        if (dbData == null) {
            return null;
        }
        if ("E-Wallet".equals(dbData)) {
            return PaymentMethod.E_Wallet;
        }
        try {
            return PaymentMethod.valueOf(dbData);
        } catch (IllegalArgumentException e) {
            // Default to COD or null if unrecognized
            return null;
        }
    }
}
