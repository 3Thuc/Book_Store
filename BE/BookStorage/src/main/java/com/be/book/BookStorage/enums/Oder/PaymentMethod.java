package com.be.book.BookStorage.enums.Oder;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum PaymentMethod {
    COD,
    CreditCard,
    @JsonProperty("E-Wallet")
    E_Wallet
}

