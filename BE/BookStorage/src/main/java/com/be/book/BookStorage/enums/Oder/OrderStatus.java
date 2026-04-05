package com.be.book.BookStorage.enums.Oder;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum OrderStatus {
    @JsonProperty("PENDING")
    pending,

    @JsonProperty("PROCESSING")
    processing,


    @JsonProperty("SHIPPED")
    shipped,

    @JsonProperty("DELIVERED")
    delivered,

    @JsonProperty("CANCEL_REQUESTED")
    cancel_requested,

    @JsonProperty("CANCELLED")
    cancelled,

    @JsonProperty("RETURN_REQUESTED")
    return_requested,

    @JsonProperty("RETURNED")
    returned,

    @JsonProperty("FAILED")
    failed
}
