package com.be.book.BookStorage.dto.Request.User;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UpdateAddressReq {
    private String idAddress;
    private String recipientName;
    private String recipientPhone;
    private String address;
    private String district;
    private String city;
    private Boolean isDefault;
}
