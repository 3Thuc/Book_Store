package com.be.book.BookStorage.dto.Request.Admin;


import com.be.book.BookStorage.enums.Status;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class CategoryReq {
    private String categoryName;
    private Status status;
}
