package com.be.book.BookStorage.events;

import com.be.book.BookStorage.entity.UserEntity;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class GoogleUserForgotPasswordEvent extends ApplicationEvent {
    private final UserEntity user;
    private final String newPassword;
    private final String requestTime;

    public GoogleUserForgotPasswordEvent(Object source, UserEntity user, String newPassword, String requestTime) {
        super(source);
        this.user = user;
        this.newPassword = newPassword;
        this.requestTime = requestTime;
    }
}
