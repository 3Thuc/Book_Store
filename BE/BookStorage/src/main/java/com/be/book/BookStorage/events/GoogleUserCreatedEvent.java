package com.be.book.BookStorage.events;

import com.be.book.BookStorage.entity.UserEntity;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class GoogleUserCreatedEvent extends ApplicationEvent {
    private final UserEntity user;
    private final String rawPassword;

    public GoogleUserCreatedEvent(Object source, UserEntity user, String rawPassword) {
        super(source);
        this.user = user;
        this.rawPassword = rawPassword;
    }
}
