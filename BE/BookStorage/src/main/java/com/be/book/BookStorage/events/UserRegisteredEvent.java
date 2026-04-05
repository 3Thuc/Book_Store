package com.be.book.BookStorage.events;

import com.be.book.BookStorage.entity.UserEntity;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class UserRegisteredEvent extends ApplicationEvent {
    private final UserEntity user;

    public UserRegisteredEvent(Object source, UserEntity user) {
        super(source);
        this.user = user;
    }

}
