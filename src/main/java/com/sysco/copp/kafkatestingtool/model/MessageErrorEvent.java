package com.sysco.copp.kafkatestingtool.model;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class MessageErrorEvent extends ApplicationEvent {
    private final String topic;
    
    public MessageErrorEvent(Object source, String topic) {
        super(source);
        this.topic = topic;
    }
}
