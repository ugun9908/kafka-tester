package com.sysco.copp.kafkatestingtool.model;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class MessageProcessedEvent extends ApplicationEvent {
    private final String topic;
    private final long processingTime;
    
    public MessageProcessedEvent(Object source, String topic, long processingTime) {
        super(source);
        this.topic = topic;
        this.processingTime = processingTime;
    }
}

