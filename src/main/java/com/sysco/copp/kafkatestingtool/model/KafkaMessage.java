package com.sysco.copp.kafkatestingtool.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessage {
    private String topic;
    private int partition;
    private long offset;
    private JsonNode key;
    private JsonNode value;
    private String timestamp;
    private long processingTime;
    private String consumerGroup;
}