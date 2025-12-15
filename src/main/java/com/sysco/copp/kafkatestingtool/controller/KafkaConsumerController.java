package com.sysco.copp.kafkatestingtool.controller;

import com.sysco.copp.kafkatestingtool.model.KafkaMessage;
import com.sysco.copp.kafkatestingtool.service.DynamicKafkaConsumerService;
import lombok.Data;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
@CrossOrigin(origins = "*")
public class KafkaConsumerController {

    private final DynamicKafkaConsumerService consumerService;

    public KafkaConsumerController(DynamicKafkaConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    @PostMapping("/config")
    public ResponseEntity<Map<String, String>> updateConfig(@RequestBody ConsumerConfigRequest request) {
        return ResponseEntity.ok(consumerService.updateDefaults(request.getTopic(), request.getGroupId()));
    }

    @GetMapping("/config")
    public ResponseEntity<Map<String, String>> readConfig() {
        return ResponseEntity.ok(consumerService.getCurrentConfig());
    }

    @PostMapping("/start")
    public ResponseEntity<String> startListening(@RequestBody StartRequest request) {
        consumerService.startListening(request.getTopic(), request.getGroupId(), request.getConcurrency());
        return ResponseEntity.ok("Listening to topic " + request.getTopic());
    }

    @PostMapping("/stop")
    public ResponseEntity<String> stopListening(@RequestBody StopRequest request) {
        consumerService.stopListening(request.getTopic(), request.getGroupId());
        return ResponseEntity.ok("Stopped listener");
    }

    @PostMapping("/stop-all")
    public ResponseEntity<String> stopAll() {
        consumerService.stopAll();
        return ResponseEntity.ok("Stopped all listeners");
    }

    @GetMapping("/messages")
    public ResponseEntity<List<KafkaMessage>> getMessages(@RequestParam String topic,
                                                          @RequestParam(defaultValue = "100") int limit) {
        return ResponseEntity.ok(consumerService.getMessages(topic, limit));
    }

    @Data
    public static class ConsumerConfigRequest {
        private String topic;
        private String groupId;
    }

    @Data
    public static class StartRequest {
        private String topic;
        private String groupId;
        private int concurrency = 1;
    }

    @Data
    public static class StopRequest {
        private String topic;
        private String groupId;
    }

    @Data
    public static class ClearRequest {
        private String topic;
    }
}