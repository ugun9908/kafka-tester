package com.sysco.copp.kafkatestingtool.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sysco.copp.kafkatestingtool.model.KafkaMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class DynamicKafkaConsumerService {

    private static final int MESSAGE_SIZE = 1000;
    private static final int MAX_WAIT_SECONDS = 30;

    private final KafkaProperties kafkaProperties;
    private final Map<String, List<KafkaMessage>> messageStore = new ConcurrentHashMap<>();
    private final Map<String, MessageListenerContainer> activeContainers = new ConcurrentHashMap<>();
    private final AtomicInteger containerIdCounter = new AtomicInteger(0);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${app.kafka.consumer.topic:}")
    private String defaultTopic;

    @Value("${app.kafka.consumer.group.id:}")
    private String defaultGroupId;

    public DynamicKafkaConsumerService(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    public synchronized void startListening(String topic, String groupId, int concurrency) {
        String resolvedTopic = resolveTopic(topic);
        String resolvedGroupId = resolveGroup(groupId);
        int safeConcurrency = Math.max(1, concurrency);

        String containerKey = generateContainerKey(resolvedTopic, resolvedGroupId);
        MessageListenerContainer existingContainer = activeContainers.get(containerKey);
        if (existingContainer != null) {
            if (existingContainer.isRunning()) {
                log.info("Already listening to topic: {} with group: {}", resolvedTopic, resolvedGroupId);
                return;
            }
            existingContainer.stop();
            activeContainers.remove(containerKey);
        }

        try {
            Map<String, Object> props = createConsumerProperties(resolvedGroupId);
            DefaultKafkaConsumerFactory<Object, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(props);

            ContainerProperties containerProps = new ContainerProperties(resolvedTopic);
            containerProps.setGroupId(resolvedGroupId);
            containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

            String containerId = String.format("kafka-listener-%s-%s-%d",
                    sanitize(resolvedTopic), sanitize(resolvedGroupId), containerIdCounter.incrementAndGet());

            ConcurrentMessageListenerContainer<Object, Object> container =
                    new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);

            container.setConcurrency(safeConcurrency);
            container.setBeanName(containerId);
            container.setCommonErrorHandler(new DefaultErrorHandler((record, exception) ->
                    log.error("Error processing record from topic: {}, offset: {}, partition: {}",
                            record.topic(), record.offset(), record.partition(), exception)));

            container.setupMessageListener((AcknowledgingMessageListener<Object, Object>)
                    (record, acknowledgment) -> handleMessage(resolvedTopic, resolvedGroupId, record, acknowledgment));

            container.start();
            waitForContainerStart(container);

            activeContainers.put(containerKey, container);
            messageStore.putIfAbsent(resolvedTopic, new ArrayList<>());

            log.info("Started listening to topic: {} with group: {} (concurrency: {})",
                    resolvedTopic, resolvedGroupId, safeConcurrency);
        } catch (Exception ex) {
            activeContainers.remove(containerKey);
            throw new IllegalStateException("Failed to start listener: " + ex.getMessage(), ex);
        }
    }

    public synchronized Map<String, String> updateDefaults(String topic, String groupId) {
        if (StringUtils.hasText(topic)) {
            this.defaultTopic = topic;
        }
        if (StringUtils.hasText(groupId)) {
            this.defaultGroupId = groupId;
        }
        return getCurrentConfig();
    }

    public Map<String, String> getCurrentConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("topic", defaultTopic);
        config.put("groupId", defaultGroupId);
        return config;
    }

    public boolean isListening(String topic, String groupId) {
        String resolvedTopic = resolveTopic(topic);
        String resolvedGroupId = resolveGroup(groupId);
        String key = generateContainerKey(resolvedTopic, resolvedGroupId);
        MessageListenerContainer container = activeContainers.get(key);
        return container != null && container.isRunning();
    }

    public void stopListening(String topic, String groupId) {
        String resolvedTopic = resolveTopic(topic);
        String resolvedGroupId = resolveGroup(groupId);
        String containerKey = generateContainerKey(resolvedTopic, resolvedGroupId);

        MessageListenerContainer container = activeContainers.get(containerKey);
        if (container == null) {
            log.info("No active listener for topic: {} with group: {}", resolvedTopic, resolvedGroupId);
            return;
        }

        container.stop();
        activeContainers.remove(containerKey);
        log.info("Stopped listener for topic: {} with group: {}", resolvedTopic, resolvedGroupId);
    }

    public void stopAll() {
        activeContainers.forEach((key, container) -> {
            try {
                container.stop();
            } catch (Exception ex) {
                log.warn("Failed to stop container {}", key, ex);
            }
        });
        activeContainers.clear();
    }

    public void clearMessages(String topic) {
        String resolvedTopic = resolveTopic(topic);
        messageStore.remove(resolvedTopic);
    }

    public List<KafkaMessage> getMessages(String topic, int limit) {
        String resolvedTopic = resolveTopic(topic);
        List<KafkaMessage> messages = messageStore.getOrDefault(resolvedTopic, new ArrayList<>());
        synchronized (messages) {
            int fromIndex = Math.max(0, messages.size() - Math.max(1, limit));
            return new ArrayList<>(messages.subList(fromIndex, messages.size()));
        }
    }

    private void handleMessage(String topic, String groupId, ConsumerRecord<Object, Object> record, Acknowledgment ack) {
        try {
            JsonNode keyNode = toJsonNode(record.key());
            JsonNode valueNode = toJsonNode(record.value());

            KafkaMessage message = KafkaMessage.builder()
                    .topic(topic)
                    .partition(record.partition())
                    .offset(record.offset())
                    .key(keyNode)
                    .value(valueNode)
                    .timestamp(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                    .processingTime(0)
                    .consumerGroup(groupId)
                    .build();

            List<KafkaMessage> messages = messageStore.computeIfAbsent(topic, key -> new ArrayList<>());
            synchronized (messages) {
                messages.add(message);
                if (messages.size() > MESSAGE_SIZE) {
                    messages.remove(0);
                }
            }

            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Error processing message", ex);
            ack.acknowledge();
        }
    }

    private JsonNode toJsonNode(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof JsonNode jsonNode) {
            return jsonNode;
        }
        try {
            return objectMapper.readTree(raw.toString());
        } catch (Exception ignored) {
            return objectMapper.getNodeFactory().textNode(raw.toString());
        }
    }

    private Map<String, Object> createConsumerProperties(String groupId) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    private void waitForContainerStart(MessageListenerContainer container) throws InterruptedException {
        int waitCount = 0;
        while (!container.isRunning() && waitCount < MAX_WAIT_SECONDS * 10) {
            Thread.sleep(100);
            waitCount++;
        }
        if (!container.isRunning()) {
            container.stop();
            throw new IllegalStateException("Container failed to start within " + MAX_WAIT_SECONDS + " seconds");
        }
    }

    private String resolveTopic(String topic) {
        if (StringUtils.hasText(topic)) {
            return topic;
        }
        if (StringUtils.hasText(defaultTopic)) {
            return defaultTopic;
        }
        throw new IllegalArgumentException("Topic must be provided via request or app.kafka.consumer.topic");
    }

    private String resolveGroup(String groupId) {
        if (StringUtils.hasText(groupId)) {
            return groupId;
        }
        if (StringUtils.hasText(defaultGroupId)) {
            return defaultGroupId;
        }
        throw new IllegalArgumentException("GroupId must be provided via request or app.kafka.consumer.group.id");
    }

    private String generateContainerKey(String topic, String groupId) {
        return topic + "::" + groupId;
    }

    private String sanitize(String value) {
        return value.replaceAll("[^a-zA-Z0-9]", "-");
    }
}

