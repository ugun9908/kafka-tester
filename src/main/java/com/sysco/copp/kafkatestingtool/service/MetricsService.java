package com.sysco.copp.kafkatestingtool.service;
import com.sysco.copp.kafkatestingtool.model.MessageErrorEvent;
import com.sysco.copp.kafkatestingtool.model.MessageProcessedEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MetricsService {

    // Metrics tracking
    private final AtomicLong totalProcessedMessages = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final ConcurrentLinkedDeque<TimestampedMetric> messageRateHistory = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<TimestampedMetric> errorRateHistory = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<TimestampedMetric> processingTimeHistory = new ConcurrentLinkedDeque<>();
    private final Map<String, TopicMetrics> topicMetricsMap = new ConcurrentHashMap<>();

    // Sliding window for rate calculation (last 60 seconds)
    private final ConcurrentLinkedDeque<MessageEvent> recentMessages = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<Long> recentProcessingTimes = new ConcurrentLinkedDeque<>();

    private volatile long lastMessageCount = 0;
    private volatile long lastErrorCount = 0;
    private static final int MAX_PROCESSING_TIMES = 1000; // Limit memory usage
    private volatile LocalDateTime lastCalculation = LocalDateTime.now();

    @Data
    private static class TimestampedMetric {
        private final LocalDateTime timestamp;
        private final double value;

        public TimestampedMetric(double value) {
            this.timestamp = LocalDateTime.now();
            this.value = value;
        }
    }

    @Data
    private static class MessageEvent {
        private final LocalDateTime timestamp;
        private final String topic;

        public MessageEvent(String topic) {
            this.timestamp = LocalDateTime.now();
            this.topic = topic;
        }
    }

    @Data
    public static class TopicMetrics {
        private long messageCount = 0;
        private long errorCount = 0;
        private double avgProcessingTime = 0;
        private LocalDateTime lastMessageTime;
        private double messageRate = 0;
    }

    @EventListener
    public void handleMessageProcessed(MessageProcessedEvent event) {
        recordMessageProcessed(event.getTopic(), event.getProcessingTime());
        log.debug("Processed message event for topic: {} with processing time: {}ms",
                event.getTopic(), event.getProcessingTime());
    }

    @EventListener
    public void handleMessageError(MessageErrorEvent event) {
        recordError(event.getTopic());
        log.debug("Processed error event for topic: {}", event.getTopic());
    }

    private void recordMessageProcessed(String topic, long processingTime) {
        totalProcessedMessages.incrementAndGet();
        recentMessages.add(new MessageEvent(topic));
        recentProcessingTimes.add(processingTime);

        // Update topic-specific metrics
        TopicMetrics topicMetrics = topicMetricsMap.computeIfAbsent(topic, k -> new TopicMetrics());
        topicMetrics.messageCount++;
        topicMetrics.lastMessageTime = LocalDateTime.now();
        topicMetrics.avgProcessingTime = (topicMetrics.avgProcessingTime * (topicMetrics.messageCount - 1) + processingTime) / topicMetrics.messageCount;

        // Keep only last 1000 processing times for memory efficiency
        while (recentProcessingTimes.size() > MAX_PROCESSING_TIMES) {
            recentProcessingTimes.pollFirst();
        }
    }


    private void recordError(String topic) {
        totalErrors.incrementAndGet();

        TopicMetrics topicMetrics = topicMetricsMap.get(topic);
        if (topicMetrics != null) {
            topicMetrics.errorCount++;
        }
    }

    public double calculateMessageRate() {
        cleanupOldEvents();

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime oneMinuteAgo = now.minusMinutes(1);

        long messagesInLastMinute = recentMessages.stream()
                .filter(event -> event.timestamp.isAfter(oneMinuteAgo))
                .count();

        return messagesInLastMinute / 60.0; // messages per second
    }

    // Get total processed messages
    public long getTotalProcessedMessages() {
        return totalProcessedMessages.get();
    }

    // Calculate average processing time
    public double getAverageProcessingTime() {
        if (recentProcessingTimes.isEmpty()) {
            return 0;
        }

        double sum = recentProcessingTimes.stream()
                .mapToDouble(Long::doubleValue)
                .sum();

        return sum / recentProcessingTimes.size();
    }

    // Calculate error rate (errors per minute)
    public double getErrorRate() {
        long currentErrors = totalErrors.get();
        LocalDateTime now = LocalDateTime.now();

        long timeDiffMinutes = ChronoUnit.MINUTES.between(lastCalculation, now);
        if (timeDiffMinutes == 0) timeDiffMinutes = 1;

        double rate = (currentErrors - lastErrorCount) / (double) timeDiffMinutes;

        lastErrorCount = currentErrors;
        lastCalculation = now;

        return Math.max(0, rate);
    }

    // Get metrics for each topic
    public Map<String, TopicMetrics> getTopicMetrics() {
        // Update message rates for each topic
        LocalDateTime oneMinuteAgo = LocalDateTime.now().minusMinutes(1);

        Map<String, Long> topicMessageCounts = recentMessages.stream()
                .filter(event -> event.timestamp.isAfter(oneMinuteAgo))
                .collect(Collectors.groupingBy(
                        MessageEvent::getTopic,
                        Collectors.counting()
                ));

        topicMetricsMap.forEach((topic, metrics) -> {
            Long count = topicMessageCounts.getOrDefault(topic, 0L);
            metrics.messageRate = count / 60.0;
        });

        return new HashMap<>(topicMetricsMap);
    }

    // Get message rate history
    public List<Map<String, Object>> getMessageRateHistory(int minutes) {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(minutes);

        return messageRateHistory.stream()
                .filter(metric -> metric.timestamp.isAfter(cutoff))
                .map(metric -> {
                    Map<String, Object> point = new HashMap<>();
                    point.put("timestamp", metric.timestamp.toString());
                    point.put("value", metric.value);
                    return point;
                })
                .collect(Collectors.toList());
    }

    // Get error rate history
    public List<Map<String, Object>> getErrorRateHistory(int minutes) {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(minutes);

        return errorRateHistory.stream()
                .filter(metric -> metric.timestamp.isAfter(cutoff))
                .map(metric -> {
                    Map<String, Object> point = new HashMap<>();
                    point.put("timestamp", metric.timestamp.toString());
                    point.put("value", metric.value);
                    return point;
                })
                .collect(Collectors.toList());
    }

    // Get processing time history
    public List<Map<String, Object>> getProcessingTimeHistory(int minutes) {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(minutes);

        return processingTimeHistory.stream()
                .filter(metric -> metric.timestamp.isAfter(cutoff))
                .map(metric -> {
                    Map<String, Object> point = new HashMap<>();
                    point.put("timestamp", metric.timestamp.toString());
                    point.put("value", metric.value);
                    return point;
                })
                .collect(Collectors.toList());
    }

    // Scheduled task to record metrics history every 30 seconds
    @Scheduled(fixedDelay = 30000)
    public void recordMetricsSnapshot() {
        try {
            // Record current metrics
            messageRateHistory.add(new TimestampedMetric(calculateMessageRate()));
            errorRateHistory.add(new TimestampedMetric(getErrorRate()));
            processingTimeHistory.add(new TimestampedMetric(getAverageProcessingTime()));

            // Cleanup old history (keep last 24 hours)
            LocalDateTime cutoff = LocalDateTime.now().minusHours(24);

            removeOldMetrics(messageRateHistory, cutoff);
            removeOldMetrics(errorRateHistory, cutoff);
            removeOldMetrics(processingTimeHistory, cutoff);

            log.debug("Recorded metrics snapshot - Message Rate: {}, Error Rate: {}, Avg Processing Time: {}",
                    calculateMessageRate(), getErrorRate(), getAverageProcessingTime());

        } catch (Exception e) {
            log.error("Error recording metrics snapshot", e);
        }
    }

    private void removeOldMetrics(ConcurrentLinkedDeque<TimestampedMetric> metrics, LocalDateTime cutoff) {
        while (!metrics.isEmpty() && metrics.peekFirst().timestamp.isBefore(cutoff)) {
            metrics.pollFirst();
        }
    }

    private void cleanupOldEvents() {
        LocalDateTime cutoff = LocalDateTime.now().minusMinutes(5);

        while (!recentMessages.isEmpty() && recentMessages.peekFirst().timestamp.isBefore(cutoff)) {
            recentMessages.pollFirst();
        }
    }

    // Reset all metrics
    public void resetMetrics() {
        totalProcessedMessages.set(0);
        totalErrors.set(0);
        messageRateHistory.clear();
        errorRateHistory.clear();
        processingTimeHistory.clear();
        topicMetricsMap.clear();
        recentMessages.clear();
        recentProcessingTimes.clear();
        lastMessageCount = 0;
        lastErrorCount = 0;
        lastCalculation = LocalDateTime.now();

        log.info("All metrics have been reset");
    }
}