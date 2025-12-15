package com.sysco.copp.kafkatestingtool.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@ConditionalOnProperty(
        value = "app.kafka.static.consumer.enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class UniversalConsumer {

    @KafkaListener(
            topics = {"${app.kafka.consumer.topic}"},
            groupId = "${app.kafka.consumer.group.id}"
    )
    public void listen(Acknowledgment ack,
                       @Payload(required = false) ConsumerRecord<?, ?> consumerRecord,
                       @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) Object consumerRecordKey,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.OFFSET) String offset
    ) {
        consumeRecord(ack, consumerRecord);
    }

    private void consumeRecord(Acknowledgment ack, ConsumerRecord<?, ?> consumerRecord) {
        long startTime = System.currentTimeMillis();
        try {
            log.info("[STATIC] Consume Message with key - {} | message - {}",
                    consumerRecord.key(), consumerRecord.value());

        } catch (Exception ex) {
            log.error(String.format("[STATIC] Unable to process invalid Territory consumer object, value - %s", consumerRecord), ex);
        }
        acknowledgeConsumer(ack, consumerRecord, startTime);
    }

    private void acknowledgeConsumer(Acknowledgment ack, ConsumerRecord<?, ?> consumerRecord, long startTime) {
        ack.acknowledge();
        log.info("""
                 [STATIC] ACCOUNT_TERRITORY_DATA consumer acknowledgment: Key={} | Topic={} | Partition={} | Offset={}
                 |exec-time={} | ms|value={}
                """,
                consumerRecord.key(),
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                System.currentTimeMillis() - startTime,
                consumerRecord);
    }
}