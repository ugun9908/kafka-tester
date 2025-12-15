# Universal Kafka Consumer Tool

This tool is designed to consume messages from Kafka topics dynamically. It provides features such as message storage, WebSocket broadcasting, and topic activity monitoring. Below are the steps to set up and run the application.

---

## Prerequisites

1. **Java Development Kit (JDK)**: Ensure you have JDK 17 or higher installed.

---

## Setup Instructions

### 1. Clone the Repository
Clone the repository to your local machine:
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Configure `application.properties`
The application requires specific Kafka configurations. Open the `src/main/resources/application.properties` file and configure the following properties:

#### **Kafka Consumer Configuration**
```ini
# Kafka topic and consumer group
app.kafka.consumer.topic=<your-topic-name>
app.kafka.consumer.group.id=<your-consumer-group-id>

# Kafka deserializer configuration
spring.kafka.consumer.key-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.key.delegate.class=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=io.confluent.kafka.serializers.KafkaAvroDeserializer
spring.kafka.consumer.properties.specific.avro.reader=true
```

#### **Confluent Cloud Configuration**
If you are using Confluent Cloud, set the following properties:
```ini
spring.kafka.properties.bootstrap.servers=<your-bootstrap-servers>
spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<your-username>" password="<your-password>";
spring.kafka.properties.security.protocol=SASL_SSL
spring.kafka.properties.schema.registry.url=<your-schema-registry-url>
spring.kafka.properties.basic.auth.user.info=<your-basic-auth-user-info>
```

#### **Other Configurations**
You can adjust the following properties as needed:
```ini
# Consumer settings
spring.kafka.consumer.auto-offset-reset=latest
spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
app.kafka.concurrency=1

# Logging
logging.level.org.springframework.kafka=DEBUG
logging.level.org.apache.kafka.clients.consumer=DEBUG
```

### 3. Build the Application
Use Gradle to build the application:
```bash
./gradlew build
```

### 4. Run the Application
Run the application using the following command:
```bash
./gradlew bootRun
```

Alternatively, you can run the application directly from your IDE (e.g., IntelliJ IDEA) by running the `main` method in the `Application` class.

---

## Features

1. **Concurrent Kafka Consumer**: Consume messages from multiple topics concurrently. 
2. **Topic Monitoring**: Monitor active topics and message counts.
3. **Customizable Properties**: Configure Kafka settings via `application.properties`.

---

## Logs and Debugging
- Logs are configured to provide detailed information about Kafka consumer activity.
- You can adjust the logging level in the `application.properties` file:
  ```ini
  logging.level.com.sysco.copp.kafkatestingtool=DEBUG
  ```

---

## Troubleshooting
1. **Kafka Connection Issues**: Ensure the `bootstrap.servers` and authentication credentials are correct.
2. **Message Deserialization Errors**: Ensure that the topic you are listening to is registered with the registry and is in the Avro format
3. **Schema Registry Errors**: Verify the schema registry URL and credentials.
   
---

## Additional Notes
- The application is configured to use manual acknowledgment (`MANUAL_IMMEDIATE`) for message processing.
- By default, the application does not auto-start Kafka consumers. You can enable this by setting:
  ```ini
  spring.kafka.consumer.auto-startup=true
  spring.kafka.listener.auto-startup=true
  ```

---

## REST API Usage

Use these endpoints to control the on-demand Kafka consumer and inspect messages. Replace host/port if you changed `server.port`.

### Start Consumer
```bash
curl -X POST "http://localhost:8080/api/kafka/consumer/start" ^
  -H "Content-Type: application/json" ^
  -d "{\"topic\":\"<topic>\",\"groupId\":\"<group-id>\",\"concurrency\":1,\"maxMessages\":1000,\"autoOffsetReset\":\"latest\"}"
```

### Stop Consumer
```bash
curl -X POST "http://localhost:8080/api/kafka/consumer/stop" ^
  -H "Content-Type: application/json" ^
  -d "{\"topic\":\"<topic>\",\"groupId\":\"<group-id>\"}"
```

### Clear Stored Messages
```bash
curl -X POST "http://localhost:8080/api/kafka/consumer/clear" ^
  -H "Content-Type: application/json" ^
  -d "{\"topic\":\"<topic>\",\"groupId\":\"<group-id>\"}"
```

### Fetch Consumed Messages
```bash
curl "http://localhost:8080/api/kafka/consumer/messages?topic=<topic>&groupId=<group-id>"
```
Sample response:
```json
[
  {
    "topic": "product.fct.sysco-list-header.0",
    "partition": 0,
    "offset": 338011,
    "key": {
      "site_id": "074",
      "list_id": "UG120",
      "seller_id": "BHNP"
    },
    "value": {
      "list_id": "UG120",
      "seller_id": "BHNP",
      "name": "UG120list",
      "site_id": "074",
      "list_level": "CUSTOMER",
      "total_items": 0,
      "created_at": "2025-12-15T12:30:29.258513Z",
      "created_by": "SYSTEM",
      "modified_at": null,
      "modified_by": null
    },
    "timestamp": "2025-12-15T18:00:35.4654422",
    "processingTime": 0,
    "consumerGroup": "order.cmd.susrelease-failure-listener"
  }
]
```

### Metrics Snapshot
```bash
curl "http://localhost:8080/api/kafka/consumer/metrics"
```

(Replace `^` with `\` on Linux/macOS shells.)

---

This completes the setup. You can now use the Universal Kafka Consumer Tool to monitor and process Kafka messages dynamically.
