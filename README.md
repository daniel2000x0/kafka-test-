# Test Kafka

Spring Boot + Apache Kafka project with a producer and consumer application, plus a Dockerized environment with ZooKeeper, Kafka Broker, and Kafdrop.

## Tech Stack

- Java 25
- Spring Boot 4.1.0
- Apache Kafka (Confluent)
- Maven
- Docker & Docker Compose
- Lombok
- Kafdrop (Kafka web UI)

## Project Structure

```
testkafka/
├── docker-compose.yml              # Kafka infrastructure (ZooKeeper, Broker, Kafdrop)
├── producer_kafka/                 # Producer application (Spring Boot)
│   ├── pom.xml
│   └── src/main/java/com/backend/producer_kafka/
│       ├── ProducerKafkaApplication.java
│       ├── config/
│       │   ├── KafkaAdminConfig.java               # Admin: auto-creates "str-topic"
│       │   └── StringProducerFactoryConfig.java     # ProducerFactory + KafkaTemplate
│       ├── resource/
│       │   └── StringProducerResource.java          # POST /producer - REST endpoint
│       └── services/
│           └── StringProducerService.java           # Sends messages to topic
└── consumer_kafka/                 # Consumer application (Spring Boot)
    ├── pom.xml
    └── src/main/java/com/backend/consumer_kafka/
        ├── ConsumerKafkaApplication.java
        ├── config/
        │   └── StringConsumerConfig.java            # ConsumerFactory + ListenerFactory
        └── listeners/
            └── SrtCustomerListener.java             # Kafka listener for "str-topic"
```

## Prerequisites

- Docker & Docker Compose
- JDK 25
- Apache Maven

## Quick Start

### 1. Start Kafka infrastructure

```bash
docker-compose up -d
```

This starts:
- **ZooKeeper** on port `2181`
- **Kafka Broker** on port `9092`
- **Kafdrop** (UI) at `http://localhost:19000`

### 2. Run the producer

```bash
cd producer_kafka
./mvnw spring-boot:run
```

Producer starts on port `8080`.

### 3. Run the consumer (in another terminal)

```bash
cd consumer_kafka
./mvnw spring-boot:run
```

Consumer starts on port `8100`.

### 4. Send a message

```bash
curl -X POST http://localhost:8080/producer \
  -H "Content-Type: text/plain" \
  -d "Hello Kafka!"
```

## Endpoints

| Method | Path         | Description                      |
|--------|--------------|----------------------------------|
| POST   | `/producer`  | Sends a message to `str-topic`   |

## Ports

| Service          | Port   |
|------------------|--------|
| Producer         | 8080   |
| Consumer         | 8100   |
| Kafka Broker     | 9092   |
| Kafdrop (UI)     | 19000  |

## Topics

The `str-topic` is auto-created when the producer starts with:
- 2 partitions
- 1 replica
