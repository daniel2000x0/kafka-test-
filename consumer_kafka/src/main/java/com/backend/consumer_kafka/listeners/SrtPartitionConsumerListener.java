package com.backend.consumer_kafka.listeners;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Consumidor con particiones especificas para demostrar el control
 * de particiones en Kafka. Cada metodo escucha una particion distinta.
 */
@Slf4j
@Component
public class SrtPartitionConsumerListener {

    /**
     * Escucha la particion 0 del topic "str-topic" dentro del grupo "group-1".
     */
    @KafkaListener(
        groupId = "group-1",
        topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"0"}),
        containerFactory = "validMessageContainerFactory"
    )
    public void listenPartition0(String message) {
        log.info("[PARTITION-0] Mensaje recibido: {}", message);
    }

    /**
     * Escucha la particion 1 del topic "str-topic" dentro del grupo "group-1".
     */
    @KafkaListener(
        groupId = "group-1",
        topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}),
        containerFactory = "validMessageContainerFactory"
    )
    public void listenPartition1(String message) {
        log.info("[PARTITION-1] Mensaje recibido: {}", message);
    }

    /**
     * Consumidor adicional en "group-2" que escucha todas las particiones del topic.
     */
    @KafkaListener(
        groupId = "group-2",
        topics = "str-topic",
        containerFactory = "validMessageContainerFactory"
    )
    public void listenGroup2(String message) {
        log.info("[GROUP-2] Mensaje recibido: {}", message);
    }
}
