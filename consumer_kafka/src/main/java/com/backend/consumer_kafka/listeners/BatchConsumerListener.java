package com.backend.consumer_kafka.listeners;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.backend.consumer_kafka.dto.MessageDTO;
import com.backend.consumer_kafka.services.IStringConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Consumidor por lotes (batch) que procesa multiples mensajes
 * del topic "str-topic" en una sola invocacion.
 */
@Slf4j
@Component
public class BatchConsumerListener {

    @Autowired
    private IStringConsumerService stringConsumerService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Recibe una lista de mensajes JSON del topic "str-topic"
     * y los procesa en lote, deserializando cada uno a MessageDTO.
     */
    @KafkaListener(
        topics = "str-topic",
        groupId = "str-group-batch",
        containerFactory = "batchContainerFactory"
    )
    public void listenBatch(List<String> messages) {
        log.info("[BATCH] Recibidos {} mensajes en lote", messages.size());
        for (String json : messages) {
            try {
                MessageDTO message = objectMapper.readValue(json, MessageDTO.class);
                stringConsumerService.processMessage(message);
            } catch (Exception e) {
                log.error("[BATCH] Error al deserializar mensaje: {}", json, e);
            }
        }
    }
}
