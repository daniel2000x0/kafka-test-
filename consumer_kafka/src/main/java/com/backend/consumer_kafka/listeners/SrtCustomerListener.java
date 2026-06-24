package com.backend.consumer_kafka.listeners;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.backend.consumer_kafka.dto.MessageDTO;
import com.backend.consumer_kafka.services.IStringConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Consumidor principal que deserializa mensajes JSON del topic "str-topic"
 * y delega el procesamiento al servicio correspondiente.
 */
@Slf4j
@Component
public class SrtCustomerListener {

    @Autowired
    private IStringConsumerService stringConsumerService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Escucha mensajes del topic "str-topic" en el grupo "str-group",
     * los deserializa a MessageDTO y los procesa.
     */
    @KafkaListener(topics = "str-topic", groupId = "str-group", containerFactory = "validMessageContainerFactory")
    public void listen(String json) {
        try {
            MessageDTO message = objectMapper.readValue(json, MessageDTO.class);
            stringConsumerService.processMessage(message);
        } catch (Exception e) {
            log.error("Error al deserializar el mensaje: {}", json, e);
        }
    }
}
