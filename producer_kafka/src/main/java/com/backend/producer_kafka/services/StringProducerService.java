package com.backend.producer_kafka.services;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.backend.producer_kafka.dto.MessageDTO;
import com.backend.producer_kafka.dto.MessageResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Servicio que serializa los mensajes a JSON y los envia a Kafka.
 * Incluye un timeout de 5 segundos para la confirmacion de envio.
 */
@Service
@Slf4j
public class StringProducerService implements IStringProducerService {

    private static final String TOPIC = "str-topic";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Serializa el mensaje a JSON, lo envia al topic de Kafka y
     * retorna los metadatos del mensaje (particion, offset, timestamp).
     * Si falla el envio, lanza una excepcion con el detalle del error.
     */
    @Override
    public MessageResponse sendMessage(MessageDTO message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            var result = kafkaTemplate.send(TOPIC, json).get(5, TimeUnit.SECONDS);
            var metadata = result.getRecordMetadata();
            log.info("[SEND] Mensaje enviado | id={} particion={} offset={}", message.id(), metadata.partition(), metadata.offset());
            return new MessageResponse(message.id(), message.content(), metadata.partition(), metadata.offset(), metadata.timestamp());
        } catch (Exception e) {
            log.error("[SEND] Error al enviar mensaje id={}: {}", message.id(), e.getMessage());
            throw new RuntimeException("Error al enviar mensaje a Kafka", e);
        }
    }
}
