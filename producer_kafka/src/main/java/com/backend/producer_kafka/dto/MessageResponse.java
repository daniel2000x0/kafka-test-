package com.backend.producer_kafka.dto;

import java.util.UUID;

/**
 * DTO con la respuesta del envio de un mensaje a Kafka.
 * Incluye los metadatos de Kafka: particion, offset y estado del envio.
 */
public record MessageResponse(
    UUID messageId,
    String content,
    int partition,
    long offset,
    long timestamp,
    String status
) {
    /**
     * Constructor compacto que asume estado "SENT" (enviado correctamente).
     */
    public MessageResponse(UUID messageId, String content, int partition, long offset, long timestamp) {
        this(messageId, content, partition, offset, timestamp, "SENT");
    }
}
