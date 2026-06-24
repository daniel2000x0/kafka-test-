package com.backend.consumer_kafka.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * DTO que representa un mensaje recibido a traves de Kafka.
 * Contiene los datos basicos del mensaje: identificador, contenido,
 * origen y marca de tiempo.
 */
public record MessageDTO(
    UUID id,
    String content,
    String source,
    Instant timestamp
) {}
