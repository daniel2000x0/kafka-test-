package com.backend.consumer_kafka.services;

import com.backend.consumer_kafka.dto.MessageDTO;

/**
 * Interfaz del servicio de consumo de mensajes Kafka.
 * Define el contrato para el procesamiento de mensajes entrantes.
 */
public interface IStringConsumerService {
    void processMessage(MessageDTO message);
}
