package com.backend.producer_kafka.services;

import com.backend.producer_kafka.dto.MessageDTO;
import com.backend.producer_kafka.dto.MessageResponse;

/**
 * Interfaz del servicio de produccion de mensajes Kafka.
 * Define el contrato para enviar mensajes al topic configurado.
 */
public interface IStringProducerService {
    MessageResponse sendMessage(MessageDTO message);
}
