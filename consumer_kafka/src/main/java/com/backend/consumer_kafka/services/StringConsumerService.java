package com.backend.consumer_kafka.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.backend.consumer_kafka.dto.MessageDTO;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * Servicio que procesa los mensajes recibidos desde Kafka.
 * Implementa la logica de negocio para cada mensaje entrante.
 */
@Service
@Slf4j
public class StringConsumerService implements IStringConsumerService {

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Procesa un mensaje recibido, registrando sus detalles.
     * Aqui se puede agregar logica de negocio adicional como
     * persistencia en base de datos, envio de notificaciones, etc.
     */
    @Override
    public void processMessage(MessageDTO message) {
        log.info("[PROCESS] Mensaje recibido | id={} contenido='{}' origen={} timestamp={}",
                message.id(), message.content(), message.source(), message.timestamp());
    }
}
