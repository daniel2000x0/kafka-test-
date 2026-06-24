package com.backend.consumer_kafka.listeners;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Consumidor de mensajes fallidos enviados al Dead Letter Topic (DLT).
 * Procesa los mensajes que no pudieron ser manejados despues de varios reintentos.
 */
@Slf4j
@Component
public class DltConsumerListener {

    /**
     * Escucha el DLT (Dead Letter Topic) para mensajes que fallaron
     * despues del maximo de reintentos configurado en el error handler.
     */
    @KafkaListener(
        topics = "str-topic.DLT",
        groupId = "str-group-dlt",
        containerFactory = "validMessageContainerFactory"
    )
    public void listenDlt(String message,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.OFFSET) long offset,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        log.warn("[DLT] Mensaje fallido recibido | particion={} offset={} mensaje='{}'", partition, offset, message);
    }
}
