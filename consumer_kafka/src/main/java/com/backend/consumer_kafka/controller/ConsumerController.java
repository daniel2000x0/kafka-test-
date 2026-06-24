package com.backend.consumer_kafka.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;

/**
 * Controlador REST para monitoreo del consumidor Kafka.
 * Expone endpoints de health check e informacion del servicio.
 */
@RestController
@RequestMapping("/consumer")
public class ConsumerController {

    /**
     * Endpoint de health check para verificar que el servicio esta activo.
     */
    @Operation(summary = "Health check del consumidor Kafka")
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "service", "consumer_kafka",
            "topic", "str-topic"
        ));
    }

    /**
     * Endpoint de informacion basica del servicio consumidor.
     */
    @Operation(summary = "Informacion del servicio consumidor")
    @GetMapping("/info")
    public ResponseEntity<Map<String, String>> info() {
        return ResponseEntity.ok(Map.of(
            "service", "consumer_kafka",
            "version", "1.0.0",
            "description", "Kafka message consumer service"
        ));
    }
}
