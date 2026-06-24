package com.backend.producer_kafka.resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.backend.producer_kafka.dto.MessageDTO;
import com.backend.producer_kafka.dto.MessageResponse;
import com.backend.producer_kafka.services.IStringProducerService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.validation.Valid;

/**
 * Controlador REST del productor Kafka.
 * Expone endpoints para enviar mensajes al topic y verificar el estado del servicio.
 */
@RestController
@RequestMapping("/producer")
public class StringProducerResource {

    @Autowired
    private IStringProducerService stringProducerService;

    /**
     * Envia un mensaje a Kafka. El body debe ser un JSON valido con
     * los campos de MessageDTO. Retorna los metadatos del mensaje enviado.
     */
    @Operation(summary = "Envia un mensaje a Kafka")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "201", description = "Mensaje enviado correctamente",
            content = @Content(schema = @Schema(implementation = MessageResponse.class))),
        @ApiResponse(responseCode = "400", description = "Entrada invalida"),
        @ApiResponse(responseCode = "500", description = "Error interno")
    })
    @PostMapping
    public ResponseEntity<MessageResponse> sendMessage(@Valid @RequestBody MessageDTO message) {
        var response = stringProducerService.sendMessage(message);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Endpoint de health check para verificar que el servicio esta activo.
     */
    @Operation(summary = "Health check del productor Kafka")
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("UP");
    }
}
