package com.backend.producer_kafka.exception;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import lombok.extern.slf4j.Slf4j;

/**
 * Manejador global de excepciones para la API REST del productor.
 * Procesa errores de validacion, JSON malformado y errores internos.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Maneja errores de validacion de campos (@Valid).
     * Retorna un mapa con los nombres de los campos y sus mensajes de error.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        var errors = ex.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.toMap(
                        f -> f.getField(),
                        f -> f.getDefaultMessage(),
                        (a, b) -> a));
        return ResponseEntity.badRequest().body(Map.of(
                "error", "validation_failed",
                "timestamp", Instant.now().toString(),
                "details", errors));
    }

    /**
     * Maneja errores de JSON malformado en el body de la peticion.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleMalformedJson(HttpMessageNotReadableException ex) {
        log.warn("Cuerpo de peticion malformado: {}", ex.getMessage());
        return ResponseEntity.badRequest().body(Map.of(
                "error", "malformed_request",
                "message", "El cuerpo de la peticion no es un JSON valido.",
                "timestamp", Instant.now().toString()));
    }

    /**
     * Manejo generico de excepciones no contempladas.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneral(Exception ex) {
        log.error("Error inesperado: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "error", "internal_error",
                "message", "Ocurrio un error inesperado",
                "timestamp", Instant.now().toString()));
    }
}
