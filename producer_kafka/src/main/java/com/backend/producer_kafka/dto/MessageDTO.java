package com.backend.producer_kafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.Instant;
import java.util.UUID;

public record MessageDTO(
    UUID id,
    @NotBlank @Size(max = 500) String content,
    String source,
    Instant timestamp
) {
    public MessageDTO {
        if (id == null) id = UUID.randomUUID();
        if (timestamp == null) timestamp = Instant.now();
        if (source == null || source.isBlank()) source = "producer_kafka";
    }
}
