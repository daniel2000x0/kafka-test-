package com.backend.consumer_kafka.dto;

import java.time.Instant;
import java.util.UUID;

public record MessageDTO(
    UUID id,
    String content,
    String source,
    Instant timestamp
) {}
