package com.backend.producer_kafka.dto;

import java.time.Instant;
import java.util.UUID;

public record MessageResponse(
    UUID messageId,
    String content,
    int partition,
    long offset,
    long timestamp,
    String status
) {
    public MessageResponse(UUID messageId, String content, int partition, long offset, long timestamp) {
        this(messageId, content, partition, offset, timestamp, "SENT");
    }
}
