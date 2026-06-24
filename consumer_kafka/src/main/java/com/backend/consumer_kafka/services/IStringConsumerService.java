package com.backend.consumer_kafka.services;

import com.backend.consumer_kafka.dto.MessageDTO;

public interface IStringConsumerService {
    void processMessage(MessageDTO message);
}
