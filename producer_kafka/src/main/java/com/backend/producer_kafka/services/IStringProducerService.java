package com.backend.producer_kafka.services;

import com.backend.producer_kafka.dto.MessageDTO;
import com.backend.producer_kafka.dto.MessageResponse;

public interface IStringProducerService {
    MessageResponse sendMessage(MessageDTO message);
}
