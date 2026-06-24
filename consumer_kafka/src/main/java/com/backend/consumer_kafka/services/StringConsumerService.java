package com.backend.consumer_kafka.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.backend.consumer_kafka.dto.MessageDTO;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class StringConsumerService implements IStringConsumerService {

    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public void processMessage(MessageDTO message) {
        log.info("Received message | id={} content='{}' source={} timestamp={}",
                message.id(), message.content(), message.source(), message.timestamp());
    }
}
