package com.backend.consumer_kafka.listeners;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class SrtCostumerListener {
	@KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "str-topic",partitions = {"0"})
	,containerFactory = "validMessageContainerFactory")
public void listener (String  menssage) {
	log.info( "Recibiendo un message" ,menssage);
}
//offset->  funciona  dando un valor entero que  funcona para identificar  cada elemnto

@KafkaListener(groupId = "group-1", topicPartitions = @TopicPartition(topic = "str-topic",partitions = {"1"})
,containerFactory = "validMessageContainerFactory")
public void listener2(String message){
log.info("LISTENER2 ::: Recibiendo un mensaje {}",message);
}

@KafkaListener(groupId = "group-2",topics = "str-topic",containerFactory = "validMessageContainerFactory")

public void listener3(String message){
log.info("LISTENER3 ::: Recibiendo un mensaje {}",message);
}
}

