package com.backend.producer_kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicacion Spring Boot que actua como productor Kafka.
 * Expone una API REST para enviar mensajes al topic "str-topic"
 * y endpoints de monitoreo en el puerto 8080.
 */
@SpringBootApplication
public class ProducerKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ProducerKafkaApplication.class, args);
	}

}
