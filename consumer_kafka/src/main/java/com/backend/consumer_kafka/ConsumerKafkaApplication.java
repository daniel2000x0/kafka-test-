package com.backend.consumer_kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Aplicacion Spring Boot que actua como consumidor Kafka.
 * Escucha mensajes del topic "str-topic" y los procesa.
 * Expone endpoints de monitoreo en el puerto 8100.
 */
@SpringBootApplication
public class ConsumerKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerKafkaApplication.class, args);
	}

}
