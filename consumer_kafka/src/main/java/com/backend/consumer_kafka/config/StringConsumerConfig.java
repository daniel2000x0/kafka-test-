package com.backend.consumer_kafka.config;

import java.util.HashMap;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.util.backoff.FixedBackOff;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class StringConsumerConfig {
	@Autowired

	private KafkaProperties kafkaProperties;

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		var configs = new HashMap<String, Object>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return new DefaultKafkaConsumerFactory<>(configs);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		var configs = new HashMap<String, Object>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(configs);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public CommonErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
		var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
		var backOff = new FixedBackOff(1000L, 3);
		var handler = new DefaultErrorHandler(recoverer, backOff);
		return handler;
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> validMessageContainerFactory(
			ConsumerFactory<String, String> consumerFactory,
			CommonErrorHandler errorHandler) {
		var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
		factory.setConsumerFactory(consumerFactory);
		factory.setRecordInterceptor(validMessage());
		factory.setCommonErrorHandler(errorHandler);
		return factory;
	}

	private RecordInterceptor<String, String> validMessage() {
		return (record, consumer) -> {
			if (record.value() != null && record.value().contains("Suscribete")) {
				log.info("Message contains keyword 'Suscribete': {}", record.value());
				return record;
			}
			return record;
		};
	}
}
