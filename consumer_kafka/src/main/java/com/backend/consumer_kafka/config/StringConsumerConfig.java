package com.backend.consumer_kafka.config;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
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

/**
 * Configuracion de consumidores Kafka con soporte para:
 * - Deserializacion de mensajes String
 * - Manejo de errores con Dead Letter Topic (DLT)
 * - Interceptor de mensajes para filtrado
 * - Procesamiento por lotes (batch)
 */
@Slf4j
@Configuration
public class StringConsumerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * Configura la fabrica de consumidores con deserializadores String
     * y configuracion basica (offset mas temprano).
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    /**
     * Fabrica de productores necesaria para que el errorHandler
     * pueda reenviar mensajes fallidos al DLT.
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        configs.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    /**
     * KafkaTemplate usado internamente por el DeadLetterPublishingRecoverer
     * para publicar mensajes fallidos en el DLT.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Manejador de errores con reintentos (3 intentos con 1s de espera)
     * y publicacion en el Dead Letter Topic cuando se agotan los reintentos.
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        var backOff = new FixedBackOff(1000L, 3);
        var handler = new DefaultErrorHandler(recoverer, backOff);
        return handler;
    }

    /**
     * Fabrica de listeners principal con interceptor de validacion
     * y manejo de errores con DLT.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> validMessageContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            CommonErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordInterceptor(validMessageInterceptor());
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(3);
        return factory;
    }

    /**
     * Fabrica de listeners para procesamiento por lotes (batch),
     * permitiendo consumir multiples mensajes en una sola llamada.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> batchContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            CommonErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setBatchListener(true);
        factory.setConcurrency(2);
        return factory;
    }

    /**
     * Interceptor que permite filtrar o modificar mensajes antes de
     * que lleguen al listener. Actualmente registra los mensajes que
     * contienen la palabra clave "Suscribete".
     */
    private RecordInterceptor<String, String> validMessageInterceptor() {
        return (record, consumer) -> {
            if (record.value() != null && record.value().contains("Suscribete")) {
                log.info("[INTERCEPTOR] Mensaje con palabra clave 'Suscribete': {}", record.value());
            }
            return record;
        };
    }
}
