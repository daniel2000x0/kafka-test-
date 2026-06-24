package com.backend.producer_kafka.config;

import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Configuracion del productor Kafka con optimizaciones de rendimiento
 * y fiabilidad: acks=all, reintentos, compresion y batch.
 */
@Configuration
public class StringProducerFactoryConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * Fabrica de productores con configuracion optimizada:
     * - acks=all: espera confirmacion de todos los replicas
     * - retries=3: reintenta envios fallidos hasta 3 veces
     * - compression.type=snappy: comprime mensajes para reducir uso de red
     * - linger.ms=5: agrupa mensajes en lotes para mejorar rendimiento
     * - batch.size=16384: tamanio optimo de lote (16KB)
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        var configs = new HashMap<String, Object>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.RETRIES_CONFIG, 3);
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        configs.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    /**
     * KafkaTemplate para enviar mensajes al topic configurado.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
