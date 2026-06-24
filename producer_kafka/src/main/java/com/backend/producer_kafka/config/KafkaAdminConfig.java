package com.backend.producer_kafka.config;

import java.util.HashMap;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Configuracion administrativa de Kafka.
 * Crea el admin client y define los topics que se usaran en el proyecto.
 */
@Configuration
public class KafkaAdminConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * Cliente administrativo de Kafka para gestion de topics.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        var configs = new HashMap<String, Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    /**
     * Define los topics que se crearan automaticamente al iniciar la aplicacion:
     * - "str-topic": topic principal con 2 particiones y 1 replica
     * - "str-topic.DLT": Dead Letter Topic para mensajes fallidos con 1 particion y 1 replica
     */
    @Bean
    public KafkaAdmin.NewTopics topics() {
        return new KafkaAdmin.NewTopics(
            TopicBuilder.name("str-topic").partitions(2).replicas(1).build(),
            TopicBuilder.name("str-topic.DLT").partitions(1).replicas(1).build()
        );
    }
}
