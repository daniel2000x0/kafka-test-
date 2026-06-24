# Test Kafka

Proyecto Spring Boot con Apache Kafka que implementa un **productor** y un **consumidor** de mensajes, junto con un entorno Dockerizado con ZooKeeper, Kafka Broker y Kafdrop.

## Tecnologias

- Java 25
- Spring Boot 4.1.0
- Apache Kafka (Confluent 7.4.4)
- Maven
- Docker & Docker Compose
- Lombok
- OpenAPI / Swagger (springdoc)
- Kafdrop (Kafka web UI)
- Jackson (JSON serializacion con JavaTimeModule)

## Estructura del proyecto

```
testkafka/
├── docker-compose.yml              # Infraestructura Kafka (ZooKeeper, Broker, Kafdrop)
│
├── producer_kafka/                 # Aplicacion productora (puerto 8080)
│   ├── pom.xml
│   └── src/main/java/com/backend/producer_kafka/
│       ├── ProducerKafkaApplication.java
│       ├── config/
│       │   ├── KafkaAdminConfig.java                # Admin: crea topics automaticamente
│       │   ├── StringProducerFactoryConfig.java      # ProducerFactory con acks=all, retries, compression
│       │   ├── JacksonConfig.java                    # ObjectMapper con JavaTimeModule
│       │   └── OpenApiConfig.java                   # Documentacion Swagger
│       ├── dto/
│       │   ├── MessageDTO.java                      # Request: id, content, source, timestamp
│       │   └── MessageResponse.java                 # Response: messageId, partition, offset, status
│       ├── exception/
│       │   └── GlobalExceptionHandler.java          # Manejador global de errores
│       ├── resource/
│       │   └── StringProducerResource.java          # POST /producer, GET /producer/health
│       └── services/
│           └── StringProducerService.java           # Envia mensajes al topic
│
└── consumer_kafka/                 # Aplicacion consumidora (puerto 8100)
    ├── pom.xml
    └── src/main/java/com/backend/consumer_kafka/
        ├── ConsumerKafkaApplication.java
        ├── config/
        │   ├── StringConsumerConfig.java            # ConsumerFactory, DLT, batch, interceptor
        │   └── JacksonConfig.java                   # ObjectMapper con JavaTimeModule
        ├── controller/
        │   └── ConsumerController.java              # GET /consumer/health, GET /consumer/info
        ├── dto/
        │   └── MessageDTO.java                     # DTO del mensaje recibido
        ├── listeners/
        │   ├── SrtCustomerListener.java             # Listener principal (grupo str-group)
        │   ├── SrtPartitionConsumerListener.java    # Demostracion de particiones (0, 1) y grupo-2
        │   ├── BatchConsumerListener.java           # Consumidor por lotes (grupo str-group-batch)
        │   └── DltConsumerListener.java            # Consumidor del Dead Letter Topic
        └── services/
            ├── IStringConsumerService.java          # Interfaz del servicio
            └── StringConsumerService.java           # Procesamiento de mensajes
```

## Requisitos

- Docker & Docker Compose
- JDK 25
- Apache Maven

## Inicio rapido

### 1. Iniciar infraestructura Kafka

```bash
docker-compose up -d
```

Esto inicia:
- **ZooKeeper** en el puerto `2181`
- **Kafka Broker** en el puerto `9092`
- **Kafdrop** (UI web) en `http://localhost:19000`

### 2. Ejecutar el productor

```bash
cd producer_kafka
./mvnw spring-boot:run
```

El productor inicia en el puerto `8080`.

### 3. Ejecutar el consumidor (en otra terminal)

```bash
cd consumer_kafka
./mvnw spring-boot:run
```

El consumidor inicia en el puerto `8100`.

### 4. Enviar un mensaje

```bash
curl -X POST http://localhost:8080/producer \
  -H "Content-Type: application/json" \
  -d '{"content": "Hola Kafka!"}'
```

Respuesta exitosa:
```json
{
  "messageId": "uuid-del-mensaje",
  "content": "Hola Kafka!",
  "partition": 0,
  "offset": 0,
  "timestamp": 1719000000000,
  "status": "SENT"
}
```

## Endpoints

### Productor (puerto 8080)

| Metodo | Path                | Descripcion                                  |
|--------|---------------------|----------------------------------------------|
| POST   | `/producer`         | Envia un mensaje JSON a `str-topic`          |
| GET    | `/producer/health`  | Health check del productor                   |
| GET    | `/swagger-ui.html`  | Documentacion interactiva Swagger            |

### Consumidor (puerto 8100)

| Metodo | Path                | Descripcion                                  |
|--------|---------------------|----------------------------------------------|
| GET    | `/consumer/health`  | Health check del consumidor                  |
| GET    | `/consumer/info`    | Informacion del servicio consumidor          |

## Formato del mensaje (MessageDTO)

```json
{
  "id": "uuid (opcional, se autogenera)",
  "content": "texto del mensaje (requerido, max 500 chars)",
  "source": "origen (opcional, default: producer_kafka)",
  "timestamp": "ISO-8601 (opcional, se autogenera)"
}
```

## Consumidores

| Grupo           | Tipo        | Particiones | Descripcion                              |
|-----------------|-------------|-------------|------------------------------------------|
| `str-group`     | Individual  | Todas       | Consumidor principal con servicio        |
| `group-1`       | Particion 0 | Solo 0      | Demostracion de particion especifica     |
| `group-1`       | Particion 1 | Solo 1      | Demostracion de particion especifica     |
| `group-2`       | Individual  | Todas       | Consumidor adicional de demostracion     |
| `str-group-batch` | Batch     | Todas       | Procesamiento por lotes                  |
| `str-group-dlt` | Individual  | Todas       | Procesa mensajes del Dead Letter Topic   |

## Topics

Los topics se crean automaticamente al iniciar el productor:

| Topic             | Particiones | Replicas | Descripcion                              |
|-------------------|-------------|----------|------------------------------------------|
| `str-topic`       | 2           | 1        | Topic principal de mensajes              |
| `str-topic.DLT`   | 1           | 1        | Dead Letter Topic para mensajes fallidos |

## Puertos

| Servicio          | Puerto   |
|-------------------|----------|
| Productor         | 8080     |
| Consumidor        | 8100     |
| Kafka Broker      | 9092     |
| Kafdrop (UI)      | 19000    |

## Manejo de errores

- **Validacion**: los mensajes con `content` vacio o mayor a 500 chars reciben un `400 Bad Request`
- **JSON malformado**: el manejador global retorna `400 Bad Request` con descripcion del error
- **Reintentos**: el consumidor reintenta 3 veces con 1 segundo de espera antes de enviar al DLT
- **Dead Letter Topic**: los mensajes que fallan todos los reintentos se publican en `str-topic.DLT`

## Configuracion del productor

- `acks=all`: espera confirmacion de todos los replicas
- `retries=3`: reintenta envios fallidos
- `compression.type=snappy`: comprime mensajes
- `linger.ms=5`: agrupa mensajes en lotes
- `batch.size=16384`: tamanio optimo de lote (16KB)

## Documentacion de la API

La documentacion interactiva (Swagger) esta disponible en:
```
http://localhost:8080/swagger-ui.html
```

## Notas

- No es necesario tener Kafka instalado localmente; la infraestructura se ejecuta con Docker
- Los warnings de conexion en los tests son normales cuando no hay un broker Kafka corriendo
