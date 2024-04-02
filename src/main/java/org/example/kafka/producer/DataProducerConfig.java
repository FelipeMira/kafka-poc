package org.example.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.example.kafka.KafkaConstants;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

@Component
public class DataProducerConfig {

    @Bean
    public KafkaTemplate<Long, org.example.avro.model.DataAvro> kafkaTemplate() {
        return new KafkaTemplate<Long, org.example.avro.model.DataAvro>(
                producerFactory()
        );
    }

    @Bean
    public ProducerFactory<Long, org.example.avro.model.DataAvro> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaConstants.KAFKA_BROKERS);
        props.put(
                ProducerConfig.CLIENT_ID_CONFIG,
                KafkaConstants.CLIENT_ID);
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class);
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        props.put(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                KafkaConstants.SCHEMA_REGISTRY_URL_CONFIG);
        return new DefaultKafkaProducerFactory(props);
    }

}
