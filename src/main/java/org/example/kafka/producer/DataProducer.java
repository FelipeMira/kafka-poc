package org.example.kafka.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.KafkaConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DataProducer {

    @Autowired
    private KafkaTemplate<Long, org.example.avro.model.DataAvro> template;

    public void sendAsync(org.example.avro.model.DataAvro dataAvro) {
        ProducerRecord<Long, org.example.avro.model.DataAvro> record = new ProducerRecord<Long, org.example.avro.model.DataAvro>(
                KafkaConstants.TOPIC_NAME, dataAvro);
        ListenableFuture<SendResult<Long, org.example.avro.model.DataAvro>> future = template.send(record);
        future.addCallback(new ListenableFutureCallback<SendResult<Long, org.example.avro.model.DataAvro>>() {
            @Override
            public void onSuccess(SendResult<Long, org.example.avro.model.DataAvro> result) {
                log.info( "Pedido enviado com sucesso!");
            }
            @Override
            public void onFailure(Throwable e) {
                log.error(e.getMessage(), e);
            }
        });
    }
}
