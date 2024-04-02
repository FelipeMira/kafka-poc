package org.example.kafka.consumer;

import org.example.kafka.KafkaConstants;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import br.com.felipemira.jsonpathobject.utils.Convert;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DataConsumer {

    @KafkaListener(
            id = KafkaConstants.CLIENT_ID,
            topics = KafkaConstants.TOPIC_NAME
    )
    public void listen(org.example.avro.model.DataAvro message) {

        Convert convert = new Convert();

        String jsonMessage = convert.mapJsonPathToJsonString(message.getData());

        log.info( "Mensagem recebida='{}'", message);
        log.info("Mensagem convertida=\n'{}'", jsonMessage);
    }


}
