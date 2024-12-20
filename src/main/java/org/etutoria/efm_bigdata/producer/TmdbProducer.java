package org.etutoria.efm_bigdata.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import static java.lang.String.format;

@Service
@Slf4j
public class TmdbProducer {
    private  final KafkaTemplate<String,String> kafkaTemplate;

    public TmdbProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public  void sendMessageTmdb(String msg){
        kafkaTemplate.send("bigdata-tmdb",msg);
        System.out.println("sending message to tmdb-stream topic :: " + msg);

    }
}
