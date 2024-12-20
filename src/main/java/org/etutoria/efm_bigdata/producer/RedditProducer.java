package org.etutoria.efm_bigdata.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

@Service

@Slf4j
public class RedditProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;//permet de produire des messages

    public RedditProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessageReddit(String msg) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("bigdata-reddit", msg);//envoie le message sur le topic reddittest, le CompletableFuture permet de savoir si le message a bien été envoyé
        future.whenComplete((result, ex) -> {//si le message a bien été envoyé, on affiche le message et l'offset
            if (ex == null) {
               System.out.println(format("Sent message=[%s] with offset=[%d]", msg, result.getRecordMetadata().offset()));
            } else {//sinon on affiche un message d'erreur
                System.out.println(format("Unable to send message=[%s] due to : %s", msg, ex.getMessage()));
            }
        });
    }
}