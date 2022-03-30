package com.midnight.springkafkaerror.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
public class Consumer {


    private final KafkaTemplate<String, String> template;

    public Consumer(KafkaTemplate<String, String> template) {
        this.template = template;
    }

    @KafkaListener(topics = "products")
    public void listen(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        try{
            log.info("message consumed - key: {} , value: {}, at: {}", message.key(), message.value(), LocalDateTime.now());

            if(!message.value().equals("This is new Product1"))
                throw new RuntimeException("Exception in main consumer");
        }
        catch (Exception e){
            template.send("products-retry", message.key(), message.value());
        }

    }

}
