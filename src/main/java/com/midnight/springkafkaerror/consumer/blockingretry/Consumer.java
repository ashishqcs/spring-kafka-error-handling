package com.midnight.springkafkaerror.consumer.blockingretry;

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
        try {

            if (message.key().equals("product2"))
                throw new RuntimeException("Exception in main consumer");

            log.info("message consumed - key: {} , value: {}, at: {}", message.key(), message.value(), LocalDateTime.now());
        } catch (Exception e) {
            log.error("failed to consume - key: {}", message.key());
            //send failed event to another retry topic - only a single retry topic is maintained
            template.send("products-retry", message.key(), message.value());
        }

    }

}
