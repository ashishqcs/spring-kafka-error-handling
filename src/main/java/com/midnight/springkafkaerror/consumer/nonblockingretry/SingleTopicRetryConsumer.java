package com.midnight.springkafkaerror.consumer.nonblockingretry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.FixedDelayStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Non-blocking retry consumer using single retry topic strategy with fixed backoff policy.
 */
@Slf4j
@Component
public class SingleTopicRetryConsumer {

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000),
            fixedDelayTopicStrategy = FixedDelayStrategy.SINGLE_TOPIC)
    @KafkaListener(topics = "products-main")
    public void listen(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.info("message consumed - key: {} , value: {}, at: {}", message.key(), message.value(), LocalDateTime.now());
        throw new RuntimeException("Exception in main consumer");
    }

    @DltHandler
    public void dltListener(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("message consumed - key: {} , value: {}, at: {}", message.key(), message.value(), LocalDateTime.now());
    }
}
