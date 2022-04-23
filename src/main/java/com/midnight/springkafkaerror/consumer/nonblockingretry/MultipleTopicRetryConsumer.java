package com.midnight.springkafkaerror.consumer.nonblockingretry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Non-blocking retry consumer using multiple retry topics strategy with exponential backoff policy.
 */
@Slf4j
@Component
public class MultipleTopicRetryConsumer {

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 1000, multiplier = 1.0),
            autoCreateTopics = "false",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "products-master")
    public void listen(ConsumerRecord<String, String> message) {

        log.info("message consumed - key: {} , value: {}, topic: {}",
                message.key(),
                message.value(),
                message.topic());
        throw new RuntimeException("Exception in main consumer");
    }

    @DltHandler
    public void multipleTopicDLT(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("message consumed - key: {} , topic: {}", message.key(), message.topic());
    }

}
