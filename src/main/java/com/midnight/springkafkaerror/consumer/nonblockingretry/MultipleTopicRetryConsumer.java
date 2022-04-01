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
            backoff = @Backoff(delay = 2000, multiplier = 2.0),
            autoCreateTopics = "false",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
    @KafkaListener(topics = "products-master")
    public void listen(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        log.info("message consumed - key: {} , value: {}, at: {}", message.key(), message.value(), LocalDateTime.now());
        throw new RuntimeException("Exception in master consumer");
    }

    @DltHandler
    public void dltListener(ConsumerRecord<String, String> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("message consumed - key: {} , value: {}, at: {}", message.key(), message.value(), LocalDateTime.now());
    }

}
