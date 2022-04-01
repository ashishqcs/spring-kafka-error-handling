package com.midnight.springkafkaerror.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@EnableKafka
@Configuration
public class KafkaConfig {

    private final KafkaTemplate<Object, Object> template;

    private final ConsumerFactory<String, String> consumerFactory;

    public KafkaConfig(KafkaTemplate<Object, Object> template, ConsumerFactory<String, String> consumerFactory) {
        this.template = template;
        this.consumerFactory = consumerFactory;
    }


    //Container Factory containing bocking error handler
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaBlockingRetryContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(retryErrorHandler());
        return factory;
    }

    // This is a blocking retry (will move offset only when all tries are completed) error handler configured with
    // DeadLetterPublishingRecoverer which publishes event to DLT when tries are over
    public DefaultErrorHandler retryErrorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2000, 3));
    }
}
