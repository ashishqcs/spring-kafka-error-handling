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

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaRetryListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(retryErrorHandler());
        return factory;
    }

    public DefaultErrorHandler retryErrorHandler() {

        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(10000, 3));
    }
}
