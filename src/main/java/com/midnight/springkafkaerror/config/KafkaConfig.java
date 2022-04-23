package com.midnight.springkafkaerror.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.retrytopic.DefaultDestinationTopicResolver;
import org.springframework.kafka.retrytopic.DestinationTopicResolver;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Clock;
import java.util.Collections;

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
        consumerFactory.getListeners();
        return factory;
    }

    // This is a blocking retry (will move offset only when all tries are completed) error handler configured with
    // DeadLetterPublishingRecoverer which publishes event to DLT when tries are over
    public DefaultErrorHandler retryErrorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 3));
    }

    //Set Exception classifier within DestinationTopicResolver to control retry for particular exceptions.
    // Here we are setting retry for all exceptions
    @Bean(name = RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME)
    public DestinationTopicResolver destinationTopicResolver(ApplicationContext context) {
        DefaultDestinationTopicResolver resolver = new DefaultDestinationTopicResolver(Clock.systemUTC(), context);
        resolver.defaultFalse();
        resolver.setClassifications(Collections.emptyMap(), true);
        return resolver;
    }

}
