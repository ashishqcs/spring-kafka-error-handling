package com.midnight.springkafkaerror.consumer.blockingretry;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.midnight.springkafkaerror.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext
@SpringBootTest
@EmbeddedKafka(partitions = 1, controlledShutdown = false, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class BlockingConsumerTest {

    @Autowired
    private Producer producer;

    private ListAppender<ILoggingEvent> consumerAppender;

    private ListAppender<ILoggingEvent> retryAppender;

    private static final Logger consumerLogger = (Logger) LoggerFactory.getLogger(Consumer.class);

    private static final Logger retryLogger = (Logger) LoggerFactory.getLogger(BlockingRetryConsumer.class);

    @BeforeEach
    public void setUp() {
        consumerAppender = new ListAppender<>();
        retryAppender = new ListAppender<>();
        consumerAppender.start();
        retryAppender.start();
        consumerLogger.addAppender(consumerAppender);
        retryLogger.addAppender(retryAppender);
    }


    @Test
    void testBlockingRetryConsumer()
            throws Exception {

        producer.send("products", "product1", "This is Product1");
        producer.send("products", "product2", "This is Product2");
        producer.send("products", "product1", "This is new Product1");

        Thread.sleep(7000);

        assertThat(consumerAppender.list.get(0).getFormattedMessage()).startsWith("message consumed - key: product1");
        assertThat(consumerAppender.list).extracting(ILoggingEvent::getFormattedMessage).contains("failed to consume - key: product2");

        assertThat(retryAppender.list.size()).isEqualTo(5);
        assertThat(retryAppender.list.get(0).getFormattedMessage()).startsWith("retrying message - key: product2");
    }
}
