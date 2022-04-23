package com.midnight.springkafkaerror.consumer.nonblockingretry;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.midnight.springkafkaerror.producer.Producer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

@DirtiesContext
@SpringBootTest
@TestConfiguration("application-test.yml")
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class SingleTopicRetryConsumerTest {

    @Autowired
    private Producer producer;

    private ListAppender<ILoggingEvent> retryAppender;

    private static final Logger consumerLogger = (Logger) LoggerFactory.getLogger(SingleTopicRetryConsumer.class);

    private static final String MAIN_TOPIC = "products-main";
    private static final String RETRY_TOPIC = MAIN_TOPIC + "-retry";
    private static final String DLT_TOPIC = MAIN_TOPIC + "-dlt";

    @BeforeEach
    public void setUp() {
        retryAppender = new ListAppender<>();
        retryAppender.start();
        consumerLogger.addAppender(retryAppender);
    }


    @Test
    void testNonBlockingSingleTopicRetryConsumer() throws Exception {

        producer.send("products-main", "product1", "This is Product1");
        //KafkaTestUtils
        Thread.sleep(7000);

        assertThat(retryAppender.list).hasSize(5);
        assertThat(retryAppender.list.get(0).getFormattedMessage()).endsWith(MAIN_TOPIC);
        assertThat(retryAppender.list.get(1).getFormattedMessage()).endsWith(RETRY_TOPIC);
        assertThat(retryAppender.list.get(2).getFormattedMessage()).endsWith(RETRY_TOPIC);
        assertThat(retryAppender.list.get(3).getFormattedMessage()).endsWith(RETRY_TOPIC);
        assertThat(retryAppender.list.get(4).getFormattedMessage()).endsWith(DLT_TOPIC);
    }

}