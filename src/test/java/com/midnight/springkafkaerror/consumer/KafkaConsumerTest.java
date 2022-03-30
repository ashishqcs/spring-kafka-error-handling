package com.midnight.springkafkaerror.consumer;

import com.midnight.springkafkaerror.producer.Producer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
@EmbeddedKafka(partitions = 1, controlledShutdown = false, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class KafkaConsumerTest {

    @Autowired
    private Consumer consumer;

    @Autowired
    private Producer producer;

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingtoSimpleProducer_thenMessageReceived()
            throws Exception {

        producer.send("products", "product1", "This is Product1");
        producer.send("products", "product2", "This is Product2");
        producer.send("products", "product1", "This is new Product1");

        Thread.sleep(2000);
        System.out.println("");
        Thread.sleep(180000);
        //consumer.getLatch().await(10000, TimeUnit.MILLISECONDS);

        //assertThat(consumer.getLatch().getCount(), equalTo(0L));
        //assertThat(consumer.getPayload(), containsString("embedded-test-topic"));
    }
}
