## Spring Kafka Error Handling

### Introduction:

***
This springboot project demonstrates blocking and non-blocking error handling - retry, recovery and dlt using spring
kafka.

### Blocking Retries:

***
Re-processing failed messages is a necessary step in an event driven architecture.

Kafka internally does not provide this functionality, consumer must take care of this.

When consumer attempts to reprocess failed messages continuously in real time until all retry attempts are over and
blocks the messages ahead in the queue is blocking retry. If message still fails after all retry attempts, it is passed
to DLT (
Dead Letter Topic ).

Spring Kafka provides ways to simply implement retries at consumer side.

```java

@Configuration
public class KafkaConfig {
    
    //Container Factory containing bocking error handler
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaBlockingRetryContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(template), new FixedBackOff(2000, 3))
        );
        return factory;
    }
}
```

### Non Blocking Retries:

***

Retrying is done by creating separate retry topics and retry consumers at delayed intervals.

* Unblocks the main topic for real time traffic.
* Multiple strategies can be opted for non blocking retries like Single-Topic-Multiple-BackOff, Multiple-Topic-Exponential-BackOff.

Retrying errors untill they are pushed to DLT (It could be multiple topics like explained below **OR** single retry topic - both have their own pros and cons).

* If message processing fails, the message is forwarded to a retry topic with a back off timestamp.
* The retry topic consumer then checks the timestamp and if it's not due it pauses the consumption for that topic's
  partition.
* When it is due the partition consumption is resumed, and the message is consumed again.
* If the message processing fails again the message will be forwarded to the next retry topic, and the pattern is
  repeated until a successful processing occurs, or the attempts are exhausted,
* If all retry attempts are exhausted the message is sent to the Dead Letter Topic for visibility and diagnosis.
* Dead letter Topic messages can be reprocessed by being published back into the first retry topic. This way, they have
  no influence of the live traffic.

> Since Spring Kafka 2.7, it has added support to implement non blocking retries using different topics.

```java
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
```
`OR`

```java
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
```
