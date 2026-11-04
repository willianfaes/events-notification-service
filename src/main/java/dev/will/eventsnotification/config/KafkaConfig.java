package dev.will.eventsnotification.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Configuration
public class KafkaConfig {

    // The default name the recoverer gives the dead letter topic of registration-events.
    static final String DEAD_LETTER_TOPIC = "registration-events-dlt";

    // The API sends the payload as a JSON string with no type headers. Spring Boot hands this converter to the
    // listener containers, so a listener receives the record its method declares (here RegistrationMessage).
    @Bean
    RecordMessageConverter recordMessageConverter(JsonMapper jsonMapper) {
        return new StringJacksonJsonMessageConverter(jsonMapper);
    }

    // A message that still fails after the retries goes to the dead letter topic instead of being dropped, and the
    // consumer moves on. A message that cannot be converted is not retried at all: it would fail the same way.
    // Spring Boot hands this handler to the listener containers. The recoverer logs nothing when it succeeds, so the
    // WARN is the only sign in the logs that a message left the normal flow: where it was and why, not the payload.
    @Bean
    CommonErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafkaTemplate);
        ConsumerRecordRecoverer recoverer = (failed, exception) -> {
            deadLetters.accept(failed, exception);
            log.warn("Sent {}-{}@{} to {}: {}", failed.topic(), failed.partition(), failed.offset(), DEAD_LETTER_TOPIC,
                    NestedExceptionUtils.getMostSpecificCause(exception).getMessage());
        };
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }

    // The recoverer writes to the same partition the message came from, so the dead letter topic needs at least the
    // 3 partitions of registration-events. A topic created on first use would have only one.
    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(DEAD_LETTER_TOPIC).partitions(3).replicas(1).build();
    }
}
