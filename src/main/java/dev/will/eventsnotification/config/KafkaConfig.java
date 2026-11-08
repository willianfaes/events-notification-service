package dev.will.eventsnotification.config;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Configuration
public class KafkaConfig {

    // The default name the recoverer gives the dead letter topic of registration-events.
    static final String DEAD_LETTER_TOPIC = "registration-events-dlt";

    // Failures that say the database is unreachable, not that the message is wrong. Retrying them will work once the
    // database is back, so sending the message to the dead letter topic would only leave event_stats behind.
    private static final List<Class<? extends Exception>> DATABASE_DOWN = List.of(
            TransientDataAccessException.class,
            RecoverableDataAccessException.class,
            DataAccessResourceFailureException.class,
            CannotCreateTransactionException.class);

    // The API sends the payload as a JSON string with no type headers. Spring Boot hands this converter to the
    // listener containers, so a listener receives the record its method declares (here RegistrationMessage).
    @Bean
    RecordMessageConverter recordMessageConverter(JsonMapper jsonMapper) {
        return new StringJacksonJsonMessageConverter(jsonMapper);
    }

    // A message that still fails after the retries goes to the dead letter topic instead of being dropped, and the
    // consumer moves on. A message that cannot be converted is not retried at all: it would fail the same way.
    // While the database is down the message is retried with no limit instead, one to thirty seconds apart: its
    // partition waits, so the messages behind it keep their order. Each wait is far below max.poll.interval.ms, so
    // the consumer stays in its group. Spring Boot hands this handler to the listener containers. The recoverer logs
    // nothing when it succeeds, so the WARN is the only sign in the logs that a message left the normal flow: where
    // it was and why, not the payload.
    @Bean
    CommonErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafkaTemplate);
        ConsumerRecordRecoverer recoverer = (failed, exception) -> {
            deadLetters.accept(failed, exception);
            log.warn("Sent {}-{}@{} to {}: {}", failed.topic(), failed.partition(), failed.offset(), DEAD_LETTER_TOPIC,
                    NestedExceptionUtils.getMostSpecificCause(exception).getMessage());
        };
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));

        ExponentialBackOff untilTheDatabaseIsBack = new ExponentialBackOff(1000L, 2.0);
        untilTheDatabaseIsBack.setMaxInterval(30_000L);
        // null keeps the fixed back-off above.
        handler.setBackOffFunction((failed, exception) -> isDatabaseDown(exception) ? untilTheDatabaseIsBack : null);
        return handler;
    }

    // The listener container wraps what the listener threw, so the whole cause chain is searched.
    private static boolean isDatabaseDown(Throwable exception) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            Throwable current = cause;
            if (DATABASE_DOWN.stream().anyMatch(type -> type.isInstance(current))) {
                return true;
            }
        }
        return false;
    }

    // The recoverer writes to the same partition the message came from, so the dead letter topic needs at least the
    // 3 partitions of registration-events. A topic created on first use would have only one.
    @Bean
    NewTopic deadLetterTopic() {
        return TopicBuilder.name(DEAD_LETTER_TOPIC).partitions(3).replicas(1).build();
    }
}
