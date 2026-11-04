package dev.will.eventsnotification.statistics;

import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.await;

import dev.will.eventsnotification.AbstractIntegrationTest;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

// Messages of one event share a key, so they are consumed in the order they were sent. Each test waits until its last
// message is processed and only then checks the counter: waiting on the counter itself could pass halfway through.
class StatisticsListenerIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "registration-events";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EventStatsRepository eventStatsRepository;

    @Autowired
    private ProcessedMessageRepository processedMessageRepository;

    @Test
    @DisplayName("Given two registrations and one cancellation, then the event has one active registration")
    void shouldCountActiveRegistrations() {
        long eventId = randomEventId();

        publish(UUID.randomUUID(), "REGISTRATION_CREATED", eventId);
        publish(UUID.randomUUID(), "REGISTRATION_CREATED", eventId);
        UUID last = publish(UUID.randomUUID(), "REGISTRATION_CANCELLED", eventId);

        awaitProcessed(last);
        then(eventStatsRepository.findById(eventId)).hasValueSatisfying(stats ->
                then(stats.getActiveRegistrations()).isEqualTo(1));
    }

    @Test
    @DisplayName("Given the same message delivered twice, then it is counted once")
    void shouldCountADuplicateMessageOnce() {
        long eventId = randomEventId();
        UUID duplicated = UUID.randomUUID();

        publish(duplicated, "REGISTRATION_CREATED", eventId);
        publish(duplicated, "REGISTRATION_CREATED", eventId);
        UUID last = publish(UUID.randomUUID(), "REGISTRATION_CREATED", eventId);

        awaitProcessed(last);
        then(eventStatsRepository.findById(eventId)).hasValueSatisfying(stats ->
                then(stats.getActiveRegistrations()).isEqualTo(2));
    }

    // The same JSON the API publishes, with the event id as key.
    private UUID publish(UUID messageId, String type, long eventId) {
        String payload = """
                {"messageId":"%s","type":"%s","occurredAt":"2026-11-04T14:00:00Z","eventId":%d,"userSub":"%s"}
                """.formatted(messageId, type, eventId, UUID.randomUUID());
        kafkaTemplate.send(TOPIC, Long.toString(eventId), payload);
        return messageId;
    }

    private void awaitProcessed(UUID messageId) {
        await().atMost(TIMEOUT).until(() -> processedMessageRepository.existsById(messageId));
    }

    // Other tests publish to the same topic, so each test uses an event of its own.
    private static long randomEventId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }
}
