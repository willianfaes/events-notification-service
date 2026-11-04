package dev.will.eventsnotification.config;

import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.await;

import dev.will.eventsnotification.AbstractIntegrationTest;
import dev.will.eventsnotification.statistics.ProcessedMessageRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

class DeadLetterTopicIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "registration-events";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private ProcessedMessageRepository processedMessageRepository;

    @Test
    @DisplayName("Given a message that is not valid JSON, then it goes to the dead letter topic and the next one is still consumed")
    void shouldMoveAPoisonMessageToTheDeadLetterTopic() {
        String key = Long.toString(randomEventId());
        UUID next = UUID.randomUUID();

        kafkaTemplate.send(TOPIC, key, "not json");
        kafkaTemplate.send(TOPIC, key, validPayload(next, key, ""));

        await().atMost(TIMEOUT).until(() -> processedMessageRepository.existsById(next));
        then(readDeadLetters(key)).extracting(ConsumerRecord::value).contains("not json");
    }

    @Test
    @DisplayName("Given a message with a field the contract does not have, then it is consumed normally")
    void shouldIgnoreAFieldTheContractDoesNotHave() {
        String key = Long.toString(randomEventId());
        UUID messageId = UUID.randomUUID();

        kafkaTemplate.send(TOPIC, key, validPayload(messageId, key, ",\"addedLater\":\"x\""));

        await().atMost(TIMEOUT).until(() -> processedMessageRepository.existsById(messageId));
    }

    // The JSON the API publishes, plus whatever extra fields are given.
    private static String validPayload(UUID messageId, String eventId, String extraFields) {
        return """
                {"messageId":"%s","type":"REGISTRATION_CREATED","occurredAt":"2026-11-04T14:00:00Z","eventId":%s,"userSub":"%s"%s}
                """.formatted(messageId, eventId, UUID.randomUUID(), extraFields);
    }

    // Reads the dead letter topic from the start with a new group and keeps the records of one key.
    private List<ConsumerRecord<String, String>> readDeadLetters(String key) {
        List<ConsumerRecord<String, String>> found = new ArrayList<>();
        try (Consumer<String, String> consumer = consumerFactory.createConsumer("dlt-reader-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(KafkaConfig.DEAD_LETTER_TOPIC));
            await().atMost(TIMEOUT).until(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                    if (key.equals(record.key())) {
                        found.add(record);
                    }
                });
                return !found.isEmpty();
            });
        }
        return found;
    }

    // Other tests publish to the same topic, so each test uses an event of its own.
    private static long randomEventId() {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }
}
