package dev.will.eventsnotification.notification;

import static org.assertj.core.api.BDDAssertions.then;
import static org.awaitility.Awaitility.await;

import dev.will.eventsnotification.AbstractIntegrationTest;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;

// The fake e-mail is a log line, so the test reads the output instead of spying on a bean: a @MockitoSpyBean here
// would change the configuration and start a second context, with its own containers.
@ExtendWith(OutputCaptureExtension.class)
class NotificationListenerIntegrationTest extends AbstractIntegrationTest {

    private static final String TOPIC = "registration-events";
    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("Given a REGISTRATION_CREATED message, then a confirmation e-mail is sent to the user")
    void shouldSendConfirmationWhenRegistrationIsCreated(CapturedOutput output) {
        String userSub = UUID.randomUUID().toString();

        publish("REGISTRATION_CREATED", 42L, userSub);

        await().atMost(TIMEOUT).untilAsserted(() ->
                then(output.getOut()).contains("Fake e-mail to " + userSub + ": registration confirmed for event 42"));
    }

    @Test
    @DisplayName("Given a REGISTRATION_CANCELLED message, then a cancellation e-mail is sent to the user")
    void shouldSendCancellationWhenRegistrationIsCancelled(CapturedOutput output) {
        String userSub = UUID.randomUUID().toString();

        publish("REGISTRATION_CANCELLED", 42L, userSub);

        await().atMost(TIMEOUT).untilAsserted(() ->
                then(output.getOut()).contains("Fake e-mail to " + userSub + ": registration cancelled for event 42"));
    }

    // The same JSON the API publishes, with the event id as key.
    private void publish(String type, Long eventId, String userSub) {
        String payload = """
                {"messageId":"%s","type":"%s","occurredAt":"2026-11-04T14:00:00Z","eventId":%d,"userSub":"%s"}
                """.formatted(UUID.randomUUID(), type, eventId, userSub);
        kafkaTemplate.send(TOPIC, eventId.toString(), payload);
    }
}
