package dev.will.eventsnotification.notification;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

// Stands in for a real e-mail provider. The message carries only the user's sub (never the e-mail address), and the
// service does not call Cognito to look it up, so the "e-mail" is a log line addressed to the sub.
@Slf4j
@Component
public class EmailSender {

    public void sendConfirmation(String userSub, Long eventId) {
        log.info("Fake e-mail to {}: registration confirmed for event {}", userSub, eventId);
    }

    public void sendCancellation(String userSub, Long eventId) {
        log.info("Fake e-mail to {}: registration cancelled for event {}", userSub, eventId);
    }
}
