package dev.will.eventsnotification.notification;

import dev.will.eventsnotification.registration.RegistrationMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Its own consumer group: it reads every message of the topic, whatever the other groups of this service do.
@Component
@RequiredArgsConstructor
public class NotificationListener {

    private final EmailSender emailSender;

    @KafkaListener(topics = "registration-events", groupId = "notification")
    public void onMessage(RegistrationMessage message) {
        switch (message.type()) {
            case REGISTRATION_CREATED -> emailSender.sendConfirmation(message.userSub(), message.eventId());
            case REGISTRATION_CANCELLED -> emailSender.sendCancellation(message.userSub(), message.eventId());
        }
    }
}
