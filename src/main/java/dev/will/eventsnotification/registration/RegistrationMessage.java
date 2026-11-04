package dev.will.eventsnotification.registration;

import java.time.Instant;
import java.util.UUID;

// The contract with the Events Manager API: the JSON payload of every message on the registration-events topic.
// A copy, not a shared library: the two services only share this JSON.
public record RegistrationMessage(UUID messageId, Type type, Instant occurredAt, Long eventId, String userSub) {

    public enum Type {
        REGISTRATION_CREATED,
        REGISTRATION_CANCELLED
    }
}
