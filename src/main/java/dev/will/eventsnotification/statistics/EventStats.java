package dev.will.eventsnotification.statistics;

import dev.will.eventsnotification.registration.RegistrationMessage;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "event_stats")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class EventStats {

    // The API's event id, not generated here.
    @Id
    private Long eventId;

    private int activeRegistrations;

    public EventStats(Long eventId) {
        this.eventId = eventId;
    }

    public void apply(RegistrationMessage.Type type) {
        switch (type) {
            case REGISTRATION_CREATED -> activeRegistrations++;
            case REGISTRATION_CANCELLED -> activeRegistrations--;
        }
    }
}
