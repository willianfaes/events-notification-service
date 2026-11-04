package dev.will.eventsnotification.statistics;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

// Only the id is mapped: processed_at is filled by the database default and nothing here reads it.
@Entity
@Table(name = "processed_messages")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedMessage {

    @Id
    private UUID messageId;

    public ProcessedMessage(UUID messageId) {
        this.messageId = messageId;
    }
}
