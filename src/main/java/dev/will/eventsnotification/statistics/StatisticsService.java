package dev.will.eventsnotification.statistics;

import dev.will.eventsnotification.registration.RegistrationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class StatisticsService {

    private final EventStatsRepository eventStatsRepository;
    private final ProcessedMessageRepository processedMessageRepository;

    // Marking the message and changing the counter happen in one transaction: either both are saved or neither.
    // Checking before inserting is safe because messages of one event (and so every copy of one message) arrive on
    // the same partition, read by a single thread; the primary key of processed_messages is the last guard.
    @Transactional
    public void apply(RegistrationMessage message) {
        if (processedMessageRepository.existsById(message.messageId())) {
            log.info("Skipping message {}: already applied", message.messageId());
            return;
        }
        processedMessageRepository.save(new ProcessedMessage(message.messageId()));

        EventStats stats = eventStatsRepository.findById(message.eventId())
                .orElseGet(() -> new EventStats(message.eventId()));
        stats.apply(message.type());
        eventStatsRepository.save(stats);
    }
}
