package dev.will.eventsnotification.statistics;

import dev.will.eventsnotification.registration.RegistrationMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

// Its own consumer group, independent of notification: each group keeps its own offsets on the same topic.
@Component
@RequiredArgsConstructor
public class StatisticsListener {

    private final StatisticsService statisticsService;

    @KafkaListener(topics = "registration-events", groupId = "statistics")
    public void onMessage(RegistrationMessage message) {
        statisticsService.apply(message);
    }
}
