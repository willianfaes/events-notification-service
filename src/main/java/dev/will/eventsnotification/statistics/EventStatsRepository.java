package dev.will.eventsnotification.statistics;

import org.springframework.data.jpa.repository.JpaRepository;

public interface EventStatsRepository extends JpaRepository<EventStats, Long> {
}
