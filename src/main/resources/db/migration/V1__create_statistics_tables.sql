-- Active registrations per event, as seen from the messages. A counter, not a copy of the API's data: the event itself
-- lives in the API, so there is no foreign key and no soft delete here.
CREATE TABLE event_stats
(
    event_id             BIGINT PRIMARY KEY,
    active_registrations INT NOT NULL
);

-- Every message already applied to event_stats, written in the same transaction. Delivery is at least once, so a
-- message that is already here is skipped instead of being counted twice.
CREATE TABLE processed_messages
(
    message_id   UUID PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
