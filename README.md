# Events Notification Service

Consumes the registration messages that the [Events Manager API](https://github.com/willianfaes/events-manager-api) publishes to Kafka. The two services share nothing but the JSON on the `registration-events` topic.

Two consumer groups read the same topic, each with its own offsets:

- **`notification`** "sends" a confirmation or cancellation e-mail. It is a log line addressed to the user's `sub`: the message never carries the e-mail address, and the service does not call Cognito.
- **`statistics`** keeps the number of active registrations per event in `event_stats`. Each applied message id goes into `processed_messages` in the same transaction, so a message delivered twice is counted once.

A message that cannot be processed goes to `registration-events-dlt` with the reason in its headers, and the consumer moves on. A WARN in the log says where it came from.

Spring Boot 4.1, Java 21, Spring for Apache Kafka, Spring Data JPA, Flyway, PostgreSQL. Tests run against real Kafka and Postgres containers (Testcontainers).

## Running locally

It uses the Kafka broker and the Postgres container from the API's `docker-compose.yml`, with a database of its own.

1. In the API folder: `docker compose up -d db kafka`.
2. Once, create the database: `docker compose exec db createdb -U events_manager_app events_notification_db`.
3. Copy `src/main/resources/application-local.properties.example` to `application-local.properties` and set the password (the API's `DB_PASSWORD`).
4. Run `EventsNotificationServiceApplication` from the IDE, or `./mvnw spring-boot:run`. It listens on port 8082.

Register for an event through the API's Swagger and the fake e-mail shows up in this service's log.

## Rebuilding the statistics

`event_stats` is a projection of the topic, and the API keeps the topic's messages forever, so the counts can be thrown away and rebuilt by reading everything again. That fixes a counting bug, catches up after messages went to the dead letter topic, and fills a new statistic with the full history.

1. Stop the service.
2. Empty the tables, in `events_notification_db`:
   ```sql
   TRUNCATE event_stats, processed_messages;
   ```
3. Send the `statistics` group back to the first message, from the API folder:
   ```
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group statistics --topic registration-events --reset-offsets --to-earliest --execute
   ```
4. Start the service. It reads the topic from the start and fills `event_stats` again.
5. Compare with the active registrations in the API's database, `events_manager_db`:
   ```sql
   SELECT event_id, COUNT(*) FROM event_registrations WHERE is_deleted = false GROUP BY event_id ORDER BY event_id;
   ```
   ```sql
   SELECT event_id, active_registrations FROM event_stats WHERE active_registrations > 0 ORDER BY event_id;
   ```

Registrations made before the API started publishing messages are not in the topic, so they are missing from the rebuilt counts.

## License

MIT, see [LICENSE](LICENSE).
