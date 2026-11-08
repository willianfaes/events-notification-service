package dev.will.eventsnotification;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

// Every integration test extends this class, so they all share one Spring context. The containers are beans of that
// context: a test with a different configuration would get a second context, with its own Kafka and Postgres.
// The pool gives up on a connection after 2 s instead of 30 s, so a test that pauses Postgres sees each attempt fail
// while the database is down, instead of one attempt that waits the whole outage and then succeeds.
@SpringBootTest(properties = {
        "spring.datasource.hikari.connection-timeout=2000",
        "spring.datasource.hikari.validation-timeout=1000"
})
@Import(TestcontainersConfiguration.class)
public abstract class AbstractIntegrationTest {
}
