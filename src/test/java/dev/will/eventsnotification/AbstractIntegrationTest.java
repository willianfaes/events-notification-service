package dev.will.eventsnotification;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

// Every integration test extends this class, so they all share one Spring context. The containers are beans of that
// context: a test with a different configuration would get a second context, with its own Kafka and Postgres.
@SpringBootTest
@Import(TestcontainersConfiguration.class)
public abstract class AbstractIntegrationTest {
}
