package dev.will.eventsnotification;

import org.springframework.boot.SpringApplication;

public class TestEventsNotificationServiceApplication {

	public static void main(String[] args) {
		SpringApplication.from(EventsNotificationServiceApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
