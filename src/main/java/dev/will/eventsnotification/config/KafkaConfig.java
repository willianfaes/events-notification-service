package dev.will.eventsnotification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class KafkaConfig {

    // The API sends the payload as a JSON string with no type headers. Spring Boot hands this converter to the
    // listener containers, so a listener receives the record its method declares (here RegistrationMessage).
    @Bean
    RecordMessageConverter recordMessageConverter(JsonMapper jsonMapper) {
        return new StringJacksonJsonMessageConverter(jsonMapper);
    }
}
