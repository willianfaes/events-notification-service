FROM maven:3.9.16-amazoncorretto-21 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
LABEL org.opencontainers.image.authors="willianfaes"
WORKDIR /app
COPY --from=builder /build/target/*.jar events-notification-service.jar
RUN addgroup -S spring && adduser -S spring -G spring \
 && chown -R spring:spring /app
USER spring
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "events-notification-service.jar"]
