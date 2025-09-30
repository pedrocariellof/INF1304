FROM openjdk:11-jre-slim

WORKDIR /app

VOLUME /app/data

COPY target/consumer-service-jar-with-dependencies.jar consumer-service.jar

EXPOSE 8080

CMD ["java", "-jar", "consumer-service.jar"]