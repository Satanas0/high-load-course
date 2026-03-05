FROM maven:3.9.9-eclipse-temurin-21 AS build

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src src
RUN mvn package

FROM eclipse-temurin:21-alpine

COPY --from=build /app/target/*.jar /high-load-course.jar

CMD ["java", "-Djdk.virtualThreadScheduler.parallelism=1024", "-Djdk.virtualThreadScheduler.maxPoolSize=1024", "-jar", "/high-load-course.jar"]

