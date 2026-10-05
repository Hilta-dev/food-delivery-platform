FROM ballerina/ballerina:2201.10.0 AS build
USER root
WORKDIR /app
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/bin/*.jar /app/service.jar
CMD ["java", "-jar", "/app/service.jar"]
