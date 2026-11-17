# syntax=docker/dockerfile:1
# One Dockerfile for both services. Pick the service with --build-arg MODULE=dispatch-api|dispatch-worker.

FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY dispatch-api/pom.xml dispatch-api/
COPY dispatch-worker/pom.xml dispatch-worker/
COPY common/src common/src
COPY dispatch-api/src dispatch-api/src
COPY dispatch-worker/src dispatch-worker/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -q -DskipTests -pl ${MODULE} -am package

FROM eclipse-temurin:21-jre
ARG MODULE
WORKDIR /app
RUN useradd --system --uid 10001 app
COPY --from=build /src/${MODULE}/target/${MODULE}.jar /app/app.jar
USER app
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseZGC"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
