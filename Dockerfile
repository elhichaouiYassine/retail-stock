# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------- build
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace

# Resolve dependencies in a cacheable layer (only poms copied).
COPY pom.xml ./
COPY retail-stock-domain/pom.xml retail-stock-domain/
COPY retail-stock-infrastructure/pom.xml retail-stock-infrastructure/
COPY retail-stock-api/pom.xml retail-stock-api/
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests dependency:go-offline || true

COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -DskipTests package \
    && cp retail-stock-api/target/retail-stock-api-all.jar /workspace/app.jar

# ---------------------------------------------------------------------------- runtime
FROM eclipse-temurin:21-jre-noble
LABEL org.opencontainers.image.title="retail-stock-api" \
      org.opencontainers.image.description="Retail Stock API (Kotlin, Ktor, PostgreSQL, Kafka)"

RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --home /app app
WORKDIR /app
COPY --from=build --chown=10001:10001 /workspace/app.jar /app/app.jar

# Java 21: generational ZGC keeps p99 pauses sub-millisecond under allocation bursts.
# Heap is sized from the container limit; the remainder covers metaspace, Netty direct buffers and threads.
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -XX:MaxRAMPercentage=70.0 -XX:InitialRAMPercentage=50.0 \
 -XX:+ExitOnOutOfMemoryError -XX:+AlwaysPreTouch -Djava.security.egd=file:/dev/urandom \
 -Dio.netty.tryReflectionSetAccessible=true -Djava.io.tmpdir=/tmp" \
    LOG_FORMAT=json

USER 10001
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
