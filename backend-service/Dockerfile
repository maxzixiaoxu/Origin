# syntax=docker/dockerfile:1

# ---- Stage 1: build the jar ----
# Full JDK needed to compile. We use the project's Maven wrapper so the build
# is identical to running it locally.
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app

# Copy only the files needed to resolve dependencies first. As long as these
# don't change, Docker reuses the cached dependency layer and skips the
# (slow) re-download on every code edit.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -B -q dependency:go-offline

# Now copy the source and build. Skip tests here: CI runs them; the image
# build should be fast and deterministic.
COPY src/ src/
RUN ./mvnw -B -q clean package -DskipTests

# ---- Stage 2: runtime ----
# JRE only (no compiler) => smaller image, smaller attack surface.
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

# Run as a non-root user. If the process is ever compromised it has no root
# privileges inside the container.
RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /app/target/backend-service-*.jar app.jar

EXPOSE 8080

# Container-level health check. Docker/Compose marks the container healthy
# only once /actuator/health returns 200. Alpine ships busybox wget.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -q -O- http://localhost:8080/actuator/health | grep -q '"status":"UP"' || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
