# syntax=docker/dockerfile:1

# ---- Stage 1: build the jar ----
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app

# Copy wrapper + pom first so the (small) dependency resolution layer is cached.
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw && ./mvnw -B -q dependency:go-offline

COPY src/ src/
RUN ./mvnw -B -q clean package

# ---- Stage 2: runtime ----
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

# Non-root user, same as the backend service.
RUN addgroup -S app && adduser -S app -G app
USER app

# finalName in pom.xml makes this a stable, version-free name.
COPY --from=build /app/target/load-balancer.jar app.jar

EXPOSE 8080

# The LB's own liveness: is the process accepting connections? (Independent of
# whether any backend is healthy — that's reported inside the body.)
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -O- http://localhost:8080/lb/health | grep -q '"status":"UP"' || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
