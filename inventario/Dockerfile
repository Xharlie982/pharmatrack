FROM maven:3.9.8-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/target/*.jar app.jar
EXPOSE 8082
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
  CMD ["sh","-lc","wget -qO- http://localhost:8082${BASE_PATH:-}/healthz || exit 1"]
ENTRYPOINT ["java","-jar","/app/app.jar"]
