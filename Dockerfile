FROM gcr.io/distroless/java21-debian12:nonroot-arm64
WORKDIR /app
COPY target/query-execution-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java","-jar","app.jar"]