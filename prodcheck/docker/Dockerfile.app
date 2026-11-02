FROM eclipse-temurin:21-jdk
RUN apt-get update && apt-get install -y faketime && rm -rf /var/lib/apt/lists/*
