ARG PG_VERSION=16
FROM postgres:${PG_VERSION}
RUN apt-get update && apt-get install -y faketime && rm -rf /var/lib/apt/lists/*
