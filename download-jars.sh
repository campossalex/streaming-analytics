#!/usr/bin/env bash
# download-jars.sh — fetch connector JARs into ./lib/ for the Flink session cluster.
#
# ./lib/ is mounted read-only at /opt/flink/lib/extra/ in jobmanager, taskmanager and
# sql-client; each container copies the JARs into /opt/flink/lib/ on startup (see the
# entrypoint overrides in docker-compose.yml).

set -euo pipefail

MAVEN="https://repo1.maven.org/maven2"
FLINK_MINOR="2.3.0"
MODEL_OPENAI_VERSION="2.3.0"
KAFKA_CONNECTOR_VERSION="5.0.0-2.2"
JDBC_CONNECTOR_VERSION="4.0.0-2.0"
CDC_CONNECTOR_VERSION="3.6.0-2.2"
POSTGRES_DRIVER_VERSION="42.7.7"

LIB_DIR="$(dirname "$0")/lib"
mkdir -p "$LIB_DIR"

download() {
  local filename="$1"
  local url="$2"
  if [ -f "$LIB_DIR/$filename" ]; then
    echo "  [skip] $filename already present"
    return
  fi
  echo "  [download] $filename"
  curl -fsSL -o "$LIB_DIR/$filename" "$url"
}

echo "==> Downloading the OpenAI model provider (Flink ${FLINK_MINOR} AI functions)..."
# NOT part of the Flink distribution — released separately as flink-model-openai and
# required on the classpath by CREATE MODEL ... 'provider' = 'openai'. Without it,
# CREATE MODEL still succeeds (model DDL is lazy) and the first ML_PREDICT fails with
#   "Could not find any factories that implement ModelProviderFactory"
# The jar is self-contained: it bundles the OpenAI Java client and OkHttp.
download "flink-model-openai-${MODEL_OPENAI_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-model-openai/${MODEL_OPENAI_VERSION}/flink-model-openai-${MODEL_OPENAI_VERSION}.jar"

echo "==> Downloading Flink SQL connectors..."
# 5.0.0-2.2 is the newest Kafka release; there is no -2.3 build yet, so this is one
# Flink minor behind the runtime. Swap it if a -2.3 artifact appears.
download "flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

# The product dimension: master_product is read from PostgreSQL's logical-replication
# stream, as a versioned table for the event-time temporal join in jobs/10_sales.sql.
# The version suffix matters: CDC releases without one (3.5.0 and earlier) are built
# against Flink 1.x and die on Flink 2.x with
#   ClassNotFoundException: org.apache.flink.streaming.api.functions.source.SourceFunction
# because the legacy SourceFunction API was removed in 2.0. -2.2 is the Flink 2.x build.
download "flink-sql-connector-postgres-cdc-${CDC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-sql-connector-postgres-cdc/${CDC_CONNECTOR_VERSION}/flink-sql-connector-postgres-cdc-${CDC_CONNECTOR_VERSION}.jar"

echo "==> Downloading JDBC connector + drivers..."
# JDBC 4.x is modular: the core connector plus one artifact per dialect, and Flink ships
# no drivers. PostgreSQL is the only database, so the core, its dialect and its driver
# are all that is needed — for the sinks and the dwh catalog.
# 4.0.0-2.0 is the only JDBC release built for Flink 2.x, so it runs a few minors behind
# the 2.3 runtime.
download "flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar"

download "flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar"

download "postgresql-${POSTGRES_DRIVER_VERSION}.jar" \
  "$MAVEN/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar"

echo ""
echo "==> JARs in $LIB_DIR:"
ls -lh "$LIB_DIR"
echo ""
echo "Done. Run ./start.sh (or 'docker compose up -d') to start the cluster."
