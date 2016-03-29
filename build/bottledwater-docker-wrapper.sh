#!/bin/sh

POSTGRES_CONNECTION_STRING="hostaddr=$POSTGRES_PORT_5432_TCP_ADDR port=$POSTGRES_PORT_5432_TCP_PORT dbname=postgres user=postgres"
KAFKA_BROKER="$KAFKA_PORT_9092_TCP_ADDR:$KAFKA_PORT_9092_TCP_PORT"

if [ -n "$SCHEMA_REGISTRY_PORT_8081_TCP_ADDR" ]; then
  SCHEMA_REGISTRY_URL="http://${SCHEMA_REGISTRY_PORT_8081_TCP_ADDR}:${SCHEMA_REGISTRY_PORT_8081_TCP_PORT}"

  schema_registry_opts="--schema-registry=$SCHEMA_REGISTRY_URL"
else
  schema_registry_opts=
fi

exec /usr/local/bin/bottledwater \
    --postgres="$POSTGRES_CONNECTION_STRING" \
    --broker="$KAFKA_BROKER" \
    $schema_registry_opts \
    "$@"

