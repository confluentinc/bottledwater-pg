#!/bin/sh

POSTGRES_CONNECTION_STRING="host=postgres port=5432 dbname=postgres user=postgres"
KAFKA_BROKER="kafka:9092"

# do we have a link to the schema-registry container?
if getent hosts schema-registry >/dev/null; then
  schema_registry_opts="--schema-registry=http://schema-registry:8081"
else
  schema_registry_opts=
fi

exec /usr/local/bin/bottledwater \
    --postgres="$POSTGRES_CONNECTION_STRING" \
    --broker="$KAFKA_BROKER" \
    $schema_registry_opts \
    "$@"

