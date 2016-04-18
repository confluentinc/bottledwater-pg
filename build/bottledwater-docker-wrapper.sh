#!/usr/bin/env bash

log() { echo "$0: $@" >&2; }

declare -a bw_opts

POSTGRES_CONNECTION_STRING="hostaddr=$POSTGRES_PORT_5432_TCP_ADDR port=$POSTGRES_PORT_5432_TCP_PORT dbname=postgres user=postgres"
KAFKA_BROKER="$KAFKA_PORT_9092_TCP_ADDR:$KAFKA_PORT_9092_TCP_PORT"
bw_opts+=(--postgres="$POSTGRES_CONNECTION_STRING" --broker="$KAFKA_BROKER")

for var in "${!BOTTLED_WATER_@}"; do
  option=$(sed 's/^BOTTLED_WATER_//' <<<"$var" | tr '[:upper:]_' '[:lower:]-')
  value=${!var}
  case $(tr '[:upper:]' '[:lower:]' <<<"$value") in
    "")
      # Probably set by docker-compose env passthrough, ignore
      ;;
    true | yes | y | 1)
      # boolean options don't admit arguments
      log "Setting option --$option"
      bw_opts+=(--"$option")
      ;;
    false | no | n | 0)
      log "WARNING: Ignoring environment variable $var=$value, no support for explicitly negating option --$option"
      ;;
    *)
      log "Setting option --$option=$value"
      bw_opts+=(--"$option"="$value")
      ;;
  esac
done

if [ -n "$SCHEMA_REGISTRY_PORT_8081_TCP_ADDR" ]; then
  SCHEMA_REGISTRY_URL="http://${SCHEMA_REGISTRY_PORT_8081_TCP_ADDR}:${SCHEMA_REGISTRY_PORT_8081_TCP_PORT}"

  log "Detected schema registry, setting --schema-registry=$SCHEMA_REGISTRY_URL"
  bw_opts+=(--schema-registry="$SCHEMA_REGISTRY_URL")
fi

BOTTLEDWATER=/usr/local/bin/bottledwater
log "Running: $BOTTLEDWATER ${bw_opts[@]} $@"
exec "$BOTTLEDWATER" "${bw_opts[@]}" "$@"
