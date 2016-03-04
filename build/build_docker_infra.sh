#!/usr/bin/env bash
set -e

##########################################################
#                                                        #
# Helper script to build Bottled Water's infrastructure. #
#                                                        #
##########################################################

POSTGRES_MAJOR_VERSION=
AVRO_VERSION=1.7.7
LIBRDKAFKA_VERSION=0.9.0
REBUILD=

# List of Docker containers that must be stopped and removed.
DOCKER_CONTAINERS=(bottledwater schema-registry kafka zookeeper postgres bwbuild)

usage() {
  echo "USAGE: $1 [-r] [-b <PostgreSQL_major_version>]"
  echo ""
  echo "DESCRIPTION"
  echo "  Bottled Water's Docker infrastructure setup."
  echo "  Pulls and builds all required Docker images to start a Bottled Water's infrastructure."
  echo ""
  echo "OPTIONS"
  echo ""
  echo "  -b <PG_MAJOR> : PostgreSQL version to use in the build. Only 9.4 and 9.5 versions are supported."
  echo ""
  echo "  -r : Stops and removes all the previously built images by this script."
}

trap 'docker rm --force "${DOCKER_CONTAINERS[@]}" || echo "No containers removed."; exit 1' SIGHUP SIGINT SIGKILL

# Exit if no argument is given.
[[ $# -eq 0 ]] && {
  usage "${0##*/}"

  exit 0
}

while getopts ":b:r" opt "$@"; do
  case $opt in
    # Set PostgreSQL version.
    b) POSTGRES_MAJOR_VERSION=$OPTARG
      ;;
    # Stop and remove previous built containers.
    r) REBUILD=1
      ;;
    \?)
      echo "ERROR: Invalid option: -$OPTARG" >&2

      usage "${0##*/}"

      exit 1
      ;;
    :)
      echo "ERROR: Option -$OPTARG requires an argument!" >&2

      usage "${0##*/}"

      exit 1
      ;;
  esac
done

if [[ -n $POSTGRES_MAJOR_VERSION ]] && [[ ! $POSTGRES_MAJOR_VERSION =~ ^9\.[4|5]$ ]]; then
  echo "PostgreSQL version must be 9.4 or 9.5!"

  exit 1
fi

if [[ -n $REBUILD ]]; then
  docker rm --force "${DOCKER_CONTAINERS[@]}" || echo "No containers removed."

  [[ -z $POSTGRES_MAJOR_VERSION ]] && exit 0
fi

# Switch PostgreSQL version in docker files.
sed -i -e "s/^FROM postgres:9\.[4|5]/FROM postgres:$POSTGRES_MAJOR_VERSION/" Dockerfile.*

# Move to base directory.
cd ../

# Build the image to compile Bottled Water's dependencies.
docker build \
  -f build/Dockerfile.build \
  -t bwbuild:v1 .

# Run the build image as a daemon to retrieve the compiled Bottled Water's dependencies.
docker run -d -t \
  --name bwbuild \
  bwbuild:v1 bash

# Retrieve the compiled Bottled Water's dependencies.
docker cp bwbuild:/avro-"$AVRO_VERSION".tar.gz .
docker cp bwbuild:/librdkafka-"$LIBRDKAFKA_VERSION".tar.gz .
docker cp bwbuild:/bottledwater-ext.tar.gz .
docker cp bwbuild:/bottledwater-bin.tar.gz .

# Stop and/or remove the bwbuild image.
#docker stop bwbuild
#docker rm bwbuild

# Build the image to compile Bottled Water's dependencies.
docker build \
  -f build/Dockerfile.client \
  -t confluent/bottledwater:0.1 .

docker build \
  -f build/Dockerfile.postgres \
  -t confluent/postgres-bw:0.1 .

# Start PG database
docker run -d \
  --name postgres \
  confluent/postgres-bw:0.1

# Sleep 5 seconds to allow the database to start up.
sleep 5

# Check if bottledwater extension can be created.
docker run --rm \
  --link postgres:postgres \
  postgres:"$POSTGRES_MAJOR_VERSION" \
    sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U postgres <<EOF
create extension if not exists bottledwater;
EOF
'

# Pull required Confluent's docker images.
docker pull confluent/kafka:latest
docker pull confluent/zookeeper:latest
docker pull confluent/schema-registry:latest

# Start Zookeeper.
docker run -d \
  --name zookeeper \
  --hostname zookeeper \
  confluent/zookeeper

# Start Kafka.
docker run -d --name kafka \
  --hostname kafka \
  --link zookeeper:zookeeper \
  --env KAFKA_LOG_CLEANUP_POLICY=compact \
  confluent/kafka

# Start Schema Registry.
docker run -d --name schema-registry \
  --hostname schema-registry \
  --link zookeeper:zookeeper \
  --link kafka:kafka \
  --env SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL=none \
  confluent/schema-registry

# Start Bottled Water.
docker run -d --name bottledwater \
  --link postgres:postgres \
  --link kafka:kafka \
  --link schema-registry:schema-registry \
  confluent/bottledwater:0.1


# Let Bottled Water start.
sleep 2

# Print Bottled Water's client log to check if it started successfuly.
docker logs bottledwater

echo "Finished building Bottled Water's docker infrastructure!"
echo "Check the Bottled Water's client log above, streaming changes should be active."
