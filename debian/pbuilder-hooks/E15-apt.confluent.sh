#!/bin/sh -e

# Add the Confluent APT repository for its bleeding-edge librdkafka package.

apt-get install -y wget
wget -qO - http://packages.confluent.io/deb/2.0/archive.key | apt-key add -
echo 'deb [arch=amd64] http://packages.confluent.io/deb/2.0 stable main' > /etc/apt/sources.list.d/confluent.list
