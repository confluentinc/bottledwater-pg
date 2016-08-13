Bottled Water for PostgreSQL
============================

How do you export water from your country? Well, you first go to your reservoir, pump out all the
water, and fill it into bottles. You then go to the streams of clear mountain water flowing into
the reservoir, and tap into them, filling the fresh water into bottles as it arrives. Then you
ship those bottles all around the world.

How do you export data from your database? Well, you first take a consistent snapshot of your
entire database, and encode it in a language-independent format. You then look at the stream of
transactions writing to your database, and parse the transaction log, encoding the
inserts/updates/deletes into the same language-independent format as they happen. Then you take
that data and ship it to your other systems: build search indexes, update caches, load it into
a data warehouse, calculate analytics, monitor it for fraud, and so on.

* [Blog post explaining the background](http://blog.confluent.io/2015/04/23/bottled-water-real-time-integration-of-postgresql-and-kafka/)
* [Watch a short demo!](http://showterm.io/fde6260d684ee3a6ee692)


How it works
------------

Bottled Water uses the [logical decoding](http://www.postgresql.org/docs/9.5/static/logicaldecoding.html)
feature (introduced in PostgreSQL 9.4) to extract a consistent snapshot and a continuous stream
of change events from a database. The data is extracted at a row level, and encoded using
[Avro](http://avro.apache.org/). A client program connects to your database, extracts this data,
and relays it to [Kafka](http://kafka.apache.org/) (you could also integrate it with other systems
if you wish, but Kafka is pretty awesome).

Key features of Bottled Water are:

* Works with any PostgreSQL database (version 9.4 or later). There are no restrictions on your
  database schema.
* No schema changes are required, no triggers or additional tables. (However, you do need to be
  able to install a PostgreSQL extension on the database server. More on this below.)
* Negligible impact on database performance.
* Transactionally consistent output. That means: writes appear only when they are committed to the
  database (writes by aborted transactions are discarded), writes appear in the same order as they
  were committed (no race conditions).
* Fault-tolerant: does not lose data, even if processes crash, machines die, the network is
  interrupted, etc.


Quickstart
----------

There are several possible ways of installing and trying Bottled Water:

* [Running in Docker](#running-in-docker) is the fastest way of getting started, but currently
  only recommended for development environments.
* [Building from source](#building-from-source) is the most flexible, but also a bit fiddly.
* There are also [Ubuntu packages](https://launchpad.net/~stub/+archive/ubuntu/bottledwater),
  built by Stuart Bishop (Canonical).


Running in Docker
-----------------

The easiest way to try Bottled Water is to use the [Docker](https://www.docker.com/) images we have
prepared. You need at least 2GB of memory to run this demo, so if you're running inside a virtual
machine (such as [Boot2docker](http://boot2docker.io/) on a Mac), please check that it is big
enough.

First, install:

* [Docker](https://docs.docker.com/installation/), which is used to run the
  individual services/containers, and
* [docker-compose](https://docs.docker.com/compose/install/), which is used to
  orchestrate the interaction between services.

After the prerequisite applications are installed, you need to build the Docker containers for Bottled Water and Postgres:

    $ make docker-compose

Once the build process finishes, set up some required environment variables,
then start up Postgres, Kafka and the [Confluent schema
registry](http://confluent.io/docs/current/schema-registry/docs/intro.html) by
running `docker-compose` as follows:

    $ export KAFKA_ADVERTISED_HOST_NAME=$(docker run --rm debian:jessie ip route | awk '/^default via / { print $3 }') \
             KAFKA_LOG_CLEANUP_POLICY=compact \
             KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
    $ docker-compose up -d kafka schema-registry postgres

The `postgres-bw` image extends the
[official Postgres docker image](https://registry.hub.docker.com/_/postgres/) and adds
Bottled Water support. However, before Bottled Water can be used, it first needs to be
enabled. To do this, start a `psql` shell for the Postgres database:

    $ docker-compose run --rm psql

When the prompt appears, enable the `bottledwater` extension, and create a database with
some test data, for example:

    create extension bottledwater;
    create table test (id serial primary key, value text);
    insert into test (value) values('hello world!');

You can keep the psql terminal open, and run the following in a new terminal.

The next step is to start the Bottled Water client, which relays data from Postgres to Kafka.
You start it like this:

    $ docker-compose up -d bottledwater-avro

You can run `docker-compose logs bottledwater-avro` to see what it's doing. Now Bottled
Water has taken the snapshot, and continues to watch Postgres for any data changes. You can
see the data that has been extracted from Postgres by consuming from Kafka (the topic name
`test` must match up with the name of the table you created earlier):

    $ docker-compose run --rm kafka-avro-console-consumer \
        --from-beginning --property print.key=true --topic test

This should print out the contents of the `test` table in JSON format (key/value separated
by tab). Now go back to the `psql` terminal, and change some data — insert, update or delete
some rows in the `test` table. You should see the changes swiftly appear in the Kafka
consumer terminal.

When you're done testing, you can destroy the cluster and it's associated data volumes with:

    $ docker-compose stop
    $ docker-compose rm -vf

Building from source
--------------------

To compile Bottled Water is just a matter of:

    make && make install

For that to work, you need the following dependencies installed:

* [PostgreSQL 9.5](http://www.postgresql.org/) development libraries (PGXS and libpq).
  (Homebrew: `brew install postgresql`;
  Ubuntu: `sudo apt-get install postgresql-server-dev-9.5 libpq-dev`)
* [libsnappy](https://code.google.com/p/snappy/), a dependency of Avro.
  (Homebrew: `brew install snappy`; Ubuntu: `sudo apt-get install libsnappy-dev`)
* [avro-c](http://avro.apache.org/), the C implementation of Avro.
  (Homebrew: `brew install avro-c`; others: build from source)
* [Jansson](http://www.digip.org/jansson/), a JSON parser.
  (Homebrew: `brew install jansson`; Ubuntu: `sudo apt-get install libjansson-dev`)
* [libcurl](http://curl.haxx.se/libcurl/), a HTTP client.
  (Homebrew: `brew install curl`; Ubuntu: `sudo apt-get install libcurl4-openssl-dev`)
* [librdkafka](https://github.com/edenhill/librdkafka) (0.9.1 or later), a Kafka client.
  (Ubuntu universe: `sudo apt-get install librdkafka-dev`, but see [known gotchas](#known-gotchas-with-older-librdkafka-versions); others: build from source)

You can see the Dockerfile for
[building the quickstart images](https://github.com/ept/bottledwater-pg/blob/master/build/Dockerfile.build)
as an example of building Bottled Water and its dependencies on Debian.

If you get errors about *Package libsnappy was not found in the pkg-config search path*,
and you have Snappy installed, you may need to create `/usr/local/lib/pkgconfig/libsnappy.pc`
with contents something like the following (be sure to check which version of _libsnappy_
is installed in your system):

    Name: libsnappy
    Description: Snappy is a compression library
    Version: 1.1.2
    URL: https://google.github.io/snappy/
    Libs: -L/usr/local/lib -lsnappy
    Cflags: -I/usr/local/include


Configuration
-------------

The `make install` command above installs an extension into the Postgres installation on
your machine, which does all the work of encoding change data into Avro. There's then a
separate client program which connects to Postgres, fetches the data, and pushes it to Kafka.

To configure Bottled Water, you need to set the following in `postgresql.conf`: (If you're
using Homebrew, you can probably find it in `/usr/local/var/postgres`. On Linux, it's
probably in `/etc/postgres`.)

    wal_level = logical
    max_wal_senders = 8
    wal_keep_segments = 4
    max_replication_slots = 4

You'll also need to give yourself the replication privileges for the database. You can do
this by adding the following to `pg_hba.conf` (in the same directory, replacing `<user>`
with your login username):

    local   replication     <user>                 trust
    host    replication     <user>  127.0.0.1/32   trust
    host    replication     <user>  ::1/128        trust

Restart Postgres for the changes to take effect. Next, enable the Postgres extension that
`make install` installed previously. Start `psql -h localhost` and run:

    create extension bottledwater;

That should be all the setup on the Postgres side. Next, make sure you're running Kafka
and the [Confluent schema registry](http://confluent.io/docs/current/schema-registry/docs/index.html),
for example by following the [quickstart](http://confluent.io/docs/current/quickstart.html).

Assuming that everything is running on the default ports on localhost, you can start
Bottled Water as follows:

    ./kafka/bottledwater --postgres=postgres://localhost

The first time this runs, it will create a replication slot called `bottedwater`,
take a consistent snapshot of your database, and send it to Kafka. (You can change the
name of the replication slot with a command line flag.) When the snapshot is complete,
it switches to consuming the replication stream.

If the slot already exists, the tool assumes that no snapshot is needed, and simply
resumes the replication stream where it last left off.

When you no longer want to run Bottled Water, you have to drop its replication slot
(otherwise you'll eventually run out of disk space, as the open replication slot
prevents the WAL from getting garbage-collected). You can do this by opening `psql`
again and running:

    select pg_drop_replication_slot('bottledwater');


### Error handling

If Bottled Water encounters an error - such as failure to communicate with Kafka or
the Schema Registry - its default behaviour is for the client to exit, halting the
flow of data into Kafka.  This may seem like an odd default, but since Postgres will
retain and replay the logical replication stream until Bottled Water acknowledges it,
it ensures that:

 * it will never miss an update (every update made in Postgres will eventually be
   written to Kafka) - _provided_ that whatever caused the error is resolved
   externally (e.g.  restoring connectivity to Kafka) and the Bottled Water client is
   then restarted;

 * it will never write corrupted data to Kafka: e.g. if unable to obtain a schema id
   from the Schema Registry for the current update, rather than writing to Kafka
   without a schema id (which would leave consumers unable to parse the update), it
   will wait until the problem is resolved.

However, in some scenarios, exiting on the first error may not be desirable:

 * if there is an error publishing for one table (e.g. if Kafka is configured not to
   autocreate topics and the corresponding topic has not been explicitly created), you
   may not want to halt updates for all other tables.

 * if the reason for the error cannot be resolved quickly (e.g. Kafka misconfiguration
   or prolonged outage), Bottled Water may threaten the stability of the Postgres
   server.  This is because Postgres will store WAL on disk for all updates made since
   Bottled Water last acknowledged (i.e. successfully published) an update.  If
   Postgres has a high write throughput, Bottled Water being unavailable may cause the
   disk on the Postgres server to fill up, likely causing Postgres to crash.

To support these scenarios, Bottled Water supports an alternative error handling
policy where it will simply log that the error occurred and drop the update it was
attempting to process, acknowledging the update so that Postgres can stop retaining
WAL.  This policy can be enabled via the `--on-error` command-line switch.  N.B. that
in this mode Bottled Water can no longer guarantee to never miss an update.


Consuming data
--------------

Bottled Water creates one Kafka topic per database table, with the same name as the
table. The messages in the topic use the table's primary key (or replica identity
index, if set) as key, and the entire table row as value. With inserts and updates,
the message value is the new contents of the row. With deletes, the message value
is null, which allows Kafka's [log compaction](http://kafka.apache.org/documentation.html#compaction)
to garbage-collect deleted values.

If a table doesn't have a primary key or replica identity index, Bottled Water will
complain and refuse to start. You can override this with the `--allow-unkeyed` option.
Any inserts and updates to tables without primary key or replica identity will be
sent to Kafka as messages without a key. Deletes to such tables are not sent to Kafka.

Messages are written to Kafka by default in a binary Avro encoding, which is
efficient, but not human-readable. To view the contents of a Kafka topic, you can use
the Avro console consumer:

    ./bin/kafka-avro-console-consumer --topic test --zookeeper localhost:2181 \
        --property print.key=true


### Output formats

Bottled Water currently supports writing messages to Kafka in one of two output
formats: Avro, or JSON.  The output format is configured via the `--output-format`
command-line switch.

Avro is recommended for large scale use, since it uses a much more efficient binary
encoding for messages, defines rules for [schema
evolution](http://docs.confluent.io/1.0/avro.html), and is able to faithfully
represent a wide range of column types.  Avro output requires an instance of the
[Confluent Schema
Registry](http://docs.confluent.io/1.0/schema-registry/docs/intro.html) to be running,
and consumers will need to query the schema registry in order to decode messages.

JSON is ideal for evaluation and prototyping, or integration with languages
without good Avro library support.  JSON is human readable, and widely supported among
programming languages.  JSON output does not require a schema registry.


Known gotchas with older librdkafka versions
--------------------------------------------

It is recommended to compile Bottled Water against librdkafka version 0.9.1 or
later.  However, Bottled Water will work with older librdkafka versions, with
degraded functionality.

At time of writing, the librdkafka-dev packages in the official Ubuntu repositories
(for all releases up to 15.10) contain a release prior to 0.8.6.  This means if you
are building on Ubuntu, building librdkafka from source is recommended, until an
updated librdkafka package is available.

### librdkafka &lt; 0.8.6: empty messages sent for deletes

As noted [above](#consuming-data), Bottled Water sends a null message to represent a
row deletion.  librdkafka only added [support for null
messages](https://github.com/edenhill/librdkafka/pull/200) in [release 0.8.6](https://github.com/edenhill/librdkafka/releases/tag/0.8.6).  If Bottled Water
is compiled against a version of librdkafka prior to 0.8.6, deletes will instead be
represented by *empty* messages, i.e. a message whose payload is an empty byte
sequence.  This means that Kafka will not garbage-collect deleted values on log
compaction, and also may confuse consumers that expect all non-null message payloads
to begin with a header.

### librdkafka &lt; 0.9.0: messages partitioned randomly

librdkafka 0.9.0+ provides a "consistent partitioner", which assigns messages to
partitions based on the hash of the key.  Bottled Water takes advantage of this
to ensure that all inserts, updates and deletes for a given key get sent to the
same partition.

If Bottled Water is compiled against a version of librdkafka prior to 0.9.0,
messages will instead be assigned randomly to partitions.  If the topic
corresponding to a given table has more than one partition, this will lead to
incorrect log compaction behaviour (e.g. if the initial insert for row 42 goes
to partition 0, then a subsequent delete for row 42 goes to partition 1, then
log compaction will be unable to garbage-collect the insert).  It will also
break any consumer relying on seeing all updates relating to a given key (e.g.
for a stream-table join).


Developing
----------

If you want to work on the Bottled Water codebase, the [Docker setup](#running-in-docker) is
a good place to start.

Bottled Water ships with a [test suite](spec) that [verifies basic
functionality](spec/functional/smoke_spec.rb), [documents supported Postgres
types](spec/functional/type_specs.rb) and [tests message publishing
semantics](spec/functional/partitioning_spec.rb).  The test suite also relies on Docker and
Docker Compose.  To run it:

 1. Install Docker and Docker Compose (see [Docker setup](#running-in-docker))
 2. Install Ruby 2.2.4 (see [ruby-lang.org](https://www.ruby-lang.org/en/downloads/))
    (required to run the tests)
 3. Install Bundler: `gem install bundler`
 4. Build the Docker images: `make docker-compose`
 5. Run the tests: `make test`

If submitting a pull request, particularly one that adds new functionality, it is highly
encouraged to include tests that exercise the changed code!


Status
------

This is early alpha-quality software. It will probably break. See [this discussion
about production readiness](https://github.com/confluentinc/bottledwater-pg/issues/96),
and [Github issues](https://github.com/confluentinc/bottledwater-pg/issues)
for a list of known issues.

Bug reports and pull requests welcome.

Note that Bottled Water has nothing to do with
[Sparkling Water](https://github.com/h2oai/sparkling-water), a machine learning
engine for Spark.


License
-------

Copyright 2015 Confluent, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
software except in compliance with the License in the enclosed file called `LICENSE`.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
