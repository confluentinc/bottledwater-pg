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
* [avro-c](http://avro.apache.org/) (1.8.0 or later), the C implementation of Avro.
  (Homebrew: `brew install avro-c`; others: build from source)
* [Jansson](http://www.digip.org/jansson/), a JSON parser.
  (Homebrew: `brew install jansson`; Ubuntu: `sudo apt-get install libjansson-dev`)
* [libcurl](http://curl.haxx.se/libcurl/), a HTTP client.
  (Homebrew: `brew install curl`; Ubuntu: `sudo apt-get install libcurl4-openssl-dev`)
* [librdkafka](https://github.com/edenhill/librdkafka) (0.9.1 or later), a Kafka client.
  (Ubuntu universe: `sudo apt-get install librdkafka-dev`, but see [known gotchas](#known-gotchas-with-older-dependencies); others: build from source)

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

The first time this runs, it will create a replication slot called `bottledwater`,
take a consistent snapshot of your database, and send it to Kafka. (You can change the
name of the replication slot with the `--slot` [command line
flag](#command-line-options).) When the snapshot is complete, it switches to consuming
the replication stream.

If the slot already exists, the tool assumes that no snapshot is needed, and simply
resumes the replication stream where it last left off.

In some scenarios, if you only care about streaming ongoing changes (and not
replicating the existing database contents into Kafka), you may want to skip the
snapshot - e.g. to avoid the performance overhead of taking the snapshot, or because
you are repointing Bottled Water at a newly promoted replica.  In that case, you can
pass `--skip-snapshot` at the [command line](#command-line-options).  (This option is
ignored if the replication slot already exists.)

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
WAL.  This policy can be enabled via the `--on-error` [command-line
switch](#command-line-options).  N.B. that in this mode Bottled Water can no longer
guarantee to never miss an update.


Consuming data
--------------

Bottled Water creates one Kafka topic per database table, with the same name as the
table. The messages in the topic use the table's primary key (or replica identity
index, if set) as key, and the entire table row as value. With inserts and updates,
the message value is the new contents of the row. With deletes, the message value
is null, which allows Kafka's [log compaction](http://kafka.apache.org/documentation.html#compaction)
to garbage-collect deleted values.

If a table doesn't have a primary key or replica identity index, Bottled Water will
complain and refuse to start. You can override this with the `--allow-unkeyed`
[option](#command-line-options).  Any inserts and updates to tables without primary
key or replica identity will be sent to Kafka as messages without a key. Deletes to
such tables are not sent to Kafka.

Messages are written to Kafka by default in a binary Avro encoding, which is
efficient, but not human-readable. To view the contents of a Kafka topic, you can use
the Avro console consumer:

    ./bin/kafka-avro-console-consumer --topic test --zookeeper localhost:2181 \
        --property print.key=true


### Output formats

Bottled Water currently supports writing messages to Kafka in one of two output
formats: Avro, or JSON.  The output format is configured via the `--output-format`
[command-line switch](#command-line-options).

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


### Topic names

For each table being streamed, Bottled Water publishes messages to a corresponding
Kafka topic.  The naming convention for topics is
*\[topic_prefix\].\[postgres_schema_name\].table_name*:

 * *table_name* is the name of the table in Postgres.
 * *postgres_schema_name* is the name of the Postgres
   [schema](https://www.postgresql.org/docs/current/static/sql-createschema.html) the
   table belongs to; this is omitted if the schema is "public" (the default schema
   under the default Postgres configuration).  N.B. this requires the avro-c library
   to be [at least version 0.8.0](#avro-c--080-schema-omitted-from-topic-name).
 * *topic_prefix* is omitted by default, but may be configured via the
   `--topic-prefix` [command-line option](#command-line-options).  A prefix is useful:
       * to prevent name collisions with other topics, if the Kafka broker is also
         being used for other purposes besides Bottled Water.
       * if you want to stream several databases into the same broker, using a
         separate Bottled Water instance with a different prefix for each database.
       * to make it easier for a Kafka consumer to consume updates from all Postgres
         tables, by using a topic regex that matches the prefix.

For example:

 * with no prefix configured, a table named "users" in the public (default) schema
   would be streamed to a topic named "users".
 * with `--topic-prefix=bottledwater`, a table named "transactions" in the
   "point-of-sale" schema would be streamed to a topic named
   "bottledwater.point-of-sale.transactions".

(Support for [namespaces in
Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-37+-+Add+Namespaces+to+Kafka)
has been proposed that would replace this sort of ad-hoc prefixing, but it's still
under discussion.)


Known gotchas with older dependencies
-------------------------------------

It is recommended to compile Bottled Water against the versions of librdkafka and
avro-c specified [above](#building-from-source).  However, Bottled Water may work with
older versions, with degraded functionality.

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

### avro-c &lt; 0.8.0: schema omitted from topic name

Bottled Water encodes the Postgres schema to which tables belong in the Avro schema
namespace.  Support for accessing the schema namespace was added to the Avro C library
in version 0.8.0, so prior releases do not have access to this information.

If Bottled Water is compiled against a version of avro-c prior to 0.8.0, the schema
will be omitted from the [Kafka topic name](#topic-names).  This means that tables
with the same names in different schemas will have changes streamed to the same topic.


Command-line options
--------------------

This serves as a reference for the various command-line options accepted by the
Bottled Water client, annotated with links to the relevant areas of documentation.
If this disagrees with the output of `bottledwater --help`, then `--help` is correct
(and please file a pull request to update this reference!).


 * `-d`, `--postgres=postgres://user:pass@host:port/dbname` **(required)**:
   Connection string or URI of the PostgreSQL server.

 * `-s`, `--slot=slotname` *(default: bottledwater)*:
   Name of [replication slot](#configuration).  The slot is automatically created on
   first use.

 * `-b`, `--broker=host1[:port1],host2[:port2]...` *(default: localhost:9092)*:
   Comma-separated list of Kafka broker hosts/ports.

 * `-r`, `--schema-registry=http://hostname:port` *(default: http://localhost:8081)*:
   URL of the service where Avro schemas are registered.  (Used only for
   `--output-format=avro`.  Omit when `--output-format=json`.)

 * `-f`, `--output-format=[avro|json]` *(default: avro)*:
   How to encode the messages for writing to Kafka.  See discussion of [output
   formats](#output-formats).

 * `-u`, `--allow-unkeyed`:
   Allow export of tables that don't have a primary key.  This is [disallowed by
   default](#consuming-data), because updates and deletes need a primary key to
   identify their row.

 * `-p`, `--topic-prefix=prefix`:
   String to prepend to all [topic names](#topic-names).  e.g. with
   `--topic-prefix=postgres`, updates from table "users" will be written to topic
   "postgres.users".

 * `-e`, `--on-error=[log|exit]` *(default: exit)*:
   What to do in case of a transient error, such as failure to publish to Kafka.  See
   discussion of [error handling](#error-handling).

 * `-x`, `--skip-snapshot`:
   Skip taking a [consistent snapshot](#configuration) of the existing database
   contents and just start streaming any new updates.  (Ignored if the replication
   slot already exists.)

 * `-C`, `--kafka-config property=value`:
   Set global configuration property for Kafka producer (see [librdkafka
   docs](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)).

 * `-T`, `--topic-config property=value`:
   Set topic configuration property for Kafka producer (see [librdkafka
   docs](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)).

 * `--config-help`:
   Print the list of Kafka configuration properties.

 * `-h`, `--help`: Print this help text.


Developing
----------

If you want to work on the Bottled Water codebase, the [Docker setup](#running-in-docker) is
a good place to start.

Bottled Water ships with a [test suite](spec) that [verifies basic
functionality](spec/functional/message_spec.rb), [documents supported Postgres
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

Bottled Water has been tested on a variety of use cases and Postgres schemas, and is
believed to be fairly stable.  In particular, because of its design, it is unlikely to
corrupt the data in Postgres. However, it has not yet been run on large production
databases, or for long periods of time, so proceed with caution if you intend to use
it in production.  See [this discussion about production
readiness](https://github.com/confluentinc/bottledwater-pg/issues/96), and [Github
issues](https://github.com/confluentinc/bottledwater-pg/issues) for a list of known
issues.

Bug reports and pull requests welcome.

Note that Bottled Water has nothing to do with
[Sparkling Water](https://github.com/h2oai/sparkling-water), a machine learning
engine for Spark.


License
-------

Copyright 2015 Confluent, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
software except in compliance with the License in the enclosed file called
[`LICENSE`](LICENSE).

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

See [`CONTRIBUTORS.md`](CONTRIBUTORS.md) for a list of contributors.
