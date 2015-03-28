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

* [Watch a short demo!](http://showterm.io/fde6260d684ee3a6ee692)


How it works
------------

Bottled Water uses the [logical decoding](http://www.postgresql.org/docs/9.4/static/logicaldecoding.html)
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


Building
--------

To compile Bottled Water is just a matter of:

    make && make install

For that to work, you need the following dependencies installed:

* [PostgreSQL 9.4](http://www.postgresql.org/) development libraries (PGXS and libpq).
  (Homebrew: `brew install postgresql`; Ubuntu: `sudo apt-get install postgresql-server-dev-9.4`)
* [librdkafka](https://github.com/edenhill/librdkafka), a Kafka client.
  Build from source.
* [libsnappy](https://code.google.com/p/snappy/), a dependency of Avro.
  (Homebrew: `brew install snappy`)
* [avro-c](http://avro.apache.org/), the C implementation of Avro.
  (Homebrew: `brew install avro-c`)
* [Jansson](http://www.digip.org/jansson/), a JSON parser, version 2.6 or above.
  (Homebrew: `brew install jansson`)

If you get errors about *Package libsnappy was not found in the pkg-config search path*,
and you have Snappy installed, you may need to create `/usr/local/lib/pkgconfig/libsnappy.pc`
with contents something like the following:

    Name: libsnappy
    Description: Snappy is a compression library
    Version: 1.1.2
    URL: https://code.google.com/p/snappy/
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

Messages are written to Kafka in a binary Avro encoding, which is efficient, but not
human-readable. To view the contents of a Kafka topic, you can use the Avro console
consumer:

    ./bin/kafka-avro-console-consumer --topic test --zookeeper localhost:2181 \
        --property print.key=true


Status
------

This is early alpha-quality software. It will probably break. See `TODO.md` for a
list of known issues.
