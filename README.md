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


How to set up
-------------

Prerequisites (Mac OS using Homebrew):

    brew install avro-c postgresql

Prerequisites (Debian-based Linux distributions, e.g. Ubuntu):

    sudo apt-get install postgresql postgresql-server-dev-9.4

This project depends on the C implementation of [Avro](http://avro.apache.org/). 
If it's not in your package manager, you can 
[download the source](http://www.apache.org/dyn/closer.cgi/avro/) of avro-c
and build it yourself.

This project also requires [librdkafka](https://github.com/edenhill/librdkafka), a
C client for Kafka. Build and install it as per the README.

If you get errors about *Package libsnappy was not found in the pkg-config search path*,
and you have Snappy installed, you may need to create `/usr/local/lib/pkgconfig/libsnappy.pc`
with contents something like the following:

    Name: libsnappy
    Description: Snappy is a compression library
    Version: 1.1.2
    URL: https://code.google.com/p/snappy/
    Libs: -L/usr/local/lib -lsnappy
    Cflags: -I/usr/local/include

Config changes to `pg_hba.conf`. (If you're using Homebrew, you can probably find it in
`/usr/local/var/postgres`. On Linux, it's probably in `/etc/postgres`.)

    create extension bottledwater;
