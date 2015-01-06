SAMZA-212: PostgreSQL consumer design
=====================================

Background
----------

Samza's [state management](http://samza.incubator.apache.org/learn/documentation/0.8/container/state-management.html)
facility is designed to support stream-to-table joins, where the "table" in question is updated
from a stream of data change events. This is not just a neat feature, but it reflects a fundamental
aspect of data systems: there is a
[duality](http://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
between a stream of changes and the contents of a database. In principle, you can always generate
one from the other.

In practice, getting a log of changes out of a database is still quite hard. For example,
[GoldenGate](http://www.oracle.com/us/products/middleware/data-integration/goldengate/overview/index.html)
does this for Oracle, and LinkedIn implemented a system called [Databus](http://www.socc2012.org/s18-das.pdf)
(also for Oracle), which does a similar thing. Other databases are currently less well served.
Trying to implement change capture at the application level (e.g. by dual-writing to a database and
a message queue) is fraught with risk of race conditions and inconsistencies.

This document describes a proposal to implement change data capture for
[Postgres](http://www.postgresql.org/), integrated with [Samza](http://samza.incubator.apache.org/),
with the goal of making it very simple to use and reliable.


Requirements
------------

* It must be possible to reconstruct the entire database contents from the stream of changes,
  without missing any data or introducing any inconsistencies. Thus, when the change capture
  process is started, it must first take a consistent snapshot of the database (capture the entire
  database contents at one moment in time), and then create a stream of incremental changes since
  that snapshot.
* Fairly low latency: a change should be processed within 100ms of being committed to the database.
* Transactional consistency: changes from committed transactions are processed, and aborted
  transactions are ignored. In the case of race conditions, changes are processed in the same order
  as they were committed to the database (if you apply the writes in the same order, you get back
  the same end result).
* Must work with a replicated database setup, including failover after leader failure.
* Minimal operational burden: should not add significant load to the database, should not require
  service interruption (downtime, full table lock), and should minimize the amount of stuff that
  the DBA needs to do manually.
* Should work with any database schema, should not require any modification of the database schema
  (no additional tables or columns), and should not limit the user's ability to change the database
  schema.
* Integration with Kafka: given Kafka's growing popularity as a [unified log](http://www.manning.com/dean/),
  a common use case will be to publish the stream of database changes to Kafka, from where it can
  be consumed by other Samza jobs, archived to Hadoop, loaded into a data warehouse, etc.
* Give users flexibility of how they want data represented: encoding (JSON, Avro, Protocol Buffers,
  etc.), grouping (all changes in one stream, separate stream per table, etc.), filtering
  (redact sensitive data, e.g. password hashes from a user table, while publishing the rest),
  formatting (e.g. represent a datetime value as a Unix timestamp or as ISO 8601 string), etc.
* No loss of fidelity: change data should allow the database to be reconstructed exactly, without
  any approximation such as lossy datatype conversion.
* Fault-tolerant: should not lose any data, even if the job crashes, the network is interrupted,
  the database goes down, etc.


Integration with Samza
----------------------

One possible approach would be to implement a Postgres-to-Kafka relay without using Samza, but I
think we gain a lot from using Samza.

I propose creating a new Samza submodule, `samza-postgres`, which contains:

* All the necessary code and configuration to interface with Postgres.
* A [SystemConsumer](http://samza.incubator.apache.org/learn/documentation/0.8/api/javadocs/org/apache/samza/system/SystemConsumer.html)
  implementation which makes a consistent database snapshot and a stream of database changes
  available to a Samza job.
* Example [StreamTask](http://samza.incubator.apache.org/learn/documentation/0.8/api/javadocs/org/apache/samza/task/StreamTask.html)
  implementations which write the changes to output streams. We may have one implementation that
  writes all changes to one stream, and another implementation that uses a different output
  stream for each database table.

The thinking here is that the StreamTask implementations are quite simple, and most of the
complexity is hidden in the SystemConsumer. With this design, the StreamTask API provides a good
place for introducing customizations. For example:

* If sensitive data needs to be redacted, a user can create their own StreamTask implementation
  (based on the code of a provided implementation), adding a line or two of code that overwrites
  the sensitive fields.
* If a separate output stream per table is used, it makes sense to use the primary key of the table
  as key of the message in the output stream (to take advantage of Kafka's
  [log compaction](http://kafka.apache.org/documentation.html#compaction) feature). However, if a
  table has no primary key, custom code in the StreamTask could be used to generate a message key
  instead.
  
A user who wants to customize the change capture process can run a job that uses the unmodified
SystemConsumer but has a custom StreamTask. A user who does not need customizations can just use
one of the stock StreamTasks provided.

Besides using the StreamTask API as a point for introducing custom logic, there are other reasons
why it's good to implement change capture using Samza:

* Can use existing serdes for encoding change events in output streams
* Samza's local state is very useful for keeping track of database schemas and grouping changesets
* Samza provides a deployment mechanism (YARN, local process, Mesos in future)
* Support output systems other than Kafka
* Existing metrics framework simplifies integration with monitoring systems


Implementation
==============

PostgreSQL 9.4, which was released in December 2014, introduced a new feature:
[logical decoding](http://www.postgresql.org/docs/9.4/static/logicaldecoding.html), which decodes
the WAL into logical changesets (i.e. inserts, updates and deletes of rows, grouped by transaction).
It does most of the hard work necessary in order to extract change data: it groups writes by
transaction, reorders transactions to appear in commit order, skips aborted transactions, and
enables reliable delivery of the changes to an external consumer.

I propose building Samza's support for Postgres on top of this logical decoding feature. This still
requires some work, but it seems like a solid base to build upon. The only significant downside is
that the feature is only available in the latest version of Postgres.


Output plugin
-------------

An interesting property of Postgres' logical decoding is that it does not define a wire format in
which change data is sent over the network to a consumer. Instead, it defines an _output plugin_
API: by using this API, you can write your own code which reads changes in the database's in-memory
format, and serializes them into whatever wire format you want.

The output plugin must be written in C using the Postgres extension mechanism, and loaded into the
database server as a shared library. This may be regarded as somewhat intrusive, and requires
superuser privileges. However, since Postgres doesn't include a fully-featured output plugin, we
can't get around writing our own. (By default, Postgres only ships with an output plugin called
"test\_decoding", which is ok for testing but not suitable for real use. There doesn't seem to be
a popular third-party output plugin.)

I propose creating an output plugin which encodes database changes using
[Avro](http://avro.apache.org/). Avro is better suited than JSON because it has better support for
datatypes (binary strings, distinguishing floating-point and integers), and Avro is better suited
than Protocol Buffers or Thrift because it supports dynamically generated schemas (so we can
generate the Avro schema of the change data from the schema of the database tables).

Similarly, we can use a Postgres extension function to take a consistent snapshot of the database
(at the time when the logical replication slot was created), and encode it using the same Avro
schema. This has the nice property that the consumer can handle data from the initial snapshot and
the ongoing change stream in the same way.


Consumer
--------

Postgres provides a command-line tool called `pg_recvlogical`, a client which connects to a Postgres
server, receives the change stream (as encoded in a wire format by the output plugin), and writes it
to a file. It might be possible to build a Samza consumer around that, but it seems a bit makeshift.

I'm not sure the JDBC client for Postgres supports the necessary commands to receive a change
stream, so the best option is probably to build a consumer directly using `libpq` (the native
Postgres client library). It would have to be linked into the Samza JVM using JNI. I would normally
be hesitant to propose using JNI, but since we're already using it for LevelDB and RocksDB, it's
probably ok.

This consumer receives data from the database server in the Avro encoding generated by the output
plugin. This can be decoded into Java objects using the Java implementation of Avro, and passed to
the StreamTask. The StreamTask receives data changes on a row-by-row granularity, and it can access
columns within the row by name (since the Avro schema is generated from the database schema).

Samza doesn't yet have a serde for Avro, so if the user wants output streams to use the same Avro
encoding, they will have to provide their own serde. We can probably make the JSON serde work as
expected, and add Avro support in future (see
[SAMZA-317](https://issues.apache.org/jira/browse/SAMZA-317)).


Partitioning and parallelism
----------------------------

Postgres is a single-node database, and the replication mechanism is inherently single-threaded.
Thus there can only be a single StreamTask partition, and processing cannot be parallelized. (Data
written to output streams can be partitioned in any way, and downstream consumers can be as
parallel as you want.)

The gathering of the initial snapshot could in principle be parallelized, but I'm not sure how much
benefit that would bring, given that it's a one-time operation. Even if it takes a few hours on
a large database, that's not the end of the world.

For databases that are partitioned (sharded) at application level, it would make sense to be able
to consume changes from all shards in one Samza job. That would probably have to be done by
specifying the connection details for each shard in the Samza job config, and the consumer would
create a partition (i.e. a StreamTask) for each connection to a database shard.


Discussion
==========

I have an initial prototype of this approach working, and it is looking promising.


Building
--------

The output plugin and the JNI bindings require native C code, so they are a bit more fiddly to build
and test than pure Java/Scala code. To build it, you will need Postgres and libavro installed
(header files and shared libraries). Running integration tests will require a running Postgres
server on the local machine.

In order to avoid affecting people who are building Samza but don't need Postgres, we should make
it possible to skip the samza-postgres module of the build.

For binary releases of Samza (including JNI bindings that we publish to Maven repositories), we'll
need to build on all supported platforms. If this is makes the release process too difficult, we
could potentially move the native code to a separate project, and release it independently.
However, I'd prefer to keep it all within the Samza project if the community agrees.


Related work
------------

Various other people are working on similar problems:

* [Decoderbufs](https://github.com/xstevens/decoderbufs) is an experimental Postgres extension by
  Xavier Stevens that decodes the change stream into a protocol buffers format. It's a good start,
  but needs more work before it is ready for production use. It doesn't _per se_ have a mechanism
  for getting the data into Kafka, although Xavier mentions he has written a client which reads from
  Postgres and writes to Kafka (not open source). It also doesn't have a way of getting a consistent
  snapshot.
* [pg\_kafka](https://github.com/xstevens/pg_kafka) (also from Xavier) is a Kafka producer client
  in a Postgres function, so you could potentially produce to Kafka from a trigger.
* [PGQ](https://wiki.postgresql.org/wiki/PGQ_Tutorial) is a Postgres-based queue implementation,
  and [Skytools Londiste](http://pgfoundry.org/projects/skytools) (developed at Skype) uses it to
  provide trigger-based replication. [Bucardo](https://bucardo.org/wiki/Bucardo) is another
  trigger-based replicator. I don't know these tools in great detail, but I get the impression
  that trigger-based replication is somewhat of a hack, requiring schema changes and fiddly
  configuration, and incurring significant overhead. Also, none of these projects seems to be
  endorsed by the PostgreSQL core team, whereas logical decoding is fully supported.
* [Sqoop](http://sqoop.apache.org/) recently added support for
  [writing to Kafka](https://issues.apache.org/jira/browse/SQOOP-1852). To my knowledge, Sqoop
  can only take full snapshots of a database, and not capture an ongoing stream of changes. I
  also have doubts about the transactional consistency of its snapshots.

At present, I don't believe any of these existing projects are a viable alternative to implementing
it ourselves.
