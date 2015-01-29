Prerequisites (Mac OS using Homebrew):

    brew install avro-c postgresql

Prerequisites (Debian-based Linux distributions, e.g. Ubuntu):

    sudo apt-get install postgresql postgresql-server-dev-9.4

This project depends on the C implementation of [Avro](http://avro.apache.org/). 
If it's not in your package manager, you can 
[download the source](http://www.apache.org/dyn/closer.cgi/avro/) of avro-c
and build it yourself.

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

    create extension samza_postgres;
    select pg_create_logical_replication_slot('samza', 'samza_postgres');
