#ifndef REPLICATION_H
#define REPLICATION_H

#include "protocol_client.h"
#include <avro.h>
#include <libpq-fe.h>
#include <server/postgres_fe.h>
#include <server/access/xlogdefs.h>

struct replication_stream {
    PGconn *conn;
    XLogRecPtr recvd_lsn;
    XLogRecPtr fsync_lsn;
    int64 last_checkpoint;
    avro_schema_t frame_schema;
    avro_value_iface_t *frame_iface;
    avro_reader_t frame_reader;
    avro_value_t frame_value;
    schema_cache_t schema_cache;
};

typedef struct replication_stream *replication_stream_t;

bool checkpoint(replication_stream_t stream, int64 now);
bool start_stream(PGconn *conn, char *slot_name, XLogRecPtr position);
int poll_stream(replication_stream_t stream);
bool consume_stream(PGconn *conn, char *slot_name);

#endif /* REPLICATION_H */
