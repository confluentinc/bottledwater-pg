#ifndef REPLICATION_H
#define REPLICATION_H

#include <libpq-fe.h>
#include <server/postgres_fe.h>
#include <server/access/xlogdefs.h>

typedef struct replication_stream *replication_stream_t;

typedef bool (*parse_frame_cb)(replication_stream_t, XLogRecPtr, char *, int);

struct replication_stream {
    PGconn *conn;
	parse_frame_cb frame_cb;
    XLogRecPtr recvd_lsn;
    XLogRecPtr fsync_lsn;
    int64 last_checkpoint;
};

bool checkpoint(replication_stream_t stream, int64 now);
bool start_stream(PGconn *conn, char *slot_name, XLogRecPtr position);
int poll_stream(replication_stream_t stream);
bool consume_stream(PGconn *conn, char *slot_name);

#endif /* REPLICATION_H */
