#ifndef PROTOCOL_SERVER_H
#define PROTOCOL_SERVER_H

#include "protocol.h"
#include "postgres.h"
#include "replication/output_plugin.h"

int update_frame_with_begin_txn(avro_value_t *union_val, ReorderBufferTXN *txn);
int update_frame_with_commit_txn(avro_value_t *union_val, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

#endif /* PROTOCOL_SERVER_H */
