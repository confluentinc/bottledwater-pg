#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "avro.h"
#include "postgres.h"
#include "replication/output_plugin.h"

#define PROTOCOL_SCHEMA_NAMESPACE "org.apache.samza.postgres.protocol"

avro_schema_t schema_for_frame(void);
int update_frame_with_begin_txn(avro_value_t *union_val, ReorderBufferTXN *txn);
int update_frame_with_commit_txn(avro_value_t *union_val, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);

#endif /* PROTOCOL_H */
