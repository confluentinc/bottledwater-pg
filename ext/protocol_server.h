#ifndef PROTOCOL_SERVER_H
#define PROTOCOL_SERVER_H

#include "protocol.h"
#include "postgres.h"
#include "replication/output_plugin.h"

int update_frame_with_begin_txn(avro_value_t *frame_val, ReorderBufferTXN *txn);
int update_frame_with_commit_txn(avro_value_t *frame_val, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
int update_frame_with_insert(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple newtuple);
int update_frame_with_update(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple oldtuple, HeapTuple newtuple);
int update_frame_with_delete(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple oldtuple);

schema_cache_t schema_cache_new(MemoryContext context);
void schema_cache_free(schema_cache_t cache);

#endif /* PROTOCOL_SERVER_H */
