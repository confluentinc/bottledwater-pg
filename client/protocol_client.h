#ifndef PROTOCOL_CLIENT_H
#define PROTOCOL_CLIENT_H

#include "protocol.h"

int process_frame(avro_value_t *frame_val, schema_cache_t cache, uint64_t wal_pos);
schema_cache_t schema_cache_new(void);
void schema_cache_free(schema_cache_t cache);

#endif /* PROTOCOL_CLIENT_H */
