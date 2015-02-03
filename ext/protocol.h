/* This file (and the accompanying .c file) is shared between server-side code
 * (logical decoding output plugin) and client-side code (using libpq to connect
 * to the database). It defines the wire protocol in which the change stream
 * is sent from the server to the client. The protocol consists of "frames" (one
 * frame is generated for each call to the output plugin), and each frame may
 * contain multiple "messages". A message may indicate that a transaction started
 * or committed, that a row was inserted/updated/deleted, that a table schema
 * changed, etc. */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "avro.h"

/* When compiling for the client side, PG_CDC_CLIENT is defined. The only
 * difference is that on the server, Postgres memory contexts are used for heap
 * memory allocation, whereas on the client, regular malloc is used. */
#ifdef PG_CDC_CLIENT
#include "postgres_ext.h"
#else
#include "postgres.h"
#endif

/* Namespace for Avro records of the frame protocol */
#define PROTOCOL_SCHEMA_NAMESPACE "org.apache.samza.postgres.protocol"

/* Each message in the wire protocol is of one of these types */
#define PROTOCOL_MSG_BEGIN_TXN      0
#define PROTOCOL_MSG_COMMIT_TXN     1
#define PROTOCOL_MSG_TABLE_SCHEMA   2
#define PROTOCOL_MSG_INSERT         3


struct schema_cache_entry {
    Oid relid;                     /* Uniquely identifies a table, even when it is renamed */
    uint64_t hash;                 /* Hash of table schema, to detect changes */
    avro_schema_t row_schema;      /* Avro schema for one row of the table */
    avro_value_iface_t *row_iface; /* Avro generic interface for creating row values */
    avro_value_t row_value;        /* Avro row value, for encoding one row */
};

struct schema_cache {
#ifndef PG_CDC_CLIENT /* Memory contexts are only used on the server side */
    MemoryContext context;               /* Context in which cache entries are allocated */
#endif
    int num_entries;                     /* Number of entries in use */
    int capacity;                        /* Allocated size of entries array */
    struct schema_cache_entry **entries; /* Array of pointers to cache entries */
};

typedef struct schema_cache *schema_cache_t;


avro_schema_t schema_for_frame(void);

#endif /* PROTOCOL_H */
