#ifndef PROTOCOL_CLIENT_H
#define PROTOCOL_CLIENT_H

#include "protocol.h"
#include "postgres_ext.h"

typedef struct {
    Oid relid;                       /* Uniquely identifies a table, even when it is renamed */
    uint64_t hash;                   /* Hash of table schema, to detect changes */
    avro_schema_t row_schema;        /* Avro schema for one row of the table */
    avro_value_iface_t *row_iface;   /* Avro generic interface for creating row values */
    avro_value_t row_value;          /* Avro row value, for encoding one row */
    avro_value_t old_value;          /* Avro row value, for encoding the old value (in updates, deletes) */
    avro_reader_t avro_reader;       /* In-memory buffer reader */
} schema_list_entry;

typedef struct {
    int num_schemas;                 /* Number of schemas in use */
    int capacity;                    /* Allocated size of schemas array */
    schema_list_entry **schemas;     /* Array of pointers to schema_list_entry structs */
    avro_schema_t frame_schema;      /* Avro schema of a frame, as defined by the protocol */
    avro_value_iface_t *frame_iface; /* Avro generic interface for the frame schema */
    avro_value_t frame_value;        /* Avro value for a frame */
    avro_reader_t avro_reader;       /* In-memory buffer reader */
} frame_reader;

typedef frame_reader *frame_reader_t;

int parse_frame(frame_reader_t reader, uint64_t wal_pos, char *buf, int buflen);
frame_reader_t frame_reader_new(void);
void frame_reader_free(frame_reader_t reader);

#endif /* PROTOCOL_CLIENT_H */
