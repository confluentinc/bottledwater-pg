#ifndef PROTOCOL_CLIENT_H
#define PROTOCOL_CLIENT_H

#include "protocol.h"
#include "postgres_ext.h"

/* Parameters: context, wal_pos, xid */
typedef int (*begin_txn_cb)(void *, uint64_t, uint32_t);

/* Parameters: context, wal_pos, xid */
typedef int (*commit_txn_cb)(void *, uint64_t, uint32_t);

/* Parameters: context, wal_pos, relid,
 *             key_schema_json, key_schema_len, key_schema,
 *             row_schema_json, row_schema_len, row_schema */
typedef int (*table_schema_cb)(void *, uint64_t, Oid,
        const char *, size_t, avro_schema_t,
        const char *, size_t, avro_schema_t);

/* Parameters: context, wal_pos, relid,
 *             key_bin, key_len, key_val,
 *             new_bin, new_len, new_val */
typedef int (*insert_row_cb)(void *, uint64_t, Oid,
        const void *, size_t, avro_value_t *,
        const void *, size_t, avro_value_t *);

/* Parameters: context, wal_pos, relid,
 *             key_bin, key_len, key_val,
 *             old_bin, old_len, old_val,
 *             new_bin, new_len, new_val */
typedef int (*update_row_cb)(void *, uint64_t, Oid,
        const void *, size_t, avro_value_t *,
        const void *, size_t, avro_value_t *,
        const void *, size_t, avro_value_t *);

/* Parameters: context, wal_pos, relid,
 *             key_bin, key_len, key_val,
 *             old_bin, old_len, old_val */
typedef int (*delete_row_cb)(void *, uint64_t, Oid,
        const void *, size_t, avro_value_t *,
        const void *, size_t, avro_value_t *);

#define FRAME_READER_SYNC_PENDING EBUSY

/* Parameters: context, wal_pos
 * Return: 0 if we should acknowledge wal_pos as flushed;
 *         FRAME_READER_SYNC_PENDING if we should not because we have
 *         transactions pending sync;
 *         anything else to signify an error. */
typedef int (*keepalive_cb)(void *, uint64_t);

/* Parameters: context, err, message
 * Return: 0 if the error was handled and client should continue;
 *         nonzero if the error could not be handled and the client should
 *         report the error to the select loop.
 */
typedef int (*error_handler_cb)(void *, int err, const char *);


#define FRAME_READER_ERROR_LEN 512

typedef struct {
    Oid                 relid;       /* Uniquely identifies a table, even when it is renamed */
    avro_schema_t       key_schema;  /* Avro schema for the table's primary key or replica identity */
    avro_schema_t       row_schema;  /* Avro schema for one row of the table */
    avro_value_iface_t *key_iface;   /* Avro generic interface for creating key values */
    avro_value_iface_t *row_iface;   /* Avro generic interface for creating row values */
    avro_value_t        key_value;   /* Avro key value, for encoding one key */
    avro_value_t        row_value;   /* Avro row value, for encoding one row */
    avro_value_t        old_value;   /* Avro row value, for encoding the old value (in updates, deletes) */
    avro_reader_t       avro_reader; /* In-memory buffer reader */
} schema_list_entry;

typedef struct {
    void *cb_context;                /* Pointer that is passed to callbacks */
    begin_txn_cb on_begin_txn;       /* Called to indicate that the following events belong to one transaction */
    commit_txn_cb on_commit_txn;     /* Called to indicate the end of events from a particular transaction */
    table_schema_cb on_table_schema; /* Called when there is a new schema for a particular relation */
    insert_row_cb on_insert_row;     /* Called when a row is inserted into a relation */
    update_row_cb on_update_row;     /* Called when a row in a relation is updated */
    delete_row_cb on_delete_row;     /* Called when a row in a relation is deleted */
    keepalive_cb on_keepalive;       /* Called when server sends a keepalive message */
    error_handler_cb on_error;       /* Called when a frame cannot be read or when a callback returns a nonzero error code */
    int num_schemas;                 /* Number of schemas in use */
    int capacity;                    /* Allocated size of schemas array */
    schema_list_entry **schemas;     /* Array of pointers to schema_list_entry structs */
    avro_schema_t frame_schema;      /* Avro schema of a frame, as defined by the protocol */
    avro_value_iface_t *frame_iface; /* Avro generic interface for the frame schema */
    avro_value_t frame_value;        /* Avro value for a frame */
    avro_reader_t avro_reader;       /* In-memory buffer reader */
    char error[FRAME_READER_ERROR_LEN]; /* Buffer for error messages */
} frame_reader;

typedef frame_reader *frame_reader_t;

int parse_frame(frame_reader_t reader, uint64_t wal_pos, char *buf, int buflen);
frame_reader_t frame_reader_new(void);
void frame_reader_free(frame_reader_t reader);

int handle_keepalive(frame_reader_t reader, uint64_t wal_pos);

#endif /* PROTOCOL_CLIENT_H */
