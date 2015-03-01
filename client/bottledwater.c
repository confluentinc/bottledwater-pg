#include "connect.h"

#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <avro.h>
#include <internal/pqexpbuffer.h>

#define DB_CONNECTION_INFO "postgres://localhost/martin"
#define DB_REPLICATION_INFO "postgres://localhost/martin?replication=database&fallback_application_name=pg_to_kafka"
#define DB_REPLICATION_SLOT "bottledwater"
#define APP_NAME "bottledwater"

/* The name of the logical decoding output plugin with which the replication
 * slot is created. This must match the name of the Postgres extension. */
#define OUTPUT_PLUGIN "bottledwater"

#define check(err, call) { err = call; if (err) return err; }

#define ensure(context, call) { \
    if (call) { \
        fprintf(stderr, "%s\n", context->error); \
        exit_nicely(context); \
    } \
}

int print_begin_txn(void *context, uint64_t wal_pos, uint32_t xid);
int print_commit_txn(void *context, uint64_t wal_pos, uint32_t xid);
int print_table_schema(void *context, uint64_t wal_pos, Oid relid, const char *schema_json,
        size_t schema_len);
int print_insert_row(void *context, uint64_t wal_pos, Oid relid, const void *new_row_bin,
        size_t new_row_len, avro_value_t *new_row_val);
int print_update_row(void *context, uint64_t wal_pos, Oid relid, const void *old_row_bin,
        size_t old_row_len, avro_value_t *old_row_val, const void *new_row_bin,
        size_t new_row_len, avro_value_t *new_row_val);
int print_delete_row(void *context, uint64_t wal_pos, Oid relid, const void *old_row_bin,
        size_t old_row_len, avro_value_t *old_row_val);
void exit_nicely(client_context_t context);
client_context_t init_client(void);


int print_begin_txn(void *context, uint64_t wal_pos, uint32_t xid) {
    printf("begin xid=%u wal_pos=%X/%X\n", xid, (uint32) (wal_pos >> 32), (uint32) wal_pos);
    return 0;
}

int print_commit_txn(void *context, uint64_t wal_pos, uint32_t xid) {
    printf("commit xid=%u wal_pos=%X/%X\n", xid, (uint32) (wal_pos >> 32), (uint32) wal_pos);
    return 0;
}

int print_table_schema(void *context, uint64_t wal_pos, Oid relid, const char *schema_json,
        size_t schema_len) {
    printf("new schema for relid=%u\n", relid);
    return 0;
}

int print_insert_row(void *context, uint64_t wal_pos, Oid relid, const void *new_row_bin,
        size_t new_row_len, avro_value_t *new_row_val) {
    int err = 0;
    char *new_row_json;
	const char *table_name = avro_schema_name(avro_value_get_schema(new_row_val));
    check(err, avro_value_to_json(new_row_val, 1, &new_row_json));
    printf("insert to %s: %s\n", table_name, new_row_json);
    free(new_row_json);
    return err;
}

int print_update_row(void *context, uint64_t wal_pos, Oid relid, const void *old_row_bin,
        size_t old_row_len, avro_value_t *old_row_val, const void *new_row_bin,
        size_t new_row_len, avro_value_t *new_row_val) {
    int err = 0;
    char *old_row_json, *new_row_json;
	const char *table_name = avro_schema_name(avro_value_get_schema(new_row_val));
    check(err, avro_value_to_json(new_row_val, 1, &new_row_json));

    if (old_row_val) {
        check(err, avro_value_to_json(old_row_val, 1, &old_row_json));
        printf("update to %s: %s --> %s\n", table_name, old_row_json, new_row_json);
        free(old_row_json);
    } else {
        printf("update to %s: (?) --> %s\n", table_name, new_row_json);
    }

    free(new_row_json);
    return err;
}

int print_delete_row(void *context, uint64_t wal_pos, Oid relid, const void *old_row_bin,
        size_t old_row_len, avro_value_t *old_row_val) {
    int err = 0;
    char *old_row_json;

    if (old_row_val) {
		const char *table_name = avro_schema_name(avro_value_get_schema(old_row_val));
        check(err, avro_value_to_json(old_row_val, 1, &old_row_json));
        printf("delete to %s: %s\n", table_name, old_row_json);
        free(old_row_json);
    } else {
        printf("delete to relid %u (?)\n", relid);
    }
    return err;
}

client_context_t init_client() {
	frame_reader_t frame_reader = frame_reader_new();
    frame_reader->on_begin_txn    = print_begin_txn;
    frame_reader->on_commit_txn   = print_commit_txn;
    frame_reader->on_table_schema = print_table_schema;
    frame_reader->on_insert_row   = print_insert_row;
    frame_reader->on_update_row   = print_update_row;
    frame_reader->on_delete_row   = print_delete_row;

    client_context_t context = db_client_new();
    context->conninfo = DB_CONNECTION_INFO;
    context->app_name = APP_NAME;
    context->repl.slot_name = DB_REPLICATION_SLOT;
    context->repl.output_plugin = OUTPUT_PLUGIN;
    context->repl.frame_reader = frame_reader;
    return context;
}

void exit_nicely(client_context_t context) {
    frame_reader_free(context->repl.frame_reader);
    db_client_free(context);
    exit(1);
}

int main(int argc, char **argv) {
    client_context_t context = init_client();
    ensure(context, db_client_start(context));

    bool snapshot = false;
    if (context->sql_conn) {
        fprintf(stderr, "Created replication slot \"%s\", capturing consistent snapshot \"%s\".\n",
                context->repl.slot_name, context->repl.snapshot_name);
        snapshot = true;
    } else {
        fprintf(stderr, "Replication slot \"%s\" exists, streaming changes from %X/%X.\n",
                context->repl.slot_name,
                (uint32) (context->repl.start_lsn >> 32), (uint32) context->repl.start_lsn);
    }

    while (context->status >= 0) { /* TODO install signal handler for graceful shutdown */
        ensure(context, db_client_poll(context));

        if (snapshot && !context->sql_conn) {
            snapshot = false;
            fprintf(stderr, "Snapshot complete, streaming changes from %X/%X.\n",
                    (uint32) (context->repl.start_lsn >> 32), (uint32) context->repl.start_lsn);
        }

        if (context->status == 0) {
            ensure(context, db_client_wait(context));
        }
    }

    frame_reader_free(context->repl.frame_reader);
    db_client_free(context);
    return 0;
}
