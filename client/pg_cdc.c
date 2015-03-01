#include "replication.h"
#include "protocol_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <avro.h>
#include <internal/pqexpbuffer.h>

#define DB_CONNECTION_INFO "postgres://localhost/martin"
#define DB_REPLICATION_INFO "postgres://localhost/martin?replication=database&fallback_application_name=pg_to_kafka"
#define DB_REPLICATION_SLOT "samza"
#define OUTPUT_PLUGIN "samza_postgres"

#define check(err, call) { err = call; if (err) return err; }

struct table_context_t {
    PGconn *sql_conn, *rep_conn;
    frame_reader_t frame_reader;
};

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
void exit_nicely(struct table_context_t *context);
void exec_sql(struct table_context_t *context, char *query);
void init_table_context(struct table_context_t *context);
bool replication_slot_exists(struct table_context_t *context, char *slot_name, XLogRecPtr *start_pos);
bool dump_snapshot(struct table_context_t *context, char *snapshot_name);
void output_tuple(struct table_context_t *context, PGresult *res, int row_number);


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

void exit_nicely(struct table_context_t *context) {
    PQfinish(context->sql_conn);
    PQfinish(context->rep_conn);
    exit(1);
}

void exec_sql(struct table_context_t *context, char *query) {
    PGresult *res = PQexec(context->sql_conn, query);
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
    } else {
        fprintf(stderr, "Query failed: %s: %s",
                query, PQerrorMessage(context->sql_conn));
        PQclear(res);
        exit_nicely(context);
    }
}

void init_table_context(struct table_context_t *context) {
    context->sql_conn = PQconnectdb(DB_CONNECTION_INFO);
    if (PQstatus(context->sql_conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(context->sql_conn));
        PQfinish(context->sql_conn);
        exit(1);
    }

    context->rep_conn = PQconnectdb(DB_REPLICATION_INFO);
    if (PQstatus(context->rep_conn) != CONNECTION_OK) {
        fprintf(stderr, "Replication connection failed: %s\n", PQerrorMessage(context->rep_conn));
        PQfinish(context->rep_conn);
        PQfinish(context->sql_conn);
        exit(1);
    }

    context->frame_reader = frame_reader_new();
    context->frame_reader->cb_context = context;
    context->frame_reader->on_begin_txn = print_begin_txn;
    context->frame_reader->on_commit_txn = print_commit_txn;
    context->frame_reader->on_table_schema = print_table_schema;
    context->frame_reader->on_insert_row = print_insert_row;
    context->frame_reader->on_update_row = print_update_row;
    context->frame_reader->on_delete_row = print_delete_row;
}

/* Returns true if a replication slot with the given name already exists, and false if not.
 * In addition, if the slot already exists, start_pos is filled in with the LSN at which
 * the client should restart streaming. */
bool replication_slot_exists(struct table_context_t *context, char *slot_name, XLogRecPtr *start_pos) {
    Oid argtypes[] = { 19 }; // 19 == NAMEOID
    const char *args[] = { slot_name };

    PGresult *res = PQexecParams(context->sql_conn,
            "SELECT restart_lsn FROM pg_replication_slots where slot_name = $1",
            1, argtypes, args, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Could not check for existing replication slot: %s",
                PQerrorMessage(context->sql_conn));
        exit_nicely(context);
    }

    bool exists = (PQntuples(res) > 0);

    if (exists && start_pos && !PQgetisnull(res, 0, 0)) {
        uint32 h32, l32;
        if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &h32, &l32) != 2) {
            fprintf(stderr, "Could not parse restart LSN: \"%s\"\n", PQgetvalue(res, 0, 0));
        } else {
            *start_pos = ((uint64) h32) << 32 | l32;
        }
    }

    PQclear(res);
    return exists;
}

bool dump_snapshot(struct table_context_t *context, char *snapshot_name) {
    exec_sql(context, "BEGIN");
    exec_sql(context, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");

    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "SET TRANSACTION SNAPSHOT '%s'", snapshot_name);
    exec_sql(context, query->data);
    destroyPQExpBuffer(query);

    /* The final parameter 1 requests the results in binary format */
    if (!PQsendQueryParams(context->sql_conn, "SELECT samza_table_export('%')",
                0, NULL, NULL, NULL, NULL, 1)) {
        fprintf(stderr, "Could not dispatch snapshot fetch: %s\n",
                PQerrorMessage(context->sql_conn));
        exit_nicely(context);
    }

    if (!PQsetSingleRowMode(context->sql_conn)) {
        fprintf(stderr, "Could not activate single-row mode\n");
        exit_nicely(context);
    }

    bool ok = true;
    int tuples;

    while (ok) {
        PGresult *res = PQgetResult(context->sql_conn);
        if (!res) break; /* null result indicates that there are no more rows */

        switch (PQresultStatus(res)) {
            case PGRES_SINGLE_TUPLE:
            case PGRES_TUPLES_OK:
                tuples = PQntuples(res);
                for (int tuple = 0; tuple < tuples; tuple++) {
                    output_tuple(context, res, tuple);
                }
                break;

            default:
                ok = false;
                fprintf(stderr, "While reading rows: %s: %s\n",
                        PQresStatus(PQresultStatus(res)),
                        PQresultErrorMessage(res));
        }
        PQclear(res);
    }

    if (ok) exec_sql(context, "COMMIT");
    return ok;
}

void output_tuple(struct table_context_t *context, PGresult *res, int row_number) {
    if (PQnfields(res) != 1) {
        fprintf(stderr, "Unexpected response with %d fields\n", PQnfields(res));
        exit_nicely(context);
    }
    if (PQgetisnull(res, row_number, 0)) {
        fprintf(stderr, "Unexpected null response value\n");
        exit_nicely(context);
    }
    if (PQfformat(res, 0) != 1) { /* format 1 == binary */
        fprintf(stderr, "Unexpected response format: %d\n", PQfformat(res, 0));
        exit_nicely(context);
    }

    /* wal_pos == 0 == InvalidXLogRecPtr */
    int err = parse_frame(context->frame_reader, 0,
            PQgetvalue(res, row_number, 0),
            PQgetlength(res, row_number, 0));

    if (err) exit_nicely(context);
}


int main(int argc, char **argv) {
    char *slot_name = DB_REPLICATION_SLOT, *snapshot_name = NULL;
    XLogRecPtr start_pos = InvalidXLogRecPtr;
    bool success = true;

    struct table_context_t context;
    init_table_context(&context);

    if (replication_slot_exists(&context, slot_name, &start_pos)) {
        fprintf(stderr, "Replication slot \"%s\" exists, streaming changes from %X/%X.\n",
                slot_name, (uint32) (start_pos >> 32), (uint32) start_pos);

    } else {
        fprintf(stderr, "Creating replication slot \"%s\".\n", slot_name);
        success = create_replication_slot(context.rep_conn, slot_name, OUTPUT_PLUGIN,
                &start_pos, &snapshot_name);
        if (!success) exit_nicely(&context);

        fprintf(stderr, "Capturing consistent snapshot \"%s\".\n", snapshot_name);
        success = dump_snapshot(&context, snapshot_name);
        if (!success) exit_nicely(&context);

        fprintf(stderr, "Snapshot complete, streaming changes from %X/%X.\n",
                (uint32) (start_pos >> 32), (uint32) start_pos);
    }

    PQfinish(context.sql_conn);
    consume_stream(context.rep_conn, context.frame_reader, slot_name, start_pos);

    frame_reader_free(context.frame_reader);
    PQfinish(context.rep_conn);
    return 0;
}
