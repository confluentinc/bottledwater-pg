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

struct table_context_t {
    PGconn *sql_conn, *rep_conn;
    avro_schema_t frame_schema;
    avro_value_iface_t *frame_iface;
    avro_reader_t frame_reader;
    avro_value_t frame_value;
    schema_cache_t schema_cache;
};

void exit_nicely(struct table_context_t *context);
void exec_sql(struct table_context_t *context, char *query);
void init_table_context(struct table_context_t *context);
bool replication_slot_exists(struct table_context_t *context, char *slot_name, XLogRecPtr *start_pos);
bool dump_snapshot(struct table_context_t *context, char *snapshot_name);
void output_tuple(struct table_context_t *context, PGresult *res, int row_number);


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

    context->frame_schema = schema_for_frame();
    context->frame_iface = avro_generic_class_from_schema(context->frame_schema);
    context->frame_reader = avro_reader_memory(NULL, 0);
    avro_generic_value_new(context->frame_iface, &context->frame_value);
    context->schema_cache = schema_cache_new();
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

    avro_reader_memory_set_source(context->frame_reader,
            PQgetvalue(res, row_number, 0),
            PQgetlength(res, row_number, 0));

    // TODO use read_entirely()
    if (avro_value_read(context->frame_reader, &context->frame_value)) {
        fprintf(stderr, "Unable to parse Avro data: %s\n", avro_strerror());
        exit_nicely(context);
    }

    /* wal_pos == 0 == InvalidXLogRecPtr */
    if (process_frame(&context->frame_value, context->schema_cache, 0)) {
        fprintf(stderr, "Error processing frame data: %s\n", avro_strerror());
        exit_nicely(context);
    }
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
    consume_stream(context.rep_conn, slot_name, start_pos);

    schema_cache_free(context.schema_cache);
    avro_value_decref(&context.frame_value);
    avro_reader_free(context.frame_reader);
    avro_value_iface_decref(context.frame_iface);
    avro_schema_decref(context.frame_schema);

    PQfinish(context.rep_conn);
    return 0;
}
