#include "replication.h"

#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <avro.h>

#define DB_CONNECTION_INFO "postgres://localhost/martin"
#define DB_REPLICATION_INFO "postgres://localhost/martin?replication=database&fallback_application_name=pg_to_kafka"
#define DB_TABLE "test"
#define DB_REPLICATION_SLOT "samza"

struct table_context_t {
    PGconn *conn;
    avro_schema_t schema;
    avro_value_iface_t *avro_iface;
    avro_reader_t avro_reader;
    avro_value_t avro_value;
};

void exit_nicely(PGconn *conn);
void exec_query(PGconn *conn, char *query);
void binary_value(char *value, int length);
void output_tuple(struct table_context_t *context, PGresult *res, int row_number);
void init_table_context(PGconn *conn, struct table_context_t *context);


void exit_nicely(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

void exec_query(PGconn *conn, char *query) {
    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
    } else {
        fprintf(stderr, "Query failed: %s (query was: %s)\n", PQerrorMessage(conn), query);
        PQclear(res);
        exit_nicely(conn);
    }
}

void output_tuple(struct table_context_t *context, PGresult *res, int row_number) {
    if (PQnfields(res) != 1) {
        fprintf(stderr, "Unexpected response with %d fields\n", PQnfields(res));
        exit_nicely(context->conn);
    }
    if (PQgetisnull(res, row_number, 0)) {
        fprintf(stderr, "Unexpected null response value\n");
        exit_nicely(context->conn);
    }
    if (PQfformat(res, 0) != 1) { /* format 1 == binary */
        fprintf(stderr, "Unexpected response format: %d\n", PQfformat(res, 0));
        exit_nicely(context->conn);
    }

    avro_reader_memory_set_source(context->avro_reader,
            PQgetvalue(res, row_number, 0),
            PQgetlength(res, row_number, 0));

    if (avro_value_read(context->avro_reader, &context->avro_value)) {
        fprintf(stderr, "Unable to parse Avro data: %s\n", avro_strerror());
        exit_nicely(context->conn);
    }

    char *json;
    if (avro_value_to_json(&context->avro_value, 1, &json)) {
        fprintf(stderr, "Error converting value to JSON: %s\n", avro_strerror());
        exit_nicely(context->conn);
    }

    printf("%s\n", json);
    free(json);
}

/* Queries the database for the Avro schema of a table. */
void init_table_context(PGconn *conn, struct table_context_t *context) {
    PGresult *res = PQexec(conn, "SELECT samza_table_schema('" DB_TABLE "')");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Schema query failed: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    if (PQntuples(res) != 1 || PQnfields(res) != 1) {
        fprintf(stderr, "Unexpected schema query result format: %d tuples with %d fields\n",
                PQntuples(res), PQnfields(res));
        PQclear(res);
        exit_nicely(conn);
    }

    int err = avro_schema_from_json_length(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0),
            &context->schema);
    if (err) {
        fprintf(stderr, "Could not parse table schema: %s", avro_strerror());
        PQclear(res);
        exit_nicely(conn);
    }
    PQclear(res);

    context->avro_reader = avro_reader_memory(NULL, 0);
    context->avro_iface = avro_generic_class_from_schema(context->schema);
    avro_generic_value_new(context->avro_iface, &context->avro_value);
}

int main(int argc, char **argv) {
    PGconn *conn = PQconnectdb(DB_CONNECTION_INFO);

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    exec_query(conn, "BEGIN");
    exec_query(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
    /* exec_query(conn, "SET TRANSACTION SNAPSHOT '...'"); */

    struct table_context_t context;
    context.conn = conn;
    init_table_context(conn, &context);

    /* The final parameter 1 requests the results in binary format */
    if (!PQsendQueryParams(conn, "SELECT samza_table_export('" DB_TABLE "')", 0, NULL, NULL, NULL, NULL, 1)) {
        fprintf(stderr, "Could not dispatch snapshot fetch: %s\n", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    if (!PQsetSingleRowMode(conn)) {
        fprintf(stderr, "Could not activate single-row mode\n");
        exit_nicely(conn);
    }

    int error = 0, tuples, total = 0;

    for (;;) {
        PGresult *res = PQgetResult(conn);
        if (!res) break; /* null result indicates that there are no more rows */

        switch (PQresultStatus(res)) {
            case PGRES_SINGLE_TUPLE:
            case PGRES_TUPLES_OK:
                tuples = PQntuples(res);
                for (int tuple = 0; tuple < tuples; tuple++) {
                    total++;
                    output_tuple(&context, res, tuple);
                }
                break;

            default:
                error = 1;
                fprintf(stderr, "While reading rows: %s: %s\n",
                        PQresStatus(PQresultStatus(res)),
                        PQresultErrorMessage(res));
        }
        PQclear(res);
    }

    if (!error) {
        exec_query(conn, "COMMIT");
        PQfinish(conn);

        conn = PQconnectdb(DB_REPLICATION_INFO);
        consume_stream(conn, DB_REPLICATION_SLOT);
    }

    avro_value_decref(&context.avro_value);
    avro_reader_free(context.avro_reader);
    avro_value_iface_decref(context.avro_iface);
    avro_schema_decref(context.schema);

    if (error) exit_nicely(conn);

    PQfinish(conn);
    return 0;
}
