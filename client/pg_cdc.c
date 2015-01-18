#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <avro.h>

#define DB_CONNECTION_INFO "postgres://localhost/martin"
#define DB_TABLE "test"
#define OUTPUT_FILENAME "output.avro"

struct table_context_t {
    PGconn *conn;
    avro_schema_t schema;
    avro_value_iface_t *avro_iface;
    avro_reader_t avro_reader;
    avro_value_t avro_value;
    avro_file_writer_t output;
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

    if (avro_file_writer_append_value(context->output, &context->avro_value)) {
        fprintf(stderr, "Unable to write tuple to output file: %s\n", avro_strerror());
        exit_nicely(context->conn);
    }
}

/* Queries the database for the Avro schema of a table.
 * Creates an Avro output file with that schema. */
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

    const char *fn = OUTPUT_FILENAME;
    remove(fn); /* Delete the output file if it exists */

    int error = avro_file_writer_create(fn, context->schema, &context->output);
    if (error) {
        fprintf(stderr, "Error creating %s: %s\n", fn, avro_strerror());
        exit_nicely(context->conn);
    }
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

    avro_file_writer_close(context.output);
    avro_value_decref(&context.avro_value);
    avro_reader_free(context.avro_reader);
    avro_value_iface_decref(context.avro_iface);
    avro_schema_decref(context.schema);

    if (error) exit_nicely(conn);

    exec_query(conn, "COMMIT");
    PQfinish(conn);

    fprintf(stderr, "%d rows exported\n", total);
    return 0;
}
