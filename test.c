#include "oid2avro.h"

#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"

#define DB_CONNECTION_INFO "postgres://localhost/martin"
#define DB_TABLE "test"
#define OUTPUT_FILENAME "output.avro"

struct table_context_t {
    PGconn *conn;
    avro_schema_t schema;
    avro_value_iface_t *avro_iface;
    avro_value_t avro_value;
    avro_file_writer_t output;
};

void exit_nicely(PGconn *conn);
void exec_query(PGconn *conn, char *query);
void binary_value(char *value, int length);
void output_tuple(struct table_context_t *context, PGresult *res, int row_number);
void init_table_context(PGresult *res, struct table_context_t *context);


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

void binary_value(char *value, int length) {
    for (int i = 0; i < length; i++) {
        printf("%02hhx", value[i]);
    }
}

void output_tuple(struct table_context_t *context, PGresult *res, int row_number) {
    int columns = PQnfields(res);

    for (int column = 0; column < columns; column++) {
        avro_value_t union_value, field_value;
        const char *fieldname = NULL;
        avro_value_get_by_index(&context->avro_value, column, &union_value, &fieldname);
        if (column > 0) printf(", ");
        printf("%s = ", fieldname);

        if (PQgetisnull(res, row_number, column)) {
            printf("null");
            avro_value_set_branch(&union_value, 0, &field_value);

        } if (PQfformat(res, column) == 1) { /* format 1 == binary */
            char *value = PQgetvalue(res, row_number, column);
            int length = PQgetlength(res, row_number, column);
            binary_value(value, length);
            avro_value_set_branch(&union_value, 1, &field_value);
            Datum *datum_p = (Datum *) value; // FIXME not sure that is correct
            pg_datum_to_avro(*datum_p, PQftype(res, column), &field_value);

        } else {
            fprintf(stderr, "Unexpected response format: %d\n", PQfformat(res, column));
            exit_nicely(context->conn);
        }
        printf(" (oid = %d, mod = %d)", PQftype(res, column), PQfmod(res, column));
    }
    printf("\n");

    if (avro_file_writer_append_value(context->output, &context->avro_value)) {
        fprintf(stderr, "Unable to write tuple to output file: %s\n", avro_strerror());
        exit_nicely(context->conn);
    }
}

/* Inspects a tuple from a result set to determine the output schema.
 * Creates an Avro output file with that schema. */
void init_table_context(PGresult *res, struct table_context_t *context) {
    context->schema = avro_schema_record("Tuple", "postgres");
    int columns = PQnfields(res);
    for (int column = 0; column < columns; column++) {
        avro_schema_t column_schema = oid_to_schema(PQftype(res, column), 1);
        avro_schema_record_field_append(context->schema, PQfname(res, column), column_schema);
        avro_schema_decref(column_schema);
    }

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

    /* The final parameter 1 requests the results in binary format */
    if (!PQsendQueryParams(conn, "SELECT xmin, xmax, * FROM " DB_TABLE, 0, NULL, NULL, NULL, NULL, 1)) {
        fprintf(stderr, "Could not dispatch snapshot fetch: %s\n", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    if (!PQsetSingleRowMode(conn)) {
        fprintf(stderr, "Could not activate single-row mode\n");
        exit_nicely(conn);
    }

    int error = 0, tuples, total = 0;

    struct table_context_t context;
    context.conn = conn;

    for (;;) {
        PGresult *res = PQgetResult(conn);
        if (!res) break; /* null result indicates that there are no more rows */

        switch (PQresultStatus(res)) {
            case PGRES_SINGLE_TUPLE:
            case PGRES_TUPLES_OK:
                tuples = PQntuples(res);
                for (int tuple = 0; tuple < tuples; tuple++) {
                    if (total == 0) init_table_context(res, &context);
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

    if (total > 0) {
        avro_file_writer_close(context.output);
        avro_value_decref(&context.avro_value);
        avro_value_iface_decref(context.avro_iface);
        avro_schema_decref(context.schema);
    }
    if (error) exit_nicely(conn);

    exec_query(conn, "COMMIT");
    PQfinish(conn);

    fprintf(stderr, "%d rows exported\n", total);
    return 0;
}
