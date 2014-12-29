#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"
#include "avro.h"

#define DB_CONNECTION_INFO "postgres://localhost/martin"
#define DB_TABLE "test"
#define OUTPUT_FILENAME "output.avro"

struct table_context_t {
    PGconn *conn;
    avro_schema_t schema;
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
    avro_datum_t tuple = avro_record(context->schema), value_datum, union_datum;
    char *value;
    int columns = PQnfields(res);

    for (int column = 0; column < columns; column++) {
        if (column > 0) printf(", ");
        char *colname = PQfname(res, column);
        printf("%s = ", colname);
        avro_schema_t field_schema = avro_schema_record_field_get(context->schema, colname);

        if (PQgetisnull(res, row_number, column)) {
            printf("null");
            value_datum = avro_null();
            union_datum = avro_union(field_schema, 0, value_datum);
            avro_record_set(tuple, colname, union_datum);
            avro_datum_decref(value_datum);
            avro_datum_decref(union_datum);

        } else {
            switch (PQfformat(res, column)) {
                case 0: /* text */
                    value = PQgetvalue(res, row_number, column);
                    printf("'%s'", value);
                    value_datum = avro_string(value);
                    union_datum = avro_union(field_schema, 1, value_datum);
                    avro_record_set(tuple, colname, union_datum);
                    avro_datum_decref(value_datum);
                    avro_datum_decref(union_datum);
                    break;

                case 1: /* binary */
                    binary_value(PQgetvalue(res, row_number, column), PQgetlength(res, row_number, column));
                    value_datum = avro_string(value);
                    union_datum = avro_union(field_schema, 1, value_datum);
                    avro_record_set(tuple, colname, union_datum);
                    avro_datum_decref(value_datum);
                    avro_datum_decref(union_datum);
                    break;

                default:
                    fprintf(stderr, "Unknown response format: %d\n", PQfformat(res, column));
                    exit_nicely(context->conn);
            }
        }
        printf(" (oid = %d, mod = %d)", PQftype(res, column), PQfmod(res, column));
    }
    printf("\n");

    if (avro_file_writer_append(context->output, tuple)) {
        fprintf(stderr, "Unable to write tuple to output file: %s\n", avro_strerror());
        exit_nicely(context->conn);
    }
    avro_datum_decref(tuple);
}

/* Inspects a tuple from a result set to determine the output schema.
 * Creates an Avro output file with that schema. */
void init_table_context(PGresult *res, struct table_context_t *context) {
    context->schema = avro_schema_record("Tuple", "postgres");
    int columns = PQnfields(res);
    for (int column = 0; column < columns; column++) {
        avro_schema_t null_schema = avro_schema_null();
        avro_schema_t string_schema = avro_schema_string();
        avro_schema_t union_schema = avro_schema_union();
        avro_schema_union_append(union_schema, null_schema);
        avro_schema_union_append(union_schema, string_schema);
        avro_schema_record_field_append(context->schema, PQfname(res, column), union_schema);
        avro_schema_decref(null_schema);
        avro_schema_decref(string_schema);
        avro_schema_decref(union_schema);
    }

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

    /* The final parameter 0 requests the results in text format */
    if (!PQsendQueryParams(conn, "SELECT xmin, xmax, * FROM " DB_TABLE, 0, NULL, NULL, NULL, NULL, 0)) {
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
        avro_schema_decref(context.schema);
    }
    if (error) exit_nicely(conn);

    exec_query(conn, "COMMIT");
    PQfinish(conn);

    fprintf(stderr, "%d rows exported\n", total);
    return 0;
}
