#include <stdio.h>
#include <stdlib.h>
#include "libpq-fe.h"

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

void output_tuple(PGconn *conn, PGresult *res, int row_number) {
    int columns = PQnfields(res);
    for (int column = 0; column < columns; column++) {
        if (column > 0) printf(", ");
        printf("%s = ", PQfname(res, column));
        if (PQgetisnull(res, row_number, column)) {
            printf("null");
        } else {
            switch (PQfformat(res, column)) {
                case 0: /* text */
                    printf("'%s'", PQgetvalue(res, row_number, column));
                    break;
                case 1: /* binary */
                    binary_value(PQgetvalue(res, row_number, column), PQgetlength(res, row_number, column));
                    break;
                default:
                    fprintf(stderr, "Unknown response format: %d\n", PQfformat(res, column));
                    exit_nicely(conn);
            }
        }
        printf(" (oid = %d, mod = %d)", PQftype(res, column), PQfmod(res, column));
    }
    printf("\n");
}

int main(int argc, char **argv) {
    PGconn *conn = PQconnectdb("postgres://localhost/martin");

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    exec_query(conn, "BEGIN");
    exec_query(conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
    /* exec_query(conn, "SET TRANSACTION SNAPSHOT '...'"); */

    /* The final parameter 1 requests the results in binary format */
    if (!PQsendQueryParams(conn, "SELECT xmin, xmax, * FROM test", 0, NULL, NULL, NULL, NULL, 1)) {
        fprintf(stderr, "Could not dispatch snapshot fetch: %s\n", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    if (!PQsetSingleRowMode(conn)) {
        fprintf(stderr, "Could not activate single-row mode\n");
        exit_nicely(conn);
    }

    int error = 0, tuples;
    for (;;) {
        PGresult *res = PQgetResult(conn);
        if (!res) break; /* null result indicates that there are no more rows */

        switch (PQresultStatus(res)) {
            case PGRES_SINGLE_TUPLE:
            case PGRES_TUPLES_OK:
                tuples = PQntuples(res);
                for (int tuple = 0; tuple < tuples; tuple++) {
                    output_tuple(conn, res, tuple);
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

    if (error) exit_nicely(conn);

    exec_query(conn, "COMMIT");
    PQfinish(conn);
    return 0;
}
