#include "connect.h"
#include "replication.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <internal/pqexpbuffer.h>

#define DEFAULT_TABLE "%%"

/* Wrap around a function call to bail on error. */
#define check(err, call) { err = call; if (err) return err; }

/* Similar to the check() macro, but for calls to functions in the replication stream
 * module. Since those functions have their own error message buffer, if an error
 * occurs, we need to copy the message to our own context's buffer. */
#define checkRepl(err, context, call) { \
    err = call; \
    if (err) { \
        strncpy((context)->error, (context)->repl.error, CLIENT_CONTEXT_ERROR_LEN); \
        return err; \
    } \
}

void client_error(client_context_t context, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));
int exec_sql(client_context_t context, char *query);
int client_connect(client_context_t context);
void client_sql_disconnect(client_context_t context);
int replication_slot_exists(client_context_t context, bool *exists);
int snapshot_start(client_context_t context);
int snapshot_poll(client_context_t context);
int snapshot_tuple(client_context_t context, PGresult *res, int row_number);

// TODO refactor this code, I don't wanna get a list of oids inside connect.c
int lookup_table_oids(client_context_t context);

/* Allocates a client_context struct. After this is done and before
 * db_client_start() is called, various fields in the struct need to be
 * initialized. */
client_context_t db_client_new() {
    client_context_t context = malloc(sizeof(client_context));
    memset(context, 0, sizeof(client_context));
    return context;
}


/* Closes any network connections, if applicable, and frees the client_context struct. */
void db_client_free(client_context_t context) {
    client_sql_disconnect(context);
    if (context->repl.conn) PQfinish(context->repl.conn);
    if (context->repl.table_ids) free(context->repl.table_ids);
    if (context->repl.schema_pattern) free(context->repl.schema_pattern);
    if (context->repl.table_pattern) free(context->repl.table_pattern);
    if (context->repl.snapshot_name) free(context->repl.snapshot_name);
    if (context->repl.output_plugin) free(context->repl.output_plugin);
    if (context->repl.slot_name) free(context->repl.slot_name);
    if (context->error_policy) free(context->error_policy);
    if (context->app_name) free(context->app_name);
    if (context->conninfo) free(context->conninfo);
    if (context->order_by) free(context->order_by);
    free(context);
}

void db_client_set_error_policy(client_context_t context, const char *policy) {
    if (context->error_policy) free(context->error_policy);
    context->error_policy = strdup(policy);
}


/* Connects to the Postgres server (using context->conninfo for server info and
 * context->app_name as client name), and checks whether replication slot
 * context->repl.slot_name already exists. If yes, sets up the context to start
 * receiving the stream of changes from that slot. If no, creates the slot, and
 * initiates the consistent snapshot. */
int db_client_start(client_context_t context) {
    int err = 0;
    bool slot_exists;

    check(err, client_connect(context));
    checkRepl(err, context, replication_stream_check(&context->repl));

    // this a hacky way to get list of oids from stream->tables, stream->schemas
    // TODO refactor it
    // Get a list of oids, which we want to stream
    // If error, there's something wrong, the extension will send nothing
    // Default: the extension will send everything
    check(err, lookup_table_oids(context));
    check(err, replication_slot_exists(context, &slot_exists));

    if (slot_exists) {
        context->slot_created = false;
    } else {
        checkRepl(err, context, replication_slot_create(&context->repl));
        context->slot_created = true;

        if (!context->skip_snapshot) {
            context->taking_snapshot = true;
            check(err, snapshot_start(context));

            /* we'll switch over to replication in db_client_poll after the
             * snapshot finishes */
            return err;
        }
    }

    client_sql_disconnect(context);
    context->taking_snapshot = false;

    checkRepl(err, context, replication_stream_start(&context->repl, context->error_policy));

    return err;
}


/* Checks whether new data has arrived from the server (on either the snapshot
 * connection or the replication connection, as appropriate). If yes, it is
 * processed, and context->status is set to 1. If no data is available, this
 * function does not block, but returns immediately, and context->status is set
 * to 0. If the data stream has ended, context->status is set to -1. */
int db_client_poll(client_context_t context) {
    int err = 0;

    if (context->sql_conn) {
        /* To make PQgetResult() non-blocking, check PQisBusy() first */
        if (PQisBusy(context->sql_conn)) {
            context->status = 0;
            return err;
        }

        check(err, snapshot_poll(context));
        context->status = 1;

        /* If the snapshot is finished, switch over to the replication stream */
        if (!context->sql_conn) {
            checkRepl(err, context, replication_stream_start(&context->repl, context->error_policy));
        }
        return err;

    } else {
        checkRepl(err, context, replication_stream_poll(&context->repl));
        context->status = context->repl.status;
        return err;
    }
}


/* Blocks until more data is received from the server. You don't have to use
 * this if you have your own select loop. */
int db_client_wait(client_context_t context) {
    fd_set input_mask;
    FD_ZERO(&input_mask);

    int rep_fd = PQsocket(context->repl.conn);
    int max_fd = rep_fd;
    FD_SET(rep_fd, &input_mask);

    if (context->sql_conn) {
        int sql_fd = PQsocket(context->sql_conn);
        if (sql_fd > max_fd) max_fd = sql_fd;
        FD_SET(sql_fd, &input_mask);
    }

    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;

    int ret = select(max_fd + 1, &input_mask, NULL, NULL, &timeout);

    if (ret == 0 || (ret < 0 && errno == EINTR)) {
        return 0; /* timeout or signal */
    }
    if (ret < 0) {
        client_error(context, "select() failed: %s", strerror(errno));
        return errno;
    }

    /* Data has arrived on the socket */
    if (!PQconsumeInput(context->repl.conn)) {
        client_error(context, "Could not receive replication data: %s",
                PQerrorMessage(context->repl.conn));
        return EIO;
    }
    if (context->sql_conn && !PQconsumeInput(context->sql_conn)) {
        client_error(context, "Could not receive snapshot data: %s",
                PQerrorMessage(context->sql_conn));
        return EIO;
    }
    return 0;
}


/* Updates the context's statically allocated error buffer with a message. */
void client_error(client_context_t context, char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(context->error, CLIENT_CONTEXT_ERROR_LEN, fmt, args);
    va_end(args);
}


/* Executes a SQL command that returns no results. */
int exec_sql(client_context_t context, char *query) {
    PGresult *res = PQexec(context->sql_conn, query);
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        return 0;
    } else {
        client_error(context, "Query failed: %s: %s", query, PQerrorMessage(context->sql_conn));
        PQclear(res);
        return EIO;
    }
}


/* Establishes two network connections to a Postgres server: one for SQL, and one
 * for replication. context->conninfo contains the connection string or URL to connect
 * to, and context->app_name is the client name (which appears, for example, in
 * pg_stat_activity). Returns 0 on success. */
int client_connect(client_context_t context) {
    if (!context->conninfo || context->conninfo[0] == '\0') {
        client_error(context, "conninfo must be set in client context");
        return EINVAL;
    }
    if (!context->app_name || context->app_name[0] == '\0') {
        client_error(context, "app_name must be set in client context");
        return EINVAL;
    }

    context->sql_conn = PQconnectdb(context->conninfo);
    if (PQstatus(context->sql_conn) != CONNECTION_OK) {
        client_error(context, "Connection to database failed: %s", PQerrorMessage(context->sql_conn));
        return EIO;
    }

    /* Parse the connection string into key-value pairs */
    char *error = NULL;
    PQconninfoOption *parsed_opts = PQconninfoParse(context->conninfo, &error);
    if (!parsed_opts) {
        client_error(context, "Replication connection info: %s", error);
        PQfreemem(error);
        return EIO;
    }

    /* Copy the key-value pairs into a new structure with added replication options */
    PQconninfoOption *option;
    int optcount = 2; /* replication, fallback_application_name */
    for (option = parsed_opts; option->keyword != NULL; option++) {
        if (option->val != NULL && option->val[0] != '\0') optcount++;
    }

    const char **keys = malloc((optcount + 1) * sizeof(char *));
    const char **values = malloc((optcount + 1) * sizeof(char *));
    int i = 0;

    for (option = parsed_opts; option->keyword != NULL; option++) {
        if (option->val != NULL && option->val[0] != '\0') {
            keys[i] = option->keyword;
            values[i] = option->val;
            i++;
        }
    }

    keys[i] = "replication";               values[i] = "database";        i++;
    keys[i] = "fallback_application_name"; values[i] = context->app_name; i++;
    keys[i] = NULL;                        values[i] = NULL;

    int err = 0;
    context->repl.conn = PQconnectdbParams(keys, values, true);
    if (PQstatus(context->repl.conn) != CONNECTION_OK) {
        client_error(context, "Replication connection failed: %s", PQerrorMessage(context->repl.conn));
        err = EIO;
    }

    free(keys);
    free(values);
    PQconninfoFree(parsed_opts);
    return err;
}


void client_sql_disconnect(client_context_t context) {
    if (!context->sql_conn) return;

    PQfinish(context->sql_conn);
    context->sql_conn = NULL;
}


/* Sets *exists to true if a replication slot with the name context->repl.slot_name
 * already exists, and false if not. In addition, if the slot already exists,
 * context->repl.start_lsn is filled in with the LSN at which the client should
 * restart streaming. */
int replication_slot_exists(client_context_t context, bool *exists) {
    if (!context->repl.slot_name || context->repl.slot_name[0] == '\0') {
        client_error(context, "repl.slot_name must be set in client context");
        return EINVAL;
    }

    int err = 0;
    Oid argtypes[] = { 19 }; // 19 == NAMEOID
    const char *args[] = { context->repl.slot_name };

    PGresult *res = PQexecParams(context->sql_conn,
            "SELECT restart_lsn FROM pg_replication_slots where slot_name = $1",
            1, argtypes, args, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        client_error(context, "Could not check for existing replication slot: %s",
                PQerrorMessage(context->sql_conn));
        err = EIO;
        goto done;
    }

    *exists = (PQntuples(res) > 0 && !PQgetisnull(res, 0, 0));

    if (*exists) {
        uint32 h32, l32;
        if (sscanf(PQgetvalue(res, 0, 0), "%X/%X", &h32, &l32) != 2) {
            client_error(context, "Could not parse restart LSN: \"%s\"", PQgetvalue(res, 0, 0));
            err = EIO;
            goto done;
        } else {
            context->repl.start_lsn = ((uint64) h32) << 32 | l32;
        }
    }

done:
    PQclear(res);
    return err;
}


/* Initiates the non-blocking capture of a consistent snapshot of the database,
 * using the exported snapshot context->repl.snapshot_name. */
int snapshot_start(client_context_t context) {
    if (!context->repl.snapshot_name || context->repl.snapshot_name[0] == '\0') {
        client_error(context, "snapshot_name must be set in client context");
        return EINVAL;
    }

    int err = 0;
    check(err, exec_sql(context, "BEGIN"));
    check(err, exec_sql(context, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"));

    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query, "SET TRANSACTION SNAPSHOT '%s'", context->repl.snapshot_name);
    check(err, exec_sql(context, query->data));
    destroyPQExpBuffer(query);

    PQExpBuffer snapshot_query = createPQExpBuffer();
    appendPQExpBuffer(snapshot_query,
        "SELECT bottledwater_export(table_pattern := '%s', schema_pattern := '%s',"
                                    " allow_unkeyed := '%s', error_policy := '%s',"
                                    " order_by := '%s')",
        context->repl.table_pattern,
        context->repl.schema_pattern,
        context->allow_unkeyed ? "t" : "f",
        context->error_policy,
        context->order_by ? context->order_by : "");


    if (!PQsendQueryParams(context->sql_conn, snapshot_query->data,
            0, NULL, NULL, NULL, NULL, 1)) {
        client_error(context, "Could not dispatch snapshot fetch: %s",
                PQerrorMessage(context->sql_conn));
        return EIO;
    }

    if (!PQsetSingleRowMode(context->sql_conn)) {
        client_error(context, "Could not activate single-row mode");
        return EIO;
    }

    // Invoke the begin-transaction callback with xid==0 to indicate start of snapshot
    begin_txn_cb begin_txn = context->repl.frame_reader->on_begin_txn;
    void *cb_context = context->repl.frame_reader->cb_context;
    if (begin_txn) {
        check(err, begin_txn(cb_context, context->repl.start_lsn, 0));
    }

    destroyPQExpBuffer(snapshot_query);
    return 0;
}

/* Reads the next result row from the snapshot query, parses and processes it.
 * Blocks until a new row is available, if necessary. */
int snapshot_poll(client_context_t context) {
    int err = 0;
    PGresult *res = PQgetResult(context->sql_conn);

    /* null result indicates that there are no more rows */
    if (!res) {
        check(err, exec_sql(context, "COMMIT"));
        client_sql_disconnect(context);

        // Invoke the commit callback with xid==0 to indicate end of snapshot
        commit_txn_cb on_commit = context->repl.frame_reader->on_commit_txn;
        void *cb_context = context->repl.frame_reader->cb_context;
        if (on_commit) {
            check(err, on_commit(cb_context, context->repl.start_lsn, 0));
        }
        return 0;
    }

    ExecStatusType status = PQresultStatus(res);
    if (status != PGRES_SINGLE_TUPLE && status != PGRES_TUPLES_OK) {
        client_error(context, "While reading snapshot: %s: %s",
                PQresStatus(PQresultStatus(res)),
                PQresultErrorMessage(res));
        PQclear(res);
        return EIO;
    }

    int tuples = PQntuples(res);
    for (int tuple = 0; tuple < tuples; tuple++) {
        check(err, snapshot_tuple(context, res, tuple));
    }
    PQclear(res);
    return err;
}

/* Processes one tuple of the snapshot query result set. */
int snapshot_tuple(client_context_t context, PGresult *res, int row_number) {
    if (PQnfields(res) != 1) {
        client_error(context, "Unexpected response with %d fields", PQnfields(res));
        return EIO;
    }
    if (PQgetisnull(res, row_number, 0)) {
        client_error(context, "Unexpected null response value");
        return EIO;
    }
    if (PQfformat(res, 0) != 1) { /* format 1 == binary */
        client_error(context, "Unexpected response format: %d", PQfformat(res, 0));
        return EIO;
    }

    /* wal_pos == 0 == InvalidXLogRecPtr */
    int err = parse_frame(context->repl.frame_reader, 0, PQgetvalue(res, row_number, 0),
            PQgetlength(res, row_number, 0));
    if (err) {
        client_error(context, "Error parsing frame data: %s", context->repl.frame_reader->error);
    }
    return err;
}

/* Lookup for table oids from schema_pattern and table_pattern
   If schema_pattern == % and table_pattern == % then BW will get all tables in db */
int lookup_table_oids(client_context_t context) {
    if (strcmp(context->repl.table_pattern, "%%") == 0 && strcmp(context->repl.schema_pattern, "%%") == 0) {
        // All tables will be streamed
        return 0;
    }

    PQExpBuffer query = createPQExpBuffer();
    appendPQExpBuffer(query,
          "SELECT c.oid"
          " FROM pg_catalog.pg_class c"
          " JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
          " WHERE c.relkind = 'r' AND"
          " c.relname SIMILAR TO '%s' AND" // get table that has name similar to table_pattern
                                           // pattern syntax follows
                                           // https://www.postgresql.org/docs/current/static/functions-matching.html
          " n.nspname NOT LIKE 'pg_%%' AND n.nspname != 'information_schema' AND"
          " n.nspname SIMILAR TO '%s' AND" // only get table has schema similar to schema_pattern
                                           // pattern syntax follows
                                           // https://www.postgresql.org/docs/current/static/functions-matching.html
          " c.relpersistence = 'p'",
        context->repl.table_pattern,
        context->repl.schema_pattern);

    PGresult *res = PQexec(context->sql_conn, query->data);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        client_error(context, "Failed to lookup table ids: %s.", PQerrorMessage(context->sql_conn));
        PQclear(res);
        return EIO;
    }

    // Query returns zero row, mean there's no tables match with table_pattern and schema_pattern
    if (PQntuples(res) == 0) {
        client_error(context, "Couldn't find any tables matching: schemas %s, tables %s.",
                    context->repl.schema_pattern, context->repl.table_pattern);
        PQclear(res);
        return EIO;
    }

    // Query returns zero fields, it means there something wrong with the query :D
    if (PQnfields(res) == 0) {
        client_error(context, "Unexpected result when looking up table ids with (table_schema %s, schema_pattern %s).",
                context->repl.table_pattern, context->repl.schema_pattern);
        PQclear(res);
        return EIO;
    }

    int i;
    int rows = PQntuples(res);
    PQExpBuffer table_ids = createPQExpBuffer();

    appendPQExpBuffer(table_ids, "%s", rows > 0 ? PQgetvalue(res, 0, 0): "");
    for (i = 1; i < rows; ++i) {
        appendPQExpBuffer(table_ids, ".");
        appendPQExpBuffer(table_ids, "%s", PQgetvalue(res, i, 0) ? PQgetvalue(res, i, 0) : "");
    }
    context->repl.table_ids = strdup(table_ids->data);

    PQclear(res);
    destroyPQExpBuffer(query);
    destroyPQExpBuffer(table_ids);
    return 0;
}
