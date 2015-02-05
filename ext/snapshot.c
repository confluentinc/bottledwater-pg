#include "io_util.h"
#include "oid2avro.h"
#include "protocol_server.h"

#include <string.h>
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

typedef struct {
    Oid relid;
    char *namespace;
    char *relname;
    Relation rel;
} export_table;

/* State that we need to remember between calls of samza_table_export */
typedef struct {
    export_table *tables;
    int num_tables, current_table;
    avro_schema_t frame_schema;
    avro_value_iface_t *frame_iface;
    avro_value_t frame_value;
    schema_cache_t schema_cache;
    Portal cursor;
} export_state;

void get_table_list(export_state *state, text *table_pattern);
void open_next_table(export_state *state);
void close_current_table(export_state *state);
bytea *format_snapshot_row(export_state *state);
avro_schema_t schema_for_relname(char *relname);


PG_FUNCTION_INFO_V1(samza_table_schema);

/* Given the name of a table, generates an Avro schema for that table, and returns it
 * as a JSON string. */
Datum samza_table_schema(PG_FUNCTION_ARGS) {
    bytea *json;
    avro_schema_t schema = schema_for_relname(NameStr(*PG_GETARG_NAME(0)));
    int err = try_writing(&json, &write_schema_json, schema);
    avro_schema_decref(schema);

    if (err) {
        elog(ERROR, "samza_table_schema: Could not encode schema as JSON: %s", avro_strerror());
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TEXT_P(json);
    }
}

PG_FUNCTION_INFO_V1(samza_frame_schema);

/* Returns a JSON string containing the frame schema of the logical log output plugin.
 * This should be used by clients to decode the data streamed from the log, allowing
 * schema evolution to handle version changes of the plugin. */
Datum samza_frame_schema(PG_FUNCTION_ARGS) {
    bytea *json;
    avro_schema_t schema = schema_for_frame();
    int err = try_writing(&json, &write_schema_json, schema);
    avro_schema_decref(schema);

    if (err) {
        elog(ERROR, "samza_frame_schema: Could not encode schema as JSON: %s", avro_strerror());
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TEXT_P(json);
    }
}


PG_FUNCTION_INFO_V1(samza_table_export);

/* Given the name of a table, returns a set of byte array values containing the table contents
 * encoded as Avro (one byte array per row of the table). */
Datum samza_table_export(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    export_state *state;
    int ret;

    if (SRF_IS_FIRSTCALL()) {
        /* On first call of the function, determine the list of tables to export */
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        if ((ret = SPI_connect()) < 0) {
            elog(ERROR, "samza_table_export: SPI_connect returned %d", ret);
        }

        state = (export_state *) palloc(sizeof(export_state));
        state->current_table = 0;
        state->frame_schema = schema_for_frame();
        state->frame_iface = avro_generic_class_from_schema(state->frame_schema);
        avro_generic_value_new(state->frame_iface, &state->frame_value);
        state->schema_cache = schema_cache_new(funcctx->multi_call_memory_ctx);
        funcctx->user_fctx = state;

        get_table_list(state, PG_GETARG_TEXT_P(0));
        if (state->num_tables > 0) open_next_table(state);

        MemoryContextSwitchTo(oldcontext);
    }

    /* On every call of the function, try to fetch one row from the current cursor,
     * and process it. If the current cursor has no more rows, move on to the next
     * table. */
    funcctx = SRF_PERCALL_SETUP();
    state = (export_state *) funcctx->user_fctx;

    while (state->current_table < state->num_tables) {
        SPI_cursor_fetch(state->cursor, true, 1);

        if (SPI_processed == 0) {
            close_current_table(state);
            state->current_table++;
            if (state->current_table < state->num_tables) open_next_table(state);
        } else {
            SRF_RETURN_NEXT(funcctx, PointerGetDatum(format_snapshot_row(state)));
        }
    }

    schema_cache_free(state->schema_cache);
    avro_value_decref(&state->frame_value);
    avro_value_iface_decref(state->frame_iface);
    avro_schema_decref(state->frame_schema);
    SPI_finish();
    SRF_RETURN_DONE(funcctx);
}

/* Queries the PG catalog to get a list of tables (matching the given table name pattern)
 * that we should export. The pattern is given to the LIKE operator, so "%" means any
 * table. Selects only ordinary tables (no views, foreign tables, etc) and excludes any
 * PG system tables. Updates export_state with the list of tables.
 *
 * Also takes a shared lock on all the tables we're going to export, to make sure they
 * aren't dropped or schema-altered before we get around to reading them. (Ordinary
 * writes to the table, i.e. insert/update/delete, are not affected.) */
void get_table_list(export_state *state, text *table_pattern) {
    Oid argtypes[] = { TEXTOID };
    Datum args[] = { PointerGetDatum(table_pattern) };

    int ret = SPI_execute_with_args(
            "SELECT c.oid, n.nspname, c.relname "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE relkind = 'r' AND c.relname LIKE $1 AND "
            "n.nspname NOT LIKE 'pg_%' AND n.nspname != 'information_schema' AND "
            "c.relpersistence = 'p' AND c.relispopulated = 't'",
            1, argtypes, args, NULL, true, 0);
    if (ret != SPI_OK_SELECT) {
        elog(ERROR, "Could not fetch table list: SPI_execute_with_args returned %d", ret);
    }

    state->tables = palloc(SPI_processed * sizeof(export_table));
    state->num_tables = SPI_processed;

    for (int i = 0; i < SPI_processed; i++) {
        bool oid_null, namespace_null, relname_null;
        HeapTuple tuple = SPI_tuptable->vals[i];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;

        Datum oid_d       = heap_getattr(tuple, 1, tupdesc, &oid_null);
        Datum namespace_d = heap_getattr(tuple, 2, tupdesc, &namespace_null);
        Datum relname_d   = heap_getattr(tuple, 3, tupdesc, &relname_null);
        if (oid_null || namespace_null || relname_null) {
            elog(ERROR, "get_table_list: unexpected null value");
        }

        export_table *table = &state->tables[i];
        table->relid     = DatumGetObjectId(oid_d);
        table->namespace = NameStr(*DatumGetName(namespace_d));
        table->relname   = NameStr(*DatumGetName(relname_d));
        table->rel       = relation_open(table->relid, AccessShareLock);
    }

    SPI_freetuptable(SPI_tuptable);
}

/* Starts a query to dump all the rows from state->tables[state->current_table].
 * Updates the state accordingly. */
void open_next_table(export_state *state) {
    export_table *table = &state->tables[state->current_table];

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT * FROM %s",
            quote_qualified_identifier(table->namespace, table->relname));

    SPIPlanPtr plan = SPI_prepare_cursor(query.data, 0, NULL, CURSOR_OPT_NO_SCROLL);
    if (!plan) {
        elog(ERROR, "samza_table_export: SPI_prepare_cursor failed with error %d", SPI_result);
    }
    state->cursor = SPI_cursor_open(NULL, plan, NULL, NULL, true);
}

/* When the current cursor has no more rows to return, this function closes it,
 * frees the associated resources, and releases the table lock. */
void close_current_table(export_state *state) {
    export_table *table = &state->tables[state->current_table];
    relation_close(table->rel, AccessShareLock);

    SPI_cursor_close(state->cursor);
    SPI_freetuptable(SPI_tuptable);
}

/* Call this when SPI_tuptable contains one row of a table, fetched from a cursor.
 * This function encodes that tuple as Avro and returns it as a byte array. */
bytea *format_snapshot_row(export_state *state) {
    export_table *table = &state->tables[state->current_table];
    bytea *output;

    if (SPI_processed != 1) {
        elog(ERROR, "Expected exactly 1 row from cursor, but got %d rows", SPI_processed);
    }
    if (avro_value_reset(&state->frame_value)) {
        elog(ERROR, "Avro value reset failed: %s", avro_strerror());
    }

    HeapTuple row = SPI_tuptable->vals[0];
    if (update_frame_with_insert(&state->frame_value, state->schema_cache, table->rel, row)) {
        elog(ERROR, "samza_table_export: Avro conversion failed: %s", avro_strerror());
    }
    if (try_writing(&output, &write_avro_binary, &state->frame_value)) {
        elog(ERROR, "samza_table_export: writing Avro binary failed: %s", avro_strerror());
    }

    SPI_freetuptable(SPI_tuptable);
    return output;
}

/* Given the name of a table (relation), generates an Avro schema for it. */
avro_schema_t schema_for_relname(char *relname) {
    List *relname_list = stringToQualifiedNameList(relname);
    RangeVar *relvar = makeRangeVarFromNameList(relname_list);
    Relation rel = relation_openrv(relvar, AccessShareLock);
    avro_schema_t schema = schema_for_relation(rel, true);
    relation_close(rel, AccessShareLock);
    return schema;
}
