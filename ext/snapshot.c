#include "io_util.h"
#include "oid2avro.h"
#include "protocol_server.h"
#include "error_policy.h"

#include <string.h>
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

typedef struct {
    Oid relid;
    Relation rel;
    char *namespace;
    char *rel_name;
    char repl_ident;
    char *index_name;
    char *order_by_column;
} export_table;

/* State that we need to remember between calls of bottledwater_export */
typedef struct {
    MemoryContext memcontext;
    export_table *tables;
    error_policy_t error_policy;
    int num_tables, current_table;
    avro_schema_t frame_schema;
    avro_value_iface_t *frame_iface;
    avro_value_t frame_value;
    schema_cache_t schema_cache;
    Portal cursor;
} export_state;

void print_tupdesc(char *title, TupleDesc tupdesc);
void get_table_list(export_state *state, text *table_pattern,
                    text *schema_pattern, bool allow_unkeyed,
                    List *order_columns);
void open_next_table(export_state *state);
void close_current_table(export_state *state);
bytea *format_snapshot_row(export_state *state);
bytea *schema_for_relname(char *relname, bool get_key);
List *textToQualifiedNameList1(text *textval);
char *check_order_by_column(char *relname, List *order_columns);


PG_FUNCTION_INFO_V1(bottledwater_key_schema);

/* Given the name of a table, generates an Avro schema for the key (replica identity)
 * of that table, and returns it as a JSON string. */
Datum bottledwater_key_schema(PG_FUNCTION_ARGS) {
    char *table_name = NameStr(*PG_GETARG_NAME(0));
    bytea *json = schema_for_relname(table_name, true);
    if (!json) {
        elog(ERROR, "Table \"%s\" does not have a primary key or replica identity", table_name);
    }
    PG_RETURN_TEXT_P(json);
}


PG_FUNCTION_INFO_V1(bottledwater_row_schema);

/* Given the name of a table, generates an Avro schema for the rows of that table,
 * and returns it as a JSON string. */
Datum bottledwater_row_schema(PG_FUNCTION_ARGS) {
    bytea *json = schema_for_relname(NameStr(*PG_GETARG_NAME(0)), false);
    PG_RETURN_TEXT_P(json);
}


PG_FUNCTION_INFO_V1(bottledwater_frame_schema);

/* Returns a JSON string containing the frame schema of the logical log output plugin.
 * This should be used by clients to decode the data streamed from the log, allowing
 * schema evolution to handle version changes of the plugin. */
Datum bottledwater_frame_schema(PG_FUNCTION_ARGS) {
    bytea *json;
    avro_schema_t schema = schema_for_frame();
    int err = try_writing(&json, &write_schema_json, schema);
    avro_schema_decref(schema);

    if (err) {
        elog(ERROR, "bottledwater_frame_schema: Could not encode schema as JSON: %s", avro_strerror());
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TEXT_P(json);
    }
}


PG_FUNCTION_INFO_V1(bottledwater_export);

/* Given a search pattern for tables ('%' matches all tables), returns a set of byte array values.
 * Each byte array is a frame of our wire protocol, containing schemas and/or rows of the selected
 * tables. This is a set-returning function (SRF), which means it gets called once for each row of
 * output, allowing us to stream through large datasets without loading everything into memory.
 *
 * SRF docs: http://www.postgresql.org/docs/9.4/static/xfunc-c.html#XFUNC-C-RETURN-SET */
Datum bottledwater_export(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    MemoryContext oldcontext;
    export_state *state;
    int ret;
    text *table_pattern;
    text *schema_pattern;
    List *order_columns;
    bool allow_unkeyed;
    bytea *result;

    oldcontext = CurrentMemoryContext;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();

        /* Initialize the SPI (server programming interface), which allows us to make SQL queries
         * within this function. Note SPI_connect() switches to its own memory context, but we
         * actually want to use multi_call_memory_ctx, so we call SPI_connect() first. */
        if ((ret = SPI_connect()) < 0) {
            elog(ERROR, "bottledwater_export: SPI_connect returned %d", ret);
        }

        /* Things allocated in this memory context will live until SRF_RETURN_DONE(). */
        MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        state = (export_state *) palloc(sizeof(export_state));

        state->memcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                  "bottledwater_export per-tuple context",
                                                  ALLOCSET_DEFAULT_MINSIZE,
                                                  ALLOCSET_DEFAULT_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);

        state->current_table = 0;
        state->frame_schema = schema_for_frame();
        state->frame_iface = avro_generic_class_from_schema(state->frame_schema);
        avro_generic_value_new(state->frame_iface, &state->frame_value);
        state->schema_cache = schema_cache_new(funcctx->multi_call_memory_ctx);
        funcctx->user_fctx = state;

        table_pattern = PG_GETARG_TEXT_P(0);
        schema_pattern = PG_GETARG_TEXT_P(1);
	allow_unkeyed = PG_GETARG_BOOL(2);
        state->error_policy = parse_error_policy(TextDatumGetCString(PG_GETARG_TEXT_P(3)));
        order_columns = textToQualifiedNameList1(PG_GETARG_TEXT_P(4));

        get_table_list(state, table_pattern, schema_pattern, allow_unkeyed, order_columns);
        if (state->num_tables > 0) open_next_table(state);
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
            /* SPI_cursor_fetch() leaves us in the SPI mem. context */
            MemoryContextSwitchTo(state->memcontext);

            /* clear any prior tuple result memory */
            MemoryContextReset(state->memcontext);

            result = format_snapshot_row(state);

            MemoryContextSwitchTo(oldcontext);

            /* don't forget to clear the SPI temp context */
            SPI_freetuptable(SPI_tuptable);

            if (result != NULL) {
                SRF_RETURN_NEXT(funcctx, PointerGetDatum(result));
            }
        }
    }

    schema_cache_free(state->schema_cache);
    avro_value_decref(&state->frame_value);
    avro_value_iface_decref(state->frame_iface);
    avro_schema_decref(state->frame_schema);
    SPI_finish();
    SRF_RETURN_DONE(funcctx);
}

/* Queries the PG catalog to get a list of tables (matching the given table name pattern and schema pattern)
 * that we should export. The pattern is given to the LIKE operator, so "%" means any
 * table. Selects only ordinary tables (no views, foreign tables, etc) and excludes any
 * PG system tables. Updates export_state with the list of tables.
 *
 * Also takes a shared lock on all the tables we're going to export, to make sure they
 * aren't dropped or schema-altered before we get around to reading them. (Ordinary
 * writes to the table, i.e. insert/update/delete, are not affected.) */
void get_table_list(export_state *state, text *table_pattern,
                    text *schema_pattern, bool allow_unkeyed,
                    List *order_columns) {
    Oid argtypes[] = { TEXTOID, TEXTOID };
    Datum args[] = { PointerGetDatum(table_pattern), PointerGetDatum(schema_pattern) };
    StringInfoData errors;

    int ret = SPI_execute_with_args(
            // c is the class of the table (which stores, amongst other things, the table name).
            // n is the namespace (i.e. schema).
            // i is an index on the table (refined below).
            // ic is the class of the index (from which we get the name of the index).
            "SELECT c.oid, n.nspname, c.relname, c.relreplident, ic.relname AS indname "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "

            // Find all indexes on the table
            "LEFT JOIN pg_catalog.pg_index i ON c.oid = i.indrelid AND i.indisvalid AND i.indisready AND "

            // For REPLICA_IDENTITY_DEFAULT ('d') and REPLICA_IDENTITY_FULL ('f'), find the primary key.
            // For REPLICA_IDENTITY_INDEX ('i'), find the referenced index.
            // For REPLICA_IDENTITY_NOTHING ('n'), don't match any index, even if it exists.
            "((c.relreplident IN ('d', 'f') AND i.indisprimary) OR (c.relreplident = 'i' AND i.indisreplident)) "

            // Join with pg_class again to get the name of the index
            "LEFT JOIN pg_catalog.pg_class ic ON i.indexrelid = ic.oid "

            // Select only ordinary tables ('r' == RELKIND_RELATION) matching the required name pattern
            "WHERE c.relkind = 'r' AND c.relname SIMILAR TO $1 AND "
            "n.nspname NOT LIKE 'pg_%' AND n.nspname != 'information_schema' AND n.nspname SIMILAR TO $2 AND " // not a system table
            "c.relpersistence = 'p'", // 'p' == RELPERSISTENCE_PERMANENT (not unlogged or temporary)

            2, argtypes, args, NULL, true, 0);

    if (ret != SPI_OK_SELECT) {
        elog(ERROR, "Could not fetch table list: SPI_execute_with_args returned %d", ret);
    }

    state->tables = palloc0(SPI_processed * sizeof(export_table));
    state->num_tables = SPI_processed;
    initStringInfo(&errors);

    for (int i = 0; i < SPI_processed; i++) {
        bool oid_null, namespace_null, relname_null, replident_null, indname_null;
        HeapTuple tuple = SPI_tuptable->vals[i];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        export_table *table;

        Datum oid_d       = heap_getattr(tuple, 1, tupdesc, &oid_null);
        Datum namespace_d = heap_getattr(tuple, 2, tupdesc, &namespace_null);
        Datum relname_d   = heap_getattr(tuple, 3, tupdesc, &relname_null);
        Datum replident_d = heap_getattr(tuple, 4, tupdesc, &replident_null);
        Datum indname_d   = heap_getattr(tuple, 5, tupdesc, &indname_null);

        if (oid_null || namespace_null || relname_null || replident_null) {
            elog(ERROR, "get_table_list: unexpected null value");
        }

        table = &state->tables[i];
        table->relid      = DatumGetObjectId(oid_d);
        table->rel        = relation_open(table->relid, AccessShareLock);
        table->namespace  = pstrdup(NameStr(*DatumGetName(namespace_d)));
        table->rel_name   = pstrdup(NameStr(*DatumGetName(relname_d)));
        table->repl_ident = DatumGetChar(replident_d);
        table->order_by_column = check_order_by_column(table->rel_name, order_columns);

        if (!indname_null) {
            table->index_name = pstrdup(NameStr(*DatumGetName(indname_d)));

            elog(INFO, "bottledwater_export: Table %s is keyed by index %s",
                    quote_qualified_identifier(table->namespace, table->rel_name), table->index_name);

        } else if (table->repl_ident == REPLICA_IDENTITY_NOTHING) {
            appendStringInfo(&errors, "\t%s is using REPLICA IDENTITY NOTHING.\n",
                    quote_qualified_identifier(table->namespace, table->rel_name));
        } else {
            appendStringInfo(&errors, "\t%s does not have a primary key.\n",
                    quote_qualified_identifier(table->namespace, table->rel_name));
        }

        for (int j = 0; j < i; j++) {
            if (table->relid == state->tables[j].relid) {
                elog(ERROR, "get_table_list: table %s has ambiguous primary key (%s and %s)",
                        table->rel_name, table->index_name, state->tables[j].index_name);
            }
        }
    }

    SPI_freetuptable(SPI_tuptable);

    if (errors.len > 0) {
        if (allow_unkeyed) {
            elog(INFO, "bottledwater_export: The following tables will be exported without a key:\n%s",
                    errors.data);
        } else {
            elog(ERROR, "bottledwater_export: The following tables do not have a replica identity key:\n%s"
                    "\tPlease give them a primary key or set REPLICA IDENTITY USING INDEX.\n"
                    "\tTo ignore this issue, and export them anyway, use --allow-unkeyed\n"
                    "\t(note that export of updates and deletes will then be incomplete).",
                    errors.data);
        }
    }
}

/* Starts a query to dump all the rows from state->tables[state->current_table].
 * Updates the state accordingly. */
void open_next_table(export_state *state) {
    export_table *table = &state->tables[state->current_table];
    SPIPlanPtr plan;

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, "SELECT * FROM %s",
            quote_qualified_identifier(table->namespace, table->rel_name));

    if (table->order_by_column){
        appendStringInfo(&query, " ORDER BY %s", table->order_by_column);
        elog(INFO, "bottledwater_export: Table %s is ordered by %s",
                quote_qualified_identifier(table->namespace, table->rel_name), table->order_by_column);
    }

    plan = SPI_prepare_cursor(query.data, 0, NULL, CURSOR_OPT_NO_SCROLL);
    if (!plan) {
        elog(ERROR, "bottledwater_export: SPI_prepare_cursor failed with error %d", SPI_result);
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

    if (update_frame_with_insert(&state->frame_value, state->schema_cache, table->rel,
            SPI_tuptable->tupdesc, SPI_tuptable->vals[0])) {
        elog(INFO, "Failed tuptable: %s", schema_debug_info(table->rel, SPI_tuptable->tupdesc));
        elog(INFO, "Failed relation: %s", schema_debug_info(table->rel, RelationGetDescr(table->rel)));
        error_policy_handle(state->error_policy, "bottledwater_export: Avro conversion failed", avro_strerror());
        /* if handling the error didn't exit early, it should be safe to fall
         * through, because we'll just write the frame without the message that
         * failed (so potentially it'll be an empty frame)
         */
    }
    if (try_writing(&output, &write_avro_binary, &state->frame_value)) {
        error_policy_handle(state->error_policy, "bottledwater_export: writing Avro binary failed", avro_strerror());
        /* if we didn't exit early, then output remains uninitialised */
        return NULL;
    }
    return output;
}

/* Given the name of a table (relation), generates an Avro schema for either the rows
 * or the key (replica identity) of the table. */
bytea *schema_for_relname(char *relname, bool get_key) {
    int err;
    bytea *json;
    avro_schema_t schema;
    List *relname_list = stringToQualifiedNameList(relname);
    RangeVar *relvar = makeRangeVarFromNameList(relname_list);
    Relation rel = relation_openrv(relvar, AccessShareLock);

    if (get_key) {
        err = schema_for_table_key(rel, &schema);
    } else {
        err = schema_for_table_row(rel, &schema);
    }

    relation_close(rel, AccessShareLock);
    if (err) {
        elog(ERROR, "bottledwater_table_schema: Could not get schema for relname %s: %s",
                relname, avro_strerror());
    }
    if (!schema) return NULL;

    err = try_writing(&json, &write_schema_json, schema);
    avro_schema_decref(schema);

    if (err) {
        elog(ERROR, "bottledwater_table_schema: Could not encode schema as JSON: %s",
                avro_strerror());
    }
    return json;
}

/* This is a function from postgres src/backend/utils/adt/varlena.c line 3110
 * I just wanna reuse it with different separator ',', for a more easier future
 * refactor, I keep it as the same as the original code (name, params, etc) */
List *
textToQualifiedNameList1(text *textval)
{
	char	   *rawname;
	List	   *result = NIL;
	List	   *namelist;
	ListCell   *l;

	/* Convert to C string (handles possible detoasting). */
	/* Note we rely on being able to modify rawname below. */
	rawname = text_to_cstring(textval);

	if (!SplitIdentifierString(rawname, ',', &namelist) || namelist == NIL) {
        return result;
    }

	foreach(l, namelist)
	{
		char	   *curname = (char *) lfirst(l);

		result = lappend(result, makeString(pstrdup(curname)));
	}

	pfree(rawname);
	list_free(namelist);

	return result;
}

char *
check_order_by_column(char *relname, List *order_columns) {
    ListCell *l;
    char *column_name = NULL;

    if (relname && order_columns) {
        foreach(l, order_columns) {
            char *val = strVal(lfirst(l));
            char *equals;
            if (val && (equals = strchr(val, '=')) && (strncmp(relname, val, equals - val) == 0)) {
                column_name = pstrdup(equals + 1);
                break;
            }
        }
    }

    return column_name;
}
