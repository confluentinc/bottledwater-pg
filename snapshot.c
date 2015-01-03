#include "oid2avro.h"

#include <string.h>
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

#define INIT_BUFFER_LENGTH 16384
#define MAX_BUFFER_LENGTH 1048576

/* Function that writes something using the Avro writer that is passed to it. Must return 0
 * on success, ENOSPC if the buffer was too small (the operation will be retried), and any
 * other value to indicate any other error (the operation will not be retried). */
typedef int (*try_writing_cb)(avro_writer_t, void *);

int try_writing(bytea **output, try_writing_cb cb, void *context);
avro_schema_t schema_for_relname(char *relname);
int write_schema_json(avro_writer_t writer, void *context);
int write_avro_binary(avro_writer_t writer, void *context);


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

int write_schema_json(avro_writer_t writer, void *context) {
    return avro_schema_to_json((avro_schema_t) context, writer);
}


/* State that we need to remember between calls of samza_table_export */
typedef struct {
    Portal cursor;
    avro_schema_t schema;
    avro_value_iface_t *avro_iface;
    avro_value_t avro_value;
} export_state;


PG_FUNCTION_INFO_V1(samza_table_export);

/* Given the name of a table, returns a set of byte array values containing the table contents
 * encoded as Avro (one byte array per row of the table). */
Datum samza_table_export(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    export_state *state;
    int ret;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* Construct the query to execute. TODO needs quoting? Use quote_qualified_identifier
         * (defined in src/backend/utils/adt/ruleutils.c). */
        char *relname = NameStr(*PG_GETARG_NAME(0));
        char *prefix = "SELECT xmin, xmax, * FROM ";
        int relname_len = strlen(relname), prefix_len = strlen(prefix);

        char *query = (char *) palloc(prefix_len + relname_len + 1); /* +1 for final null byte */
        memcpy(query, prefix, prefix_len);
        memcpy(query + prefix_len, relname, relname_len + 1);

        /* Submit the query to the database using the SPI interface */
        if ((ret = SPI_connect()) < 0) {
            elog(ERROR, "samza_table_export: SPI_connect returned %d", ret);
        }

        SPIPlanPtr plan = SPI_prepare_cursor(query, 0, NULL, CURSOR_OPT_NO_SCROLL);
        if (!plan) {
            elog(ERROR, "samza_table_export: SPI_prepare_cursor failed with error %d", SPI_result);
        }

        /* Things that we need for the duration of the table scan */
        state = (export_state *) palloc(sizeof(export_state));
        state->cursor = SPI_cursor_open(NULL, plan, NULL, NULL, true);
        state->schema = schema_for_relname(relname);
        state->avro_iface = avro_generic_class_from_schema(state->schema);
        avro_generic_value_new(state->avro_iface, &state->avro_value);
        funcctx->user_fctx = state;

        pfree(query);
        MemoryContextSwitchTo(oldcontext);
    }

    /* On every call of the function, fetch one row from the cursor and process it */
    funcctx = SRF_PERCALL_SETUP();
    state = (export_state *) funcctx->user_fctx;

    SPI_cursor_fetch(state->cursor, true, 1);

    if (SPI_processed > 0) {
        HeapTuple row = SPI_tuptable->vals[0];
        bytea *output;

        ret = update_avro_with_tuple(&state->avro_value, state->schema, SPI_tuptable->tupdesc, row);
        if (ret) {
            elog(ERROR, "samza_table_export: Avro conversion failed: %s", avro_strerror());
            PG_RETURN_NULL();
        }

        ret = try_writing(&output, &write_avro_binary, &state->avro_value);
        if (ret) {
            elog(ERROR, "samza_table_export: writing Avro binary failed: %s", avro_strerror());
            PG_RETURN_NULL();
        }

        SPI_freetuptable(SPI_tuptable);
        SRF_RETURN_NEXT(funcctx, PointerGetDatum(output));

    } else {
        avro_value_decref(&state->avro_value);
        avro_value_iface_decref(state->avro_iface);
        avro_schema_decref(state->schema);
        SPI_freetuptable(SPI_tuptable);
        SPI_cursor_close(state->cursor);
        SPI_finish();
        pfree(state);
        SRF_RETURN_DONE(funcctx);
    }
}

int write_avro_binary(avro_writer_t writer, void *context) {
    return avro_value_write(writer, (avro_value_t *) context);
}


/* Given the name of a table (relation), generates an Avro schema for it. */
avro_schema_t schema_for_relname(char *relname) {
    List *relname_list = stringToQualifiedNameList(relname);
    RangeVar *relvar = makeRangeVarFromNameList(relname_list);
    Relation rel = relation_openrv(relvar, AccessShareLock);
    avro_schema_t schema = schema_for_relation(rel);
    relation_close(rel, AccessShareLock);
    return schema;
}

/* Allocates a fixed-length buffer and tries to write something to it using the Avro writer API.
 * If it doesn't fit, increases the buffer size and tries again. The actual writing operation
 * is given as a callback; the context argument is passed to the callback. On success (return
 * value 0), output is set to a palloc'ed byte array of the right size. */
int try_writing(bytea **output, try_writing_cb cb, void *context) {
    int size = INIT_BUFFER_LENGTH, err = ENOSPC;

    while (err == ENOSPC && size <= MAX_BUFFER_LENGTH) {
        *output = (bytea *) palloc(size);
        avro_writer_t writer = avro_writer_memory(VARDATA(*output), size - VARHDRSZ);
        err = (*cb)(writer, context);

        if (err == 0) {
            SET_VARSIZE(*output, avro_writer_tell(writer) + VARHDRSZ);
        } else if (err == ENOSPC) {
            size *= 4;
            pfree(*output);
        }
        avro_writer_free(writer);
    }

    return err;
}
