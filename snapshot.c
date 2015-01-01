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

#define INIT_JSON_SCHEMA_LENGTH 16384
#define MAX_JSON_SCHEMA_LENGTH 1048576

PG_FUNCTION_INFO_V1(samza_table_schema);

Datum samza_table_schema(PG_FUNCTION_ARGS) {
    /* Open the relation with the given name */
    char *relname = NameStr(*PG_GETARG_NAME(0));
    List *relname_list = stringToQualifiedNameList(relname);
    RangeVar *relvar = makeRangeVarFromNameList(relname_list);

    Relation rel = relation_openrv(relvar, AccessShareLock);
    avro_schema_t schema = relation_to_avro_schema(rel);
    relation_close(rel, AccessShareLock);

    /* Try to convert the schema to JSON in a fixed-length buffer. If it doesn't fit,
     * double the buffer size and try again. */
    text *retval;
    int retval_size = INIT_JSON_SCHEMA_LENGTH, err = ENOSPC;

    while (err == ENOSPC && retval_size <= MAX_JSON_SCHEMA_LENGTH) {
        retval = (text *) palloc(retval_size);
        avro_writer_t writer = avro_writer_memory(VARDATA(retval), retval_size - VARHDRSZ);
        err = avro_schema_to_json(schema, writer);

        if (err == 0) {
            SET_VARSIZE(retval, avro_writer_tell(writer) + VARHDRSZ);
        } else if (err == ENOSPC) {
            retval_size *= 2;
            pfree(retval);
        }
        avro_writer_free(writer);
    }

    avro_schema_decref(schema);

    if (err) {
        elog(ERROR, "samza_table_schema: Could not encode schema as JSON: %s", avro_strerror());
        PG_RETURN_NULL();
    } else {
        PG_RETURN_TEXT_P(retval);
    }
}


/* State that we need to remember between calls of samza_table_export */
typedef struct {
    Portal cursor;
} export_state;

PG_FUNCTION_INFO_V1(samza_table_export);

Datum samza_table_export(PG_FUNCTION_ARGS) {
    FuncCallContext *funcctx;
    export_state *state;
    int ret;

    if (SRF_IS_FIRSTCALL()) {
        funcctx = SRF_FIRSTCALL_INIT();
        MemoryContext oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        state = (export_state *) palloc(sizeof(export_state));
        funcctx->user_fctx = state;

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

        state->cursor = SPI_cursor_open(NULL, plan, NULL, NULL, true);
        pfree(query);

        MemoryContextSwitchTo(oldcontext);
    }

    /* On every call of the function, fetch one row from the cursor and process it */
    funcctx = SRF_PERCALL_SETUP();
    state = (export_state *) funcctx->user_fctx;

    SPI_cursor_fetch(state->cursor, true, 1);

    if (SPI_processed > 0) {
        HeapTuple row = SPI_tuptable->vals[0];

        bool null1, null2, null3, null4;
        Datum val1 = heap_getattr(row, 1, SPI_tuptable->tupdesc, &null1);
        Datum val2 = heap_getattr(row, 2, SPI_tuptable->tupdesc, &null2);
        Datum val3 = heap_getattr(row, 3, SPI_tuptable->tupdesc, &null3);
        Datum val4 = heap_getattr(row, 4, SPI_tuptable->tupdesc, &null4);

        char buf[100];
        sprintf(buf, "%d, %d, %d, %d", DatumGetInt32(val1), DatumGetInt32(val2), DatumGetInt32(val3), DatumGetInt32(val4));

        int retval_size = VARHDRSZ + strlen(buf);
        text *retval = (text *) palloc(retval_size);

        SET_VARSIZE(retval, retval_size);
        memcpy(VARDATA(retval), buf, strlen(buf));

        SPI_freetuptable(SPI_tuptable);
        SRF_RETURN_NEXT(funcctx, PointerGetDatum(retval));

    } else {
        SPI_freetuptable(SPI_tuptable);
        SPI_cursor_close(state->cursor);
        SPI_finish();
        pfree(state);
        SRF_RETURN_DONE(funcctx);
    }
}
