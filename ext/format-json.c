#include "logdecoder.h"
#include "format-json.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "utils/json.h"
#include "utils/lsyscache.h"

#include "io_util.h"
#include "protocol_server.h"

static void output_json_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_json_shutdown(LogicalDecodingContext *ctx);
static void output_json_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_json_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_json_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);


void output_format_json_init(OutputPluginCallbacks *cb) {
    elog(DEBUG1, "bottledwater: output_format_json_init");
    cb->startup_cb = output_json_startup;
    cb->begin_cb = output_json_begin_txn;
    cb->change_cb = output_json_change;
    cb->commit_cb = output_json_commit_txn;
    cb->shutdown_cb = output_json_shutdown;
}

static void output_json_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
}

static void output_json_shutdown(LogicalDecodingContext *ctx) {
}

static void output_json_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "{ \"command\": \"BEGIN\", \"xid\": %u }", txn->xid);
    OutputPluginWrite(ctx, true);
}

static void output_json_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "{ \"command\": \"COMMIT\", \"xid\": %u }", txn->xid);
    OutputPluginWrite(ctx, true);
}

static void output_json_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        Relation rel, ReorderBufferChange *change) {
    HeapTuple oldtuple = NULL, newtuple = NULL;
    Datum olddatum = NULL, newdatum = NULL, oldjson = NULL, newjson = NULL;
    text *oldtext = NULL, *newtext = NULL;
    const char *command = NULL;

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_json_change: insert action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;

            command = "INSERT";
            break;

        case REORDER_BUFFER_CHANGE_UPDATE:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_json_change: update action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;

            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            command = "UPDATE";
            break;

        case REORDER_BUFFER_CHANGE_DELETE:
            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            command = "DELETE";
            break;

        default:
            elog(ERROR, "output_json_change: unknown change action %d", change->action);
    }

    if (newtuple) {
        newdatum = heap_copy_tuple_as_datum(newtuple, RelationGetDescr(rel));
        newjson = DirectFunctionCall1(row_to_json, newdatum);
        newtext = DatumGetTextP(newjson);
    }
    if (oldtuple) {
        olddatum = heap_copy_tuple_as_datum(oldtuple, RelationGetDescr(rel));
        oldjson = DirectFunctionCall1(row_to_json, olddatum);
        oldtext = DatumGetTextP(oldjson);
    }

    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "{ \"xid\": %u, \"wal_pos\": \"%X/%X\"",
                     txn->xid, (uint32) (change->lsn >> 32), (uint32) change->lsn);
    appendStringInfo(ctx->out, ", \"command\": \"%s\"", command);
    appendStringInfo(ctx->out, ", \"relname\": \"%s\"", RelationGetRelationName(rel));
    appendStringInfo(ctx->out, ", \"relnamespace\": \"%s\"", get_namespace_name(RelationGetNamespace(rel)));
    if (newtuple) {
        appendStringInfoString(ctx->out, ", \"newtuple\": ");
        appendBinaryStringInfo(ctx->out, VARDATA_ANY(newtext), VARSIZE_ANY_EXHDR(newtext));
    }
    if (oldtuple) {
        appendStringInfoString(ctx->out, ", \"oldtuple\": ");
        appendBinaryStringInfo(ctx->out, VARDATA_ANY(oldtext), VARSIZE_ANY_EXHDR(oldtext));
    }
    appendStringInfoString(ctx->out, " }");
    OutputPluginWrite(ctx, true);

    if (oldtuple) {
        pfree(DatumGetPointer(oldjson));
        pfree(DatumGetPointer(olddatum));
    }
    if (newtuple) {
        pfree(DatumGetPointer(newjson));
        pfree(DatumGetPointer(newdatum));
    }
}
