#include "logdecoder.h"
#include "format-avro.h"
#include "io_util.h"
#include "protocol_server.h"
#include "oid2avro.h"

static void output_avro_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_avro_shutdown(LogicalDecodingContext *ctx);
static void output_avro_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_avro_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_avro_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);

typedef struct {
    avro_schema_t frame_schema;
    avro_value_iface_t *frame_iface;
    avro_value_t frame_value;
    schema_cache_t schema_cache;
} plugin_state_avro;

void reset_frame(plugin_state_avro *state);
int write_frame(LogicalDecodingContext *ctx, plugin_state_avro *state);


void output_format_avro_init(OutputPluginCallbacks *cb) {
    elog(DEBUG1, "bottledwater: output_format_avro_init");
    cb->startup_cb = output_avro_startup;
    cb->begin_cb = output_avro_begin_txn;
    cb->change_cb = output_avro_change;
    cb->commit_cb = output_avro_commit_txn;
    cb->shutdown_cb = output_avro_shutdown;
}

static void output_avro_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    plugin_state_avro *state = palloc(sizeof(plugin_state_avro));
    private_state(ctx) = state;
    opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

    state->frame_schema = schema_for_frame();
    state->frame_iface = avro_generic_class_from_schema(state->frame_schema);
    avro_generic_value_new(state->frame_iface, &state->frame_value);
    state->schema_cache = schema_cache_new(ctx->context);
}

static void output_avro_shutdown(LogicalDecodingContext *ctx) {
    plugin_state_avro *state = private_state(ctx);

    schema_cache_free(state->schema_cache);
    avro_value_decref(&state->frame_value);
    avro_value_iface_decref(state->frame_iface);
    avro_schema_decref(state->frame_schema);
}

static void output_avro_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    plugin_state_avro *state = private_state(ctx);
    reset_frame(state);

    if (update_frame_with_begin_txn(&state->frame_value, txn)) {
        elog(ERROR, "output_avro_begin_txn: Avro conversion failed: %s", avro_strerror());
    }
    if (write_frame(ctx, state)) {
        elog(ERROR, "output_avro_begin_txn: writing Avro binary failed: %s", avro_strerror());
    }
}

static void output_avro_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    plugin_state_avro *state = private_state(ctx);
    reset_frame(state);

    if (update_frame_with_commit_txn(&state->frame_value, txn, commit_lsn)) {
        elog(ERROR, "output_avro_commit_txn: Avro conversion failed: %s", avro_strerror());
    }
    if (write_frame(ctx, state)) {
        elog(ERROR, "output_avro_commit_txn: writing Avro binary failed: %s", avro_strerror());
    }
}

static void output_avro_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        Relation rel, ReorderBufferChange *change) {
    int err = 0;
    HeapTuple oldtuple = NULL, newtuple = NULL;
    plugin_state_avro *state = private_state(ctx);
    reset_frame(state);

    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_avro_change: insert action without a tuple");
            }
            newtuple = &change->data.tp.newtuple->tuple;
            err = update_frame_with_insert(&state->frame_value, state->schema_cache, rel,
                    RelationGetDescr(rel), newtuple);
            break;

        case REORDER_BUFFER_CHANGE_UPDATE:
            if (!change->data.tp.newtuple) {
                elog(ERROR, "output_avro_change: update action without a tuple");
            }
            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            newtuple = &change->data.tp.newtuple->tuple;
            err = update_frame_with_update(&state->frame_value, state->schema_cache, rel, oldtuple, newtuple);
            break;

        case REORDER_BUFFER_CHANGE_DELETE:
            if (change->data.tp.oldtuple) {
                oldtuple = &change->data.tp.oldtuple->tuple;
            }
            err = update_frame_with_delete(&state->frame_value, state->schema_cache, rel, oldtuple);
            break;

        default:
            elog(ERROR, "output_avro_change: unknown change action %d", change->action);
    }

    if (err) {
        elog(ERROR, "output_avro_change: row conversion failed: %s", avro_strerror());
    }
    if (write_frame(ctx, state)) {
        elog(ERROR, "output_avro_change: writing Avro binary failed: %s", avro_strerror());
    }
}

void reset_frame(plugin_state_avro *state) {
    if (avro_value_reset(&state->frame_value)) {
        elog(ERROR, "Avro value reset failed: %s", avro_strerror());
    }
}

int write_frame(LogicalDecodingContext *ctx, plugin_state_avro *state) {
    int err = 0;
    bytea *output = NULL;

    check(err, try_writing(&output, &write_avro_binary, &state->frame_value));

    OutputPluginPrepareWrite(ctx, true);
    appendBinaryStringInfo(ctx->out, VARDATA(output), VARSIZE(output) - VARHDRSZ);
    OutputPluginWrite(ctx, true);

    pfree(output);
    return err;
}
