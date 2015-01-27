#include "io_util.h"
#include "protocol_server.h"
#include "oid2avro.h"

#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "utils/memutils.h"

/* Entry point when Postgres loads the plugin */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void output_avro_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_avro_shutdown(LogicalDecodingContext *ctx);
static void output_avro_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_avro_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_avro_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);

typedef struct {
    MemoryContext memctx; /* reset after every change event, to prevent leaks */
    avro_schema_t frame_schema;
    avro_value_iface_t *frame_iface;
    avro_value_t frame_value;
} plugin_state;

void _PG_init() {
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);
    cb->startup_cb = output_avro_startup;
    cb->begin_cb = output_avro_begin_txn;
    cb->change_cb = output_avro_change;
    cb->commit_cb = output_avro_commit_txn;
    cb->shutdown_cb = output_avro_shutdown;
}

static void output_avro_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

    plugin_state *state = palloc(sizeof(plugin_state));
    ctx->output_plugin_private = state;

    state->memctx = AllocSetContextCreate(ctx->context, "Avro decoder context",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    state->frame_schema = schema_for_frame();
    state->frame_iface = avro_generic_class_from_schema(state->frame_schema);
    avro_generic_value_new(state->frame_iface, &state->frame_value);
}

static void output_avro_shutdown(LogicalDecodingContext *ctx) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContextDelete(state->memctx);

    avro_value_decref(&state->frame_value);
    avro_value_iface_decref(state->frame_iface);
    avro_schema_decref(state->frame_schema);
}

static void output_avro_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);
    bytea *output = NULL;

    int err = update_frame_with_begin_txn(&state->frame_value, txn);
    if (err) {
        elog(ERROR, "output_avro_begin_txn: Avro conversion failed: %s", avro_strerror());
    } else {
        err = try_writing(&output, &write_avro_binary, &state->frame_value);
        if (err) {
            elog(ERROR, "output_avro_begin_txn: writing Avro binary failed: %s", avro_strerror());
        }
    }

    if (!err) {
        OutputPluginPrepareWrite(ctx, true);
        appendBinaryStringInfo(ctx->out, VARDATA(output), VARSIZE(output) - VARHDRSZ);
        OutputPluginWrite(ctx, true);
    }

    if (output) pfree(output);
    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}

static void output_avro_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);
    bytea *output = NULL;

    int err = update_frame_with_commit_txn(&state->frame_value, txn, commit_lsn);
    if (err) {
        elog(ERROR, "output_avro_commit_txn: Avro conversion failed: %s", avro_strerror());
    } else {
        err = try_writing(&output, &write_avro_binary, &state->frame_value);
        if (err) {
            elog(ERROR, "output_avro_commit_txn: writing Avro binary failed: %s", avro_strerror());
        }
    }

    if (!err) {
        OutputPluginPrepareWrite(ctx, true);
        appendBinaryStringInfo(ctx->out, VARDATA(output), VARSIZE(output) - VARHDRSZ);
        OutputPluginWrite(ctx, true);
    }

    if (output) pfree(output);
    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}

static void output_avro_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        Relation rel, ReorderBufferChange *change) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    // ...

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}
