#include "logdecoder.h"
#include "format-avro.h"
#include "format-json.h"
#include "nodes/parsenodes.h"
#include "utils/elog.h"

/* Entry point when Postgres loads the plugin */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void output_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void output_shutdown(LogicalDecodingContext *ctx);
static void output_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void output_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void output_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel, ReorderBufferChange *change);


void _PG_init() {
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb) {
    elog(DEBUG1, "bottledwater: _PG_output_plugin_init");
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);
    cb->startup_cb = output_startup;
    cb->begin_cb = output_begin_txn;
    cb->change_cb = output_change;
    cb->commit_cb = output_commit_txn;
    cb->shutdown_cb = output_shutdown;
}

static void output_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
        bool is_init) {
    plugin_state *state;
    void (*format_init_func)(OutputPluginCallbacks *) = NULL;

    elog(DEBUG1, "bottledwater: output_startup: is_init=%s", is_init ? "true" : "false");
    if (is_init) {
        return;
    }
    state = palloc0(sizeof(plugin_state));
    ctx->output_plugin_private = state;

    state->memctx = AllocSetContextCreate(ctx->context, "Bottledwater decoder context",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    if (ctx->output_plugin_options) {
        ListCell *o;

        foreach(o, ctx->output_plugin_options) {
          DefElem *e;

          e = (DefElem *) lfirst(o);
          if (strcasecmp(e->defname, "FORMAT") == 0) {
              char *str;

              if (e->arg) {
                str = ((Value *) e->arg)->val.str;
              } else {
                  ereport(ERROR,
                          (errcode(ERRCODE_UNDEFINED_PARAMETER),
                           errmsg("FORMAT option requires an argument")));
              }

              if (format_init_func) {
                  ereport(ERROR,
                          (errcode(ERRCODE_AMBIGUOUS_PARAMETER), // TODO: a better one?
                           errmsg("multiple FORMAT options specified for output plugin")));
              }

              if (strcasecmp(str, "AVRO") == 0) {
                  format_init_func = output_format_avro_init;
              } else if (strcasecmp(str, "JSON") == 0) {
                  format_init_func = output_format_json_init;
              } else {
                  ereport(ERROR,
                          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("unsupported FORMAT option: %s", str)));
              }
          }
        }
    }

    // use default format, if not created earlier
    if (!format_init_func) {
        format_init_func = output_format_avro_init;
    }
    state->format_cb = palloc0(sizeof(OutputPluginCallbacks));
    format_init_func(state->format_cb);

    state->format_cb->startup_cb(ctx, opt, is_init);
}

static void output_shutdown(LogicalDecodingContext *ctx) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx;

    /* state can be NULL if we are in CreateReplicationSlot */
    if (state) {
        oldctx = MemoryContextSwitchTo(state->memctx);

        state->format_cb->shutdown_cb(ctx);

        MemoryContextSwitchTo(oldctx);
        MemoryContextDelete(state->memctx);
    }
}

static void output_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    state->format_cb->begin_cb(ctx, txn);

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}

static void output_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    state->format_cb->commit_cb(ctx, txn, commit_lsn);

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}

static void output_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
        Relation rel, ReorderBufferChange *change) {
    plugin_state *state = ctx->output_plugin_private;
    MemoryContext oldctx = MemoryContextSwitchTo(state->memctx);

    state->format_cb->change_cb(ctx, txn, rel, change);

    MemoryContextSwitchTo(oldctx);
    MemoryContextReset(state->memctx);
}
