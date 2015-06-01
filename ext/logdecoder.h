#ifndef LOGDECODER_H
#define LOGDECODER_H

#include "postgres.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "utils/memutils.h"

typedef struct {
    MemoryContext memctx; /* reset after every change event, to prevent leaks */
    OutputPluginCallbacks *format_cb;
    void *format_state;
} plugin_state;

#define private_state(ctx) (((plugin_state *) ctx->output_plugin_private)->format_state)

#endif /* LOGDECODER_H */
