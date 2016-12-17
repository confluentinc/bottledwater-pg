#include "connect.h"
#include "json.h"
#include "logger.h"
#include "registry.h"
#include "ini.h"
#include "oid2avro.h"

#include <librdkafka/rdkafka.h>
#include <assert.h>
#include <getopt.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

//#define DEBUG 1

#define DEFAULT_REPLICATION_SLOT "bottledwater"
#define APP_NAME "bottledwater"

/* The name of the logical decoding output plugin with which the replication
 * slot is created. This must match the name of the Postgres extension. */
#define OUTPUT_PLUGIN "bottledwater"

#define DEFAULT_BROKER_LIST "localhost:9092"
#define DEFAULT_SCHEMA_REGISTRY "http://localhost:8081"

#define DEFAULT_SCHEMA_PATTERN "%%"
#define DEFAULT_TABLE_PATTERN "%%"

#define TABLE_NAME_BUFFER_LENGTH 128

#define check(err, call) { err = call; if (err) return err; }

#define ensure(context, call) { \
    if (call) { \
        fatal_error((context), "%s", (context)->client->error); \
    } \
}

#define fatal_error(context, fmt, ...) { \
    log_fatal(fmt, ##__VA_ARGS__); \
    exit_nicely((context), 1); \
}
#define vfatal_error(context, fmt, args) { \
    vlog_fatal(fmt, args); \
    exit_nicely((context), 1); \
}

#define config_error(fmt, ...) fprintf(stderr, fmt "\n", ##__VA_ARGS__)


#define PRODUCER_CONTEXT_ERROR_LEN 512
#define MAX_IN_FLIGHT_TRANSACTIONS 1000
/* leave room for one extra empty element so the circular buffer can
 * distinguish between empty and full */
#define XACT_LIST_LEN (MAX_IN_FLIGHT_TRANSACTIONS + 1)


typedef enum {
    OUTPUT_FORMAT_UNDEFINED = 0,
    OUTPUT_FORMAT_AVRO,
    OUTPUT_FORMAT_JSON
} format_t;

static const char* DEFAULT_OUTPUT_FORMAT_NAME = "avro";
static const format_t DEFAULT_OUTPUT_FORMAT = OUTPUT_FORMAT_AVRO;


typedef enum {
    ERROR_POLICY_UNDEFINED = 0,
    ERROR_POLICY_LOG,
    ERROR_POLICY_EXIT
} error_policy_t;

static const char* DEFAULT_ERROR_POLICY_NAME = PROTOCOL_ERROR_POLICY_EXIT;
static const error_policy_t DEFAULT_ERROR_POLICY = ERROR_POLICY_EXIT;


typedef struct {
    uint32_t xid;         /* Postgres transaction identifier */
    int recvd_events;     /* Number of row-level events received so far for this transaction */
    int pending_events;   /* Number of row-level events waiting to be acknowledged by Kafka */
    uint64_t commit_lsn;  /* WAL position of the transaction's commit event */
} transaction_info;

typedef struct {
    client_context_t client;            /* The connection to Postgres */
    schema_registry_t registry;         /* Submits Avro schemas to schema registry */
    char *brokers;                      /* Comma-separated list of host:port for Kafka brokers */
    transaction_info xact_list[XACT_LIST_LEN]; /* Circular buffer */
    int xact_head;                      /* Index into xact_list currently being received from PG */
    int xact_tail;                      /* Oldest index in xact_list not yet acknowledged by Kafka */
    rd_kafka_conf_t *kafka_conf;
    rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_t *kafka;
    table_mapper_t mapper;              /* Remembers topics and schemas for tables we've seen */
    format_t output_format;             /* How to encode messages for writing to Kafka */
    char *topic_prefix;                 /* String to be prepended to all topic names */
    error_policy_t error_policy;        /* What to do in case of a transient error */
    char error[PRODUCER_CONTEXT_ERROR_LEN];
    char *key;                          /* Key to use as Kafka key*/
} producer_context;

typedef producer_context *producer_context_t;

static inline int xact_list_length(producer_context_t context) {
    return (XACT_LIST_LEN + /* normalise negative length in case of wraparound */
            context->xact_head + 1 - context->xact_tail)
        % XACT_LIST_LEN;
}

static inline bool xact_list_full(producer_context_t context) {
    return xact_list_length(context) == XACT_LIST_LEN - 1;
}

static inline bool xact_list_empty(producer_context_t context) {
    return xact_list_length(context) == 0;
}


typedef struct {
    producer_context_t context;
    uint64_t wal_pos;
    Oid relid;
    transaction_info *xact;
    avro_value_t *key_val;// primary key/replica identity encoded in avro_value_t use for partitioner if any
    // avro_value_t *new_val;
} msg_envelope;

typedef msg_envelope *msg_envelope_t;

static char *progname;
static int received_shutdown_signal = 0;
static int unfinished_snapshot = 1;

void usage(void);
void parse_options(producer_context_t context, int argc, char **argv);
char *parse_config_option(char *option);
void init_schema_registry(producer_context_t context, const char *url);
const char* output_format_name(format_t format);
void set_output_format(producer_context_t context, const char *format);
void set_error_policy(producer_context_t context, const char *policy);
const char* error_policy_name(error_policy_t format);
void set_kafka_config(producer_context_t context, const char *property, const char *value);
void set_topic_config(producer_context_t context, const char *property, const char *value);
char* topic_name_from_avro_schema(avro_schema_t schema);

static int handle_error(producer_context_t context, int err, const char *fmt, ...) __attribute__ ((format (printf, 3, 4)));

static int on_begin_txn(void *_context, uint64_t wal_pos, uint32_t xid);
static int on_commit_txn(void *_context, uint64_t wal_pos, uint32_t xid);
static int on_table_schema(void *_context, uint64_t wal_pos, Oid relid,
        const char *key_schema_json, size_t key_schema_len, avro_schema_t key_schema,
        const char *row_schema_json, size_t row_schema_len, avro_schema_t row_schema);
static int on_insert_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val);
static int on_update_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val);
static int on_delete_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val);
static int on_keepalive(void *_context, uint64_t wal_pos);
static int on_client_error(void *_context, int err, const char *message);
int send_kafka_msg(producer_context_t context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len,
        const void *val_bin, size_t val_len,
        avro_value_t *key_val); // add new key_val, this is encoded primary key
static void on_deliver_msg(rd_kafka_t *kafka, const rd_kafka_message_t *msg, void *envelope);
static int32_t on_customized_paritioner_cb(const rd_kafka_topic_t *rkt, const void *keydata, size_t keylen,
                                        int32_t partition_cnt, void *rkt_opaque, void *msg_opaque);
void maybe_checkpoint(producer_context_t context);
void backpressure(producer_context_t context);
client_context_t init_client(void);
producer_context_t init_producer(client_context_t client);
void start_producer(producer_context_t context);
void exit_nicely(producer_context_t context, int status);


void usage() {
    fprintf(stderr,
            "Exports a snapshot of a PostgreSQL database, followed by a stream of changes,\n"
            "and sends the data to a Kafka cluster.\n\n"
            "Usage:\n  %s [OPTION]...\n\nOptions:\n"
            "  -d, --postgres=postgres://user:pass@host:port/dbname   (required)\n"
            "                          Connection string or URI of the PostgreSQL server.\n"
            "  -s, --slot=slotname     Name of replication slot   (default: %s)\n"
            "                          The slot is automatically created on first use.\n"
            "  -b, --broker=host1[:port1],host2[:port2]...   (default: %s)\n"
            "                          Comma-separated list of Kafka broker hosts/ports.\n"
            "  -r, --schema-registry=http://hostname:port   (default: %s)\n"
            "                          URL of the service where Avro schemas are registered.\n"
            "                          Used only for --output-format=avro.\n"
            "                          Omit when --output-format=json.\n"
            "  -f, --output-format=[avro|json]   (default: %s)\n"
            "                          How to encode the messages for writing to Kafka.\n"
            "  -u, --allow-unkeyed     Allow export of tables that don't have a primary key.\n"
            "                          This is disallowed by default, because updates and\n"
            "                          deletes need a primary key to identify their row.\n"
            "  -p, --topic-prefix=prefix\n"
            "                          String to prepend to all topic names.\n"
            "                          e.g. with --topic-prefix=postgres, updates from table\n"
            "                          'users' will be written to topic 'postgres.users'.\n"
            "  -e, --on-error=[log|exit]   (default: %s)\n"
            "                          What to do in case of a transient error, such as\n"
            "                          failure to publish to Kafka.\n"
            "  -o, --schemas=schema1|schema2  (default: all schemas)\n"
            "                          Pattern specifying which schemas to stream.  If this\n"
            "                          is not specified, all schemas\n"
            "                          will be selected.  The pattern syntax is as per the\n"
            "                          SQL `SIMILAR TO` operator: see\n"
            "         https://www.postgresql.org/docs/current/static/functions-matching.html\n"
            "  -i, --tables=table1|table2   (default: all tables)\n"
            "                          Pattern specifying which tables to stream.  If this\n"
            "                          is not specified, all tables in the selected schemas\n"
            "                          will be streamed.  The pattern syntax is as per the\n"
            "                          SQL `SIMILAR TO` operator: see\n"
            "         https://www.postgresql.org/docs/current/static/functions-matching.html\n"
            "  -x, --skip-snapshot     Skip taking a consistent snapshot of the existing\n"
            "                          database contents and just start streaming any new\n"
            "                          updates.  (Ignored if the replication slot already\n"
            "                          exists.)\n"
            "  -C, --kafka-config property=value\n"
            "                          Set global configuration property for Kafka producer\n"
            "                          (see --config-help for list of properties).\n"
            "  -T, --topic-config property=value\n"
            "                          Set topic configuration property for Kafka producer.\n"
            "  -k, --key=value\n"
            "                          Field for using as key to send to Kafka, if not exists\n"
            "                          then use PRIMARY KEY or REPLICA IDENTITY\n"
            "  -b, --order-by=table1=column,table2=column\n"
            "                          This option is used for config snapshot these specified tables\n"
            "                          order by specified column\n"
            "  -g, --config-file=value\n"
            "                          Instead of passing config by command line,\n"
            "                          you can use config-file to config bottledwater\n"
            "                          if you use config-file, others option will has no effect\n"
            "  --config-help           Print the list of configuration properties. See also:\n"
            "            https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md\n"
            "  -h, --help\n"
            "                          Print this help text.\n",

            progname,
            DEFAULT_REPLICATION_SLOT,
            DEFAULT_BROKER_LIST,
            DEFAULT_SCHEMA_REGISTRY,
            DEFAULT_OUTPUT_FORMAT_NAME,
            DEFAULT_ERROR_POLICY_NAME);
    exit(1);
}

static int handler(void* _context, const char* section,
          const char* name, const char* value) {

    producer_context_t context = (producer_context_t) _context;
    char *tmp_config_name; // temporary variable for store kafka config and kafka topic config name
    char *tmp_config_value; // temporary variable for store kafka config and kafka topic config value

    #define MATCH(s, n) strcmp(section, s) == 0 && strcmp(name, n) == 0
    if (MATCH("kafka", "kafka-config")) {
        tmp_config_name = strdup(value);
        tmp_config_value = parse_config_option(tmp_config_name);
        set_kafka_config(context, tmp_config_name, tmp_config_value);
        free(tmp_config_name);

    } else if (MATCH("kafka", "topic-config")) {
        tmp_config_name = strdup(value);
        tmp_config_value = parse_config_option(tmp_config_name);
        set_topic_config(context, tmp_config_name, tmp_config_value);
        free(tmp_config_name);

    } else if (MATCH("bottledwater", "postgres")) {
        if (context->client->conninfo) {
            free(context->client->conninfo);
        }
        context->client->conninfo = strdup(value);

    } else if (MATCH("bottledwater", "slot")) {
        if (context->client->repl.slot_name) {
            free(context->client->repl.slot_name);
        }
        context->client->repl.slot_name = strdup(value);

    } else if (MATCH("bottledwater", "broker")) {
        if (context->brokers) {
            free(context->brokers);
        }
        context->brokers = strdup(value);

    } else if (MATCH("schema-registry", "schema-registry")) {
        if (context->registry) {
            schema_registry_free(context->registry);
        }
        init_schema_registry(context, value);

    } else if (MATCH("bottledwater", "output-format")) {
        set_output_format(context, value);

    } else if (MATCH("bottledwater", "allow-unkeyed")) {
        context->client->allow_unkeyed = atoi(value);

    } else if (MATCH("bottledwater", "topic-prefix")) {
        if (context->topic_prefix) {
            free(context->topic_prefix);
        }
        context->topic_prefix = strdup(value);

    } else if (MATCH("bottledwater", "on-error")) {
        set_error_policy(context, value);

    } else if (MATCH("bottledwater", "schemas")) {
        if (context->client->repl.schema_pattern) {
            free(context->client->repl.schema_pattern);
        }
        context->client->repl.schema_pattern = strdup(value);

    } else if (MATCH("bottledwater", "tables")) {
        if (context->client->repl.table_pattern) {
            free(context->client->repl.table_pattern);
        }
        context->client->repl.table_pattern = strdup(value);

    } else if (MATCH("bottledwater", "key")) {
        if (context->key) {
            free(context->key);
        }
        context->key = strdup(value);

        rd_kafka_topic_conf_set_partitioner_cb(context->topic_conf, &on_customized_paritioner_cb);

    } else if (MATCH("bottledwater", "skip-snapshot")) {
        context->client->skip_snapshot = atoi(value);

    } else if (MATCH("bottledwater", "order-by")){
        if (context->client->order_by) {
            free(context->client->order_by);
        }
        context->client->order_by = strdup(value);

    } else {
        config_error("Error while parsing configuration file");
        config_error("Unknown argument: %s", optarg);
        config_error("Please run program with -h --help option for Usage information");
        exit(1); // unknown section/option
    }
    return 1;
}

/* Parse command-line options */
void parse_options(producer_context_t context, int argc, char **argv) {

    static struct option options[] = {
        {"postgres",        required_argument, NULL, 'd'},
        {"slot",            required_argument, NULL, 's'},
        {"broker",          required_argument, NULL, 'b'},
        {"schema-registry", required_argument, NULL, 'r'},
        {"output-format",   required_argument, NULL, 'f'},
        {"allow-unkeyed",   no_argument,       NULL, 'u'},
        {"topic-prefix",    required_argument, NULL, 'p'},
        {"on-error",        required_argument, NULL, 'e'},
        {"schemas",         required_argument, NULL, 'o'},
        {"tables",          required_argument, NULL, 'i'},
        {"skip-snapshot",   no_argument,       NULL, 'x'},
        {"kafka-config",    required_argument, NULL, 'C'},
        {"topic-config",    required_argument, NULL, 'T'},
        {"schemas",         required_argument, NULL, 'o'},
        {"tables",          required_argument, NULL, 'i'},
        {"key",             required_argument, NULL, 'k'},
        {"order-by",        required_argument, NULL, 'a'},
        {"config-file",     required_argument, NULL, 'g'},
        {"config-help",     no_argument,       NULL,  1 },
        {"help",            no_argument,       NULL, 'h'},
        {NULL,              0,                 NULL,  0 }
    };

    progname = argv[0];

    int option_index;
    bool continue_parse_options = true;
    while (continue_parse_options) {
        int c = getopt_long(argc, argv, "d:s:b:r:f:up:e:xC:T:i:o:k:a:g:h", options, &option_index);
        if (c == -1) break;

        switch (c) {
            case 'd':
                context->client->conninfo = strdup(optarg);
                break;
            case 's':
                context->client->repl.slot_name = strdup(optarg);
                break;
            case 'b':
                context->brokers = strdup(optarg);
                break;
            case 'r':
                init_schema_registry(context, optarg);
                break;
            case 'f':
                set_output_format(context, optarg);
                break;
            case 'u':
                context->client->allow_unkeyed = true;
                break;
            case 'p':
                context->topic_prefix = strdup(optarg);
                break;
            case 'e':
                set_error_policy(context, optarg);
                break;
            case 'x':
                context->client->skip_snapshot = true;
                break;
            case 'C':
                set_kafka_config(context, optarg, parse_config_option(optarg));
                break;
            case 'o':
                context->client->repl.schema_pattern = strdup(optarg);
                break;
            case 'T':
                set_topic_config(context, optarg, parse_config_option(optarg));
                break;
            case 'i':
                context->client->repl.table_pattern = strdup(optarg);
                break;
            case 'k':
                context->key = strdup(optarg);
                rd_kafka_topic_conf_set_partitioner_cb(context->topic_conf, &on_customized_paritioner_cb);
                break;
            case 'g':
                if (ini_parse(optarg, handler, context) == 0) {
                    continue_parse_options = false;
                }
                break;
            case 'a':
                context->client->order_by = strdup(optarg);
                break;
            case 1:
                rd_kafka_conf_properties_show(stderr);
                exit(0);
                break;
            case 'h':
            default:
                usage();
        }
    }

    if ((!context->client->conninfo || optind < argc) && continue_parse_options) usage();

    if (context->output_format == OUTPUT_FORMAT_AVRO && !context->registry) {
        init_schema_registry(context, DEFAULT_SCHEMA_REGISTRY);
    } else if (context->output_format == OUTPUT_FORMAT_JSON && context->registry) {
        config_error("Specifying --schema-registry doesn't make sense for "
                     "--output-format=json");
        usage();
    }
}

/* Splits an option string by equals sign. Modifies the option argument to be
 * only the part before the equals sign, and returns a pointer to the part after
 * the equals sign. */
char *parse_config_option(char *option) {
    char *equals = strchr(option, '=');
    if (!equals) {
        log_error("%s: Expected configuration in the form property=value, not \"%s\"",
                  progname, option);
        exit(1);
    }

    // Overwrite equals sign with null, to split key and value into two strings
    *equals = '\0';
    return equals + 1;
}

void init_schema_registry(producer_context_t context, const char *url) {
    context->registry = schema_registry_new(url);

    if (!context->registry) {
        log_error("Failed to initialise schema registry!");
        exit(1);
    }
}

void set_output_format(producer_context_t context, const char *format) {
    if (!strcmp("avro", format)) {
        context->output_format = OUTPUT_FORMAT_AVRO;
    } else if (!strcmp("json", format)) {
        context->output_format = OUTPUT_FORMAT_JSON;
    } else {
        config_error("invalid output format (expected avro or json): %s", format);
        exit(1);
    }
}

const char* output_format_name(format_t format) {
    switch (format) {
    case OUTPUT_FORMAT_AVRO: return "Avro";
    case OUTPUT_FORMAT_JSON: return "JSON";
    case OUTPUT_FORMAT_UNDEFINED: return "undefined (probably a bug)";
    default: return "unknown (probably a bug)";
    }
}

void set_error_policy(producer_context_t context, const char *policy) {
    if (!strcmp(PROTOCOL_ERROR_POLICY_LOG, policy)) {
        context->error_policy = ERROR_POLICY_LOG;
    } else if (!strcmp(PROTOCOL_ERROR_POLICY_EXIT, policy)) {
        context->error_policy = ERROR_POLICY_EXIT;
    } else {
        config_error("invalid error policy (expected log or exit): %s", policy);
        exit(1);
    }

    db_client_set_error_policy(context->client, policy);
}

const char* error_policy_name(error_policy_t policy) {
    switch (policy) {
        case ERROR_POLICY_LOG: return PROTOCOL_ERROR_POLICY_LOG;
        case ERROR_POLICY_EXIT: return PROTOCOL_ERROR_POLICY_EXIT;
        case ERROR_POLICY_UNDEFINED: return "undefined (probably a bug)";
        default: return "unknown (probably a bug)";
    }
}

void set_kafka_config(producer_context_t context, const char *property, const char *value) {
    if (rd_kafka_conf_set(context->kafka_conf, property, value,
                context->error, PRODUCER_CONTEXT_ERROR_LEN) != RD_KAFKA_CONF_OK) {
        config_error("%s: %s", progname, context->error);
        exit(1);
    }
}

void set_topic_config(producer_context_t context, const char *property, const char *value) {
    if (rd_kafka_topic_conf_set(context->topic_conf, property, value,
                context->error, PRODUCER_CONTEXT_ERROR_LEN) != RD_KAFKA_CONF_OK) {
        config_error("%s: %s", progname, context->error);
        exit(1);
    }
}

char* topic_name_from_avro_schema(avro_schema_t schema) {

    const char *table_name = avro_schema_name(schema);

#ifdef AVRO_1_8
    /* Gets the avro schema namespace which contains the Postgres schema name */
    const char *namespace = avro_schema_namespace(schema);
#else
#warning "avro-c older than 1.8.0, will not include Postgres schema in Kafka topic name"
    const char namespace[] = "dummy";
#endif

    char topic_name[TABLE_NAME_BUFFER_LENGTH];
    /* Strips the beginning part of the namespace to extract the Postgres schema name
     * and init topic_name with it */
    int matched = sscanf(namespace, GENERATED_SCHEMA_NAMESPACE ".%s", topic_name);
    /* If the sscanf doesn't find a match with GENERATED_SCHEMA_NAMESPACE,
     * or if the Postgres schema name is 'public', we just init topic_name with the table_name. */
    if (!matched || !strcmp(topic_name, "public")) {
        strncpy(topic_name, table_name, TABLE_NAME_BUFFER_LENGTH);
        topic_name[TABLE_NAME_BUFFER_LENGTH - 1] = '\0';
    /* Otherwise we append to the topic_name previously initialized with the schema_name a "."
     * separator followed by the table_name.                    */
    } else {
        strncat(topic_name, ".", TABLE_NAME_BUFFER_LENGTH - strlen(topic_name) - 1);
        strncat(topic_name, table_name, TABLE_NAME_BUFFER_LENGTH - strlen(topic_name) - 1);
    }

    return strdup(topic_name);
}

static int handle_error(producer_context_t context, int err, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);

    switch (context->error_policy) {
    case ERROR_POLICY_LOG:
        vlog_error(fmt, args);
        err = 0;
        break;
    case ERROR_POLICY_EXIT:
        vfatal_error(context, fmt, args);
    default:
        fatal_error(context, "invalid error policy %s",
                    error_policy_name(context->error_policy));
    }

    va_end(args);

    return err;
}

static int on_begin_txn(void *_context, uint64_t wal_pos, uint32_t xid) {
    producer_context_t context = (producer_context_t) _context;
    replication_stream_t stream = &context->client->repl;

    if (xid == 0) {
        if (!(context->xact_tail == 0 && xact_list_empty(context))) {
            fatal_error(context, "Expected snapshot to be the first transaction.");
        }

        log_info("Created replication slot \"%s\", capturing consistent snapshot \"%s\".",
                 stream->slot_name, stream->snapshot_name);
    }

    // If the circular buffer is full, we have to block and wait for some transactions
    // to be delivered to Kafka and acknowledged for the broker.
    while (xact_list_full(context)) {
#ifdef DEBUG
        log_warn("Too many transactions in flight, applying backpressure");
#endif
        backpressure(context);
    }

    context->xact_head = (context->xact_head + 1) % XACT_LIST_LEN;
    transaction_info *xact = &context->xact_list[context->xact_head];
    xact->xid = xid;
    xact->recvd_events = 0;
    xact->pending_events = 0;
    xact->commit_lsn = 0;

    return 0;
}

static int on_commit_txn(void *_context, uint64_t wal_pos, uint32_t xid) {
    producer_context_t context = (producer_context_t) _context;
    transaction_info *xact = &context->xact_list[context->xact_head];

    if (xid == 0) {
        unfinished_snapshot = 0;
        log_info("Snapshot complete, streaming changes from %X/%X.",
                 (uint32) (wal_pos >> 32), (uint32) wal_pos);
    }

    if (xid != xact->xid) {
        fatal_error(context,
                    "Mismatched begin/commit events (xid %u in flight, xid %u committed)",
                    xact->xid, xid);
    }

    xact->commit_lsn = wal_pos;
    maybe_checkpoint(context);
    return 0;
}


static int on_table_schema(void *_context, uint64_t wal_pos, Oid relid,
        const char *key_schema_json, size_t key_schema_len, avro_schema_t key_schema,
        const char *row_schema_json, size_t row_schema_len, avro_schema_t row_schema) {
    producer_context_t context = (producer_context_t) _context;

    char *topic_name = topic_name_from_avro_schema(row_schema);

    table_metadata_t table = table_mapper_update(context->mapper, relid, topic_name,
            key_schema_json, key_schema_len, row_schema_json, row_schema_len);

    free(topic_name);

    if (!table) {
        log_error("%s", context->mapper->error);
        /*
         * Can't really handle the error since we're in a callback.
         * See comment in body of table_mapper_update() in table_mapper.c for
         * discussion of the implications of an error registering the table.
         */
        return 1;
    }

    return 0;
}


static int on_insert_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val) {
    producer_context_t context = (producer_context_t) _context;
    return send_kafka_msg(context, wal_pos, relid, key_bin, key_len, new_bin, new_len, key_val);
}

static int on_update_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val,
        const void *new_bin, size_t new_len, avro_value_t *new_val) {
    producer_context_t context = (producer_context_t) _context;
    return send_kafka_msg(context, wal_pos, relid, key_bin, key_len, new_bin, new_len, key_val);
}

static int on_delete_row(void *_context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len, avro_value_t *key_val,
        const void *old_bin, size_t old_len, avro_value_t *old_val) {
    producer_context_t context = (producer_context_t) _context;
    if (key_bin)
        return send_kafka_msg(context, wal_pos, relid, key_bin, key_len, NULL, 0, key_val);
    else
        return 0; // delete on unkeyed table --> can't do anything
}

static int on_keepalive(void *_context, uint64_t wal_pos) {
    producer_context_t context = (producer_context_t) _context;

    if (xact_list_empty(context)) {
        return 0;
    } else {
        return FRAME_READER_SYNC_PENDING;
    }
}

static int on_client_error(void *_context, int err, const char *message) {
    producer_context_t context = (producer_context_t) _context;
    return handle_error(context, err, "Client error: %s", message);
}


int send_kafka_msg(producer_context_t context, uint64_t wal_pos, Oid relid,
        const void *key_bin, size_t key_len,
        const void *val_bin, size_t val_len,
        avro_value_t *key_val) {

    table_metadata_t table = table_mapper_lookup(context->mapper, relid);
    if (!table) {
        log_error("relid %d" PRIu32 " has no registered schema", relid);
        return 1;
    }

    transaction_info *xact = &context->xact_list[context->xact_head];
    xact->recvd_events++;
    xact->pending_events++;

    msg_envelope_t envelope = malloc(sizeof(msg_envelope));
    memset(envelope, 0, sizeof(msg_envelope));
    envelope->context = context;
    envelope->wal_pos = wal_pos;
    envelope->relid = relid;
    envelope->xact = xact;
    envelope->key_val = key_val; // this will be used in partitioner call back
    //envelope->new_val = new_val;

    void *key = NULL, *val = NULL;
    size_t key_encoded_len, val_encoded_len;
    int err;

    switch (context->output_format) {
    case OUTPUT_FORMAT_JSON:
        err = json_encode_msg(table,
                key_bin, key_len, (char **) &key, &key_encoded_len,
                val_bin, val_len, (char **) &val, &val_encoded_len);

        if (err) {
            log_error("%s: error %s encoding JSON for topic %s",
                      progname, strerror(err), rd_kafka_topic_name(table->topic));
            return err;
        }
        break;
    case OUTPUT_FORMAT_AVRO:
        err = schema_registry_encode_msg(table->key_schema_id, table->row_schema_id,
                key_bin, key_len, &key, &key_encoded_len,
                val_bin, val_len, &val, &val_encoded_len);

        if (err) {
            log_error("%s: error %s encoding Avro for topic %s",
                      progname, strerror(err), rd_kafka_topic_name(table->topic));
            return err;
        }
        break;
    default:
        fatal_error(context, "invalid output format %s",
                    output_format_name(context->output_format));
    }

    bool enqueued = false;
    while (!enqueued) {
        int err = rd_kafka_produce(table->topic,
                RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_FREE,
                val, val == NULL ? 0 : val_encoded_len,
                key, key == NULL ? 0 : key_encoded_len,
                envelope);
        enqueued = (err == 0);

        // If data from Postgres is coming in faster than we can send it on to Kafka, we
        // create backpressure by blocking until the producer's queue has drained a bit.
        if (rd_kafka_errno2err(errno) == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
#ifdef DEBUG
            log_warn("Kafka producer queue is full, applying backpressure");
#endif
            backpressure(context);

        } else if (err != 0) {
            log_error("%s: Failed to produce to Kafka (topic %s): %s",
                      progname,
                      rd_kafka_topic_name(table->topic),
                      rd_kafka_err2str(rd_kafka_errno2err(errno)));
            if (val != NULL) free(val);
            if (key != NULL) free(key);
            return err;
        }
    }

    if (key)
        free(key);

    return 0;
}

/* Called by Kafka producer once per message before it's sent, to compute which partition
 * the message will go to. This function is a wrapper of rd_kafka_msg_partitioner_consistent.
 * It seems like this will be called before returning from rd_kafka_produce
 * NOTE this is from librdkafka, in the future please check that note in librakafka
 *
 * Produce: creates a new message, runs the partitioner and enqueues
 *          into on the selected partition.
 *
 * Returns 0 on success or -1 on error.
 *
 * If the function returns -1 and RD_KAFKA_MSG_F_FREE was specified, then
 * the memory associated with the payload is still the caller's
 * responsibility.
 */
static int32_t on_customized_paritioner_cb(const rd_kafka_topic_t *rkt, const void *keydata, size_t keylen,
                                        int32_t partition_cnt, void *rkt_opaque, void *msg_opaque) {
    msg_envelope_t envelope = (msg_envelope_t) msg_opaque;
    char *key = envelope->context->mapper->key;
    avro_value_t *key_val = envelope->key_val;
    int err;

    // Only check for keylen because we've already checked that keydata is NULL or not before calling this function
    if (keylen != 0 && key_val) {

        // We only get specified key in primary key
        // Because of the structure
        // First get key_field from key_val by name
        // Then get branch from key_field
        avro_value_t key_field;
        err = avro_value_get_by_name(key_val, key, &key_field, NULL);
        if (err) {
#ifdef DEBUG
            log_warn("on_customized_paritioner_cb : %s", avro_strerror());
#endif
            goto default_partitioner;
        }

        avro_value_t key_field_branch;
        err = avro_value_get_current_branch(&key_field, &key_field_branch);
        if (err) {
#ifdef DEBUG
            log_warn("on_customized_paritioner_cb : %s", avro_strerror());
#endif
            goto default_partitioner;
        }

        // the key_field_branch is avro int
        // we use avro_value_hash to hash the key_field_branch then mod it with partition_cnt
        return avro_value_hash(&key_field_branch) % partition_cnt;
    }

default_partitioner:
#if RD_KAFKA_VERSION >= 0x000901ff
    /* librdkafka 0.9.1 provides a "consistent_random" partitioner, which is
     * a good choice for us: "Uses consistent hashing to map identical keys
     * onto identical partitions, and messages without keys will be assigned
     * via the random partitioner." */
     return rd_kafka_msg_partitioner_consistent_random(rkt,
                                                       keydata,
                                                       keylen,
                                                       partition_cnt,
                                                       rkt_opaque,
                                                       msg_opaque);
#else
    // #if RD_KAFKA_VERSION >= 0x00090000
    /* librdkafka 0.9.0 provides a "consistent hashing partitioner", which we
     * can use to ensure that all updates for a given key go to the same
     * partition.  However, for unkeyed messages (such as we send for tables
     * with no primary key), it sends them all to the same partition, rather
     * than randomly partitioning them as would be preferable for scalability.
     */
    return rd_kafka_msg_partitioner_consistent(rkt,
                                               keydata,
                                               keylen,
                                               partition_cnt,
                                               rkt_opaque,
                                               msg_opaque);
#endif

}

/* Called by Kafka producer once per message sent, to report the delivery status
 * (whether success or failure). */
static void on_deliver_msg(rd_kafka_t *kafka, const rd_kafka_message_t *msg, void *opaque) {
    // The pointer that is the last argument to rd_kafka_produce is passed back
    // to us in the _private field in the struct. Seems a bit risky to rely on
    // a field called _private, but it seems to be the only way?
    msg_envelope_t envelope = (msg_envelope_t) msg->_private;

    int err;
    if (msg->err) {
        err = handle_error(envelope->context, msg->err,
                "Message delivery to topic %s failed: %s",
                rd_kafka_topic_name(msg->rkt),
                rd_kafka_err2str(msg->err));
        // err == 0 if handled
    } else {
        // Message successfully delivered to Kafka
        err = 0;
    }

    if (!err) {
        envelope->xact->pending_events--;
        maybe_checkpoint(envelope->context);
    }
    free(envelope);
}


/* When a Postgres transaction has been durably written to Kafka (i.e. we've seen the
 * commit event from Postgres, so we know the transaction is complete, and the Kafka
 * broker has acknowledged all messages in the transaction), we checkpoint it. This
 * allows the WAL for that transaction to be cleaned up in Postgres. */
void maybe_checkpoint(producer_context_t context) {
    transaction_info *xact = &context->xact_list[context->xact_tail];

    while (xact->pending_events == 0 && (xact->commit_lsn > 0 || xact->xid == 0)) {

        // Set the replication stream's "fsync LSN" (i.e. the WAL position up to which
        // the data has been durably written). This will be sent back to Postgres in the
        // next keepalive message, and used as the restart position if this client dies.
        // This should ensure that no data is lost (although messages may be duplicated).
        replication_stream_t stream = &context->client->repl;

        if (stream->fsync_lsn > xact->commit_lsn) {
            log_warn("%s: Commits not in WAL order! "
                     "Checkpoint LSN is %X/%X, commit LSN is %X/%X.", progname,
                     (uint32) (stream->fsync_lsn >> 32), (uint32) stream->fsync_lsn,
                     (uint32) (xact->commit_lsn  >> 32), (uint32) xact->commit_lsn);
        }

        if (stream->fsync_lsn < xact->commit_lsn) {
            log_debug("Checkpointing %d events for xid %u, WAL position %X/%X.",
                      xact->recvd_events, xact->xid,
                      (uint32) (xact->commit_lsn >> 32), (uint32) xact->commit_lsn);
        }

        stream->fsync_lsn = xact->commit_lsn;

        // xid==0 is the initial snapshot transaction. Clear the flag when it's complete.
        if (xact->xid == 0 && xact->commit_lsn > 0) {
            context->client->taking_snapshot = false;
        }

        context->xact_tail = (context->xact_tail + 1) % XACT_LIST_LEN;

        if (xact_list_empty(context)) break;

        xact = &context->xact_list[context->xact_tail];
    }
}


/* If the producing of messages to Kafka can't keep up with the consuming of messages from
 * Postgres, this function applies backpressure. It blocks for a little while, until a
 * timeout or until some network activity occurs in the Kafka client. At the same time, it
 * keeps the Postgres connection alive (without consuming any more data from it). This
 * function can be called in a loop until the buffer has drained. */
void backpressure(producer_context_t context) {
    rd_kafka_poll(context->kafka, 200);

    if (received_shutdown_signal) {
        log_info("%s during backpressure. Shutting down...", strsignal(received_shutdown_signal));
        exit_nicely(context, unfinished_snapshot);
    }

    // Keep the replication connection alive, even if we're not consuming data from it.
    int err = replication_stream_keepalive(&context->client->repl);
    if (err) {
        fatal_error(context, "While sending standby status update for keepalive: %s",
                    context->client->repl.error);
    }
}


/* Initializes the client context, which holds everything we need to know about
 * our connection to Postgres. */
client_context_t init_client() {
    frame_reader_t frame_reader = frame_reader_new();
    frame_reader->on_begin_txn    = on_begin_txn;
    frame_reader->on_commit_txn   = on_commit_txn;
    frame_reader->on_table_schema = on_table_schema;
    frame_reader->on_insert_row   = on_insert_row;
    frame_reader->on_update_row   = on_update_row;
    frame_reader->on_delete_row   = on_delete_row;
    frame_reader->on_keepalive    = on_keepalive;
    frame_reader->on_error        = on_client_error;

    client_context_t client = db_client_new();
    client->app_name = strdup(APP_NAME);
    db_client_set_error_policy(client, DEFAULT_ERROR_POLICY_NAME);
    client->allow_unkeyed = false;
    client->order_by = NULL;
    client->repl.slot_name = strdup(DEFAULT_REPLICATION_SLOT);
    client->repl.output_plugin = strdup(OUTPUT_PLUGIN);
    client->repl.frame_reader = frame_reader;
    client->repl.schema_pattern = strdup(DEFAULT_SCHEMA);
    client->repl.table_pattern = strdup(DEFAULT_TABLE);
    client->repl.table_ids = strdup(DEFAULT_TABLE);
    return client;
}

/* Initializes the producer context, which holds everything we need to know about
 * our connection to Kafka. */
producer_context_t init_producer(client_context_t client) {
    producer_context_t context = malloc(sizeof(producer_context));
    memset(context, 0, sizeof(producer_context));
    client->repl.frame_reader->cb_context = context;

    context->client = client;

    context->output_format = DEFAULT_OUTPUT_FORMAT;
    context->error_policy = DEFAULT_ERROR_POLICY;

    context->brokers = strdup(DEFAULT_BROKER_LIST);
    context->kafka_conf = rd_kafka_conf_new();
    context->topic_conf = rd_kafka_topic_conf_new();

    context->xact_head = XACT_LIST_LEN - 1;
    /* xact_tail and xact_list are set to zero by memset() above; this results
     * in the circular buffer starting out empty, since the tail is one ahead
     * of the head. */

#if RD_KAFKA_VERSION >= 0x000901ff
    /* librdkafka 0.9.1 provides a "consistent_random" partitioner, which is
     * a good choice for us: "Uses consistent hashing to map identical keys
     * onto identical partitions, and messages without keys will be assigned
     * via the random partitioner." */
    rd_kafka_topic_conf_set_partitioner_cb(context->topic_conf, &rd_kafka_msg_partitioner_consistent_random);
#elif RD_KAFKA_VERSION >= 0x00090000
#warning "rdkafka 0.9.0, using consistent partitioner - unkeyed messages will all get sent to a single partition!"
    /* librdkafka 0.9.0 provides a "consistent hashing partitioner", which we
     * can use to ensure that all updates for a given key go to the same
     * partition.  However, for unkeyed messages (such as we send for tables
     * with no primary key), it sends them all to the same partition, rather
     * than randomly partitioning them as would be preferable for scalability.
     */
    rd_kafka_topic_conf_set_partitioner_cb(context->topic_conf, &rd_kafka_msg_partitioner_consistent);
#else
#warning "rdkafka older than 0.9.0, messages will be partitioned randomly!"
    /* librdkafka prior to 0.9.0 does not provide a consistent partitioner, so
     * each message will be assigned to a random partition.  This will lead to
     * incorrect log compaction behaviour: e.g. if the initial insert for row
     * 42 goes to partition 0, then a subsequent delete for row 42 goes to
     * partition 1, then log compaction will be unable to garbage-collect the
     * insert. It will also break any consumer relying on seeing all updates
     * relating to a given key (e.g. for a stream-table join). */
#endif

    set_topic_config(context, "produce.offset.report", "true");
    rd_kafka_conf_set_dr_msg_cb(context->kafka_conf, on_deliver_msg);
    return context;
}

/* Connects to Kafka. This should be done before connecting to Postgres, as it
 * simply calls exit(1) on failure. */
void start_producer(producer_context_t context) {
    context->kafka = rd_kafka_new(RD_KAFKA_PRODUCER, context->kafka_conf,
            context->error, PRODUCER_CONTEXT_ERROR_LEN);
    if (!context->kafka) {
        log_error("%s: Could not create Kafka producer: %s", progname, context->error);
        exit(1);
    }

    if (rd_kafka_brokers_add(context->kafka, context->brokers) == 0) {
        log_error("%s: No valid Kafka brokers specified", progname);
        exit(1);
    }

    context->mapper = table_mapper_new(
            context->kafka,
            context->topic_conf,
            context->registry,
            context->topic_prefix,
            context->key);

    log_info("Writing messages to Kafka in %s format",
             output_format_name(context->output_format));
}

/* Shuts everything down and exits the process. */
void exit_nicely(producer_context_t context, int status) {
    fprintf(stderr, "Exit nicely. Bye bye !\n");
    // If a snapshot was in progress and not yet complete, and an error occurred, try to
    // drop the replication slot, so that the snapshot is retried when the user tries again.
    if (context->client->taking_snapshot && status != 0) {
        log_info("Dropping replication slot since the snapshot did not complete successfully.");
        if (replication_slot_drop(&context->client->repl) != 0) {
            log_error("%s: %s", progname, context->client->repl.error);
        }
    }

    if (context->topic_prefix) free(context->topic_prefix);

    if (context->key) free(context->key);

    if (context->brokers) free(context->brokers);

    table_mapper_free(context->mapper);

    if (context->registry) schema_registry_free(context->registry);

    frame_reader_free(context->client->repl.frame_reader);

    db_client_free(context->client);

    if (context->kafka) rd_kafka_destroy(context->kafka);

    curl_global_cleanup();

    rd_kafka_wait_destroyed(2000);
    exit(status);
}

static void handle_shutdown_signal(int sig) {
    received_shutdown_signal = sig;
}


int main(int argc, char **argv) {
    curl_global_init(CURL_GLOBAL_ALL);
    signal(SIGINT, handle_shutdown_signal);
    signal(SIGTERM, handle_shutdown_signal);

    producer_context_t context = init_producer(init_client());
    parse_options(context, argc, argv);
    start_producer(context);
    ensure(context, db_client_start(context->client));

    replication_stream_t stream = &context->client->repl;

    if (!context->client->slot_created) {
        log_info("Replication slot \"%s\" exists, streaming changes from %X/%X.",
                 stream->slot_name,
                 (uint32) (stream->start_lsn >> 32), (uint32) stream->start_lsn);
    } else if (context->client->skip_snapshot) {
        log_info("Created replication slot \"%s\", skipping snapshot and streaming changes from %X/%X.",
                 stream->slot_name,
                 (uint32) (stream->start_lsn >> 32), (uint32) stream->start_lsn);
    } else {
        assert(context->client->taking_snapshot);
    }

    while (context->client->status >= 0 && !received_shutdown_signal) {

        ensure(context, db_client_poll(context->client));

        if (context->client->status == 0) {
            ensure(context, db_client_wait(context->client));
        }

        rd_kafka_poll(context->kafka, 0);
    }

    if (received_shutdown_signal) {
        log_info("%s, shutting down...", strsignal(received_shutdown_signal));
    }

    exit_nicely(context, unfinished_snapshot);
    return 0;
}
