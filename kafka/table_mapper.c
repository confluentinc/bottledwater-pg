/* Mapping of tables to metadata needed for writing to Kafka:
 *   * the Kafka topic to produce updates to (derived from Avro schema record
 *     name, in turn derived from table name)
 *   * the schema ids for keys and rows, assigned by the schema registry
 *     (needed for Avro output)
 *   * the Avro schemas for keys and rows (needed to convert the Avro-binary-
 *     encoded values received from the Postgres extension into JSON output) */

#include "table_mapper.h"

#include <stdarg.h>


table_metadata_t table_metadata_new(table_mapper_t mapper, Oid relid);
int table_metadata_update_topic(table_mapper_t mapper, table_metadata_t table, const char* topic_name);
int table_metadata_update_schema(table_mapper_t mapper, table_metadata_t table, int is_key, const char* schema_json, size_t schema_len);
void table_metadata_set_schema_id(table_metadata_t table, int is_key, int schema_id);
void table_metadata_set_schema(table_metadata_t table, int is_key, avro_schema_t new_schema);
void table_metadata_free(table_metadata_t table);

void mapper_error(table_mapper_t mapper, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));

#define logf(...) fprintf(stderr, __VA_ARGS__)


/* Creates a new table_mapper.  Takes references to (but does not adopt
 * ownership of) the Kafka producer connection and topic configuration (so it
 * can create the topics associated with each table), and the schema registry
 * client (so it can register schemas and retrieve schema ids).
 *
 * The registry parameter may be NULL if running without a schema registry. */
table_mapper_t table_mapper_new(
        rd_kafka_t *kafka,
        rd_kafka_topic_conf_t *topic_conf,
        schema_registry_t registry) {
    table_mapper_t mapper = malloc(sizeof(table_mapper));
    memset(mapper, 0, sizeof(table_mapper));

    mapper->num_tables = 0;
    mapper->capacity = 16;
    mapper->tables = malloc(mapper->capacity * sizeof(void*));

    mapper->kafka = kafka;
    mapper->topic_conf = topic_conf;
    mapper->registry = registry;

    return mapper;
}

/* Returns the currently registered metadata for the table with the given
 * relid, or NULL if there is no metadata for that relid. */
table_metadata_t table_mapper_lookup(table_mapper_t mapper, Oid relid) {
    for (int i = 0; i < mapper->num_tables; i++) {
        table_metadata_t table = mapper->tables[i];
        if (table->relid == relid) return table;
    }
    return NULL;
}

/* Updates the metadata for the table with the given relid, replacing any
 * previously known metadata.  Re-updating an already known relid with the
 * same topic name and schemas is idempotent.  Otherwise, there are a couple of
 * side effects:
 *
 *  * will open the named topic, closing the old one if necessary.
 *  * if running with a schema registry, will register the schemas.
 *
 * Returns the updated metadata record on success, or NULL on failure.  Consult
 * mapper->error for the error message on failure. */
table_metadata_t table_mapper_update(table_mapper_t mapper, Oid relid,
        const char* topic_name,
        const char* key_schema_json, size_t key_schema_len,
        const char* row_schema_json, size_t row_schema_len) {
    table_metadata_t table = table_mapper_lookup(mapper, relid);
    if (table) {
        logf("Updating metadata for table %" PRIu32 " (topic \"%s\")\n", relid, topic_name);
    } else {
        logf("Registering metadata for table %" PRIu32 " (topic \"%s\")\n", relid, topic_name);
        table = table_metadata_new(mapper, relid);
    }

    int err;

    err = table_metadata_update_topic(mapper, table, topic_name);
    if (err) return NULL;

    err = table_metadata_update_schema(mapper, table, 1, key_schema_json, key_schema_len);
    if (err) return NULL;

    err = table_metadata_update_schema(mapper, table, 0, row_schema_json, row_schema_len);
    if (err) return NULL;

    return table;
}

/* Destroys the table mapper along with all stored metadata.  Will close any
 * associated topics. */
void table_mapper_free(table_mapper_t mapper) {
    for (int i = 0; i < mapper->num_tables; i++) {
        table_metadata_t table = mapper->tables[i];
        table_metadata_free(table);
        free(table);
    }

    free(mapper->tables);

    free(mapper);
}


table_metadata_t table_metadata_new(table_mapper_t mapper, Oid relid) {
    if (mapper->num_tables == mapper->capacity) {
        mapper->capacity *= 4;
        mapper->tables = realloc(mapper->tables, mapper->capacity * sizeof(void*));
    }

    table_metadata_t table = malloc(sizeof(table_metadata));
    memset(table, 0, sizeof(table_metadata));
    mapper->tables[mapper->num_tables] = table;
    mapper->num_tables++;

    table->relid = relid;
    table->key_schema_id = TABLE_MAPPER_SCHEMA_ID_MISSING;
    table->row_schema_id = TABLE_MAPPER_SCHEMA_ID_MISSING;

    return table;
}

/* Returns 0 on success.  On failure, sets mapper->error and returns nonzero. */
int table_metadata_update_topic(table_mapper_t mapper, table_metadata_t table, const char* topic_name) {
    const char* prev_topic_name = NULL;
    if (table->topic) prev_topic_name = rd_kafka_topic_name(table->topic);

    if (!table->topic) {
        logf("Registering topic \"%s\" for table %" PRIu32 "\n", topic_name, table->relid);
    } else if (strcmp(topic_name, prev_topic_name)) {
        logf("Registering new topic (was \"%s\", now \"%s\") for table %" PRIu32 "\n", prev_topic_name, topic_name, table->relid);

        rd_kafka_topic_destroy(table->topic);
    } else return 0; // topic name didn't change, nothing to do

    table->topic = rd_kafka_topic_new(mapper->kafka, topic_name,
            rd_kafka_topic_conf_dup(mapper->topic_conf));
    if (!table->topic) {
        mapper_error(mapper, "Cannot open Kafka topic %s: %s", topic_name,
                rd_kafka_err2str(rd_kafka_errno2err(errno)));
        return -1;
    }

    return 0;
}

/* Returns 0 on success.  On failure, sets mapper->error and returns nonzero. */
int table_metadata_update_schema(table_mapper_t mapper, table_metadata_t table, int is_key, const char* schema_json, size_t schema_len) {
    int prev_schema_id = is_key ? table->key_schema_id : table->row_schema_id;
    int schema_id = TABLE_MAPPER_SCHEMA_ID_MISSING;

    int err;

    if (mapper->registry) {
        err = schema_registry_request(mapper->registry, rd_kafka_topic_name(table->topic), is_key,
                schema_json, schema_len,
                &schema_id);
        if (err) {
            mapper_error(mapper, "Failed to register %s schema: %s",
                    is_key ? "key" : "row", mapper->registry->error);
            return err;
        }

        table_metadata_set_schema_id(table, is_key, schema_id);
    }

    avro_schema_t schema;

    /* If running with a schema registry, we can use the registry to detect
     * if the schema we just saw is the same as the one we remembered
     * previously (since the registry guarantees to return the same id for
     * identical schemas).  If the registry returns the same id as before, we
     * can skip parsing the new schema and just keep the previous one.
     *
     * However, if we're running without a registry, it's not so easy to detect
     * whether or not the schema changed, so in that case we just always parse
     * the new schema.  (We could store the previous schema JSON and strcmp()
     * it with the new JSON, but that probably wouldn't save much over just
     * parsing the JSON, given this isn't a hot code path.) */
    if (prev_schema_id == TABLE_MAPPER_SCHEMA_ID_MISSING || prev_schema_id != schema_id) {
        if (schema_json) {
            err = avro_schema_from_json_length(schema_json, schema_len, &schema);

            if (err) {
                mapper_error(mapper, "Could not parse %s schema: %s",
                        is_key ? "key" : "row", avro_strerror());
                return err;
            }
        } else {
            schema = NULL;
        }

        table_metadata_set_schema(table, is_key, schema);

        if (schema) avro_schema_decref(schema);
    }

    return 0;
}

void table_metadata_set_schema_id(table_metadata_t table, int is_key, int schema_id) {
    if (is_key) {
        table->key_schema_id = schema_id;
    } else {
        table->row_schema_id = schema_id;
    }
}

/* Doesn't take ownership of new_schema; will take a copy (incref) if needed. */
void table_metadata_set_schema(table_metadata_t table, int is_key, avro_schema_t new_schema) {
    const char* what;
    avro_schema_t* schema;
    if (is_key) {
        what = "key";
        schema = &table->key_schema;
    } else {
        what = "row";
        schema = &table->row_schema;
    }

    if (*schema == new_schema) {
        /* identical schema, nothing to do */
    } else if (!*schema) {
        logf("Storing %s schema for table %" PRIu32 "\n", what, table->relid);

        *schema = avro_schema_incref(new_schema);
    } else if (!new_schema) {
        logf("Forgetting stored %s schema for table %" PRIu32 "\n", what, table->relid);

        avro_schema_decref(*schema);
        *schema = NULL;
    } else {
        logf("Updating stored %s schema for table %" PRIu32 "\n", what, table->relid);

        avro_schema_decref(*schema);
        *schema = avro_schema_incref(new_schema);
    }
}

void table_metadata_free(table_metadata_t table) {
    if (table->topic) {
        rd_kafka_topic_destroy(table->topic);
    }
    if (table->row_schema) avro_schema_decref(table->row_schema);
    if (table->key_schema) avro_schema_decref(table->key_schema);
}


/* Updates the mapper's statically allocated error buffer with a message. */
void mapper_error(table_mapper_t mapper, char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(mapper->error, TABLE_MAPPER_ERROR_LEN, fmt, args);
    va_end(args);
}
