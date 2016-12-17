/* Mapping of tables to metadata needed for writing to Kafka:
 *   * the Kafka topic to produce updates to (derived from Avro schema record
 *     name, in turn derived from table name)
 *   * the schema ids for keys and rows, assigned by the schema registry
 *     (needed for Avro output)
 *   * the Avro schemas for keys and rows (needed to convert the Avro-binary-
 *     encoded values received from the Postgres extension into JSON output) */

#include "logger.h"
#include "table_mapper.h"

#include <stdarg.h>
#include <string.h>


table_metadata_t table_metadata_new(table_mapper_t mapper, Oid relid);
int table_metadata_update_topic(table_mapper_t mapper, table_metadata_t table, const char* table_name);
int table_metadata_update_schema(table_mapper_t mapper, table_metadata_t table, int is_key, const char* schema_json, size_t schema_len);
void table_metadata_set_schema_id(table_metadata_t table, int is_key, int schema_id);
void table_metadata_set_schema(table_metadata_t table, int is_key, avro_schema_t new_schema);
void table_metadata_free(table_metadata_t table);

void mapper_error(table_mapper_t mapper, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));


/* Creates a new table_mapper.  Takes references to (but does not adopt
 * ownership of) the Kafka producer connection and topic configuration (so it
 * can create the topics associated with each table), and the schema registry
 * client (so it can register schemas and retrieve schema ids).  Takes a copy
 * of topic_prefix (unless it is NULL).
 *
 * The registry parameter may be NULL if running without a schema registry. */
table_mapper_t table_mapper_new(
        rd_kafka_t *kafka,
        rd_kafka_topic_conf_t *topic_conf,
        schema_registry_t registry,
        const char *topic_prefix,
        const char *key) {
    table_mapper_t mapper = malloc(sizeof(table_mapper));
    memset(mapper, 0, sizeof(table_mapper));

    mapper->num_tables = 0;
    mapper->capacity = 16;
    mapper->tables = malloc(mapper->capacity * sizeof(void*));

    mapper->kafka = kafka;
    mapper->topic_conf = topic_conf;
    mapper->registry = registry;

    if (topic_prefix != NULL) {
        mapper->topic_prefix = strdup(topic_prefix);
    }

    if (key != NULL) {
        mapper->key = strdup(key);
    }
    return mapper;
}

/* Returns the currently registered metadata for the table with the given
 * relid, or NULL if there is no metadata for that relid. */
table_metadata_t table_mapper_lookup(table_mapper_t mapper, Oid relid) {
    for (int i = 0; i < mapper->num_tables; i++) {
        table_metadata_t table = mapper->tables[i];
        if (!table->deleted && table->relid == relid) return table;
    }
    return NULL;
}

/* Updates the metadata for the table with the given relid, replacing any
 * previously known metadata.  Re-updating an already known relid with the
 * same table name and schemas is idempotent.  Otherwise, there are a couple of
 * side effects:
 *
 *  * will open the named topic, closing the old one if necessary.
 *  * if running with a schema registry, will register the schemas.
 *
 * Returns the updated metadata record on success, or NULL on failure.  Consult
 * mapper->error for the error message on failure. */
table_metadata_t table_mapper_update(table_mapper_t mapper, Oid relid,
        const char* table_name,
        const char* key_schema_json, size_t key_schema_len,
        const char* row_schema_json, size_t row_schema_len) {
    table_metadata_t table = table_mapper_lookup(mapper, relid);
    if (table) {
        log_info("Updating metadata for table %s (relid %" PRIu32 ")", table_name, relid);
    } else {
        log_info("Registering metadata for table %s (relid %" PRIu32 ")", table_name, relid);
        table = table_metadata_new(mapper, relid);
    }

    /* It's a tricky question what the right error handling behaviour should be
     * here, e.g. in the case of transient failure of the schema registry:
     *
     * a) we could register the table with a schema id of
     *    TABLE_MAPPER_SCHEMA_ID_MISSING.  This would keep data flowing, but
     *    mean we actually publich records to Kafka with that schema id,
     *    resulting in the Kafka replica of the database containing these
     *    less-helpful schema-missing records.  A sufficiently motivated
     *    consumer would be able to detect this and conclude that there was a
     *    problem with the schema registry.  Repair would currently require
     *    dropping the BW replication slot to reset the replication state, and
     *    restarting BW to re-publish the topics (relying on compaction and key
     *    identity to avoid duplicate records).
     * b) BW could drop updates without publishing them to Kafka.  This means
     *    accepting data loss (the Kafka replica will be missing some updates),
     *    but at least whenever we do publish records, they will be complete.
     *    Repair looks similar to above.
     * c) BW could stop consuming the logical replication stream until the error
     *    is resolved.  This avoids data loss, but creates other problems:
     *      - we don't currently have any mechanism to retry registering
     *        the table schema, so we won't actually notice when the error is
     *        resolved.
     *      - we'd need some work to make sure we actually replay any updates.
     *      - Postgres will keep buffering WAL until we start consuming again,
     *        so we threaten the stability of Postgres if the error persists.
     *
     * This might need to end up being a configuration choice.  For now, we
     * choose option b) - we leave the table unregistered (by marking it as
     * deleted), which means send_kafka_msg in bottledwater.c will fail to look
     * up the schema and invoke its error handling policy.
     */
    int err;

    err = table_metadata_update_topic(mapper, table, table_name);
    if (err) goto error;

    err = table_metadata_update_schema(mapper, table, 1, key_schema_json, key_schema_len);
    if (err) goto error;

    err = table_metadata_update_schema(mapper, table, 0, row_schema_json, row_schema_len);
    if (err) goto error;

    return table;

error:
    /* Mark the table as deleted so we don't try to proceed with incomplete
     * information.
     *
     * It would be preferable to actually delete the table record (i.e. free
     * the memory and avoid registering it in the mapper->tables array).
     * However because we're using a plain array to register tables, doing so
     * would be messy (especially if updating an existing record, in which case
     * we'd have to shuffle records around).  So for now we just set this flag.
     *
     * N.B. this means the record will *never* be freed!  So we should revisit
     * this if we anticipate having a large number of deleted records.
     */
    if (table) table->deleted = 1;
    return NULL;
}

/* Destroys the table mapper along with all stored metadata.  Will close any
 * associated topics. */
void table_mapper_free(table_mapper_t mapper) {
    if (mapper->topic_prefix) free(mapper->topic_prefix);
    if (mapper->key) free(mapper->key);

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
int table_metadata_update_topic(table_mapper_t mapper, table_metadata_t table, const char* table_name) {
    const char* prev_table_name = table->table_name;

    if (table->topic) {
        if (strcmp(table_name, prev_table_name)) {
            log_info("Registering new table (was \"%s\", now \"%s\") for relid %" PRIu32, prev_table_name, table_name, table->relid);

            free(table->table_name);
            rd_kafka_topic_destroy(table->topic);
        } else return 0; // table name didn't change, nothing to do
    }

    table->table_name = strdup(table_name);

    /* Kafka topic naming convention: [topic_prefix].[postgres_schema_name].table_name
     *
     *   - topic_prefix is optional, set via the --topic-prefix command-line option;
     *   - postgres_schema_name is omitted if the schema is "public";
     *   - dot separators are omitted if not needed
     *   - schema and table names are sanitised in the extension to be valid
     *     Avro identifiers: see make_avro_safe in ext/oid2avro.c
     *
     * See the README for more discussion of topic naming. */

    const char *topic_name;
    /* both branches set topic_name to a pointer we don't need to free,
     * since rd_kafka_topic_new below is going to copy it anyway */
    if (mapper->topic_prefix != NULL) {
        char prefixed_name[TABLE_MAPPER_MAX_TOPIC_LEN];
        int size = snprintf(prefixed_name, TABLE_MAPPER_MAX_TOPIC_LEN,
                "%s%c%s",
                mapper->topic_prefix, TABLE_MAPPER_TOPIC_PREFIX_DELIMITER, table_name);

        if (size >= TABLE_MAPPER_MAX_TOPIC_LEN) {
            mapper_error(mapper, "prefixed topic name is too long (max %d bytes): prefix %s, table name %s",
                    TABLE_MAPPER_MAX_TOPIC_LEN, mapper->topic_prefix, table_name);
            return -1;
        }

        topic_name = prefixed_name;
        /* needn't free topic_name because prefixed_name was stack-allocated */
    } else {
        topic_name = table_name;
        /* needn't free topic_name because it aliases table_name which we don't own */
    }

    log_info("Opening Kafka topic \"%s\" for table \"%s\"", topic_name, table_name);

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
        err = schema_registry_request(mapper->registry, rd_kafka_topic_name(table->topic), is_key, mapper->key,
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

            // filter key, get field that we want to use as key for kafka
            // TODO write a filter function instead of adding lines of code here

            // if (is_key && mapper->key && avro_schema_record_field_get_index(schema, mapper->key) != -1) {
            //     tmp = avro_schema_record(avro_schema_name(schema), avro_schema_namespace(schema));
            //     key = avro_schema_record_field_get(schema, mapper->key);
            //     avro_schema_record_field_append(tmp, mapper->key, key);
            //     if (schema) avro_schema_decref(schema);
            //     schema = avro_schema_copy(tmp);
            //     if (tmp) avro_schema_decref(tmp);
            // }

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

    if (avro_schema_equal(*schema, new_schema)) {
        /* identical schema, nothing to do */
    } else if (!*schema) {
        log_info("Storing %s schema for table %" PRIu32, what, table->relid);

        *schema = avro_schema_incref(new_schema);
    } else if (!new_schema) {
        log_info("Forgetting stored %s schema for table %" PRIu32, what, table->relid);

        avro_schema_decref(*schema);
        *schema = NULL;
    } else {
        log_info("Updating stored %s schema for table %" PRIu32, what, table->relid);

        avro_schema_decref(*schema);
        *schema = avro_schema_incref(new_schema);
    }
}

void table_metadata_free(table_metadata_t table) {
    if (table->table_name) free(table->table_name);
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
