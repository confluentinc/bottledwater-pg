/* TODO docs */

#include "table_mapper.h"

#include <stdarg.h>


table_metadata_t table_metadata_new(table_mapper_t mapper, Oid relid);
int table_metadata_update_topic(table_mapper_t mapper, table_metadata_t table, const char* topic_name);
int table_metadata_update_schema(table_mapper_t mapper, table_metadata_t table, int is_key, const char* schema_json, size_t schema_len);
void table_metadata_set_schema_id(table_metadata_t table, int is_key, int schema_id);
void table_metadata_set_schema(table_metadata_t table, int is_key, avro_schema_t schema);
void table_metadata_free(table_metadata_t table);

void mapper_error(table_mapper_t mapper, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));


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

table_metadata_t table_mapper_lookup(table_mapper_t mapper, Oid relid) {
    for (int i = 0; i < mapper->num_tables; i++) {
        table_metadata_t table = mapper->tables[i];
        if (table->relid == relid) return table;
    }
    return NULL;
}

table_metadata_t table_mapper_update(table_mapper_t mapper, Oid relid,
        const char* topic_name,
        const char* key_schema_json, size_t key_schema_len,
        const char* row_schema_json, size_t row_schema_len) {
    table_metadata_t table = table_mapper_lookup(mapper, relid);
    if (!table) {
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

int table_metadata_update_topic(table_mapper_t mapper, table_metadata_t table, const char* topic_name) {
    if (!table->topic || strcmp(topic_name, rd_kafka_topic_name(table->topic))) {
        if (table->topic) rd_kafka_topic_destroy(table->topic);

        table->topic = rd_kafka_topic_new(mapper->kafka, topic_name,
                rd_kafka_topic_conf_dup(mapper->topic_conf));
        if (!table->topic) {
            mapper_error(mapper, "Cannot open Kafka topic %s: %s", topic_name,
                    rd_kafka_err2str(rd_kafka_errno2err(errno)));
            return -1;
        }
    }

    return 0;
}

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

void table_metadata_set_schema(table_metadata_t table, int is_key, avro_schema_t schema) {
    if (is_key) {
        if (table->key_schema) avro_schema_decref(table->key_schema);
        table->key_schema = schema;
    } else {
        if (table->row_schema) avro_schema_decref(table->row_schema);
        table->row_schema = schema;
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
