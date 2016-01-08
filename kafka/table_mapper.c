/* TODO docs */

#include "table_mapper.h"


table_metadata_t table_metadata_new(table_mapper_t mapper, Oid relid);
void table_metadata_reset(table_metadata_t table);


table_mapper_t table_mapper_new(void) {
    table_mapper_t mapper = malloc(sizeof(table_mapper));
    memset(mapper, 0, sizeof(table_mapper));

    mapper->num_tables = 0;
    mapper->capacity = 16;
    mapper->tables = malloc(mapper->capacity * sizeof(void*));

    return mapper;
}

table_metadata_t table_mapper_lookup(table_mapper_t mapper, Oid relid) {
    for (int i = 0; i < mapper->num_tables; i++) {
        table_metadata_t table = mapper->tables[i];
        if (table->relid == relid) return table;
    }
    return NULL;
}

table_metadata_t table_mapper_replace(table_mapper_t mapper, Oid relid) {
    table_metadata_t table = table_mapper_lookup(mapper, relid);
    if (table) {
        table_metadata_reset(table);
        return table;
    } else {
        return table_metadata_new(mapper, relid);
    }
}

void table_mapper_free(table_mapper_t mapper) {
    for (int i = 0; i < mapper->num_tables; i++) {
        table_metadata_t table = mapper->tables[i];
        table_metadata_reset(table);
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

    return table;
}

void table_metadata_reset(table_metadata_t table) {
    /* TODO reenable this once registry's topic_list no longer owns topics
    if (table->topic) rd_kafka_topic_destroy(table->topic);
    */
    if (table->row_schema) avro_schema_decref(table->row_schema);
    if (table->key_schema) avro_schema_decref(table->key_schema);
    if (table->topic_name) free(table->topic_name);
}
