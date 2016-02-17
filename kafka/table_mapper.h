#ifndef TABLE_MAPPER_H
#define TABLE_MAPPER_H

#include "registry.h"

#include <avro.h>
#include <librdkafka/rdkafka.h>
#include <postgresql/postgres_ext.h>


#define TABLE_MAPPER_SCHEMA_ID_MISSING (-1)
#define TABLE_MAPPER_ERROR_LEN 512


typedef struct {
    Oid relid;                  /* Uniquely identifies a table, even when it is renamed */
    rd_kafka_topic_t *topic;    /* Kafka topic to which messages are produced */
    int key_schema_id;          /* Identifier for the current key schema, assigned by the registry */
    avro_schema_t key_schema;   /* Schema to use for converting key values to JSON */
    int row_schema_id;          /* Identifier for the current row schema, assigned by the registry */
    avro_schema_t row_schema;   /* Schema to use for converting row values to JSON */
} table_metadata;

typedef table_metadata *table_metadata_t;


typedef struct {
    char error[TABLE_MAPPER_ERROR_LEN]; /* Buffer for error messages */
    rd_kafka_t *kafka;                  /* Reference to the Kafka connection (so we can create topics) */
    rd_kafka_topic_conf_t *topic_conf;  /* Reference to the Kafka topic configuration */
    schema_registry_t registry;         /* Reference to the schema registry client */
    int num_tables;                     /* Number of tables known */
    int capacity;                       /* Allocated size of tables array */
    table_metadata **tables;            /* Array of pointers to table_metadata structs */
} table_mapper;

typedef table_mapper *table_mapper_t;

table_mapper_t table_mapper_new(
        rd_kafka_t *kafka,
        rd_kafka_topic_conf_t *topic_conf,
        schema_registry_t registry);
table_metadata_t table_mapper_lookup(table_mapper_t mapper, Oid relid);
table_metadata_t table_mapper_update(table_mapper_t mapper, Oid relid,
        const char* topic_name,
        const char* key_schema_json, size_t key_schema_len,
        const char* row_schema_json, size_t row_schema_len);
void table_mapper_free(table_mapper_t mapper);


#endif /* TABLE_MAPPER_H */
