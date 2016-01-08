#ifndef TABLE_MAPPER_H
#define TABLE_MAPPER_H

#include <avro.h>
#include <librdkafka/rdkafka.h>
#include <postgresql/postgres_ext.h>


typedef struct {
    Oid relid;                  /* Uniquely identifies a table, even when it is renamed */
    char *topic_name;           /* Derived from schema record name, in turn derived from table name */
    int key_schema_id;          /* Identifier for the current key schema, assigned by the registry */
    avro_schema_t key_schema;   /* Schema to use for parsing key values */
    int row_schema_id;          /* Identifier for the current row schema, assigned by the registry */
    avro_schema_t row_schema;   /* Schema to use for parsing row values */
    rd_kafka_topic_t *topic;    /* Kafka topic to which messages are produced */
} table_metadata;

typedef table_metadata *table_metadata_t;


typedef struct {
    int num_tables;           /* Number of tables known */
    int capacity;             /* Allocated size of tables array */
    table_metadata **tables;  /* Array of pointers to table_metadata structs */
} table_mapper;

typedef table_mapper *table_mapper_t;

table_mapper_t table_mapper_new(void);
table_metadata_t table_mapper_lookup(table_mapper_t mapper, Oid relid);
table_metadata_t table_mapper_replace(table_mapper_t mapper, Oid relid);
void table_mapper_free(table_mapper_t mapper);


#endif /* TABLE_MAPPER_H */
