#ifndef OID2AVRO_H
#define OID2AVRO_H

#include "avro.h"
#include "postgres.h"
#include "access/htup.h"
#include "utils/rel.h"

#define GENERATED_SCHEMA_NAMESPACE "com.dattran.bottledwater.dbschema"
#define PREDEFINED_SCHEMA_NAMESPACE "com.dattran.bottledwater.datatypes"

Relation table_key_index(Relation rel);
int schema_for_table_key(Relation rel, avro_schema_t *schema_out);
int schema_for_table_row(Relation rel, avro_schema_t *schema_out);
int tuple_to_avro_row(avro_value_t *output_val, TupleDesc tupdesc, HeapTuple tuple);
int tuple_to_avro_key(avro_value_t *output_val, TupleDesc tupdesc, HeapTuple tuple,
        Relation rel, Form_pg_index key_index);

#endif /* OID2AVRO_H */
