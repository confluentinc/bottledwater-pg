#ifndef OID2AVRO_H
#define OID2AVRO_H

#include "avro.h"
#include "postgres.h"
#include "access/htup.h"
#include "utils/rel.h"

#define GENERATED_SCHEMA_NAMESPACE "org.apache.samza.postgres.dbschema"
#define PREDEFINED_SCHEMA_NAMESPACE "org.apache.samza.postgres.datatypes"

avro_schema_t schema_for_relation(Relation rel, bool with_meta);
int update_avro_with_tuple(avro_value_t *output_val, avro_schema_t schema,
        TupleDesc tupdesc, HeapTuple tuple);

#endif /* OID2AVRO_H */
