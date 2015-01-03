#ifndef OID2AVRO_H
#define OID2AVRO_H

#include "avro.h"
#include "postgres.h"
#include "utils/rel.h"

#define GENERATED_SCHEMA_NAMESPACE "org.apache.samza.postgres.dbschema"

avro_schema_t schema_for_relation(Relation rel);
avro_schema_t schema_for_oid(Oid typid, bool nullable);
int update_avro_with_datum(avro_value_t *output_value, Oid typid, bool nullable, Datum pg_datum);

#endif /* OID2AVRO_H */
