#ifndef OID2AVRO_H
#define OID2AVRO_H

#include "avro.h"
#include "postgres.h"
#include "utils/rel.h"

#define GENERATED_SCHEMA_NAMESPACE "org.apache.samza.postgres.dbschema"

avro_schema_t relation_to_avro_schema(Relation rel);
avro_schema_t oid_to_schema(Oid typid, int nullable);
int pg_datum_to_avro(Datum pg_datum, Oid typid, avro_value_t *output_value);

#endif /* OID2AVRO_H */
