#include "oid2avro.h"

#include "funcapi.h"
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"

/*
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
*/

int pg_string_to_avro(Datum pg_datum, Oid typid, avro_value_t *output_value);
int pg_bytes_to_avro(Datum pg_datum, avro_value_t *output_value);


/* Generates an Avro schema corresponding to a given table (relation). */
avro_schema_t relation_to_avro_schema(Relation rel) {
    StringInfoData namespace;
    initStringInfo(&namespace);
    appendStringInfoString(&namespace, GENERATED_SCHEMA_NAMESPACE);

    /* TODO ensure that names abide by Avro's requirements */
    char *rel_namespace = get_namespace_name(get_rel_namespace(RelationGetRelid(rel)));
    if (rel_namespace) appendStringInfo(&namespace, ".%s", rel_namespace);

    char *relname = NameStr(RelationGetForm(rel)->relname);
    avro_schema_t record_schema = avro_schema_record(relname, namespace.data);
    avro_schema_t column_schema;

    Form_pg_attribute xmin = SystemAttributeDefinition(MinTransactionIdAttributeNumber, true);
    column_schema = oid_to_schema(xmin->atttypid, 0);
    avro_schema_record_field_append(record_schema, "xmin", column_schema);
    avro_schema_decref(column_schema);

    Form_pg_attribute xmax = SystemAttributeDefinition(MaxTransactionIdAttributeNumber, true);
    column_schema = oid_to_schema(xmax->atttypid, 0);
    avro_schema_record_field_append(record_schema, "xmax", column_schema);
    avro_schema_decref(column_schema);

    TupleDesc tupdesc = RelationGetDescr(rel);
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; // skip dropped columns

        column_schema = oid_to_schema(attr->atttypid, !(attr->attnotnull));
        avro_schema_record_field_append(record_schema, NameStr(attr->attname), column_schema);
        avro_schema_decref(column_schema);
    }

    return record_schema;
}

/* Generates an Avro schema that can be used to encode a Postgres type
 * with the given OID. */
avro_schema_t oid_to_schema(Oid typid, int nullable) {
    avro_schema_t value_schema;

    switch (typid) {
        case BOOLOID:
            value_schema = avro_schema_boolean();
            break;
        case FLOAT4OID:
            value_schema = avro_schema_float();
            break;
        case FLOAT8OID:
            value_schema = avro_schema_double();
            break;
        case NUMERICOID:
            value_schema = avro_schema_double(); /* FIXME use decimal logical type: http://avro.apache.org/docs/1.7.7/spec.html#Decimal */
            break;
        case INT2OID:
        case INT4OID:
            value_schema = avro_schema_int();
            break;
        case INT8OID:
        case OIDOID: /* Oid is unsigned int */
            value_schema = avro_schema_long();
            break;
        case BYTEAOID:
            value_schema = avro_schema_bytes();
            break;
        default:
            value_schema = avro_schema_string();
            break;
    }

    if (nullable) {
        avro_schema_t null_schema = avro_schema_null();
        avro_schema_t union_schema = avro_schema_union();
        avro_schema_union_append(union_schema, null_schema);
        avro_schema_union_append(union_schema, value_schema);
        avro_schema_decref(null_schema);
        avro_schema_decref(value_schema);
        return union_schema;
    } else {
        return value_schema;
    }
}

int pg_string_to_avro(Datum pg_datum, Oid typid, avro_value_t *output_value) {
    avro_value_set_string(output_value, "FIXME");
    return 0;

    /* The following looks plausible, but unfortunately doesn't build...

    Oid output_func;
    bool is_varlena;

    getTypeOutputInfo(typid, &output_func, &is_varlena);

    if (is_varlena && VARATT_IS_EXTERNAL_ONDISK(pg_datum)) {
        // TODO can we load this from disk?
        *avro_out = avro_string("TODO load from disk");
        return 1;
    } else if (is_varlena) {
        pg_datum = PointerGetDatum(PG_DETOAST_DATUM(pg_datum));
    }

    // TODO fmgr.c says about OidOutputFunctionCall: "These functions are only to be used
    // in seldom-executed code paths.  They are not only slow but leak memory.
    char *str = OidOutputFunctionCall(output_func, pg_datum);
    *avro_out = avro_string(str);
    pfree(str);

    return 0;
    */
}

int pg_bytes_to_avro(Datum pg_datum, avro_value_t *output_value) {
    /* Linker error: undefined symbol _pg_detoast_datum

    text *txt = DatumGetByteaP(pg_datum);
    char *str = VARDATA(txt);
    size_t size = VARSIZE(txt) - VARHDRSZ;
    avro_value_set_bytes(output_value, str, size);
    */
    return 0;
}


/* Translates a Postgres datum into an Avro value. */
int pg_datum_to_avro(Datum pg_datum, Oid typid, avro_value_t *output_value) {
    avro_value_reset(output_value);

    switch (typid) {
        case BOOLOID:
            avro_value_set_boolean(output_value, DatumGetBool(pg_datum));
            break;
        case FLOAT4OID:
            //avro_value_set_float(output_value, DatumGetFloat4(pg_datum)); // Linker error: undefined symbol _DatumGetFloat4
            break;
        case FLOAT8OID:
            //avro_value_set_double(output_value, DatumGetFloat8(pg_datum)); // Linker error: undefined symbol _DatumGetFloat8
            break;
        case NUMERICOID:
            //DatumGetNumeric(pg_datum); // Linker error: undefined symbol _pg_detoast_datum
            break;
        case INT2OID:
            avro_value_set_int(output_value, DatumGetInt16(pg_datum));
            break;
        case INT4OID:
            avro_value_set_int(output_value, DatumGetInt32(pg_datum));
            break;
        case INT8OID:
            avro_value_set_long(output_value, DatumGetInt64(pg_datum));
            break;
        case OIDOID:
            avro_value_set_long(output_value, DatumGetObjectId(pg_datum));
            break;
        case BYTEAOID:
            pg_bytes_to_avro(pg_datum, output_value);
            break;
        default:
            pg_string_to_avro(pg_datum, typid, output_value);
            break;
    }

    /*
    avro_value_t value_datum, union_datum;
    if (is_avro_schema(output_schema) && is_avro_union(output_schema)) {
        union_datum = avro_union(output_schema, 1, value_datum);
        avro_datum_decref(value_datum);
        return union_datum;
    } else {
        return value_datum;
    }
    */
    return 0;
}
