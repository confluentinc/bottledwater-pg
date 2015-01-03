#include "oid2avro.h"

#include "funcapi.h"
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/cash.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

/*
#include "replication/output_plugin.h"
#include "replication/logical.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
*/

#define check(err, call) { err = call; if (err) return err; }

#ifndef HAVE_INT64_TIMESTAMP
#error Expecting timestamps to be represented as integers, not as floating-point.
#endif

avro_schema_t schema_for_numeric(void);
int update_avro_with_string(avro_value_t *output_value, Datum pg_datum, Oid typid);
int update_avro_with_bytes(avro_value_t *output_value, Datum pg_datum);


/* Generates an Avro schema corresponding to a given table (relation). */
avro_schema_t schema_for_relation(Relation rel) {
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
    column_schema = schema_for_oid(xmin->atttypid, false);
    avro_schema_record_field_append(record_schema, "xmin", column_schema);
    avro_schema_decref(column_schema);

    Form_pg_attribute xmax = SystemAttributeDefinition(MaxTransactionIdAttributeNumber, true);
    column_schema = schema_for_oid(xmax->atttypid, false);
    avro_schema_record_field_append(record_schema, "xmax", column_schema);
    avro_schema_decref(column_schema);

    TupleDesc tupdesc = RelationGetDescr(rel);
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; // skip dropped columns

        column_schema = schema_for_oid(attr->atttypid, !(attr->attnotnull));
        avro_schema_record_field_append(record_schema, NameStr(attr->attname), column_schema);
        avro_schema_decref(column_schema);
    }

    return record_schema;
}

/* Generates an Avro schema that can be used to encode a Postgres type
 * with the given OID. */
avro_schema_t schema_for_oid(Oid typid, bool nullable) {
    avro_schema_t value_schema;

    switch (typid) {
        /* Numeric-like types */
        case BOOLOID:    /* boolean: 'true'/'false' */
            value_schema = avro_schema_boolean();
            break;
        case FLOAT4OID:  /* real, float4: 32-bit floating point number */
            value_schema = avro_schema_float();
            break;
        case FLOAT8OID:  /* double precision, float8: 64-bit floating point number */
            value_schema = avro_schema_double();
            break;
        case INT2OID:    /* smallint, int2: 16-bit signed integer */
        case INT4OID:    /* integer, int, int4: 32-bit signed integer */
            value_schema = avro_schema_int();
            break;
        case INT8OID:    /* bigint, int8: 64-bit signed integer */
        case CASHOID:    /* money: monetary amounts, $d,ddd.cc, stored as 64-bit signed integer */
        case OIDOID:     /* oid: Oid is unsigned int */
        case REGPROCOID: /* regproc: RegProcedure is Oid */
        case XIDOID:     /* xid: TransactionId is uint32 */
        case CIDOID:     /* cid: CommandId is uint32 */
            value_schema = avro_schema_long();
            break;
        case NUMERICOID: /* numeric(p, s), decimal(p, s): arbitrary precision number */
            value_schema = schema_for_numeric();
            break;

        /* Binary string types */
        case BYTEAOID:   /* bytea: variable-length byte array */
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

avro_schema_t schema_for_numeric() {
    return avro_schema_double(); /* FIXME use decimal logical type: http://avro.apache.org/docs/1.7.7/spec.html#Decimal */
}

int update_avro_with_string(avro_value_t *output_value, Datum pg_datum, Oid typid) {
    int err = 0;
    avro_value_set_string(output_value, "FIXME");
    return err;

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

int update_avro_with_bytes(avro_value_t *output_value, Datum pg_datum) {
    /* Linker error: undefined symbol _pg_detoast_datum

    text *txt = DatumGetByteaP(pg_datum);
    char *str = VARDATA(txt);
    size_t size = VARSIZE(txt) - VARHDRSZ;
    avro_value_set_bytes(output_value, str, size);
    */
    return 0;
}


/* Translates a Postgres datum into an Avro value. */
int update_avro_with_datum(avro_value_t *output_value, Oid typid, bool nullable, Datum pg_datum) {
    int err = 0;
    check(err, avro_value_reset(output_value));

    switch (typid) {
        case BOOLOID:
            check(err, avro_value_set_boolean(output_value, DatumGetBool(pg_datum)));
            break;
        case FLOAT4OID:
            check(err, avro_value_set_float(output_value, DatumGetFloat4(pg_datum)));
            break;
        case FLOAT8OID:
            check(err, avro_value_set_double(output_value, DatumGetFloat8(pg_datum)));
            break;
        case INT2OID:
            check(err, avro_value_set_int(output_value, DatumGetInt16(pg_datum)));
            break;
        case INT4OID:
            check(err, avro_value_set_int(output_value, DatumGetInt32(pg_datum)));
            break;
        case INT8OID:
            check(err, avro_value_set_long(output_value, DatumGetInt64(pg_datum)));
            break;
        case CASHOID:
            check(err, avro_value_set_long(output_value, DatumGetCash(pg_datum)));
            break;
        case OIDOID:
        case REGPROCOID:
            check(err, avro_value_set_long(output_value, DatumGetObjectId(pg_datum)));
            break;
        case XIDOID:
            check(err, avro_value_set_long(output_value, DatumGetTransactionId(pg_datum)));
            break;
        case CIDOID:
            check(err, avro_value_set_long(output_value, DatumGetCommandId(pg_datum)));
            break;
        case NUMERICOID:
            DatumGetNumeric(pg_datum); // TODO
            break;
        case BYTEAOID:
            check(err, update_avro_with_bytes(output_value, pg_datum));
            break;
        default:
            check(err, update_avro_with_string(output_value, pg_datum, typid));
            break;
    }

    return err;
}
