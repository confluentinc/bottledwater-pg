#include "oid2avro.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

#define check(err, call) { err = call; if (err) return err; }

#ifndef HAVE_INT64_TIMESTAMP
#error Expecting timestamps to be represented as integers, not as floating-point.
#endif

avro_schema_t schema_for_oid(Oid typid, bool nullable);
avro_schema_t schema_for_numeric(void);
avro_schema_t schema_for_date(bool nullable);
avro_schema_t schema_for_time_tz(void);
avro_schema_t schema_for_timestamp(bool nullable, bool with_tz);
avro_schema_t schema_for_interval(void);
void schema_for_date_fields(avro_schema_t record_schema);
void schema_for_time_fields(avro_schema_t record_schema);
avro_schema_t schema_for_special_times(avro_schema_t record_schema, bool nullable);

int update_avro_with_datum(avro_value_t *output_val, Oid typid, bool nullable, Datum pg_datum);
int update_avro_with_date(avro_value_t *union_val, bool nullable, DateADT date);
int update_avro_with_time_tz(avro_value_t *record_val, TimeTzADT *time);
int update_avro_with_timestamp(avro_value_t *union_val, bool nullable, bool with_tz, Timestamp timestamp);
int update_avro_with_interval(avro_value_t *record_val, Interval *interval);
int update_avro_with_string(avro_value_t *output_val, Datum pg_datum, Oid typid);
int update_avro_with_bytes(avro_value_t *output_val, Datum pg_datum);


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
        if (attr->attisdropped) continue; /* skip dropped columns */

        column_schema = schema_for_oid(attr->atttypid, !(attr->attnotnull));
        avro_schema_record_field_append(record_schema, NameStr(attr->attname), column_schema);
        avro_schema_decref(column_schema);
    }

    return record_schema;
}

/* Translates a Postgres heap tuple (one row of a table) into the Avro schema generated
 * by schema_for_relation. */
int update_avro_with_tuple(avro_value_t *output_val, avro_schema_t schema,
        TupleDesc tupdesc, HeapTuple tuple) {
    int err = 0, field = 0;
    check(err, avro_value_reset(output_val));

    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; /* skip dropped columns */

        avro_value_t field_val;
        avro_schema_t field_schema = avro_schema_record_field_get_by_index(schema, field);
        check(err, avro_value_get_by_index(output_val, field, &field_val, NULL));

        bool isnull, nullable = false;
        if (is_avro_union(field_schema)) {
            nullable = is_avro_null(avro_schema_union_branch(field_schema, 0));
        }

        Datum datum = heap_getattr(tuple, i + 1, tupdesc, &isnull);
        if (isnull && !nullable) {
            elog(ERROR, "got a null value on a column with non-null constraint");
            return 1;
        }

        if (isnull) {
            check(err, avro_value_set_branch(&field_val, 0, NULL));
        } else {
            check(err, update_avro_with_datum(&field_val, attr->atttypid, nullable, datum));
        }

        field++;
    }

    return err;
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

        /* Date/time types. We don't bother with abstime, reltime and tinterval (which are based
         * on Unix timestamps with 1-second resolution), as they are deprecated. */
        case DATEOID:        /* date: 32-bit signed integer, resolution of 1 day */
            return schema_for_date(nullable);
        case TIMEOID:        /* time without time zone: microseconds since start of day */
            value_schema = avro_schema_long();
            break;
        case TIMETZOID:      /* time with time zone, timetz: time of day with time zone */
            value_schema = schema_for_time_tz();
            break;
        case TIMESTAMPOID:   /* timestamp without time zone: datetime, microseconds since epoch */
            return schema_for_timestamp(nullable, false);
        case TIMESTAMPTZOID: /* timestamp with time zone, timestamptz: datetime with time zone */
            return schema_for_timestamp(nullable, true);
        case INTERVALOID:    /* @ <number> <units>, time interval */
            value_schema = schema_for_interval();
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


/* Translates a Postgres datum into an Avro value. */
int update_avro_with_datum(avro_value_t *output_val, Oid typid, bool nullable, Datum pg_datum) {
    int err = 0;
    avro_value_t branch_val;

    /* Types that handle nullability themselves */
    if (!nullable || typid == DATEOID || typid == TIMESTAMPOID || typid == TIMESTAMPTZOID) {
        branch_val = *output_val;
    } else {
        check(err, avro_value_set_branch(output_val, 1, &branch_val));
    }

    switch (typid) {
        case BOOLOID:
            check(err, avro_value_set_boolean(&branch_val, DatumGetBool(pg_datum)));
            break;
        case FLOAT4OID:
            check(err, avro_value_set_float(&branch_val, DatumGetFloat4(pg_datum)));
            break;
        case FLOAT8OID:
            check(err, avro_value_set_double(&branch_val, DatumGetFloat8(pg_datum)));
            break;
        case INT2OID:
            check(err, avro_value_set_int(&branch_val, DatumGetInt16(pg_datum)));
            break;
        case INT4OID:
            check(err, avro_value_set_int(&branch_val, DatumGetInt32(pg_datum)));
            break;
        case INT8OID:
            check(err, avro_value_set_long(&branch_val, DatumGetInt64(pg_datum)));
            break;
        case CASHOID:
            check(err, avro_value_set_long(&branch_val, DatumGetCash(pg_datum)));
            break;
        case OIDOID:
        case REGPROCOID:
            check(err, avro_value_set_long(&branch_val, DatumGetObjectId(pg_datum)));
            break;
        case XIDOID:
            check(err, avro_value_set_long(&branch_val, DatumGetTransactionId(pg_datum)));
            break;
        case CIDOID:
            check(err, avro_value_set_long(&branch_val, DatumGetCommandId(pg_datum)));
            break;
        case NUMERICOID:
            DatumGetNumeric(pg_datum); // TODO
            break;
        case DATEOID:
            check(err, update_avro_with_date(output_val, nullable, DatumGetDateADT(pg_datum)));
            break;
        case TIMEOID:
            check(err, avro_value_set_long(&branch_val, DatumGetTimeADT(pg_datum)));
            break;
        case TIMETZOID:
            check(err, update_avro_with_time_tz(&branch_val, DatumGetTimeTzADTP(pg_datum)));
            break;
        case TIMESTAMPOID:
            check(err, update_avro_with_timestamp(output_val, nullable, false, DatumGetTimestamp(pg_datum)));
            break;
        case TIMESTAMPTZOID:
            check(err, update_avro_with_timestamp(output_val, nullable, true, DatumGetTimestampTz(pg_datum)));
            break;
        case INTERVALOID:
            check(err, update_avro_with_interval(&branch_val, DatumGetIntervalP(pg_datum)));
            break;
        case BYTEAOID:
            check(err, update_avro_with_bytes(&branch_val, pg_datum));
            break;
        default:
            check(err, update_avro_with_string(&branch_val, pg_datum, typid));
            break;
    }

    return err;
}

avro_schema_t schema_for_numeric() {
    return avro_schema_double(); /* FIXME use decimal logical type: http://avro.apache.org/docs/1.7.7/spec.html#Decimal */
}

avro_schema_t schema_for_special_times(avro_schema_t record_schema, bool nullable) {
    avro_schema_t enum_schema = avro_schema_enum("SpecialTime"); // TODO needs namespace
    avro_schema_enum_symbol_append(enum_schema, "POS_INFINITY");
    avro_schema_enum_symbol_append(enum_schema, "NEG_INFINITY");

    avro_schema_t union_schema = avro_schema_union();
    if (nullable) {
        avro_schema_t null_schema = avro_schema_null();
        avro_schema_union_append(union_schema, null_schema);
        avro_schema_decref(null_schema);
    }
    avro_schema_union_append(union_schema, record_schema);
    avro_schema_union_append(union_schema, enum_schema);
    avro_schema_decref(record_schema);
    avro_schema_decref(enum_schema);
    return union_schema;
}

void schema_for_date_fields(avro_schema_t record_schema) {
    avro_schema_t column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "year", column_schema);
    avro_schema_decref(column_schema);

    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "month", column_schema);
    avro_schema_decref(column_schema);

    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "day", column_schema);
    avro_schema_decref(column_schema);
}

void schema_for_time_fields(avro_schema_t record_schema) {
    avro_schema_t column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "hour", column_schema);
    avro_schema_decref(column_schema);

    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "minute", column_schema);
    avro_schema_decref(column_schema);

    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "second", column_schema);
    avro_schema_decref(column_schema);

    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "micro", column_schema);
    avro_schema_decref(column_schema);
}

avro_schema_t schema_for_date(bool nullable) {
    avro_schema_t record_schema = avro_schema_record("Date", PREDEFINED_SCHEMA_NAMESPACE);
    schema_for_date_fields(record_schema);
    return schema_for_special_times(record_schema, nullable);
}

int update_avro_with_date(avro_value_t *union_val, bool nullable, DateADT date) {
    int err = 0, branch_offset = nullable ? 1 : 0;
    int year, month, day;
    avro_value_t enum_val, record_val, year_val, month_val, day_val;

    if (DATE_NOT_FINITE(date)) {
        check(err, avro_value_set_branch(union_val, 1 + branch_offset, &enum_val));
        if (DATE_IS_NOBEGIN(date)) {
            avro_value_set_enum(&enum_val, 1);
        } else {
            avro_value_set_enum(&enum_val, 0);
        }
    } else {
        j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

        check(err, avro_value_set_branch(union_val, 0 + branch_offset, &record_val));
        check(err, avro_value_get_by_index(&record_val, 0, &year_val,  NULL));
        check(err, avro_value_get_by_index(&record_val, 1, &month_val, NULL));
        check(err, avro_value_get_by_index(&record_val, 2, &day_val,   NULL));
        check(err, avro_value_set_int(&year_val,  year));
        check(err, avro_value_set_int(&month_val, month));
        check(err, avro_value_set_int(&day_val,   day));
    }
    return err;
}

avro_schema_t schema_for_time_tz() {
    avro_schema_t record_schema = avro_schema_record("TimeTZ", PREDEFINED_SCHEMA_NAMESPACE);

    /* microseconds since midnight */
    avro_schema_t column_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "micro", column_schema);
    avro_schema_decref(column_schema);

    /* time zone offset, in seconds relative to GMT (positive for zones east of Greenwich,
     * negative for zones west of Greenwich) */
    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "zoneOffset", column_schema);
    avro_schema_decref(column_schema);

    return record_schema;
}

int update_avro_with_time_tz(avro_value_t *record_val, TimeTzADT *time) {
    int err = 0;
    avro_value_t micro_val, zone_val;

    check(err, avro_value_get_by_index(record_val, 0, &micro_val, NULL));
    check(err, avro_value_get_by_index(record_val, 1, &zone_val,  NULL));
    check(err, avro_value_set_long(&micro_val, time->time));
    /* Negate the timezone offset because PG internally uses negative values for locations
     * east of GMT, but ISO 8601 does it the other way round. */
    check(err, avro_value_set_int(&zone_val, -time->zone));

    return err;
}

/* We represent a timestamp using a record of year, month, day, hours, minutes, seconds and
 * microseconds. We do this rather than a simple microseconds-since-epoch value because
 * PG's timestamp uses the Julian calendar internally. The Julian calendar avoids the
 * complexities of special-cased leap years and leap seconds, but means that a epoch-based
 * timestamp may be interpreted differently by a consumer that is expecting a Gregorian
 * calendar. We can side-step the problem of calendar conversion by instead returning a
 * value broken down by year, month, day etc. -- because that's most likely the format
 * in which the data was inserted into the database in the first place, so this format
 * hopefully minimizes surprises.
 *
 * For timestamp (without time zone), the values are returned in GMT. For timestamp with
 * time zone, the time is converted into the time zone configured on the PG server:
 * http://www.postgresql.org/docs/9.4/static/runtime-config-client.html#GUC-TIMEZONE
 * Postgres internally stores the value in GMT either way (and doesn't store the time
 * zone), so the datatype only determines whether time zone conversion happens on output. */
avro_schema_t schema_for_timestamp(bool nullable, bool with_tz) {
    avro_schema_t record_schema = avro_schema_record("DateTime", PREDEFINED_SCHEMA_NAMESPACE);
    schema_for_date_fields(record_schema);
    schema_for_time_fields(record_schema);

    if (with_tz) {
        avro_schema_t column_schema = avro_schema_int();
        avro_schema_record_field_append(record_schema, "zoneOffset", column_schema);
        avro_schema_decref(column_schema);
    }
    return schema_for_special_times(record_schema, nullable);
}

int update_avro_with_timestamp(avro_value_t *union_val, bool nullable, bool with_tz,
                               Timestamp timestamp) {
    int err = 0, tz_offset, branch_offset = nullable ? 1 : 0;
    avro_value_t enum_val, record_val, year_val, month_val, day_val, hour_val,
                 minute_val, second_val, micro_val, zone_val;
    struct pg_tm decoded;
    fsec_t fsec;

    if (TIMESTAMP_NOT_FINITE(timestamp)) {
        check(err, avro_value_set_branch(union_val, 1 + branch_offset, &enum_val));
        if (TIMESTAMP_IS_NOBEGIN(timestamp)) {
            check(err, avro_value_set_enum(&enum_val, 1));
        } else {
            check(err, avro_value_set_enum(&enum_val, 0));
        }
        return err;
    }

    err = timestamp2tm(timestamp, with_tz ? &tz_offset : NULL, &decoded, &fsec, NULL, NULL);
    if (err) {
        ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range")));
        return 1;
    }

    check(err, avro_value_set_branch(union_val, 0 + branch_offset, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &year_val,   NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &month_val,  NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &day_val,    NULL));
    check(err, avro_value_get_by_index(&record_val, 3, &hour_val,   NULL));
    check(err, avro_value_get_by_index(&record_val, 4, &minute_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 5, &second_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 6, &micro_val,  NULL));
    check(err, avro_value_set_int(&year_val,   decoded.tm_year));
    check(err, avro_value_set_int(&month_val,  decoded.tm_mon));
    check(err, avro_value_set_int(&day_val,    decoded.tm_mday));
    check(err, avro_value_set_int(&hour_val,   decoded.tm_hour));
    check(err, avro_value_set_int(&minute_val, decoded.tm_min));
    check(err, avro_value_set_int(&second_val, decoded.tm_sec));
    check(err, avro_value_set_int(&micro_val,  fsec));

    if (with_tz) {
        check(err, avro_value_get_by_index(&record_val, 7, &zone_val, NULL));
        /* Negate the timezone offset because PG internally uses negative values for
         * locations east of GMT, but ISO 8601 does it the other way round. */
        check(err, avro_value_set_int(&zone_val, -tz_offset));
    }
    return err;
}

avro_schema_t schema_for_interval() {
    avro_schema_t record_schema = avro_schema_record("Interval", PREDEFINED_SCHEMA_NAMESPACE);
    schema_for_date_fields(record_schema);
    schema_for_time_fields(record_schema);
    return record_schema;
}

int update_avro_with_interval(avro_value_t *record_val, Interval *interval) {
    int err = 0;
    avro_value_t year_val, month_val, day_val, hour_val, minute_val, second_val, micro_val;
    struct pg_tm decoded;
    fsec_t fsec;

    interval2tm(*interval, &decoded, &fsec);
    check(err, avro_value_get_by_index(record_val, 0, &year_val,   NULL));
    check(err, avro_value_get_by_index(record_val, 1, &month_val,  NULL));
    check(err, avro_value_get_by_index(record_val, 2, &day_val,    NULL));
    check(err, avro_value_get_by_index(record_val, 3, &hour_val,   NULL));
    check(err, avro_value_get_by_index(record_val, 4, &minute_val, NULL));
    check(err, avro_value_get_by_index(record_val, 5, &second_val, NULL));
    check(err, avro_value_get_by_index(record_val, 6, &micro_val,  NULL));
    check(err, avro_value_set_int(&year_val,   decoded.tm_year));
    check(err, avro_value_set_int(&month_val,  decoded.tm_mon));
    check(err, avro_value_set_int(&day_val,    decoded.tm_mday));
    check(err, avro_value_set_int(&hour_val,   decoded.tm_hour));
    check(err, avro_value_set_int(&minute_val, decoded.tm_min));
    check(err, avro_value_set_int(&second_val, decoded.tm_sec));
    check(err, avro_value_set_int(&micro_val,  fsec));

    return err;
}

int update_avro_with_string(avro_value_t *output_val, Datum pg_datum, Oid typid) {
    int err = 0;
    avro_value_set_string(output_val, "FIXME");
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

int update_avro_with_bytes(avro_value_t *output_val, Datum pg_datum) {
    /* Linker error: undefined symbol _pg_detoast_datum

    text *txt = DatumGetByteaP(pg_datum);
    char *str = VARDATA(txt);
    size_t size = VARSIZE(txt) - VARHDRSZ;
    avro_value_set_bytes(output_val, str, size);
    */
    return 0;
}
