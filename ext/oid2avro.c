#include "io_util.h"
#include "oid2avro.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

#ifndef HAVE_INT64_TIMESTAMP
#error Expecting timestamps to be represented as integers, not as floating-point.
#endif

typedef struct {
    avro_schema_t date_schema;         /* Predefined data type for "date" */
    avro_schema_t time_tz_schema;      /* Predefined data type for "time with time zone" */
    avro_schema_t datetime_schema;     /* Predefined data type for "timestamp without time zone" */
    avro_schema_t datetime_tz_schema;  /* Predefined data type for "timestamp with time zone" */
    avro_schema_t interval_schema;     /* Predefined data type for "interval" */
    avro_schema_t special_time_schema; /* Predefined data type for enum of +infinity, -infinity */
} predef_schema;

avro_schema_t schema_for_oid(predef_schema *predef, Oid typid);
avro_schema_t schema_for_numeric(predef_schema *predef);
avro_schema_t schema_for_date(predef_schema *predef);
avro_schema_t schema_for_time_tz(predef_schema *predef);
avro_schema_t schema_for_timestamp(predef_schema *predef, bool with_tz);
avro_schema_t schema_for_interval(predef_schema *predef);
void schema_for_date_fields(avro_schema_t record_schema);
void schema_for_time_fields(avro_schema_t record_schema);
avro_schema_t schema_for_special_times(predef_schema *predef, avro_schema_t record_schema);

int update_avro_with_datum(avro_value_t *output_val, Oid typid, Datum pg_datum);
int update_avro_with_date(avro_value_t *union_val, DateADT date);
int update_avro_with_time_tz(avro_value_t *record_val, TimeTzADT *time);
int update_avro_with_timestamp(avro_value_t *union_val, bool with_tz, Timestamp timestamp);
int update_avro_with_interval(avro_value_t *record_val, Interval *interval);
int update_avro_with_bytes(avro_value_t *output_val, bytea *bytes);
int update_avro_with_char(avro_value_t *output_val, char c);
int update_avro_with_string(avro_value_t *output_val, Oid typid, Datum pg_datum);


static char *make_avro_safe(const char *raw, bool is_namespace);


/* Returns the relation object for the index that we're going to use as key for a
 * particular table. (Indexes are relations too!) Returns null if the table is unkeyed.
 * The return value is opened with a shared lock; call relation_close() when finished. */
Relation table_key_index(Relation rel) {
    char replident = rel->rd_rel->relreplident;
    Oid repl_ident_oid;
    List *indexes;
    ListCell *index_oid;

    if (replident == REPLICA_IDENTITY_NOTHING) {
        return NULL;
    }

    if (replident == REPLICA_IDENTITY_INDEX) {
        repl_ident_oid = RelationGetReplicaIndex(rel);
        if (repl_ident_oid != InvalidOid) {
            return relation_open(repl_ident_oid, AccessShareLock);
        }
    }

    // There doesn't seem to be a convenient way of getting the primary key index for
    // a table, so we have to iterate over all the table's indexes.
    indexes = RelationGetIndexList(rel);

    foreach(index_oid, indexes) {
        Relation index_rel = relation_open(lfirst_oid(index_oid), AccessShareLock);
        Form_pg_index index = index_rel->rd_index;

        if (IndexIsValid(index) && IndexIsReady(index) && index->indisprimary) {
            list_free(indexes);
            return index_rel;
        }
        relation_close(index_rel, AccessShareLock);
    }

    list_free(indexes);
    return NULL;
}


/* Generates an Avro schema for the key (replica identity or primary key) of a
 * given table and sets *schema_out to point to it.
 *
 * Returns 0 if successful, nonzero if an error occurred generating the schema.
 * If the table is unkeyed, sets *schema_out to NULL and returns 0. */
int schema_for_table_key(Relation rel, avro_schema_t *schema_out) {
    Relation index_rel;
    int err;

    index_rel = table_key_index(rel);
    if (!index_rel) {
        *schema_out = NULL;
        return 0;
    }

    err = schema_for_table_row(index_rel, schema_out);

    relation_close(index_rel, AccessShareLock);
    return err;
}


/* Generates an Avro schema corresponding to a given table (relation) and sets
 * *schema_out to point to it.
 *
 * Returns 0 if successful, nonzero if an error occurred generating the schema.
 * If the table is unkeyed, sets *schema_out to NULL and returns 0. */
int schema_for_table_row(Relation rel, avro_schema_t *schema_out) {
    char *rel_namespace, *relname, *relname_avro_safe, *rel_namespace_avro_safe;
    char *attname_avro_safe;
    StringInfoData namespace;
    avro_schema_t record_schema, column_schema;
    TupleDesc tupdesc;
    predef_schema predef;
    int err = 0;

    memset(&predef, 0, sizeof(predef_schema));
    initStringInfo(&namespace);
    appendStringInfoString(&namespace, GENERATED_SCHEMA_NAMESPACE);

    rel_namespace = get_namespace_name(RelationGetNamespace(rel));
    if (rel_namespace) appendStringInfo(&namespace, ".%s", rel_namespace);

    rel_namespace_avro_safe = make_avro_safe(namespace.data, true);

    relname = RelationGetRelationName(rel);
    relname_avro_safe = make_avro_safe(relname, false);

    record_schema = avro_schema_record(relname_avro_safe, rel_namespace_avro_safe);
    free(relname_avro_safe);
    free(rel_namespace_avro_safe);
    if (record_schema == NULL) {
        *schema_out = NULL;
        return EINVAL;
    }

    tupdesc = RelationGetDescr(rel);

    if (tupdesc->natts == 0) {
        /* Special case for table schemas with no columns.  (You can create
         * such a table via `CREATE TABLE no_columns ()`, but more likely you'd
         * get there by dropping all the columns from an existing table.)
         *
         * We need to special-case this because avro-c doesn't seem to like
         * record schemas with no fields. */
        column_schema = avro_schema_boolean();
        err = avro_schema_record_field_append(record_schema, "dummy", column_schema);
        avro_schema_decref(column_schema);
    }

    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; /* skip dropped columns */

        attname_avro_safe = make_avro_safe(NameStr(attr->attname), false);
        column_schema = schema_for_oid(&predef, attr->atttypid);

        err = avro_schema_record_field_append(record_schema, attname_avro_safe, column_schema);

        avro_schema_decref(column_schema);
        free(attname_avro_safe);

        if (err) break;
    }

    *schema_out = record_schema;
    return err;
}


/* Translates a Postgres heap tuple (one row of a table) into the Avro schema generated
 * by schema_for_table_row(). */
int tuple_to_avro_row(avro_value_t *output_val, TupleDesc tupdesc, HeapTuple tuple) {
    int err = 0, field = 0;
    check(err, avro_value_reset(output_val));

    for (int i = 0; i < tupdesc->natts; i++) {
        avro_value_t field_val;
        bool isnull;
        Datum datum;

        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; /* skip dropped columns */

        check(err, avro_value_get_by_index(output_val, field, &field_val, NULL));

        datum = heap_getattr(tuple, i + 1, tupdesc, &isnull);

        if (isnull) {
            check(err, avro_value_set_branch(&field_val, 0, NULL));
        } else {
            check(err, update_avro_with_datum(&field_val, attr->atttypid, datum));
        }

        field++;
    }

    return err;
}


/* Extracts the fields that constitute the primary key/replica identity from a tuple,
 * and translates them into an Avro value in the schema generated by
 * schema_for_table_key(). tupdesc describes the the format of the tuple (which may or
 * may not include dropped columns). rel is the table from which the tuple has come,
 * and key_index is the primary key/replica identity index we're using. */
int tuple_to_avro_key(avro_value_t *output_val, TupleDesc tupdesc, HeapTuple tuple,
        Relation rel, Form_pg_index key_index) {
    int err = 0;
    TupleDesc rel_tupdesc = RelationGetDescr(rel);
    check(err, avro_value_reset(output_val));

    for (int field = 0; field < key_index->indkey.dim1; field++) {
        Form_pg_attribute attr;
        avro_value_t field_val;
        bool isnull;
        Datum datum;

        int attnum = key_index->indkey.values[field] - 1;

        // rel_tupdesc->attrs[attnum] is the indexed attribute. To figure out which
        // attribute number in the tuple this corresponds to, we need to see if there
        // are any columns that are dropped in rel_tupdesc but not in tupdesc.
        int tup_i = 0;
        for (int rel_i = 0; rel_i < attnum; rel_i++) {
            if (!rel_tupdesc->attrs[rel_i]->attisdropped || tupdesc->attrs[tup_i]->attisdropped) tup_i++;
        }

        if (tup_i >= tupdesc->natts || tupdesc->attrs[tup_i]->attisdropped) {
            elog(ERROR, "index refers to non-existent attribute number %d", attnum);
        }

        attr = tupdesc->attrs[tup_i];
        check(err, avro_value_get_by_index(output_val, field, &field_val, NULL));

        datum = heap_getattr(tuple, tup_i + 1, tupdesc, &isnull);

        if (isnull) {
            check(err, avro_value_set_branch(&field_val, 0, NULL));
        } else {
            check(err, update_avro_with_datum(&field_val, attr->atttypid, datum));
        }
    }

    return 0;
}


/* Generates an Avro schema that can be used to encode a Postgres type
 * with the given OID. */
avro_schema_t schema_for_oid(predef_schema *predef, Oid typid) {
    avro_schema_t value_schema, null_schema, union_schema;

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
            value_schema = schema_for_numeric(predef);
            break;

        /* Date/time types. We don't bother with abstime, reltime and tinterval (which are based
         * on Unix timestamps with 1-second resolution), as they are deprecated. */
        case DATEOID:        /* date: 32-bit signed integer, resolution of 1 day */
            return schema_for_date(predef);
        case TIMEOID:        /* time without time zone: microseconds since start of day */
            value_schema = avro_schema_long();
            break;
        case TIMETZOID:      /* time with time zone, timetz: time of day with time zone */
            value_schema = schema_for_time_tz(predef);
            break;
        case TIMESTAMPOID:   /* timestamp without time zone: datetime, microseconds since epoch */
            return schema_for_timestamp(predef, false);
        case TIMESTAMPTZOID: /* timestamp with time zone, timestamptz: datetime with time zone */
            return schema_for_timestamp(predef, true);
        case INTERVALOID:    /* @ <number> <units>, time interval */
            value_schema = schema_for_interval(predef);
            break;

        /* Binary string types */
        case BYTEAOID:   /* bytea: variable-length byte array */
            value_schema = avro_schema_bytes();
            break;
        case BITOID:     /* fixed-length bit string */
        case VARBITOID:  /* variable-length bit string */
        case UUIDOID:    /* UUID datatype */
        case LSNOID:     /* PostgreSQL LSN datatype */
        case MACADDROID: /* XX:XX:XX:XX:XX:XX, MAC address */
        case INETOID:    /* IP address/netmask, host address, netmask optional */
        case CIDROID:    /* network IP address/netmask, network address */

        /* Geometric types */
        case POINTOID:   /* geometric point '(x, y)' */
        case LSEGOID:    /* geometric line segment '(pt1,pt2)' */
        case PATHOID:    /* geometric path '(pt1,...)' */
        case BOXOID:     /* geometric box '(lower left,upper right)' */
        case POLYGONOID: /* geometric polygon '(pt1,...)' */
        case LINEOID:    /* geometric line */
        case CIRCLEOID:  /* geometric circle '(center,radius)' */

            /* range types... decompose like array types? */

        /* JSON types */
        case JSONOID:    /* json: Text-based JSON */
        case JSONBOID:   /* jsonb: Binary JSON */

        /* String-like types: fall through to the default, which is to create a string representation */
        case CHAROID:    /* "char": single character */
        case NAMEOID:    /* name: 63-byte type for storing system identifiers */
        case TEXTOID:    /* text: variable-length string, no limit specified */
        case BPCHAROID:  /* character(n), char(length): blank-padded string, fixed storage length */
        case VARCHAROID: /* varchar(length): non-blank-padded string, variable storage length */
        default:
            value_schema = avro_schema_string();
            break;
    }

    /* Make a union of value_schema with null. Some types are already a union,
     * in which case they must include null as the first branch of the union,
     * and return directly from the function without getting here (otherwise
     * we'd get a union inside a union, which is not valid Avro). */
    null_schema = avro_schema_null();
    union_schema = avro_schema_union();
    avro_schema_union_append(union_schema, null_schema);
    avro_schema_union_append(union_schema, value_schema);
    avro_schema_decref(null_schema);
    avro_schema_decref(value_schema);
    return union_schema;
}


/* Translates a Postgres datum into an Avro value. */
int update_avro_with_datum(avro_value_t *output_val, Oid typid, Datum pg_datum) {
    int err = 0;
    avro_value_t branch_val;

    /* Types that handle nullability themselves */
    if (typid == DATEOID || typid == TIMESTAMPOID || typid == TIMESTAMPTZOID) {
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
            /* There is no implementation for Decimal type in apache/avro package for c language.
             * We use logic for "double" type to avoid "0.0" values.
             */
            check(err, avro_value_set_double(&branch_val, atof(numeric_normalize(DatumGetNumeric(pg_datum)))));
            break;
        case DATEOID:
            check(err, update_avro_with_date(output_val, DatumGetDateADT(pg_datum)));
            break;
        case TIMEOID:
            check(err, avro_value_set_long(&branch_val, DatumGetTimeADT(pg_datum)));
            break;
        case TIMETZOID:
            check(err, update_avro_with_time_tz(&branch_val, DatumGetTimeTzADTP(pg_datum)));
            break;
        case TIMESTAMPOID:
            check(err, update_avro_with_timestamp(output_val, false, DatumGetTimestamp(pg_datum)));
            break;
        case TIMESTAMPTZOID:
            check(err, update_avro_with_timestamp(output_val, true, DatumGetTimestampTz(pg_datum)));
            break;
        case INTERVALOID:
            check(err, update_avro_with_interval(&branch_val, DatumGetIntervalP(pg_datum)));
            break;
        case BYTEAOID:
            check(err, update_avro_with_bytes(&branch_val, DatumGetByteaP(pg_datum)));
            break;
        case CHAROID:
            check(err, update_avro_with_char(&branch_val, DatumGetChar(pg_datum)));
            break;
        case NAMEOID:
            check(err, avro_value_set_string(&branch_val, NameStr(*DatumGetName(pg_datum))));
            break;
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
            check(err, avro_value_set_string(&branch_val, TextDatumGetCString(pg_datum)));
            break;
        default:
            check(err, update_avro_with_string(&branch_val, typid, pg_datum));
            break;
    }

    return err;
}

avro_schema_t schema_for_numeric(predef_schema *predef) {
    return avro_schema_double(); /* FIXME use decimal logical type: http://avro.apache.org/docs/1.7.7/spec.html#Decimal */
}

avro_schema_t schema_for_special_times(predef_schema *predef, avro_schema_t record_schema) {
    avro_schema_t union_schema, null_schema, enum_schema;

    union_schema = avro_schema_union();
    null_schema = avro_schema_null();
    avro_schema_union_append(union_schema, null_schema);
    avro_schema_decref(null_schema);

    avro_schema_union_append(union_schema, record_schema);
    avro_schema_decref(record_schema);

    if (predef->special_time_schema) {
        enum_schema = avro_schema_link(predef->special_time_schema);
    } else {
        enum_schema = avro_schema_enum("SpecialTime"); // TODO needs namespace
        avro_schema_enum_symbol_append(enum_schema, "POS_INFINITY");
        avro_schema_enum_symbol_append(enum_schema, "NEG_INFINITY");
        predef->special_time_schema = enum_schema;
    }

    avro_schema_union_append(union_schema, enum_schema);
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

avro_schema_t schema_for_date(predef_schema *predef) {
    if (predef->date_schema) {
        return schema_for_special_times(predef, avro_schema_link(predef->date_schema));
    } else {
        predef->date_schema = avro_schema_record("Date", PREDEFINED_SCHEMA_NAMESPACE);
        schema_for_date_fields(predef->date_schema);
        return schema_for_special_times(predef, predef->date_schema);
    }
}

int update_avro_with_date(avro_value_t *union_val, DateADT date) {
    int err = 0;
    int year, month, day;
    avro_value_t enum_val, record_val, year_val, month_val, day_val;

    if (DATE_NOT_FINITE(date)) {
        check(err, avro_value_set_branch(union_val, 2, &enum_val));
        if (DATE_IS_NOBEGIN(date)) {
            avro_value_set_enum(&enum_val, 1);
        } else {
            avro_value_set_enum(&enum_val, 0);
        }
    } else {
        j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

        check(err, avro_value_set_branch(union_val, 1, &record_val));
        check(err, avro_value_get_by_index(&record_val, 0, &year_val,  NULL));
        check(err, avro_value_get_by_index(&record_val, 1, &month_val, NULL));
        check(err, avro_value_get_by_index(&record_val, 2, &day_val,   NULL));
        check(err, avro_value_set_int(&year_val,  year));
        check(err, avro_value_set_int(&month_val, month));
        check(err, avro_value_set_int(&day_val,   day));
    }
    return err;
}

avro_schema_t schema_for_time_tz(predef_schema *predef) {
    avro_schema_t record_schema, column_schema;

    if (predef->time_tz_schema) {
        return avro_schema_link(predef->time_tz_schema);
    }

    record_schema = avro_schema_record("TimeTZ", PREDEFINED_SCHEMA_NAMESPACE);

    /* microseconds since midnight */
    column_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "micro", column_schema);
    avro_schema_decref(column_schema);

    /* time zone offset, in seconds relative to GMT (positive for zones east of Greenwich,
     * negative for zones west of Greenwich) */
    column_schema = avro_schema_int();
    avro_schema_record_field_append(record_schema, "zoneOffset", column_schema);
    avro_schema_decref(column_schema);

    predef->time_tz_schema = record_schema;
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

/* Should a date/time value be represented using a record (year, month, day, hours, minutes,
 * seconds and microseconds), or a ISO8601 string, or a timestamp (number of microseconds
 * since epoch)? Depends how the data is going to be consumed -- the formats ought to be
 * equivalent.
 *
 * If HAVE_INT64_TIMESTAMP (a compile-time option) is defined, Postgres internally stores
 * timestamps as a microsecond-resolution 64-bit integer with an epoch of 1 January 2000.
 * If it is not defined, Postgres uses a floating-point representation instead -- however,
 * the integer representation seems to be the default and the common case, so we only support
 * that for now.
 *
 * The Postgres docs and source code comments claim that Postgres internally uses the Julian
 * calendar, but that doesn't seem to be true: it treats years that are divisible by 100
 * (but not divisible by 400) as non-leapyears, which means it's actually using the Gregorian
 * calendar. That's fortunate, because Unix timestamps also use the Gregorian calendar, so
 * conversion between Postgres timestamps and Unix timestamps is easy. (Leap seconds are
 * ignored by both Postgres and Unix timestamps.)
 *
 * For timestamp (without time zone), the values are returned in UTC. For timestamp with
 * time zone, the time is converted into the time zone configured on the PG server:
 * http://www.postgresql.org/docs/9.4/static/runtime-config-client.html#GUC-TIMEZONE
 * Postgres internally stores the value in UTC either way (and doesn't store the time
 * zone), so the datatype only determines whether time zone conversion happens on output.
 *
 * Clients can force UTC output by setting the environment variable PGTZ=UTC, or by
 * executing "SET SESSION TIME ZONE UTC;".
 */
avro_schema_t schema_for_timestamp(predef_schema *predef, bool with_tz) {
    avro_schema_t record_schema;

    if (with_tz && predef->datetime_tz_schema) {
        return schema_for_special_times(predef,
                avro_schema_link(predef->datetime_tz_schema));
    }
    if (!with_tz && predef->datetime_schema) {
        return schema_for_special_times(predef,
                avro_schema_link(predef->datetime_schema));
    }

    record_schema = avro_schema_record(with_tz ? "DateTimeTZ" : "DateTime",
            PREDEFINED_SCHEMA_NAMESPACE);
    schema_for_date_fields(record_schema);
    schema_for_time_fields(record_schema);

    if (with_tz) {
        avro_schema_t column_schema = avro_schema_int();
        avro_schema_record_field_append(record_schema, "zoneOffset", column_schema);
        avro_schema_decref(column_schema);
        predef->datetime_tz_schema = record_schema;
    } else {
        predef->datetime_schema = record_schema;
    }

    return schema_for_special_times(predef, record_schema);
}

int update_avro_with_timestamp(avro_value_t *union_val, bool with_tz, Timestamp timestamp) {
    int err = 0, tz_offset;
    avro_value_t enum_val, record_val, year_val, month_val, day_val, hour_val,
                 minute_val, second_val, micro_val, zone_val;
    struct pg_tm decoded;
    fsec_t fsec;

    if (TIMESTAMP_NOT_FINITE(timestamp)) {
        check(err, avro_value_set_branch(union_val, 2, &enum_val));
        if (TIMESTAMP_IS_NOBEGIN(timestamp)) {
            check(err, avro_value_set_enum(&enum_val, 1));
        } else {
            check(err, avro_value_set_enum(&enum_val, 0));
        }
        return err;
    }

    // Postgres timestamp is microseconds since 2000-01-01. You can convert it to the
    // Unix epoch (1970-01-01) like this:
    //    timestamp + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY
    //
    // To get a Unix timestamp (with second resolution rather than microsecond), further
    // divide by 1e6.

    err = timestamp2tm(timestamp, with_tz ? &tz_offset : NULL, &decoded, &fsec, NULL, NULL);
    if (err) {
        ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                 errmsg("timestamp out of range")));
        return 1;
    }

    check(err, avro_value_set_branch(union_val, 1, &record_val));
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

avro_schema_t schema_for_interval(predef_schema *predef) {
    if (predef->interval_schema) {
        return avro_schema_link(predef->interval_schema);
    } else {
        predef->interval_schema = avro_schema_record("Interval", PREDEFINED_SCHEMA_NAMESPACE);
        schema_for_date_fields(predef->interval_schema);
        schema_for_time_fields(predef->interval_schema);
        return predef->interval_schema;
    }
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

int update_avro_with_bytes(avro_value_t *output_val, bytea *bytes) {
    return avro_value_set_bytes(output_val, VARDATA(bytes), VARSIZE(bytes) - VARHDRSZ);
}

int update_avro_with_char(avro_value_t *output_val, char c) {
    char str[2];
    str[0] = c;
    str[1] = '\0';
    return avro_value_set_string(output_val, str);
}

/* For any datatypes that we don't know, this function converts them into a string
 * representation (which is always required by a datatype). */
int update_avro_with_string(avro_value_t *output_val, Oid typid, Datum pg_datum) {
    int err = 0;
    Oid output_func;
    bool is_varlena;
    char *str;

    getTypeOutputInfo(typid, &output_func, &is_varlena);

    // According to this link
    // http://postgresql.nabble.com/ERROR-missing-chunk-number-0-for-toast-value-while-using-logical-decoder-td5909194.html#a5912389
    // bottledwater should check this condition VARATT_IS_EXTERNAL_ONDISK
    // before detoasting the pg_datum
    if (is_varlena && !VARATT_IS_EXTERNAL_ONDISK(pg_datum)) {
        pg_datum = PointerGetDatum(PG_DETOAST_DATUM(pg_datum));
    }

    /* This looks up the output function by OID on every call. Might be a bit faster
     * to do cache the output function info (like how printtup() does it). */
    str = OidOutputFunctionCall(output_func, pg_datum);
    err = avro_value_set_string(output_val, str);
    pfree(str);

    return err;
}


/* Sanitises the `raw` string to be a valid Avro identifier using an encoding
 * similar to the "percent encoding" used in URLs.  Unsupported characters are
 * replaced by a hexadecimal representation:
 *      e.g. "person/name" -> "person_2f_name"
 *
 * Valid Avro identifiers start with [A-Za-z_] and subsequently contain only
 * [A-Za-z0-9_], as per https://avro.apache.org/docs/1.8.1/spec.html#names
 *
 * If `is_namespace` is true, then dots ('.') will also be passed through
 * unencoded, provided they are neither the first nor the last character; this
 * is intended for namespaces which are a dot-separated sequence of names.
 *
 * Returns a malloc'd string which the caller is responsible for freeing.
 *
 * N.B.:
 *  * This encoding is not entirely unambiguous, since:
 *           "person_2e_name" -> "person_2e_name"
 *    This could be worked around by treating '_' as an invalid character and
 *    encoding it too (e.g. "person_2e_name" -> "person_5f_2e_5f_name"), but
 *    that seems ugly, especially since we ourselves generate names like
 *    "<relname>_pkey".
 *  * The encoding is done bytewise, and so for identifiers containing
 *    non-ASCII characters the result is a bit unintuitive: it is simply the
 *    underscore encoding of the bytes representing those characters in the
 *    server encoding (default UTF-8).  e.g.:
 *           "crêpes" -> "cr_c3__aa_pes" */
static char *make_avro_safe(const char *raw, bool is_namespace) {
    const size_t length = strlen(raw);

    /* Allocate enough space for the worst case, where we have to encode every
     * character, requiring 4 bytes per character (_xx_).  This is rather
     * wasteful in the common case, but we expect this will get freed soon, and
     * anyway these are unlikely to be very large strings.
     *
     * (To be precise, we never need to encode the null terminator byte, and
     * thus this always wastes at least three bytes.) */
    char *encoded = malloc(4 * length);

    char *pe = encoded;
    for (size_t index = 0; index < length; ++index) {
        const char c = raw[index];
        if (
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                c == '_' ||
                (c == '.' && is_namespace && index > 0 && index < length - 1) ||
                (c >= '0' && c <= '9' && index > 0)) {
            *pe++ = c;
        } else {
            sprintf(pe, "_%.2x_", (unsigned char) c);
            pe += 4;
        }
    }
    *pe = '\0';
    return encoded;
}
