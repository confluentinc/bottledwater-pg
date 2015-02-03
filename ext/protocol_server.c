/* Conversion of Postgres server-side structures into the wire protocol, which
 * is emitted by the output plugin and consumed by the client. */

#include "protocol_server.h"
#include "io_util.h"
#include "oid2avro.h"

#include <stdarg.h>
#include "utils/lsyscache.h"

int update_frame_with_table_schema(avro_value_t *frame_val, struct schema_cache_entry *entry);
int update_frame_with_insert_raw(avro_value_t *frame_val, Oid relid, bytea *value_bin);
int schema_cache_lookup(schema_cache_t cache, Relation rel, struct schema_cache_entry **entry_out);
struct schema_cache_entry *cache_entry_new(schema_cache_t cache);
void cache_entry_update(struct schema_cache_entry *entry, Relation rel);
uint64 fnv_hash(uint64 base, char *str, int len);
uint64 fnv_format(uint64 base, char *fmt, ...);
uint64 schema_hash_for_relation(Relation rel);

/* http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-param */
#define FNV_HASH_BASE UINT64CONST(0xcbf29ce484222325)
#define FNV_HASH_PRIME UINT64CONST(0x100000001b3)
#define FNV_HASH_BUFSIZE 256

int update_frame_with_begin_txn(avro_value_t *frame_val, ReorderBufferTXN *txn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, 0, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    return err;
}

int update_frame_with_commit_txn(avro_value_t *frame_val, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val, lsn_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, 1, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &lsn_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    check(err, avro_value_set_long(&lsn_val, commit_lsn));
    return err;
}

int update_frame_with_insert(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple tuple) {
    int err = 0;
    struct schema_cache_entry *entry;
    bytea *value_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    check(err, avro_value_reset(&entry->row_value));
    check(err, update_avro_with_tuple(&entry->row_value, entry->row_schema, RelationGetDescr(rel), tuple));
    check(err, try_writing(&value_bin, &write_avro_binary, &entry->row_value));
    check(err, update_frame_with_insert_raw(frame_val, RelationGetRelid(rel), value_bin));

    pfree(value_bin);
    return err;
}

int update_frame_with_table_schema(avro_value_t *frame_val, struct schema_cache_entry *entry) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, hash_val, schema_val;
    bytea *json = NULL;

    check(err, try_writing(&json, &write_schema_json, entry->row_schema));
    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, 2, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &hash_val,   NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &schema_val, NULL));
    check(err, avro_value_set_long(&relid_val, entry->relid));
    check(err, avro_value_set_fixed(&hash_val, &entry->hash, 8));
    check(err, avro_value_set_string_len(&schema_val, VARDATA(json), VARSIZE(json) - VARHDRSZ));

    pfree(json);
    return err;
}

int update_frame_with_insert_raw(avro_value_t *frame_val, Oid relid, bytea *value_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, value_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, 3, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &value_val,  NULL));
    check(err, avro_value_set_long(&relid_val, relid));
    check(err, avro_value_set_bytes(&value_val, VARDATA(value_bin), VARSIZE(value_bin) - VARHDRSZ));
    return err;
}

/* Creates a new schema cache. All palloc allocations for this cache will be
 * performed in the given memory context. */
schema_cache_t schema_cache_new(MemoryContext context) {
    MemoryContext oldctx = MemoryContextSwitchTo(context);
    schema_cache_t cache = palloc0(sizeof(struct schema_cache));
    cache->context = context;
    cache->num_entries = 0;
    cache->capacity = 16;
    cache->entries = palloc0(cache->capacity * sizeof(void*));
    MemoryContextSwitchTo(oldctx);
    return cache;
}

/* Obtains the schema cache entry for the given relation, creating or updating it if necessary.
 * If the schema hasn't changed since the last invocation, a cached value is used and 0 is returned.
 * If the schema has changed, 1 is returned. If the schema has not been seen before, 2 is returned. */
int schema_cache_lookup(schema_cache_t cache, Relation rel, struct schema_cache_entry **entry_out) {
    Oid relid = RelationGetRelid(rel);

    for (int i = 0; i < cache->num_entries; i++) {
        struct schema_cache_entry *entry = cache->entries[i];
        if (entry->relid != relid) continue;

        uint64 hash = schema_hash_for_relation(rel);
        if (entry->hash == hash) {
            /* Schema has not changed */
            *entry_out = entry;
            return 0;

        } else {
            /* Schema has changed since we last saw it -- update the cache */
            avro_value_decref(&entry->row_value);
            avro_value_iface_decref(entry->row_iface);
            avro_schema_decref(entry->row_schema);
            cache_entry_update(entry, rel);
            *entry_out = entry;
            return 1;
        }
    }

    /* Schema not previously seen -- create a new cache entry */
    struct schema_cache_entry *entry = cache_entry_new(cache);
    cache_entry_update(entry, rel);
    *entry_out = entry;
    return 2;
}

/* Adds a new entry to the cache, allocated within the cache's memory context.
 * Returns a pointer to the new entry. */
struct schema_cache_entry *cache_entry_new(schema_cache_t cache) {
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);
    if (cache->num_entries == cache->capacity) {
        cache->capacity *= 4;
        cache->entries = repalloc(cache->entries, cache->capacity * sizeof(void*));
    }

    struct schema_cache_entry *new_entry = palloc0(sizeof(struct schema_cache_entry));
    cache->entries[cache->num_entries] = new_entry;
    cache->num_entries++;

    MemoryContextSwitchTo(oldctx);
    return new_entry;
}

void cache_entry_update(struct schema_cache_entry *entry, Relation rel) {
    entry->relid = RelationGetRelid(rel);
    entry->hash = schema_hash_for_relation(rel);
    entry->row_schema = schema_for_relation(rel, false);
    entry->row_iface = avro_generic_class_from_schema(entry->row_schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);
}

/* Frees all the memory structures associated with a schema cache. */
void schema_cache_free(schema_cache_t cache) {
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);

    for (int i = 0; i < cache->num_entries; i++) {
        struct schema_cache_entry *entry = cache->entries[i];
        avro_value_decref(&entry->row_value);
        avro_value_iface_decref(entry->row_iface);
        avro_schema_decref(entry->row_schema);
        pfree(entry);
    }

    pfree(cache->entries);
    pfree(cache);
    MemoryContextSwitchTo(oldctx);
}

/* FNV-1a hash algorithm. Can be called incrementally for chunks of data, by using
 * the return value of one call as 'base' argument to the next call. For the first
 * call, use FNV_HASH_BASE as base. */
uint64 fnv_hash(uint64 base, char *str, int len) {
    uint64 hash = base;
    for (int i = 0; i < len; i++) {
        hash = (hash ^ str[i]) * FNV_HASH_PRIME;
    }
    return hash;
}

/* Evaluates a format string with arguments, and then hashes it. */
uint64 fnv_format(uint64 base, char *fmt, ...) {
    static char str[FNV_HASH_BUFSIZE];
    va_list args;

    va_start(args, fmt);
    int len = vsnprintf(str, FNV_HASH_BUFSIZE, fmt, args);
    va_end(args);

    if (len >= FNV_HASH_BUFSIZE) {
        elog(WARNING, "fnv_format: FNV_HASH_BUFSIZE is too small (must be at least %d)", len + 1);
        len = FNV_HASH_BUFSIZE - 1;
    }

    return fnv_hash(base, str, len);
}

/* Computes a hash over the definition of a relation. This is used to efficiently detect
 * schema changes: if the hash is unchanged, the schema is (very likely) unchanged, but any
 * change in the table definition will cause a different value to be returned. */
uint64 schema_hash_for_relation(Relation rel) {
    uint64 hash = fnv_format(FNV_HASH_BASE, "oid=%u name=%s ns=%s\n",
            RelationGetRelid(rel),
            RelationGetRelationName(rel),
            get_namespace_name(RelationGetNamespace(rel)));

    TupleDesc tupdesc = RelationGetDescr(rel);
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        if (attr->attisdropped) continue; /* skip dropped columns */

        hash = fnv_format(hash, "attname=%s typid=%u notnull=%d\n",
                NameStr(attr->attname), attr->atttypid, attr->attnotnull);
    }

    return hash;
}
