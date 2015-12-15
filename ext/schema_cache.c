/* Maintains server-side state relating to conversion of Postgres relations to Avro schemas. */

#include "schema_cache.h"
#include "lib/stringinfo.h"
#include "access/heapam.h"
#include "utils/lsyscache.h"

schema_cache_entry *schema_cache_entry_new(schema_cache_t cache);
void schema_cache_entry_update(schema_cache_entry *entry, Relation rel);
void schema_cache_entry_decrefs(schema_cache_entry *entry);
uint64 fnv_hash(uint64 base, char *str, int len);
uint64 fnv_format(uint64 base, char *fmt, ...) __attribute__ ((format (printf, 2, 3)));
uint64 schema_hash_for_relation(Relation rel);
void tupdesc_debug_info(StringInfo msg, TupleDesc tupdesc);

/* http://www.isthe.com/chongo/tech/comp/fnv/index.html#FNV-param */
#define FNV_HASH_BASE UINT64CONST(0xcbf29ce484222325)
#define FNV_HASH_PRIME UINT64CONST(0x100000001b3)
#define FNV_HASH_BUFSIZE 256

/* Creates a new schema cache. All palloc allocations for this cache will be
 * performed in the given memory context. */
schema_cache_t schema_cache_new(MemoryContext context) {
    MemoryContext oldctx = MemoryContextSwitchTo(context);
    schema_cache_t cache = palloc0(sizeof(schema_cache));
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
int schema_cache_lookup(schema_cache_t cache, Relation rel, schema_cache_entry **entry_out) {
    Oid relid = RelationGetRelid(rel);
    schema_cache_entry *entry;

    for (int i = 0; i < cache->num_entries; i++) {
        uint64 hash;
        entry = cache->entries[i];
        if (entry->relid != relid) continue;

        hash = schema_hash_for_relation(rel);
        if (entry->hash == hash) {
            /* Schema has not changed */
            *entry_out = entry;
            return 0;

        } else {
            /* Schema has changed since we last saw it -- update the cache */
            schema_cache_entry_decrefs(entry);
            schema_cache_entry_update(entry, rel);
            *entry_out = entry;
            return 1;
        }
    }

    /* Schema not previously seen -- create a new cache entry */
    entry = schema_cache_entry_new(cache);
    schema_cache_entry_update(entry, rel);
    *entry_out = entry;
    return 2;
}

/* Adds a new entry to the cache, allocated within the cache's memory context.
 * Returns a pointer to the new entry. */
schema_cache_entry *schema_cache_entry_new(schema_cache_t cache) {
    schema_cache_entry *new_entry;
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);

    if (cache->num_entries == cache->capacity) {
        cache->capacity *= 4;
        cache->entries = repalloc(cache->entries, cache->capacity * sizeof(void*));
    }

    new_entry = palloc0(sizeof(schema_cache_entry));
    cache->entries[cache->num_entries] = new_entry;
    cache->num_entries++;

    MemoryContextSwitchTo(oldctx);
    return new_entry;
}

/* Populates a schema cache entry with the information from a given table. */
void schema_cache_entry_update(schema_cache_entry *entry, Relation rel) {
    entry->relid = RelationGetRelid(rel);
    entry->hash = schema_hash_for_relation(rel);
    entry->key_schema = schema_for_table_key(rel);
    entry->row_schema = schema_for_table_row(rel);
    entry->row_iface = avro_generic_class_from_schema(entry->row_schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);

    if (entry->key_schema) {
        entry->key_iface = avro_generic_class_from_schema(entry->key_schema);
        avro_generic_value_new(entry->key_iface, &entry->key_value);
    }
}

/* Decrements the reference counts for a schema cache entry. */
void schema_cache_entry_decrefs(schema_cache_entry *entry) {
    avro_value_decref(&entry->row_value);
    avro_value_iface_decref(entry->row_iface);
    avro_schema_decref(entry->row_schema);

    if (entry->key_schema) {
        avro_value_decref(&entry->key_value);
        avro_value_iface_decref(entry->key_iface);
        avro_schema_decref(entry->key_schema);
    }
}

/* Frees all the memory structures associated with a schema cache. */
void schema_cache_free(schema_cache_t cache) {
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);

    for (int i = 0; i < cache->num_entries; i++) {
        schema_cache_entry *entry = cache->entries[i];
        schema_cache_entry_decrefs(entry);
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
    int len;

    va_start(args, fmt);
    len = vsnprintf(str, FNV_HASH_BUFSIZE, fmt, args);
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

        hash = fnv_format(hash, "attname=%s typid=%u\n", NameStr(attr->attname), attr->atttypid);
    }

    if (RelationGetForm(rel)->relkind == RELKIND_RELATION) {
        Relation index_rel = table_key_index(rel);
        if (index_rel) {
            hash = (hash * FNV_HASH_PRIME) ^ schema_hash_for_relation(index_rel);
            relation_close(index_rel, AccessShareLock);
        }
    }

    return hash;
}

/* Append debug information about table columns to a string buffer. */
void tupdesc_debug_info(StringInfo msg, TupleDesc tupdesc) {
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = tupdesc->attrs[i];
        appendStringInfo(msg, "\n\t%4d. attrelid = %u, attname = %s, atttypid = %u, attlen = %d, "
                "attnum = %d, attndims = %d, atttypmod = %d, attnotnull = %d, "
                "atthasdef = %d, attisdropped = %d, attcollation = %u",
                i, attr->attrelid, NameStr(attr->attname), attr->atttypid, attr->attlen,
                attr->attnum, attr->attndims, attr->atttypmod, attr->attnotnull,
                attr->atthasdef, attr->attisdropped, attr->attcollation);
    }
}

/* Returns a palloc'ed string with information about a table schema, for debugging. */
char *schema_debug_info(Relation rel, TupleDesc tupdesc) {
    StringInfoData msg;
    initStringInfo(&msg);
    appendStringInfo(&msg, "relation oid=%u name=%s ns=%s relkind=%c",
            RelationGetRelid(rel),
            RelationGetRelationName(rel),
            get_namespace_name(RelationGetNamespace(rel)),
            RelationGetForm(rel)->relkind);

    if (!tupdesc) tupdesc = RelationGetDescr(rel);
    tupdesc_debug_info(&msg, tupdesc);

    if (RelationGetForm(rel)->relkind == RELKIND_RELATION) {
        Relation index_rel = table_key_index(rel);
        if (index_rel) {
            appendStringInfo(&msg, "\nreplica identity index: oid=%u name=%s ns=%s",
                    RelationGetRelid(index_rel),
                    RelationGetRelationName(index_rel),
                    get_namespace_name(RelationGetNamespace(index_rel)));
            tupdesc_debug_info(&msg, RelationGetDescr(index_rel));
            relation_close(index_rel, AccessShareLock);
        }
    }

    return msg.data;
}
