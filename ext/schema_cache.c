/* Maintains server-side state relating to conversion of Postgres relations to Avro schemas. */

#include "schema_cache.h"
#include "lib/stringinfo.h"
#include "access/heapam.h"
#include "access/tupdesc.h"
#include "utils/lsyscache.h"

schema_cache_entry *schema_cache_entry_new(schema_cache_t cache);
void schema_cache_entry_update(schema_cache_t cache, schema_cache_entry *entry, Relation rel);
bool schema_cache_entry_changed(schema_cache_entry *entry, Relation rel);
void schema_cache_entry_decrefs(schema_cache_entry *entry);
void tupdesc_debug_info(StringInfo msg, TupleDesc tupdesc);

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
        entry = cache->entries[i];
        if (entry->relid != relid) continue;

        if (!schema_cache_entry_changed(entry, rel)) {
            /* Schema has not changed */
            *entry_out = entry;
            return 0;

        } else {
            /* Schema has changed since we last saw it -- update the cache */
            schema_cache_entry_decrefs(entry);
            schema_cache_entry_update(cache, entry, rel);
            *entry_out = entry;
            return 1;
        }
    }

    /* Schema not previously seen -- create a new cache entry */
    entry = schema_cache_entry_new(cache);
    schema_cache_entry_update(cache, entry, rel);
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
void schema_cache_entry_update(schema_cache_t cache, schema_cache_entry *entry, Relation rel) {
    entry->relid = RelationGetRelid(rel);
    entry->ns_id = RelationGetNamespace(rel);
    strcpy(NameStr(entry->relname), RelationGetRelationName(rel));
    strcpy(NameStr(entry->ns_name), get_namespace_name(entry->ns_id));

    Relation index_rel = table_key_index(rel);
    if (index_rel) {
        entry->key_id = RelationGetRelid(index_rel);
        entry->keyns_id = RelationGetNamespace(index_rel);
        strcpy(NameStr(entry->key_name), RelationGetRelationName(index_rel));
        strcpy(NameStr(entry->keyns_name), get_namespace_name(entry->keyns_id));
    } else {
        entry->key_id = InvalidOid;
        entry->keyns_id = InvalidOid;
    }

    /* Make a copy of the tuple descriptors in the cache's memory context */
    MemoryContext oldctx = MemoryContextSwitchTo(cache->context);
    if (index_rel) {
        entry->key_tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(index_rel));
    } else {
        entry->key_tupdesc = NULL;
    }
    entry->row_tupdesc = CreateTupleDescCopyConstr(RelationGetDescr(rel));
    MemoryContextSwitchTo(oldctx);
    relation_close(index_rel, AccessShareLock);

    entry->key_schema = schema_for_table_key(rel);
    entry->row_schema = schema_for_table_row(rel);
    entry->row_iface = avro_generic_class_from_schema(entry->row_schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);

    if (entry->key_schema) {
        entry->key_iface = avro_generic_class_from_schema(entry->key_schema);
        avro_generic_value_new(entry->key_iface, &entry->key_value);
    }
}

/* Returns false if the schema of the given relation matches the cache entry,
 * and returns true if it has changed. This is detected by keeping a copy of
 * the schema information in the cache entry. An alternative way of implementing
 * this might be to use event triggers:
 * http://www.postgresql.org/docs/9.4/static/event-triggers.html */
bool schema_cache_entry_changed(schema_cache_entry *entry, Relation rel) {
    if (entry->relid != RelationGetRelid(rel)) return true;
    if (entry->ns_id != RelationGetNamespace(rel)) return true;
    if (strcmp(NameStr(entry->relname), RelationGetRelationName(rel)) != 0) return true;
    if (strcmp(NameStr(entry->ns_name), get_namespace_name(entry->ns_id)) != 0) return true;

    Relation index_rel = table_key_index(rel);
    bool changed = false;
    if (index_rel && OidIsValid(entry->key_id)) {
        if (entry->key_id != RelationGetRelid(index_rel)) changed = true;
        if (entry->keyns_id != RelationGetNamespace(index_rel)) changed = true;
        if (strcmp(NameStr(entry->key_name), RelationGetRelationName(index_rel)) != 0) changed = true;
        if (strcmp(NameStr(entry->keyns_name), get_namespace_name(entry->keyns_id)) != 0) changed = true;
        if (!equalTupleDescs(entry->key_tupdesc, RelationGetDescr(index_rel))) changed = true;
    } else if (index_rel || OidIsValid(entry->key_id)) {
        changed = true;
    }
    relation_close(index_rel, AccessShareLock);
    if (changed) return true;

    return !equalTupleDescs(entry->row_tupdesc, RelationGetDescr(rel));
}

/* Decrements the reference counts for a schema cache entry. */
void schema_cache_entry_decrefs(schema_cache_entry *entry) {
    if (entry->key_tupdesc) pfree(entry->key_tupdesc);
    if (entry->row_tupdesc) pfree(entry->row_tupdesc);

    avro_value_decref(&entry->row_value);
    avro_value_iface_decref(entry->row_iface);
    avro_schema_decref(entry->row_schema);

    if (entry->key_schema) {
        avro_value_decref(&entry->key_value);
        avro_value_iface_decref(entry->key_iface);
        avro_schema_decref(entry->key_schema);
    }

    memset(entry, 0, sizeof(schema_cache_entry));
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
