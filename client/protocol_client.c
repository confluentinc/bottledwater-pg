/* Implements the client side of the wire protocol between the output plugin
 * and the client application. */

#include "protocol_client.h"
#include <stdlib.h>

#define check(err, call) { err = call; if (err) return err; }

#define check_alloc(x) \
    do { \
        if (!(x)) { \
            fprintf(stderr, "Memory allocation failed at %s:%d\n", __FILE__, __LINE__); \
            exit(1); \
        } \
    } while (0)


int process_frame_begin_txn(avro_value_t *record_val);
int process_frame_commit_txn(avro_value_t *record_val);
int process_frame_table_schema(avro_value_t *record_val, schema_cache_t cache);
int process_frame_insert(avro_value_t *record_val, schema_cache_t cache);
int process_frame_insert_decoded(Oid relid, avro_schema_t schema, avro_value_t *row_val);
struct schema_cache_entry *schema_cache_lookup(schema_cache_t cache, int64_t relid);
struct schema_cache_entry *schema_cache_replace(schema_cache_t cache, int64_t relid);
struct schema_cache_entry *schema_cache_entry_new(schema_cache_t cache);


int process_frame(avro_value_t *frame_val, schema_cache_t cache, uint64_t wal_pos) {
    int err = 0, msg_type;
    size_t num_messages;
    avro_value_t msg_val, union_val, record_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_get_size(&msg_val, &num_messages));

    for (int i = 0; i < num_messages; i++) {
        check(err, avro_value_get_by_index(&msg_val, i, &union_val, NULL));
        check(err, avro_value_get_discriminant(&union_val, &msg_type));
        check(err, avro_value_get_current_branch(&union_val, &record_val));

        switch (msg_type) {
            case PROTOCOL_MSG_BEGIN_TXN:
                check(err, process_frame_begin_txn(&record_val));
                break;
            case PROTOCOL_MSG_COMMIT_TXN:
                check(err, process_frame_commit_txn(&record_val));
                break;
            case PROTOCOL_MSG_TABLE_SCHEMA:
                check(err, process_frame_table_schema(&record_val, cache));
                break;
            case PROTOCOL_MSG_INSERT:
                check(err, process_frame_insert(&record_val, cache));
                break;
            default:
                avro_set_error("Unknown message type %d", msg_type);
                return EINVAL;
        }
    }
    return err;
}

int process_frame_begin_txn(avro_value_t *record_val) {
    int err = 0;
    avro_value_t xid_val;
    int64_t xid;

    check(err, avro_value_get_by_index(record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_long(&xid_val, &xid));
    return err;
}

int process_frame_commit_txn(avro_value_t *record_val) {
    int err = 0;
    avro_value_t xid_val;
    int64_t xid;

    check(err, avro_value_get_by_index(record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_long(&xid_val, &xid));
    return err;
}

int process_frame_table_schema(avro_value_t *record_val, schema_cache_t cache) {
    int err = 0;
    avro_value_t relid_val, hash_val, schema_val;
    int64_t relid;
    const void *hash;
    const char *schema_json;
    size_t hash_len, schema_len;
    avro_schema_t schema;

    check(err, avro_value_get_by_index(record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(record_val, 1, &hash_val,   NULL));
    check(err, avro_value_get_by_index(record_val, 2, &schema_val, NULL));
    check(err, avro_value_get_long(&relid_val, &relid));
    check(err, avro_value_get_fixed(&hash_val, &hash, &hash_len));
    check(err, avro_value_get_string(&schema_val, &schema_json, &schema_len));
    check(err, avro_schema_from_json_length(schema_json, schema_len - 1, &schema));

    struct schema_cache_entry *entry = schema_cache_replace(cache, relid);
    entry->relid = relid;
    entry->hash = *((uint64_t *) hash);
    entry->row_schema = schema;
    entry->row_iface = avro_generic_class_from_schema(schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);

    return err;
}

int process_frame_insert(avro_value_t *record_val, schema_cache_t cache) {
    int err = 0;
    avro_value_t relid_val, value_val;
    int64_t relid;
    const void *value_bin;
    size_t value_len;

    check(err, avro_value_get_by_index(record_val, 0, &relid_val, NULL));
    check(err, avro_value_get_by_index(record_val, 1, &value_val,  NULL));
    check(err, avro_value_get_long(&relid_val, &relid));
    check(err, avro_value_get_bytes(&value_val, &value_bin, &value_len));

    struct schema_cache_entry *entry = schema_cache_lookup(cache, relid);
    if (!entry) {
        avro_set_error("Received insert for unknown relid %u", relid);
        return EINVAL;
    }

    avro_reader_t reader = avro_reader_memory(value_bin, value_len);
    err = avro_value_read(reader, &entry->row_value);
    if (err) {
        avro_reader_free(reader);
        return err;
    }

    // Expect the reading of the Avro value from the buffer to entirely consume the
    // buffer contents. If there's anything left at the end, something must be wrong.
    // Avro doesn't seem to provide a way of checking how many bytes remain, so we
    // test indirectly by trying to seek forward (expecting to see an error).
    if (avro_skip(reader, 1) != ENOSPC) {
        avro_reader_free(reader);
        avro_set_error("Unexpected trailing bytes at the end of row data");
        return EINVAL;
    }

    avro_reader_free(reader);

    check(err, process_frame_insert_decoded(relid, entry->row_schema, &entry->row_value));
    return err;
}

int process_frame_insert_decoded(Oid relid, avro_schema_t schema, avro_value_t *row_val) {
    int err = 0;
    char *json;
    check(err, avro_value_to_json(row_val, 1, &json));
    printf("insert to %s: %s\n", avro_schema_name(schema), json);
    free(json);
    return err;
}

schema_cache_t schema_cache_new() {
    schema_cache_t cache = malloc(sizeof(struct schema_cache));
    check_alloc(cache);
    cache->num_entries = 0;
    cache->capacity = 16;
    cache->entries = malloc(cache->capacity * sizeof(void*));
    check_alloc(cache->entries);
    return cache;
}

/* Obtains the schema cache entry for the given relid, and returns null if there is
 * no matching entry. */
struct schema_cache_entry *schema_cache_lookup(schema_cache_t cache, int64_t relid) {
    for (int i = 0; i < cache->num_entries; i++) {
        struct schema_cache_entry *entry = cache->entries[i];
        if (entry->relid == relid) return entry;
    }
    return NULL;
}

/* If there is an existing cache entry for the given relid, it is cleared (the memory
 * it references is freed) and then returned. If there is no existing cache entry, a
 * new blank entry is returned. */
struct schema_cache_entry *schema_cache_replace(schema_cache_t cache, int64_t relid) {
    struct schema_cache_entry *entry = schema_cache_lookup(cache, relid);
    if (entry) {
        avro_value_decref(&entry->row_value);
        avro_value_iface_decref(entry->row_iface);
        avro_schema_decref(entry->row_schema);
        return entry;
    } else {
        return schema_cache_entry_new(cache);
    }
}

/* Allocates a new schema cache entry. */
struct schema_cache_entry *schema_cache_entry_new(schema_cache_t cache) {
    if (cache->num_entries == cache->capacity) {
        cache->capacity *= 4;
        cache->entries = realloc(cache->entries, cache->capacity * sizeof(void*));
        check_alloc(cache->entries);
    }

    struct schema_cache_entry *new_entry = malloc(sizeof(struct schema_cache_entry));
    check_alloc(new_entry);
    cache->entries[cache->num_entries] = new_entry;
    cache->num_entries++;

    return new_entry;
}

/* Frees all the memory structures associated with a schema cache. */
void schema_cache_free(schema_cache_t cache) {
    for (int i = 0; i < cache->num_entries; i++) {
        struct schema_cache_entry *entry = cache->entries[i];
        avro_value_decref(&entry->row_value);
        avro_value_iface_decref(entry->row_iface);
        avro_schema_decref(entry->row_schema);
        free(entry);
    }

    free(cache->entries);
    free(cache);
}
