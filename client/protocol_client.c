/* Implements the client side of the wire protocol between the output plugin
 * and the client application. */

#include "protocol_client.h"
#include <stdlib.h>
#include <string.h>

#define check(err, call) { err = call; if (err) return err; }

#define check_alloc(x) \
    do { \
        if (!(x)) { \
            fprintf(stderr, "Memory allocation failed at %s:%d\n", __FILE__, __LINE__); \
            exit(1); \
        } \
    } while (0)


int process_frame(avro_value_t *frame_val, frame_reader_t reader, uint64_t wal_pos);
int process_frame_begin_txn(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos);
int process_frame_commit_txn(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos);
int process_frame_table_schema(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos);
int process_frame_insert(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos);
int process_frame_update(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos);
int process_frame_delete(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos);
schema_list_entry *schema_list_lookup(frame_reader_t reader, int64_t relid);
schema_list_entry *schema_list_replace(frame_reader_t reader, int64_t relid);
schema_list_entry *schema_list_entry_new(frame_reader_t reader);
int read_entirely(avro_value_t *value, avro_reader_t reader, const void *buf, size_t len);


int parse_frame(frame_reader_t reader, uint64_t wal_pos, char *buf, int buflen) {
    int err = 0;
    check(err, read_entirely(&reader->frame_value, reader->avro_reader, buf, buflen));
    check(err, process_frame(&reader->frame_value, reader, wal_pos));
    return err;
}


int process_frame(avro_value_t *frame_val, frame_reader_t reader, uint64_t wal_pos) {
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
                check(err, process_frame_begin_txn(&record_val, reader, wal_pos));
                break;
            case PROTOCOL_MSG_COMMIT_TXN:
                check(err, process_frame_commit_txn(&record_val, reader, wal_pos));
                break;
            case PROTOCOL_MSG_TABLE_SCHEMA:
                check(err, process_frame_table_schema(&record_val, reader, wal_pos));
                break;
            case PROTOCOL_MSG_INSERT:
                check(err, process_frame_insert(&record_val, reader, wal_pos));
                break;
            case PROTOCOL_MSG_UPDATE:
                check(err, process_frame_update(&record_val, reader, wal_pos));
                break;
            case PROTOCOL_MSG_DELETE:
                check(err, process_frame_delete(&record_val, reader, wal_pos));
                break;
            default:
                avro_set_error("Unknown message type %d", msg_type);
                return EINVAL;
        }
    }
    return err;
}

int process_frame_begin_txn(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0;
    avro_value_t xid_val;
    int64_t xid;

    check(err, avro_value_get_by_index(record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_long(&xid_val, &xid));

    if (reader->on_begin_txn) {
        check(err, reader->on_begin_txn(reader->cb_context, wal_pos, (uint32_t) xid));
    }
    return err;
}

int process_frame_commit_txn(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0;
    avro_value_t xid_val;
    int64_t xid;

    check(err, avro_value_get_by_index(record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_long(&xid_val, &xid));

    if (reader->on_commit_txn) {
        check(err, reader->on_commit_txn(reader->cb_context, wal_pos, (uint32_t) xid));
    }
    return err;
}

int process_frame_table_schema(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
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

    schema_list_entry *entry = schema_list_replace(reader, relid);
    entry->relid = relid;
    entry->hash = *((uint64_t *) hash);
    entry->row_schema = schema;
    entry->row_iface = avro_generic_class_from_schema(schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);
    avro_generic_value_new(entry->row_iface, &entry->old_value);
    entry->avro_reader = avro_reader_memory(NULL, 0);

    if (reader->on_table_schema) {
        check(err, reader->on_table_schema(reader->cb_context, wal_pos, relid,
                    schema_json, schema_len - 1, schema));
    }
    return err;
}

int process_frame_insert(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0;
    avro_value_t relid_val, newrow_val;
    int64_t relid;
    const void *new_bin;
    size_t new_len;

    check(err, avro_value_get_by_index(record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(record_val, 1, &newrow_val, NULL));
    check(err, avro_value_get_long(&relid_val, &relid));
    check(err, avro_value_get_bytes(&newrow_val, &new_bin, &new_len));

    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (!entry) {
        avro_set_error("Received insert for unknown relid %u", relid);
        return EINVAL;
    }

    check(err, read_entirely(&entry->row_value, entry->avro_reader, new_bin, new_len));

    if (reader->on_insert_row) {
        check(err, reader->on_insert_row(reader->cb_context, wal_pos, relid,
                    new_bin, new_len, &entry->row_value));
    }
    return err;
}

int process_frame_update(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, oldrow_present;
    avro_value_t relid_val, oldrow_val, newrow_val, branch_val;
    int64_t relid;
    const void *old_bin = NULL, *new_bin = NULL;
    size_t old_len = 0, new_len = 0;

    check(err, avro_value_get_by_index(record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(record_val, 1, &oldrow_val, NULL));
    check(err, avro_value_get_by_index(record_val, 2, &newrow_val, NULL));
    check(err, avro_value_get_long(&relid_val, &relid));
    check(err, avro_value_get_discriminant(&oldrow_val, &oldrow_present));
    check(err, avro_value_get_bytes(&newrow_val, &new_bin, &new_len));

    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (!entry) {
        avro_set_error("Received update for unknown relid %u", relid);
        return EINVAL;
    }

    if (oldrow_present) {
        check(err, avro_value_get_current_branch(&oldrow_val, &branch_val));
        check(err, avro_value_get_bytes(&branch_val, &old_bin, &old_len));
        check(err, read_entirely(&entry->old_value, entry->avro_reader, old_bin, old_len));
    }

    check(err, read_entirely(&entry->row_value, entry->avro_reader, new_bin, new_len));

    if (reader->on_update_row) {
        check(err, reader->on_update_row(reader->cb_context, wal_pos, relid,
                    old_bin, old_len, old_bin ? &entry->old_value : NULL,
                    new_bin, new_len, &entry->row_value));
    }
    return err;
}

int process_frame_delete(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, oldrow_present;
    avro_value_t relid_val, oldrow_val, branch_val;
    int64_t relid;
    const void *old_bin = NULL;
    size_t old_len;

    check(err, avro_value_get_by_index(record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(record_val, 1, &oldrow_val, NULL));
    check(err, avro_value_get_long(&relid_val, &relid));
    check(err, avro_value_get_discriminant(&oldrow_val, &oldrow_present));

    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (!entry) {
        avro_set_error("Received delete for unknown relid %u", relid);
        return EINVAL;
    }

    if (oldrow_present) {
        check(err, avro_value_get_current_branch(&oldrow_val, &branch_val));
        check(err, avro_value_get_bytes(&branch_val, &old_bin, &old_len));
        check(err, read_entirely(&entry->old_value, entry->avro_reader, old_bin, old_len));
    }

    if (reader->on_delete_row) {
        check(err, reader->on_delete_row(reader->cb_context, wal_pos, relid,
                    old_bin, old_len, old_bin ? &entry->old_value : NULL));
    }
    return err;
}

frame_reader_t frame_reader_new() {
    frame_reader_t reader = malloc(sizeof(frame_reader));
    check_alloc(reader);
    memset(reader, 0, sizeof(frame_reader));
    reader->num_schemas = 0;
    reader->capacity = 16;
    reader->schemas = malloc(reader->capacity * sizeof(void*));
    check_alloc(reader->schemas);

    reader->frame_schema = schema_for_frame();
    reader->frame_iface = avro_generic_class_from_schema(reader->frame_schema);
    avro_generic_value_new(reader->frame_iface, &reader->frame_value);
    reader->avro_reader = avro_reader_memory(NULL, 0);
    return reader;
}

/* Obtains the schema list entry for the given relid, and returns null if there is
 * no matching entry. */
schema_list_entry *schema_list_lookup(frame_reader_t reader, int64_t relid) {
    for (int i = 0; i < reader->num_schemas; i++) {
        schema_list_entry *entry = reader->schemas[i];
        if (entry->relid == relid) return entry;
    }
    return NULL;
}

/* If there is an existing list entry for the given relid, it is cleared (the memory
 * it references is freed) and then returned. If there is no existing list entry, a
 * new blank entry is returned. */
schema_list_entry *schema_list_replace(frame_reader_t reader, int64_t relid) {
    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (entry) {
        avro_reader_free(entry->avro_reader);
        avro_value_decref(&entry->old_value);
        avro_value_decref(&entry->row_value);
        avro_value_iface_decref(entry->row_iface);
        avro_schema_decref(entry->row_schema);
        return entry;
    } else {
        return schema_list_entry_new(reader);
    }
}

/* Allocates a new schema list entry. */
schema_list_entry *schema_list_entry_new(frame_reader_t reader) {
    if (reader->num_schemas == reader->capacity) {
        reader->capacity *= 4;
        reader->schemas = realloc(reader->schemas, reader->capacity * sizeof(void*));
        check_alloc(reader->schemas);
    }

    schema_list_entry *new_entry = malloc(sizeof(schema_list_entry));
    check_alloc(new_entry);
    reader->schemas[reader->num_schemas] = new_entry;
    reader->num_schemas++;

    return new_entry;
}

/* Frees all the memory structures associated with a frame reader. */
void frame_reader_free(frame_reader_t reader) {
    avro_reader_free(reader->avro_reader);
    avro_value_decref(&reader->frame_value);
    avro_value_iface_decref(reader->frame_iface);
    avro_schema_decref(reader->frame_schema);

    for (int i = 0; i < reader->num_schemas; i++) {
        schema_list_entry *entry = reader->schemas[i];
        avro_reader_free(entry->avro_reader);
        avro_value_decref(&entry->old_value);
        avro_value_decref(&entry->row_value);
        avro_value_iface_decref(entry->row_iface);
        avro_schema_decref(entry->row_schema);
        free(entry);
    }

    free(reader->schemas);
    free(reader);
}

/* Parses the contents of a binary-encoded Avro buffer into an Avro value, ensuring
 * that the entire buffer is read. */
int read_entirely(avro_value_t *value, avro_reader_t reader, const void *buf, size_t len) {
    int err = 0;

    avro_reader_memory_set_source(reader, buf, len);
    check(err, avro_value_read(reader, value));

    // Expect the reading of the Avro value from the buffer to entirely consume the
    // buffer contents. If there's anything left at the end, something must be wrong.
    // Avro doesn't seem to provide a way of checking how many bytes remain, so we
    // test indirectly by trying to seek forward (expecting to see an error).
    if (avro_skip(reader, 1) != ENOSPC) {
        avro_set_error("Unexpected trailing bytes at the end of buffer");
        return EINVAL;
    }

    return err;
}