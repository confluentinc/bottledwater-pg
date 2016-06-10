/* Implements the client side of the wire protocol between the output plugin
 * and the client application. */

#include "protocol_client.h"
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#define check(err, call) { err = call; if (err) return err; }

#define check_handle(err, reader, call, fmt, ...) \
    do { \
        err = call; \
        if (err) { \
            return frame_reader_handle(reader, err, fmt, ##__VA_ARGS__); \
        } \
    } while (0)

#define check_avro(err, reader, call) { check_handle(err, reader, call, "Avro error: %s", avro_strerror()); }

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
void schema_list_entry_decrefs(schema_list_entry *entry);
int read_entirely(frame_reader_t reader, avro_value_t *value, avro_reader_t avro_reader, const void *buf, size_t len);

int frame_reader_handle(frame_reader_t reader, int err, const char *fmt, ...) __attribute__ ((format (printf, 3, 4)));


int parse_frame(frame_reader_t reader, uint64_t wal_pos, char *buf, int buflen) {
    int err = 0;
    check(err, read_entirely(reader, &reader->frame_value, reader->avro_reader, buf, buflen));
    check(err, process_frame(&reader->frame_value, reader, wal_pos));
    return err;
}


int process_frame(avro_value_t *frame_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, msg_type;
    size_t num_messages;
    avro_value_t msg_val, union_val, record_val;

    check_avro(err, reader, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check_avro(err, reader, avro_value_get_size(&msg_val, &num_messages));

    for (int i = 0; i < num_messages; i++) {
        check_avro(err, reader, avro_value_get_by_index(&msg_val, i, &union_val, NULL));
        check_avro(err, reader, avro_value_get_discriminant(&union_val, &msg_type));
        check_avro(err, reader, avro_value_get_current_branch(&union_val, &record_val));

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
                return frame_reader_handle(reader, EINVAL,
                        "Unknown message type %d", msg_type);
        }
    }
    return err;
}

int process_frame_begin_txn(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0;
    avro_value_t xid_val;
    int64_t xid;

    check_avro(err, reader, avro_value_get_by_index(record_val, 0, &xid_val, NULL));
    check_avro(err, reader, avro_value_get_long(&xid_val, &xid));

    if (reader->on_begin_txn) {
        check_handle(err, reader, reader->on_begin_txn(reader->cb_context, wal_pos, (uint32_t) xid),
                "error in begin_txn callback for xid %" PRIu64, xid);
    }
    return err;
}

int process_frame_commit_txn(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0;
    avro_value_t xid_val;
    int64_t xid;

    check_avro(err, reader, avro_value_get_by_index(record_val, 0, &xid_val, NULL));
    check_avro(err, reader, avro_value_get_long(&xid_val, &xid));

    if (reader->on_commit_txn) {
        check_handle(err, reader, reader->on_commit_txn(reader->cb_context, wal_pos, (uint32_t) xid),
                "error in commit_txn callback for xid %" PRIu64, xid);
    }
    return err;
}

int process_frame_table_schema(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, key_schema_present;
    avro_value_t relid_val, key_schema_val, row_schema_val, branch_val;
    int64_t relid;
    const char *key_schema_json = NULL, *row_schema_json;
    size_t key_schema_len = 1, row_schema_len;
    avro_schema_t key_schema = NULL, row_schema;

    check_avro(err, reader, avro_value_get_by_index(record_val, 0, &relid_val,      NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 1, &key_schema_val, NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 2, &row_schema_val, NULL));
    check_avro(err, reader, avro_value_get_long(&relid_val, &relid));
    check_avro(err, reader, avro_value_get_discriminant(&key_schema_val, &key_schema_present));
    check_avro(err, reader, avro_value_get_string(&row_schema_val, &row_schema_json, &row_schema_len));
    check_avro(err, reader, avro_schema_from_json_length(row_schema_json, row_schema_len - 1, &row_schema));

    schema_list_entry *entry = schema_list_replace(reader, relid);
    entry->relid = relid;
    entry->row_schema = row_schema;
    entry->row_iface = avro_generic_class_from_schema(row_schema);
    avro_generic_value_new(entry->row_iface, &entry->row_value);
    avro_generic_value_new(entry->row_iface, &entry->old_value);
    entry->avro_reader = avro_reader_memory(NULL, 0);

    if (key_schema_present) {
        check_avro(err, reader, avro_value_get_current_branch(&key_schema_val, &branch_val));
        check_avro(err, reader, avro_value_get_string(&branch_val, &key_schema_json, &key_schema_len));
        check_avro(err, reader, avro_schema_from_json_length(key_schema_json, key_schema_len - 1, &key_schema));
        entry->key_schema = key_schema;
        entry->key_iface = avro_generic_class_from_schema(key_schema);
        avro_generic_value_new(entry->key_iface, &entry->key_value);
    } else {
        entry->key_schema = NULL;
    }

    if (reader->on_table_schema) {
        check_handle(err, reader,
                reader->on_table_schema(reader->cb_context, wal_pos, relid,
                    key_schema_json, key_schema_len - 1, key_schema,
                    row_schema_json, row_schema_len - 1, row_schema),
                "error in table_schema callback for relid %" PRIu64, relid);
    }
    return err;
}

int process_frame_insert(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, key_present;
    avro_value_t relid_val, key_val, new_val, branch_val;
    int64_t relid;
    const void *key_bin = NULL, *new_bin = NULL;
    size_t key_len = 0, new_len = 0;

    check_avro(err, reader, avro_value_get_by_index(record_val, 0, &relid_val, NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 1, &key_val,   NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 2, &new_val,   NULL));
    check_avro(err, reader, avro_value_get_long(&relid_val, &relid));
    check_avro(err, reader, avro_value_get_discriminant(&key_val, &key_present));
    check_avro(err, reader, avro_value_get_bytes(&new_val, &new_bin, &new_len));

    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (!entry) {
        return frame_reader_handle(reader, EINVAL,
                "Received insert for unknown relid %" PRIu64, relid);
    }

    if (key_present) {
        check_avro(err, reader, avro_value_get_current_branch(&key_val, &branch_val));
        check_avro(err, reader, avro_value_get_bytes(&branch_val, &key_bin, &key_len));
        check(err, read_entirely(reader, &entry->key_value, entry->avro_reader, key_bin, key_len));
    }

    check(err, read_entirely(reader, &entry->row_value, entry->avro_reader, new_bin, new_len));

    if (reader->on_insert_row) {
        check_handle(err, reader,
                reader->on_insert_row(reader->cb_context, wal_pos, relid,
                    key_bin, key_len, key_bin ? &entry->key_value : NULL,
                    new_bin, new_len, &entry->row_value),
                "error in insert_row callback for relid %" PRIu64, relid);
    }
    return err;
}

int process_frame_update(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, key_present, old_present;
    avro_value_t relid_val, key_val, old_val, new_val, branch_val;
    int64_t relid;
    const void *key_bin = NULL, *old_bin = NULL, *new_bin = NULL;
    size_t key_len = 0, old_len = 0, new_len = 0;

    check_avro(err, reader, avro_value_get_by_index(record_val, 0, &relid_val, NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 1, &key_val,   NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 2, &old_val,   NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 3, &new_val,   NULL));
    check_avro(err, reader, avro_value_get_long(&relid_val, &relid));
    check_avro(err, reader, avro_value_get_discriminant(&key_val, &key_present));
    check_avro(err, reader, avro_value_get_discriminant(&old_val, &old_present));
    check_avro(err, reader, avro_value_get_bytes(&new_val, &new_bin, &new_len));

    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (!entry) {
        return frame_reader_handle(reader, EINVAL,
                "Received update for unknown relid %" PRIu64, relid);
    }

    if (key_present) {
        check_avro(err, reader, avro_value_get_current_branch(&key_val, &branch_val));
        check_avro(err, reader, avro_value_get_bytes(&branch_val, &key_bin, &key_len));
        check(err, read_entirely(reader, &entry->key_value, entry->avro_reader, key_bin, key_len));
    }

    if (old_present) {
        check_avro(err, reader, avro_value_get_current_branch(&old_val, &branch_val));
        check_avro(err, reader, avro_value_get_bytes(&branch_val, &old_bin, &old_len));
        check(err, read_entirely(reader, &entry->old_value, entry->avro_reader, old_bin, old_len));
    }

    check(err, read_entirely(reader, &entry->row_value, entry->avro_reader, new_bin, new_len));

    if (reader->on_update_row) {
        check_handle(err, reader,
                reader->on_update_row(reader->cb_context, wal_pos, relid,
                    key_bin, key_len, key_bin ? &entry->key_value : NULL,
                    old_bin, old_len, old_bin ? &entry->old_value : NULL,
                    new_bin, new_len, &entry->row_value),
                "error in update_row callback for relid %" PRIu64, relid);
    }
    return err;
}

int process_frame_delete(avro_value_t *record_val, frame_reader_t reader, uint64_t wal_pos) {
    int err = 0, key_present, old_present;
    avro_value_t relid_val, key_val, old_val, branch_val;
    int64_t relid;
    const void *key_bin = NULL, *old_bin = NULL;
    size_t key_len = 0, old_len = 0;

    check_avro(err, reader, avro_value_get_by_index(record_val, 0, &relid_val, NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 1, &key_val,   NULL));
    check_avro(err, reader, avro_value_get_by_index(record_val, 2, &old_val,   NULL));
    check_avro(err, reader, avro_value_get_long(&relid_val, &relid));
    check_avro(err, reader, avro_value_get_discriminant(&key_val, &key_present));
    check_avro(err, reader, avro_value_get_discriminant(&old_val, &old_present));

    schema_list_entry *entry = schema_list_lookup(reader, relid);
    if (!entry) {
        return frame_reader_handle(reader, EINVAL,
                "Received delete for unknown relid %" PRIu64, relid);
    }

    if (key_present) {
        check_avro(err, reader, avro_value_get_current_branch(&key_val, &branch_val));
        check_avro(err, reader, avro_value_get_bytes(&branch_val, &key_bin, &key_len));
        check(err, read_entirely(reader, &entry->key_value, entry->avro_reader, key_bin, key_len));
    }

    if (old_present) {
        check_avro(err, reader, avro_value_get_current_branch(&old_val, &branch_val));
        check_avro(err, reader, avro_value_get_bytes(&branch_val, &old_bin, &old_len));
        check(err, read_entirely(reader, &entry->old_value, entry->avro_reader, old_bin, old_len));
    }

    if (reader->on_delete_row) {
        check_handle(err, reader,
                reader->on_delete_row(reader->cb_context, wal_pos, relid,
                    key_bin, key_len, key_bin ? &entry->key_value : NULL,
                    old_bin, old_len, old_bin ? &entry->old_value : NULL),
                "error in delete_row callback for relid %" PRIu64, relid);
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
        schema_list_entry_decrefs(entry);
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
    memset(new_entry, 0, sizeof(schema_list_entry));
    reader->schemas[reader->num_schemas] = new_entry;
    reader->num_schemas++;

    return new_entry;
}

/* Decrements the reference counts of a schema list entry. */
void schema_list_entry_decrefs(schema_list_entry *entry) {
    avro_reader_free(entry->avro_reader);
    avro_value_decref(&entry->old_value);
    avro_value_decref(&entry->row_value);
    avro_value_iface_decref(entry->row_iface);
    avro_schema_decref(entry->row_schema);

    if (entry->key_schema) {
        avro_value_decref(&entry->key_value);
        avro_value_iface_decref(entry->key_iface);
        avro_schema_decref(entry->key_schema);
    }
}

/* Frees all the memory structures associated with a frame reader. */
void frame_reader_free(frame_reader_t reader) {
    avro_reader_free(reader->avro_reader);
    avro_value_decref(&reader->frame_value);
    avro_value_iface_decref(reader->frame_iface);
    avro_schema_decref(reader->frame_schema);

    for (int i = 0; i < reader->num_schemas; i++) {
        schema_list_entry *entry = reader->schemas[i];
        schema_list_entry_decrefs(entry);
        free(entry);
    }

    free(reader->schemas);
    free(reader);
}

int frame_reader_handle(frame_reader_t reader, int err, const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vsnprintf(reader->error, FRAME_READER_ERROR_LEN, fmt, args);
    va_end(args);

    if (reader->on_error) {
        return reader->on_error(reader->cb_context, err, reader->error);
    } else return err;
}

/* Parses the contents of a binary-encoded Avro buffer into an Avro value, ensuring
 * that the entire buffer is read. */
int read_entirely(frame_reader_t reader, avro_value_t *value, avro_reader_t avro_reader, const void *buf, size_t len) {
    int err = 0;

    avro_reader_memory_set_source(avro_reader, buf, len);
    check_avro(err, reader, avro_value_read(avro_reader, value));

    // Expect the reading of the Avro value from the buffer to entirely consume the
    // buffer contents. If there's anything left at the end, something must be wrong.
    // Avro doesn't seem to provide a way of checking how many bytes remain, so we
    // test indirectly by trying to seek forward (expecting to see an error).
    if (avro_skip(avro_reader, 1) != ENOSPC) {
        return frame_reader_handle(reader, EINVAL, "Unexpected trailing bytes at the end of buffer");
    }

    return err;
}


/* Return: 0 if we should acknowledge wal_pos as flushed;
 *         FRAME_READER_SYNC_PENDING if we should not because we have
 *         transactions pending sync;
 *         anything else to signify an error. */
int handle_keepalive(frame_reader_t reader, uint64_t wal_pos) {
    if (reader->on_keepalive) {
        int err = 0;
        check(err, reader->on_keepalive(reader->cb_context, wal_pos));
    }
    return 0;
}
