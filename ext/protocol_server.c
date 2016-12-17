/* Conversion of Postgres server-side structures into the wire protocol, which
 * is emitted by the output plugin and consumed by the client. */

#include "protocol_server.h"
#include "io_util.h"
#include "oid2avro.h"

#include <stdarg.h>
#include <string.h>
#include "access/heapam.h"

int extract_tuple_key(schema_cache_entry *entry, Relation rel, TupleDesc tupdesc, HeapTuple tuple, bytea **key_out);
int update_frame_with_table_schema(avro_value_t *frame_val, schema_cache_entry *entry);
int update_frame_with_insert_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *new_bin);
int update_frame_with_update_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *old_bin, bytea *new_bin);
int update_frame_with_delete_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *old_bin);

/* Populates a wire protocol message for a "begin transaction" event. */
int update_frame_with_begin_txn(avro_value_t *frame_val, ReorderBufferTXN *txn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_BEGIN_TXN, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    return err;
}

/* Populates a wire protocol message for a "commit transaction" event. */
int update_frame_with_commit_txn(avro_value_t *frame_val, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val, lsn_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_COMMIT_TXN, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &lsn_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    check(err, avro_value_set_long(&lsn_val, commit_lsn));
    return err;
}

/* If we're using a primary key/replica identity index for a given table, this
 * function extracts that index' columns from a row tuple, and encodes the values
 * as an Avro string using the table's key schema. */
int extract_tuple_key(schema_cache_entry *entry, Relation rel, TupleDesc tupdesc, HeapTuple tuple, bytea **key_out) {
    int err = 0;
    Relation index_rel;

    if (entry->key_schema) {
        check(err, avro_value_reset(&entry->key_value));

        index_rel = table_key_index(rel);
        err = tuple_to_avro_key(&entry->key_value, tupdesc, tuple, rel, index_rel->rd_index);
        relation_close(index_rel, AccessShareLock);

        if (err) {
            return err;
        }

        check(err, try_writing(key_out, &write_avro_binary, &entry->key_value));
    }
    return err;
}

/* Updates the given frame value with a tuple inserted into a table. The table
 * schema is automatically included in the frame if it's not in the cache. This
 * function is used both during snapshot and during stream replication.
 *
 * The TupleDesc parameter is not redundant. During stream replication, it is just
 * RelationGetDescr(rel), but during snapshot it is taken from the result set.
 * The difference is that the result set tuple has dropped (logically invisible)
 * columns omitted. */
int update_frame_with_insert(avro_value_t *frame_val, schema_cache_t cache, Relation rel, TupleDesc tupdesc, HeapTuple newtuple) {
    int err = 0;
    schema_cache_entry *entry;
    bytea *key_bin = NULL, *new_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed < 0) {
        return EINVAL;
    } else if (changed != SCHEMA_EXIST) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    check(err, extract_tuple_key(entry, rel, tupdesc, newtuple, &key_bin));
    check(err, avro_value_reset(&entry->row_value));
    check(err, tuple_to_avro_row(&entry->row_value, tupdesc, newtuple));
    check(err, try_writing(&new_bin, &write_avro_binary, &entry->row_value));
    check(err, update_frame_with_insert_raw(frame_val, RelationGetRelid(rel), key_bin, new_bin));

    if (key_bin) pfree(key_bin);
    pfree(new_bin);
    return err;
}

/* Updates the given frame with information about a table row that was modified.
 * This is used only during stream replication. */
int update_frame_with_update(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple oldtuple, HeapTuple newtuple) {
    int err = 0;
    schema_cache_entry *entry;
    bytea *old_bin = NULL, *new_bin = NULL, *old_key_bin = NULL, *new_key_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed < 0) {
        return EINVAL;
    } else if (changed != SCHEMA_EXIST) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    /* oldtuple is non-NULL when replident = FULL, or when replident = DEFAULT and there is no
     * primary key, or replident = DEFAULT and the primary key was not modified by the update. */
    if (oldtuple) {
        check(err, extract_tuple_key(entry, rel, RelationGetDescr(rel), oldtuple, &old_key_bin));
        check(err, avro_value_reset(&entry->row_value));
        check(err, tuple_to_avro_row(&entry->row_value, RelationGetDescr(rel), oldtuple));
        check(err, try_writing(&old_bin, &write_avro_binary, &entry->row_value));
    }

    check(err, extract_tuple_key(entry, rel, RelationGetDescr(rel), newtuple, &new_key_bin));
    check(err, avro_value_reset(&entry->row_value));
    check(err, tuple_to_avro_row(&entry->row_value, RelationGetDescr(rel), newtuple));
    check(err, try_writing(&new_bin, &write_avro_binary, &entry->row_value));

    if (old_key_bin != NULL && (VARSIZE(old_key_bin) != VARSIZE(new_key_bin) ||
            memcmp(VARDATA(old_key_bin), VARDATA(new_key_bin), VARSIZE(new_key_bin) - VARHDRSZ) != 0)) {
        /* If the primary key changed, turn the update into a delete and an insert. */
        check(err, update_frame_with_delete_raw(frame_val, RelationGetRelid(rel), old_key_bin, old_bin));
        check(err, update_frame_with_insert_raw(frame_val, RelationGetRelid(rel), new_key_bin, new_bin));
    } else {
        check(err, update_frame_with_update_raw(frame_val, RelationGetRelid(rel), new_key_bin, old_bin, new_bin));
    }

    if (old_key_bin) pfree(old_key_bin);
    if (new_key_bin) pfree(new_key_bin);
    if (old_bin) pfree(old_bin);
    pfree(new_bin);
    return err;
}

/* Updates the given frame with information about a table row that was deleted.
 * This is used only during stream replication. */
int update_frame_with_delete(avro_value_t *frame_val, schema_cache_t cache, Relation rel, HeapTuple oldtuple) {
    int err = 0;
    schema_cache_entry *entry;
    bytea *key_bin = NULL, *old_bin = NULL;

    int changed = schema_cache_lookup(cache, rel, &entry);
    if (changed < 0) {
        return EINVAL;
    } else if (changed != SCHEMA_EXIST) {
        check(err, update_frame_with_table_schema(frame_val, entry));
    }

    if (oldtuple) {
        check(err, extract_tuple_key(entry, rel, RelationGetDescr(rel), oldtuple, &key_bin));
        check(err, avro_value_reset(&entry->row_value));
        check(err, tuple_to_avro_row(&entry->row_value, RelationGetDescr(rel), oldtuple));
        check(err, try_writing(&old_bin, &write_avro_binary, &entry->row_value));
    }

    check(err, update_frame_with_delete_raw(frame_val, RelationGetRelid(rel), key_bin, old_bin));

    if (key_bin) pfree(key_bin);
    if (old_bin) pfree(old_bin);
    return err;
}

/* Sends Avro schemas for a table to the client. This is called the first time we send
 * row-level events for a table, as well as every time the schema changes. All subsequent
 * inserts/updates/deletes are assumed to be encoded with this schema. */
int update_frame_with_table_schema(avro_value_t *frame_val, schema_cache_entry *entry) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_schema_val,
                 row_schema_val, branch_val;
    bytea *key_schema_json = NULL, *row_schema_json = NULL;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_TABLE_SCHEMA, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,      NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_schema_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &row_schema_val, NULL));
    check(err, avro_value_set_long(&relid_val, entry->relid));

    if (entry->key_schema) {
        check(err, try_writing(&key_schema_json, &write_schema_json, entry->key_schema));
        check(err, avro_value_set_branch(&key_schema_val, 1, &branch_val));
        check(err, avro_value_set_string_len(&branch_val, VARDATA(key_schema_json),
                    VARSIZE(key_schema_json) - VARHDRSZ + 1));
        pfree(key_schema_json);
    } else {
        check(err, avro_value_set_branch(&key_schema_val, 0, NULL));
    }

    check(err, try_writing(&row_schema_json, &write_schema_json, entry->row_schema));
    check(err, avro_value_set_string_len(&row_schema_val, VARDATA(row_schema_json),
                VARSIZE(row_schema_json) - VARHDRSZ + 1));
    pfree(row_schema_json);
    return err;
}

/* Populates a wire protocol message for an insert event. */
int update_frame_with_insert_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *new_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_val, newrow_val, branch_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_INSERT, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_val,    NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &newrow_val, NULL));
    check(err, avro_value_set_long(&relid_val, relid));
    check(err, avro_value_set_bytes(&newrow_val, VARDATA(new_bin), VARSIZE(new_bin) - VARHDRSZ));

    if (key_bin) {
        check(err, avro_value_set_branch(&key_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(key_bin), VARSIZE(key_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&key_val, 0, NULL));
    }
    return err;
}

/* Populates a wire protocol message for an update event. */
int update_frame_with_update_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin,
        bytea *old_bin, bytea *new_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_val, oldrow_val, newrow_val, branch_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_UPDATE, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,  NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_val,    NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &oldrow_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 3, &newrow_val, NULL));
    check(err, avro_value_set_long(&relid_val, relid));
    check(err, avro_value_set_bytes(&newrow_val, VARDATA(new_bin), VARSIZE(new_bin) - VARHDRSZ));

    if (key_bin) {
        check(err, avro_value_set_branch(&key_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(key_bin), VARSIZE(key_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&key_val, 0, NULL));
    }

    if (old_bin) {
        check(err, avro_value_set_branch(&oldrow_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(old_bin), VARSIZE(old_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&oldrow_val, 0, NULL));
    }
    return err;
}

/* Populates a wire protocol message for a delete event. */
int update_frame_with_delete_raw(avro_value_t *frame_val, Oid relid, bytea *key_bin, bytea *old_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, relid_val, key_val, oldrow_val, branch_val;

    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, PROTOCOL_MSG_DELETE, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &relid_val,   NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &key_val,     NULL));
    check(err, avro_value_get_by_index(&record_val, 2, &oldrow_val,  NULL));
    check(err, avro_value_set_long(&relid_val, relid));

    if (key_bin) {
        check(err, avro_value_set_branch(&key_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(key_bin), VARSIZE(key_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&key_val, 0, NULL));
    }

    if (old_bin) {
        check(err, avro_value_set_branch(&oldrow_val, 1, &branch_val));
        check(err, avro_value_set_bytes(&branch_val, VARDATA(old_bin), VARSIZE(old_bin) - VARHDRSZ));
    } else {
        check(err, avro_value_set_branch(&oldrow_val, 0, NULL));
    }
    return err;
}
