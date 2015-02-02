/* Conversion of Postgres server-side structures into the wire protocol, which
 * is emitted by the output plugin and consumed by the client. */

#include "protocol_server.h"
#include "io_util.h"

#include <stdarg.h>
#include "utils/lsyscache.h"

int give_bytea_as_string(avro_value_t *dst, bytea *src);
int give_bytea_as_bytes(avro_value_t *dst, bytea *src);
uint64 fnv_hash(uint64 base, char *str, int len);
uint64 fnv_format(uint64 base, char *fmt, ...);
uint64 schema_hash_for_relation(Relation rel);

int update_frame_with_begin_txn(avro_value_t *frame_val, ReorderBufferTXN *txn) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, xid_val;

    check(err, avro_value_reset(frame_val));
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

    check(err, avro_value_reset(frame_val));
    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, 1, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &lsn_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    check(err, avro_value_set_long(&lsn_val, commit_lsn));
    return err;
}

int update_frame_with_insert(avro_value_t *frame_val, bytea *schema_json, bytea *value_bin) {
    int err = 0;
    avro_value_t msg_val, union_val, record_val, schema_val, value_val;

    check(err, avro_value_reset(frame_val));
    check(err, avro_value_get_by_index(frame_val, 0, &msg_val, NULL));
    check(err, avro_value_append(&msg_val, &union_val, NULL));
    check(err, avro_value_set_branch(&union_val, 2, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &schema_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &value_val,  NULL));
    check(err, give_bytea_as_string(&schema_val, schema_json));
    check(err, give_bytea_as_bytes(&value_val, value_bin));
    return err;
}

int give_bytea_as_string(avro_value_t *dst, bytea *src) {
    int err = 0;
    avro_wrapped_buffer_t buf;
    check(err, avro_wrapped_buffer_new(&buf, VARDATA(src), VARSIZE(src) - VARHDRSZ));
    check(err, avro_value_give_string_len(dst, &buf));
    return err;
}

int give_bytea_as_bytes(avro_value_t *dst, bytea *src) {
    int err = 0;
    avro_wrapped_buffer_t buf;
    check(err, avro_wrapped_buffer_new(&buf, VARDATA(src), VARSIZE(src) - VARHDRSZ));
    check(err, avro_value_give_bytes(dst, &buf));
    return err;
}
