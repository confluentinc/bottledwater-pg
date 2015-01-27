/* Conversion of Postgres server-side structures into the wire protocol, which
 * is emitted by the output plugin and consumed by the client. */

#include "protocol_server.h"
#include "io_util.h"

int update_frame_with_begin_txn(avro_value_t *union_val, ReorderBufferTXN *txn) {
    int err = 0;
    avro_value_t record_val, xid_val;

    check(err, avro_value_set_branch(union_val, 0, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    return err;
}

int update_frame_with_commit_txn(avro_value_t *union_val, ReorderBufferTXN *txn,
        XLogRecPtr commit_lsn) {
    int err = 0;
    avro_value_t record_val, xid_val, lsn_val;

    check(err, avro_value_set_branch(union_val, 1, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_get_by_index(&record_val, 1, &lsn_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    check(err, avro_value_set_long(&lsn_val, commit_lsn));
    return err;
}
