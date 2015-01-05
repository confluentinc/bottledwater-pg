#include "io_util.h"
#include "protocol.h"

avro_schema_t schema_for_begin_txn(void);
avro_schema_t schema_for_commit_txn(void);

avro_schema_t schema_for_frame() {
    avro_schema_t union_schema = avro_schema_union();

    avro_schema_t branch_schema = schema_for_begin_txn();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    branch_schema = schema_for_commit_txn();
    avro_schema_union_append(union_schema, branch_schema);
    avro_schema_decref(branch_schema);

    return union_schema;
}

avro_schema_t schema_for_begin_txn() {
    avro_schema_t record_schema = avro_schema_record("BeginTxn", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "xid", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
}

int update_frame_with_begin_txn(avro_value_t *union_val, ReorderBufferTXN *txn) {
    int err = 0;
    avro_value_t record_val, xid_val;

    check(err, avro_value_set_branch(union_val, 0, &record_val));
    check(err, avro_value_get_by_index(&record_val, 0, &xid_val, NULL));
    check(err, avro_value_set_long(&xid_val, txn->xid));
    return err;
}

avro_schema_t schema_for_commit_txn() {
    avro_schema_t record_schema = avro_schema_record("CommitTxn", PROTOCOL_SCHEMA_NAMESPACE);

    avro_schema_t field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "xid", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_long();
    avro_schema_record_field_append(record_schema, "lsn", field_schema);
    avro_schema_decref(field_schema);

    return record_schema;
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
