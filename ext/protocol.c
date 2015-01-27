/* Definition of the wire protocol between the output plugin (running as an extension
 * in the PostgreSQL server) and the client (which connects to the replication slot).
 * This file is linked into both server and client. */

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
