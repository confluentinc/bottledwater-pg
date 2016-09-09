/* This file (and the accompanying .c file) is shared between server-side code
 * (logical decoding output plugin) and client-side code (using libpq to connect
 * to the database). It defines the wire protocol in which the change stream
 * is sent from the server to the client. The protocol consists of "frames" (one
 * frame is generated for each call to the output plugin), and each frame may
 * contain multiple "messages". A message may indicate that a transaction started
 * or committed, that a row was inserted/updated/deleted, that a table schema
 * changed, etc. */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "avro.h"

/* Namespace for Avro records of the frame protocol */
#define PROTOCOL_SCHEMA_NAMESPACE "com.dattran.bottledwater.protocol"

/* Each message in the wire protocol is of one of these types */
#define PROTOCOL_MSG_BEGIN_TXN      0
#define PROTOCOL_MSG_COMMIT_TXN     1
#define PROTOCOL_MSG_TABLE_SCHEMA   2
#define PROTOCOL_MSG_INSERT         3
#define PROTOCOL_MSG_UPDATE         4
#define PROTOCOL_MSG_DELETE         5


/* Error policies, determining what the snapshot function and output plugin
 * should do if they encounter an error encoding a row.
 *
 * These should match the values of the bottledwater_error_policy_valid
 * constraint in bottledwater--0.1.sql.
 */
/* The default policy is "exit": an error will terminate the snapshot or
 * replication stream.  This policy should be used if avoiding data loss is the
 * top priority, since after manually resolving the error Bottled Water can be
 * restarted to retry the affected rows.
 */
#define PROTOCOL_ERROR_POLICY_EXIT "exit"
/* Under the "log" policy, an error will cause Bottled Water to skip over the
 * affected rows and continue, logging the error that occurred.  This means the
 * snapshot or replication stream may omit some updates that were successfully
 * committed to Postgres, if there was a problem encoding those updates.
 */
#define PROTOCOL_ERROR_POLICY_LOG "log"


avro_schema_t schema_for_frame(void);

#endif /* PROTOCOL_H */
