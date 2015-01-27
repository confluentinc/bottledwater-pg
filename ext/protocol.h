#ifndef PROTOCOL_H
#define PROTOCOL_H

#include "avro.h"

#define PROTOCOL_SCHEMA_NAMESPACE "org.apache.samza.postgres.protocol"

avro_schema_t schema_for_frame(void);

#endif /* PROTOCOL_H */
