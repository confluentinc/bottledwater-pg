#ifndef JSON_H
#define JSON_H


// TODO refactor so json doesn't depend on registry
#include "registry.h"

#include <librdkafka/rdkafka.h>


topic_list_entry_t json_encode_msg(schema_registry_t registry, int64_t relid,
        const void *key_bin, size_t key_len,
        char **key_out, size_t *key_len_out,
        const void *row_bin, size_t row_len,
        char **row_out, size_t *row_len_out);


#endif /* JSON_H */
