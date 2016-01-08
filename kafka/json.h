#ifndef JSON_H
#define JSON_H


#include "table_mapper.h"


int json_encode_msg(table_metadata_t table,
        const void *key_bin, size_t key_len,
        char **key_out, size_t *key_len_out,
        const void *row_bin, size_t row_len,
        char **row_out, size_t *row_len_out);


#endif /* JSON_H */
