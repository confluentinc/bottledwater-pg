/* TODO docs */

#include "json.h"

#include <avro.h>
#include <stdio.h>

int avro_bin_to_json(avro_schema_t schema,
        const void *val_bin, size_t val_len,
        char **val_out, size_t *val_len_out);


topic_list_entry_t json_encode_msg(schema_registry_t registry, int64_t relid,
        const void *key_bin, size_t key_len,
        char **key_out, size_t *key_len_out,
        const void *row_bin, size_t row_len,
        char **row_out, size_t *row_len_out) {
    topic_list_entry_t entry = topic_list_lookup(registry, relid);
    if (!entry) {
        registry_error(registry, "relid %" PRIu64 " has no registered schema", relid);
        return NULL;
    }

    int err;
    err = avro_bin_to_json(entry->key_schema, key_bin, key_len, key_out, key_len_out);
    if (err) {
      registry_error(registry, "error %d encoding key", err);
      return NULL;
    }
    err = avro_bin_to_json(entry->row_schema, row_bin, row_len, row_out, row_len_out);
    if (err) {
      registry_error(registry, "error %d encoding row", err);
      return NULL;
    }

    return entry;
}


int avro_bin_to_json(avro_schema_t schema,
        const void *val_bin, size_t val_len,
        char **val_out, size_t *val_len_out) {
    if (!val_bin) {
        *val_out = NULL;
        return 0;
    } else if (!schema) {
        /* got a value where we didn't expect one, and no schema to decode it */
        *val_out = NULL;
        return EINVAL;
    }

    avro_reader_t reader = avro_reader_memory(val_bin, val_len);

    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    // TODO error handling?
    avro_value_t value;
    avro_generic_value_new(iface, &value);
    // TODO error handling?

    int err = avro_value_read(reader, &value);
    if (err) {
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_reader_free(reader);
        return err;
    }

    err = avro_value_to_json(&value, 1, val_out);
    if (err) {
        avro_value_decref(&value);
        avro_value_iface_decref(iface);
        avro_reader_free(reader);
        return err;
    }

    *val_len_out = strlen(*val_out); // not including null terminator - to librdkafka it's just bytes

    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avro_reader_free(reader);

    return 0;
}
