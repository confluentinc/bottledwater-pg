#ifndef REGISTRY_H
#define REGISTRY_H

#include <librdkafka/rdkafka.h>
#include <curl/curl.h>
#include <avro.h>

/* 5 bytes prefix is added by schema_registry_encode_msg(). */
#define SCHEMA_REGISTRY_MESSAGE_PREFIX_LEN 5

#define SCHEMA_REGISTRY_ERROR_LEN 512

typedef struct {
    CURL *curl;                            /* HTTP client for making requests to schema registry */
    struct curl_slist *curl_headers;       /* HTTP headers for requests to schema registry */
    char curl_error[CURL_ERROR_SIZE];      /* Buffer for libcurl error messages */
    char error[SCHEMA_REGISTRY_ERROR_LEN]; /* Buffer for general error messages */
    char *registry_url;                    /* URL of server */
} schema_registry;

typedef schema_registry *schema_registry_t;

schema_registry_t schema_registry_new(const char *url);
int schema_registry_request(schema_registry_t registry, const char* name,
        int is_key, const char *key,
        const char *schema_json, size_t schema_len,
        int *schema_id_out);
int schema_registry_encode_msg(int key_schema_id, int row_schema_id,
        const void *key_bin, size_t key_len, void **key_out, size_t *key_len_out,
        const void *row_bin, size_t row_len, void **row_out, size_t *row_len_out);
void schema_registry_free(schema_registry_t reader);


#endif /* REGISTRY_H */
