#ifndef ERROR_POLICY_H
#define ERROR_POLICY_H

typedef enum {
    ERROR_POLICY_UNDEFINED = 0,
    ERROR_POLICY_LOG,
    ERROR_POLICY_EXIT
} error_policy_t;

static const error_policy_t DEFAULT_ERROR_POLICY = ERROR_POLICY_EXIT;

error_policy_t parse_error_policy(const char *str);
const char* error_policy_name(error_policy_t policy);

#endif /* ERROR_POLICY_H */
