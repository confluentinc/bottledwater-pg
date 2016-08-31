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

/* Handles an error according to the supplied policy.
 *
 * `message` should describe the context in which the error occurred, e.g. what
 * the calling function was attempting to do.
 *
 * `error` should describe the error that occurred.
 *
 * This will call `ereport` with an appropriate severity depending on the
 * policy, so this may cause an early exit from the calling function.
 */
void error_policy_handle(error_policy_t policy, const char *message, const char *error);

#endif /* ERROR_POLICY_H */
