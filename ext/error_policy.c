#include "error_policy.h"
#include "protocol.h"

#include <string.h>
#include "postgres.h"

error_policy_t parse_error_policy(const char *str) {
    if (strcmp(PROTOCOL_ERROR_POLICY_LOG, str) == 0) {
        return ERROR_POLICY_LOG;
    } else if (strcmp(PROTOCOL_ERROR_POLICY_EXIT, str) == 0) {
        return ERROR_POLICY_EXIT;
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid error_policy: %s", str)));
        return ERROR_POLICY_UNDEFINED;
    }
}

const char* error_policy_name(error_policy_t policy) {
    switch (policy) {
        case ERROR_POLICY_LOG: return PROTOCOL_ERROR_POLICY_LOG;
        case ERROR_POLICY_EXIT: return PROTOCOL_ERROR_POLICY_EXIT;
        case ERROR_POLICY_UNDEFINED: return "undefined (probably a bug)";
        default: return "unknown (probably a bug)";
    }
}


void error_policy_handle(error_policy_t policy, const char *message, const char *error) {
    switch (policy) {
    case ERROR_POLICY_LOG:
        elog(WARNING, "%s: %s", message, error);
        break;
    case ERROR_POLICY_EXIT:
        elog(ERROR, "%s: %s", message, error);
    default:
        elog(WARNING, "%s: %s", message, error);
        elog(ERROR, "error_policy_handle: unknown error policy!");
    }
}
