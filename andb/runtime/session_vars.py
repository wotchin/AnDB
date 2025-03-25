import logging
from threading import local

from andb.catalog.oid import OID_DATABASE_ANDB


# For user configuration during the session
# SessionParameter is only associated with AI-related parameters
# Separating parameters and vars is easier to adjust models
# Notice: parameters should be defined in advance, not supported to add a new one while runtime!
class SessionParameter(local):
    # Client model related parameters
    client_llm = 'external_api'
    client_external_api_model = 'deepseek-chat'
    external_api_key = None

    # Embedding model related parameters
    embed_llm = 'offline'
    embed_offline_model_path = 'sentence-transformers/all-MiniLM-L6-v2'

class SessionVars(local):
    database_oid = OID_DATABASE_ANDB
    session_xid = None


# The instantiated objects
_SESSION_PARAMS = SessionParameter()
_SESSION_VARS = SessionVars()


def set_session_value(k, v):
    if hasattr(_SESSION_PARAMS, k):
        setattr(_SESSION_PARAMS, k, v)
        logging.info(f"Set session parameter {k} to {v}, "
                     f"previous value: {getattr(_SESSION_PARAMS, k, '')}")
    elif hasattr(_SESSION_VARS, k):
        setattr(_SESSION_VARS, k, v)
        logging.info(f"Set session variable {k} to {v}, "
                     f"previous value: {getattr(_SESSION_VARS, k, '')}")
    else:
        setattr(_SESSION_VARS, k, v)
        logging.warning(f"Session variable '{k}' is not recognized but set to '{v}'")


def get_session_value(k):
    v = None
    if hasattr(_SESSION_PARAMS, k):
        v = getattr(_SESSION_PARAMS, k)
    elif hasattr(_SESSION_VARS, k):
        v = getattr(_SESSION_VARS, k)
    logging.info(f"Getting the value of session parameter/variable {k} is '{v}'.")
    return v

