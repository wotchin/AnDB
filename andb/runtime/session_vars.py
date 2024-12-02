from threading import local

from andb.catalog.oid import OID_DATABASE_ANDB
from andb.constants.macros import INVALID_XID

class SessionVars(local):
    database_oid = OID_DATABASE_ANDB
    session_xid = INVALID_XID

