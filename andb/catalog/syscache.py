from .attribute import _ANDB_ATTRIBUTE
from .class_ import _ANDB_CLASS
from .database import _ANDB_DATABASE
from .oid import INVALID_OID
from .type import _ANDB_TYPE
from .index import _ANDB_INDEX

CATALOG_ANDB_ATTRIBUTE = _ANDB_ATTRIBUTE
CATALOG_ANDB_CLASS = _ANDB_CLASS
CATALOG_ANDB_DATABASE = _ANDB_DATABASE
CATALOG_ANDB_TYPE = _ANDB_TYPE
CATALOG_ANDB_INDEX = _ANDB_INDEX

_ALL_CATALOGS = (CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS,
                 CATALOG_ANDB_DATABASE, CATALOG_ANDB_TYPE, CATALOG_ANDB_INDEX)


def get_all_catalogs():
    return _ALL_CATALOGS


def get_attribute_by_name(table_name, column_name, database_oid):
    relation_oid = CATALOG_ANDB_CLASS.get_relation_oid(table_name, database_oid)
    if relation_oid == INVALID_OID:
        return None

    return CATALOG_ANDB_ATTRIBUTE.get_table_attr(relation_oid, column_name)


__all__ = _ALL_CATALOGS
