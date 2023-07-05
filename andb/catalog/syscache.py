from .attribute import AndbAttributeTable
from .class_ import AndbClassTable
from .database import AndbDatabaseTable
from .type import AndbTypeTable

CATALOG_ANDB_ATTRIBUTE = AndbAttributeTable()
CATALOG_ANDB_CLASS = AndbClassTable()
CATALOG_ANDB_DATABASE = AndbDatabaseTable()
CATALOG_ANDB_TYPE = AndbTypeTable()

_ALL_CATALOGS = (CATALOG_ANDB_ATTRIBUTE, CATALOG_ANDB_CLASS,
                 CATALOG_ANDB_DATABASE, CATALOG_ANDB_TYPE)


def get_all_catalogs():
    return _ALL_CATALOGS
