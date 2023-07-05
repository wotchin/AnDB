import os

from andb.constants.filename import (CATALOG_DIR, BASE_DIR,
                                     XACT_DIR, LOG_DIR, WAL_DIR)
from andb.catalog.syscache import get_all_catalogs
from andb.catalog.oid import OID_DATABASE_ANDB


def initialize_data_dir(path):
    if os.path.exists(path):
        return False, 'data directory already exists.'

    os.umask(0o0077)

    # create directories
    os.mkdir(path)
    os.mkdir(os.path.join(path, CATALOG_DIR))
    os.mkdir(os.path.join(path, BASE_DIR))
    os.mkdir(os.path.join(path, XACT_DIR))
    os.mkdir(os.path.join(path, LOG_DIR))
    os.mkdir(os.path.join(path, WAL_DIR))

    # Switch current working directory to the path!
    os.chdir(path)
    # generate metadata files
    for catalog_table in get_all_catalogs():
        catalog_table.init()

    # create a default database -- andb
    os.mkdir(os.path.join(BASE_DIR, OID_DATABASE_ANDB))
