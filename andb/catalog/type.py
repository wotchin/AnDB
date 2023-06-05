from functools import partial

from andb.common import hash_functions

from ._base import OID_TYPE_START, OID_TYPE_END

VARIABLE_LENGTH = 0


def generic_cmp(a, b):
    return a - b


class SystemCatalogType:
    def __init__(self, oid, type_name, type_bytes, type_char,
                 type_default, hash_func, cmp_func=None):
        assert OID_TYPE_START <= oid < OID_TYPE_END

        self.oid = oid
        self.type_name = type_name
        self.type_bytes = type_bytes  # 0 means VARIABLE_LENGTH
        self.type_char = type_char
        self.type_default = type_default
        self.hash_func = hash_func
        if cmp_func is None:
            self.cmp_func = generic_cmp


class TypeDefiner:
    def __init__(self):
        self._oid = OID_TYPE_START
        self.defined = None

    def define(self):
        types = (
            SystemCatalogType(oid=1000, type_name='integer', type_bytes=4, type_char='i',
                              type_default=0, hash_func=partial(hash_functions.hash_int, length=4)),
            SystemCatalogType(oid=1001, type_name='bigint', type_bytes=8, type_char='q',
                              type_default=0, hash_func=partial(hash_functions.hash_int, length=8)),
            SystemCatalogType(oid=1002, type_name='real', type_bytes=4, type_char='f',
                              type_default=0., hash_func=partial(hash_functions.hash_float, length=4)),
            SystemCatalogType(oid=1003, type_name='double precision', type_bytes=8, type_char='d',
                              type_default=0., hash_func=partial(hash_functions.hash_float, length=8)),
            SystemCatalogType(oid=1004, type_name='boolean', type_bytes=1, type_char='b',
                              type_default=False, hash_func=hash_functions.hash_bool),
            SystemCatalogType(oid=1005, type_name='char', type_bytes=VARIABLE_LENGTH, type_char='c',
                              type_default=0, hash_func=hash_functions.hash_string),
            SystemCatalogType(oid=1006, type_name='varchar', type_bytes=VARIABLE_LENGTH, type_char='v',
                              type_default=0, hash_func=hash_functions.hash_string),
            SystemCatalogType(oid=1007, type_name='text', type_bytes=VARIABLE_LENGTH, type_char='c',
                              type_default=0, hash_func=hash_functions.hash_string)
        )

        # todo: check
        self.defined = types

