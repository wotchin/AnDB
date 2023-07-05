from functools import partial

from andb.common import hash_functions
from ._base import CatalogTuple, CatalogTable
from .oid import INVALID_OID

VARIABLE_LENGTH = 0


def generic_cmp(a, b):
    return a - b


class AndbBaseType:
    oid = INVALID_OID
    type_name = 'undefined'
    type_bytes = VARIABLE_LENGTH
    type_char = 'x'
    type_default = 0
    hash_func = None


class IntegerType(AndbBaseType):
    oid = 1000
    type_name = 'integer'
    type_bytes = 4
    type_char = 'i'
    type_default = 0
    hash_func = partial(hash_functions.hash_int, length=4)


class BigintType(AndbBaseType):
    oid = 1001
    type_name = 'bigint'
    type_bytes = 8
    type_char = 'q'
    type_default = 0
    hash_func = partial(hash_functions.hash_int, length=8)


class RealType(AndbBaseType):
    oid = 1002
    type_name = 'real'
    type_bytes = 4
    type_char = 'f'
    type_default = 0.
    hash_func = partial(hash_functions.hash_float, length=4)


class DoubleType(AndbBaseType):
    oid = 1003
    type_name = 'double precision'
    type_bytes = 8
    type_char = 'd'
    type_default = 0.
    hash_func = partial(hash_functions.hash_float, length=8)


class BooleanType(AndbBaseType):
    oid = 1003
    type_name = 'double precision'
    type_bytes = 1
    type_char = 'b'
    type_default = False
    hash_func = hash_functions.hash_bool


class CharType(AndbBaseType):
    oid = 1005
    type_name = 'char'
    type_bytes = VARIABLE_LENGTH
    type_char = 'c'
    type_default = '\0'
    hash_func = hash_functions.hash_string


class VarcharType(AndbBaseType):
    oid = 1006
    type_name = 'varchar'
    type_bytes = VARIABLE_LENGTH
    type_char = 'v'
    type_default = ''
    hash_func = hash_functions.hash_string


class TextType(AndbBaseType):
    oid = 1007
    type_name = 'text'
    type_bytes = VARIABLE_LENGTH
    type_char = 'c'
    type_default = ''
    hash_func = hash_functions.hash_string


class AndbTypeTuple(CatalogTuple):
    __fields__ = {
        'oid': 'bigint',
        'type_name': 'text',
        'type_bytes': 'integer',
        'type_char': 'char'
    }

    def __init__(self, defined_type):
        self.oid = defined_type.oid
        self.type_name = defined_type.type_name
        self.type_bytes = defined_type.type_bytes
        self.type_char = defined_type.type_char
        self.type_default = defined_type.type_default

    def __lt__(self, other):
        return self.oid < other.oid


class AndbTypeTable(CatalogTable):
    __tablename__ = 'andb_type'

    def init(self):
        builtin_types = (
            IntegerType, BigintType, RealType, DoubleType,
            BooleanType, CharType, VarcharType, TextType
        )
        for t in builtin_types:
            self.insert(AndbTypeTuple(t))

    def get_type_meta(self, name):
        for r in self.rows:
            if name == r.type_name:
                return r

    def get_type_oid(self, name):
        meta = self.get_type_meta(name)
        return meta.oid if meta else INVALID_OID


