from functools import partial

from andb.common import hash_functions
from andb.common import cstructure
from ._base import CatalogForm, CatalogTable
from .oid import INVALID_OID
from andb.common.utils import memoize

VARIABLE_LENGTH = 0
NULL_LENGTH = -1
VARIABLE_TYPE_HEADER_LENGTH = 4  # int4
_VARIABLE_TYPE_CTYPE = cstructure.CTYPE_TYPE_INT4


def generic_cmp(a, b):
    return a - b


class AndbBaseType:
    oid = INVALID_OID
    type_name = 'undefined'
    type_alias = ''
    type_bytes = VARIABLE_LENGTH
    type_char = 'x'
    type_default = 0
    hash_func = None

    @classmethod
    def to_bytes(cls, v):
        return cstructure.pack(cls.type_char, v)

    @classmethod
    def to_datum(cls, b):
        return cstructure.unpack(cls.type_char, b)

    @staticmethod
    def cast_to_string(v):
        return str(v)

    @staticmethod
    def cast_from_string(v):
        raise NotImplementedError()

    @classmethod
    def bytes_length(cls, b):
        if b is None:
            return NULL_LENGTH
        return cls.type_bytes


class IntegerType(AndbBaseType):
    oid = 1000
    type_name = 'integer'
    type_alias = 'int'
    type_bytes = 4
    type_char = cstructure.CTYPE_TYPE_INT4
    type_default = 0
    hash_func = partial(hash_functions.hash_int, length=4)

    @staticmethod
    def cast_from_string(v):
        return int(v)


class BigintType(AndbBaseType):
    oid = 1001
    type_name = 'bigint'
    type_bytes = 8
    type_char = cstructure.CTYPE_TYPE_INT8
    type_default = 0
    hash_func = partial(hash_functions.hash_int, length=8)

    @staticmethod
    def cast_from_string(v):
        return int(v)


class RealType(AndbBaseType):
    oid = 1002
    type_name = 'real'
    type_alias = 'float'
    type_bytes = 4
    type_char = cstructure.CTYPE_TYPE_FLOAT4
    type_default = 0.
    hash_func = partial(hash_functions.hash_float, length=4)

    @staticmethod
    def cast_from_string(v):
        return float(v)


class DoubleType(AndbBaseType):
    oid = 1003
    type_name = 'double precision'
    type_alias = 'double'
    type_bytes = 8
    type_char = cstructure.CTYPE_TYPE_FLOAT8
    type_default = 0.
    hash_func = partial(hash_functions.hash_float, length=8)

    @staticmethod
    def cast_from_string(v):
        return int(v)


class BooleanType(AndbBaseType):
    oid = 1003
    type_name = 'boolean'
    type_alias = 'bool'
    type_bytes = 1
    type_char = cstructure.CTYPE_TYPE_BOOL
    type_default = False
    hash_func = hash_functions.hash_bool

    @staticmethod
    def cast_to_string(v):
        return 'true' if v else 'false'

    @staticmethod
    def cast_from_string(v):
        return v.lower() == 'true'


class CharType(AndbBaseType):
    oid = 1005
    type_name = 'char'
    type_bytes = 1
    type_char = cstructure.CTYPE_TYPE_CHAR
    type_default = '\0'
    hash_func = hash_functions.hash_string

    @classmethod
    def to_bytes(cls, v):
        if isinstance(v, int):
            v = chr(v)
        encoded_v = str.encode(v, encoding='utf8')
        return cstructure.pack(f'{len(encoded_v)}{cls.type_char}', encoded_v)

    @classmethod
    def to_datum(cls, b):
        return cstructure.unpack(f'{len(b)}{cls.type_char}', b).decode(encoding='utf8')

    @staticmethod
    def cast_from_string(v):
        return v


class VarcharType(AndbBaseType):
    oid = 1006
    type_name = 'varchar'
    type_bytes = VARIABLE_LENGTH
    type_char = cstructure.CTYPE_TYPE_CHAR_ARRAY
    type_default = ''
    hash_func = hash_functions.hash_string

    # notice: must be truncated ahead
    @classmethod
    def to_bytes(cls, v):
        encoded_v = str.encode(v, encoding='utf8')
        return cstructure.pack(f'{len(encoded_v)}{cls.type_char}', encoded_v)

    @classmethod
    def to_datum(cls, b):
        return cstructure.unpack(f'{len(b)}{cls.type_char}', b).decode(encoding='utf8')

    @staticmethod
    def cast_from_string(v):
        return v

    @classmethod
    def bytes_length(cls, b):
        if b is None:
            return NULL_LENGTH
        return len(b)


class TextType(AndbBaseType):
    oid = 1007
    type_name = 'text'
    type_bytes = VARIABLE_LENGTH
    type_char = cstructure.CTYPE_TYPE_CHAR_ARRAY
    type_default = ''
    hash_func = hash_functions.hash_string

    @classmethod
    def to_bytes(cls, v):
        encoded_v = str.encode(v, encoding='utf8')
        return (cstructure.pack(_VARIABLE_TYPE_CTYPE, len(encoded_v)) +
                cstructure.pack(f'{len(encoded_v)}{cls.type_char}', encoded_v))

    @classmethod
    def to_datum(cls, b):
        assert len(b) >= VARIABLE_TYPE_HEADER_LENGTH
        b_length = cstructure.unpack(_VARIABLE_TYPE_CTYPE, b[:VARIABLE_TYPE_HEADER_LENGTH])
        b_content = b[VARIABLE_TYPE_HEADER_LENGTH: VARIABLE_TYPE_HEADER_LENGTH + b_length]
        return cstructure.unpack(f'{len(b_content)}{cls.type_char}', b_content).decode(encoding='utf8')

    @staticmethod
    def cast_from_string(v):
        return v

    @classmethod
    def bytes_length(cls, b):
        if b is None:
            return NULL_LENGTH
        assert len(b) >= VARIABLE_TYPE_HEADER_LENGTH
        b_length = cstructure.unpack(_VARIABLE_TYPE_CTYPE, b[:VARIABLE_TYPE_HEADER_LENGTH])
        return b_length


class AndbTypeForm(CatalogForm):
    __fields__ = {
        'oid': 'bigint',
        'type_name': 'text',
        'type_alias': 'text',
        'type_bytes': 'integer',
        'type_char': 'char'
    }

    def __init__(self, defined_type):
        self.oid = defined_type.oid
        self.type_name = defined_type.type_name
        self.type_alias = defined_type.type_alias
        self.type_bytes = defined_type.type_bytes
        self.type_char = defined_type.type_char
        self.type_default = defined_type.type_default

    def __lt__(self, other):
        return self.oid < other.oid


_BUILTIN_TYPES = (
    IntegerType, BigintType, RealType, DoubleType,
    BooleanType, CharType, VarcharType, TextType
)

_BUILTIN_TYPES_DICT = {i.type_name: i for i in _BUILTIN_TYPES}


class AndbTypeTable(CatalogTable):
    __tablename__ = 'andb_type'

    def init(self):

        for t in _BUILTIN_TYPES:
            self.insert(AndbTypeForm(t))

    def __init__(self):
        super().__init__()
        self._lookup_cache = {}

    def get_type_form(self, name):
        if len(self._lookup_cache) == 0:
            for r in self.rows:
                self._lookup_cache[r.type_name] = r
                if r.type_alias != '':
                    self._lookup_cache[r.type_alias] = r
        r = self._lookup_cache[name]
        return _BUILTIN_TYPES_DICT[r.type_name]

    def get_type_oid(self, name):
        meta = self.get_type_form(name)
        return meta.oid if meta else INVALID_OID

    @memoize
    def get_type_name(self, oid):
        for r in self.rows:
            if r.oid == oid:
                return r.type_name

    def cast_datum_to_bytes(self, type_name, datum):
        meta = self.get_type_form(type_name)
        return meta.to_bytes(datum)

    def cast_bytes_to_datum(self, type_name, bytes_):
        meta = self.get_type_form(type_name)
        return meta.to_datum(bytes_)


_ANDB_TYPE = AndbTypeTable()
