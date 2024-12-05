from andb.catalog.buitin_functions import cosine_distance
from andb.catalog.type import cast_value
from ._base import CatalogTable, CatalogForm
from .oid import (
    OID_SYSTEM_TABLE_FUNCTIONS,
    INVALID_OID,
    OID_FUNCTION_START,
    OID_FUNCTION_END,
    OID_DATABASE_ANDB
)
from andb.errno.errors import DDLException
from functools import partial

class FunctionKinds:
    BUILTIN = 'b'
    USER_DEFINED = 'u'

class AndbFunctionForm(CatalogForm):
    __fields__ = {
        'oid': 'bigint',
        'database_oid': 'bigint',
        'name': 'text',
        'kind': 'char',
        'return_type': 'text',
        'arg_count': 'int',
        'arg_types': 'text',  # Comma-separated types
    }

    def __init__(self, oid, database_oid, name, kind, return_type, arg_types, arg_count):
        self.oid = oid
        self.database_oid = database_oid
        self.name = name
        self.kind = kind
        self.return_type = return_type
        self.arg_types = arg_types
        self.arg_count = arg_count
    
    def __lt__(self, other):
        return self.oid < other.oid
    
BUILTIN_FUNCTIONS = [
    {
        'name': 'cosine_distance',
        'return_type': 'float',
        'arg_types': ['vector', 'vector'],
        'callback': cosine_distance
    },
]

class AndbFunctionTable(CatalogTable):
    __tablename__ = 'andb_function'
    __oid__ = OID_SYSTEM_TABLE_FUNCTIONS
    __form__ = AndbFunctionForm

    def __init__(self):
        super().__init__()
        self.builtin_functions = {}
        # construct builtin function mapping
        for func in BUILTIN_FUNCTIONS:
            self.builtin_functions[func['name']] = func['callback']

    def init(self):
        # initialize builtin functions
        for func in BUILTIN_FUNCTIONS:
            self.register_builtin_function(
                name=func['name'],
                return_type=func['return_type'],
                arg_types=func['arg_types'],
                callback=func['callback']
        )

    def register_builtin_function(self, name, return_type, arg_types, callback):
        """
        register builtin function to catalog and memory mapping.
        """
        next_oid = self.allocate_oid()
        if next_oid == INVALID_OID:
            raise DDLException(f'Function OID {next_oid} cannot be allocated.')

        # insert to catalog table
        self.insert(AndbFunctionForm(
            oid=next_oid,
            database_oid=OID_DATABASE_ANDB,
            name=name,
            kind=FunctionKinds.BUILTIN,
            return_type=return_type,
            arg_types=','.join(arg_types),
            arg_count=len(arg_types)
        ))


    def allocate_oid(self):
        if len(self.rows) == 0:
            return OID_FUNCTION_START
        oid = self.rows[-1].oid + 1
        if oid > OID_FUNCTION_END:
            return INVALID_OID
        return oid

    def get_function_oid(self, name, database_oid=None, kind=None):
        """
        get function OID by name (and optional database OID and kind).
        """
        results = self.search(lambda r: r.name == name and
                                     (database_oid is None or r.database_oid == database_oid) and
                                     (kind is None or r.kind == kind))
        if len(results) != 1:
            return INVALID_OID
        return results[0].oid

    def get_callback_by_name(self, name, database_oid=None):
        """
        get callback function by name.
        """
        # only handle builtin functions
        if name in self.builtin_functions:
            return self.builtin_functions[name]
        # maybe functions are not loaded yet
        
        # TODO: handle user-defined functions
        raise NotImplementedError(f"Function '{name}' not implemented.")
    
    def get_function_types(self, function_name, database_oid):
        results = self.search(lambda r: r.name == function_name and
                                     r.database_oid == database_oid)
        if len(results) != 1:
            raise DDLException(f'Function {function_name} not found.')
        return results[0].arg_types.split(','), results[0].return_type
        
    def perform_function(self, function_name, database_oid, args):
        callback = self.get_callback_by_name(function_name, database_oid)

        arg_types, return_type = self.get_function_types(function_name, database_oid)
        casted_args = [cast_value(arg, arg_type) for arg, arg_type in zip(args, arg_types)]
        return_value = callback(*casted_args)
        return cast_value(return_value, return_type)

_ANDB_FUNCTIONS = AndbFunctionTable()
