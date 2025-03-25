import threading
import os

from andb.cmd.setup import setup_data_dir
from andb.initializer import init_all_database_components
from andb.entrance import execute_simple_query
from andb.net.postgres import PGHandler, start_server
from andb.executor.portal import ExecuteResultSet, ExecutionResult
from andb.catalog.type import (IntegerType, BigintType,
                               RealType, DoubleType,
                               BooleanType, CharType,
                               VarcharType, TextType,
                               VectorType)
from andb.catalog import CATALOG_ANDB_TYPE
from andb.net.postgres import Int8Field, TextField
from andb.runtime.session_vars import set_session_value, get_session_value

# TODO: change it
TEST_DATA_DIRECTOR = os.path.join(os.path.realpath(os.path.dirname(__file__)),
                                  'local_client_data')

# TODO: for the quick development, we only support integer8 type to a standard counterpart.
# other types are all transferred to the text.
_PG_TYPE_MAPPER = {
    IntegerType: Int8Field,

}


def bytes_to_str(b):
    return b.strip(b'\x00').decode()


def init_database():
    if os.path.exists(TEST_DATA_DIRECTOR):
        print("data directory already exists, reusing it then.")
    else:
        setup_data_dir(TEST_DATA_DIRECTOR)
    init_all_database_components(TEST_DATA_DIRECTOR)


class AnDBHandler(PGHandler):
    def set_session_info(self, parameters):
        pair = []
        for i, p in enumerate(parameters):
            pair.append(bytes_to_str(p))
            if i % 2 != 0:
                k, v = pair
                set_session_value(k, v)
                pair.clear()
        set_session_value('id', str(threading.get_native_id()))
        set_session_value('client', '%s:%d' % self.client_address)

    def check_password(self, password: bytes):
        # TODO: ACL by andb_auth
        client = get_session_value('client')
        user = get_session_value('user')
        return bytes_to_str(password) == 'andb_temp_password'  # TODO: change the testing password

    def query(self, sql):
        result = execute_simple_query(sql)
        if isinstance(result, ExecuteResultSet):
            # for SELECT clause
            fields = []
            rows = []
            for form in result.attr_forms:
                andb_type = CATALOG_ANDB_TYPE.get_type_form_by_oid(form.type_oid)
                pg_type = _PG_TYPE_MAPPER.get(andb_type, TextField)
                fields.append(pg_type(form.name))

            for tup in result.tuples:
                row = []  # row for client, tuple for internal database
                for i, value in enumerate(tup):
                    if isinstance(fields[i], TextField):
                        row.append(str(value))
                    else:
                        row.append(value)
                    rows.append(row)
            if not result.notice:
                self.notice = result.notice
            if not result.warning:
                self.warning = result.warning

        elif isinstance(result, ExecutionResult):
            # for non-SELECT clause
            # temporary implementation as follows
            fields = [Int8Field('effect rows')]
            rows = [result.effect_rows]
        else:
            raise NotImplementedError('Not supported the result type.')
        return fields, rows


def run_andb_server():
    init_database()
    start_server('localhost', 54321, AnDBHandler)


if __name__ == '__main__':
    run_andb_server()
