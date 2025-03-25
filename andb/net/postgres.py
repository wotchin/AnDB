"""Implement the Postgres wire-protocol for AnDB, which allows AnDB to reuse PostgreSQL client."""
import io
import logging
import socketserver
import struct
from enum import Enum
from typing import Union

from andb.errno.errors import RollbackError


class FeMessageType(Enum):
    PASSWORD_MESSAGE = b'p'
    QUERY = b'Q'
    TERMINATION = b'X'


class IOBuffer:
    def __init__(self, buffer=None):
        if not buffer:
            # employ bytearray() is also okay
            buffer = io.BytesIO()
        self.buffer = buffer

    def read_byte(self):
        return self.buffer.read(1)

    def read_bytes(self, n):
        data = self.buffer.read(n)
        if not data:
            raise IOError('cannot read from buffer.')
        return data

    def read_int32(self):
        data = self.read_bytes(4)
        return struct.unpack('!i', data)[0]

    def read_int16(self):
        data = self.read_bytes(2)
        return struct.unpack('!H', data)[0]

    def read_parameters(self, n):
        data = self.read_bytes(n)
        return data.split(b'\x00')

    def write_bytes(self, value: bytes):
        self.buffer.write(value)
        self.buffer.flush()

    def write_int32(self, v):
        data = struct.pack('!i', v)
        self.write_bytes(data)

    def write_int16(self, v):
        data = struct.pack('!h', v)
        self.write_bytes(data)

    def write_string(self, v: Union[str, bytes]):
        if isinstance(v, str):
            bytes_content = v.encode()
        elif isinstance(v, bytes):
            bytes_content = v
        else:
            raise
        self.write_bytes(bytes_content)
        # bug here?
        self.write_bytes(b'\x00')

    def to_bytes(self):
        return self.buffer.getvalue()

    def __bytes__(self):
        return self.to_bytes()


# PostgreSQL protocol reference:
# https://www.postgresql.org/docs/current/protocol-flow.html
# https://www.postgresql.org/docs/current/protocol-message-formats.html
class Message:
    def __init__(self, buffer: IOBuffer):
        self.buffer = buffer

    def read(self, **kwargs):
        pass

    def write(self, **kwargs):
        pass


class StartupMessage(Message):
    def read(self):
        length = self.buffer.read_int32()
        version = self.buffer.read_int32()
        major, minor = version >> 16, version & 0xffff
        parameters = self.buffer.read_parameters(length - 8)
        return (major, minor), parameters


class SSLRequest(Message):
    def read(self, **kwargs):
        msglen = self.buffer.read_int32()
        sslcode = self.buffer.read_int32()
        return sslcode


class ErrorResponse(Message):
    def write(self, severity, code, message):
        buf = IOBuffer()
        buf.write_bytes(b'S')
        buf.write_string(severity)
        buf.write_bytes(b'C')
        buf.write_string(code)
        buf.write_bytes(b'M')
        buf.write_string(message)
        bytes_ = buf.to_bytes()

        self.buffer.write_bytes(b'E')
        self.buffer.write_int32(4 + len(bytes_) + 1)
        self.buffer.write_string(bytes_)


class ClearPassword(Message):
    def read(self, **kwargs):
        length = self.buffer.read_int32()
        password = self.buffer.read_bytes(length - 4)
        return password


class AuthenticationOk(Message):
    def write(self, **kwargs):
        self.buffer.write_bytes(
            struct.pack("!cii", b'R', 8, 0)
        )


class AuthenticationCleartextPassword(Message):
    def write(self, **kwargs):
        self.buffer.write_bytes(
            struct.pack("!cii", b'R', 8, 3)
        )


class AuthenticationMD5Password(Message):
    pass


class ReadyForQuery(Message):
    def write(self, idle=True, failed=False):
        status_indicators = {
            (True, False): b'I',
            (True, True): b'I',
            (False, True): b'T',
            (False, False): b'E',
        }
        self.buffer.write_bytes(
            struct.pack("!cic", b'Z', 5, status_indicators[(idle, failed)])
        )


class NoticeResponse(Message):
    def write_none(self):
        self.buffer.write_bytes(b'N')

    def write(self, severity, code, message):
        buf = IOBuffer()
        buf.write_bytes(b'S')
        buf.write_string(severity)
        buf.write_bytes(b'C')
        buf.write_string(code)
        buf.write_bytes(b'M')
        buf.write_string(message)
        bytes_ = buf.to_bytes()

        self.buffer.write_bytes(b'N')
        self.buffer.write_int32(4 + len(bytes_) + 1)
        self.buffer.write_string(bytes_)


class CommandComplete(Message):
    def write(self, tag: bytes):
        self.buffer.write_bytes(
            struct.pack("!ci", b'C', 4 + len(tag))
        )
        self.buffer.write_bytes(tag)


class RowDescription(Message):
    def write(self, fields):
        buf = IOBuffer()
        for field in fields:
            buf.write_string(field.name)
            buf.write_int32(0)
            buf.write_int16(0)
            buf.write_int32(field.oid)
            buf.write_int16(field.type_len)
            buf.write_int32(-1)
            buf.write_int16(0)
        bytes_ = buf.to_bytes()

        self.buffer.write_bytes(
            struct.pack('!ciH', b'T', 6 + len(bytes_), len(fields))
        )
        self.buffer.write_bytes(bytes_)


class DataRow(Message):
    @staticmethod
    def _encode(v):
        if v is None:
            return b'null'  # temp only
        return str(v).encode()

    def write(self, rows):
        for row in rows:
            buf = IOBuffer()
            for v in row:
                value_bytes = self._encode(v)
                logging.debug("[postgres protocol] value bytes from client: '%s'", value_bytes)
                buf.write_int32(len(value_bytes))
                buf.write_bytes(value_bytes)
            bytes_ = buf.to_bytes()
            logging.debug("[postgres protocol] length of bytes: %d, "
                          "length of rows: %d, bytes: '%s'.",
                           bytes_, len(bytes_), len(row))
            self.buffer.write_bytes(
                struct.pack('!ciH', b'D', 4 + 2 + len(bytes_), len(row))
            )
            self.buffer.write_bytes(bytes_)


class QueryMessage(Message):
    def read(self, **kwargs):
        msglen = self.buffer.read_int32()
        sql = self.buffer.read_bytes(msglen - 4)
        return sql


class Field:
    def __init__(self, name, oid, type_len):
        self.name = name
        self.oid = oid
        self.type_len = type_len


class Int8Field(Field):
    def __init__(self, name):
        super().__init__(name, 20, 8)


class TextField(Field):
    def __init__(self, name):
        super().__init__(name, 25, -1)


class QueryResult(Message):
    def write(self, fields, rows):
        RowDescription(self.buffer).write(fields)
        DataRow(self.buffer).write(rows)
        CommandComplete(self.buffer).write(b'SELECT\x00')


class PGHandler(socketserver.StreamRequestHandler):
    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)
        self.notice = None
        self.warning = None

    def set_session_info(self, parameters):
        logging.info('[postgres protocol] set parameters: %s', parameters)

    def check_password(self, password):
        # TODO: support password
        assert len(password) >= 0
        return True

    def hello_andb(self, sql):
        # mock and debug only
        fields = [Int8Field('show_integer'), TextField('show_text')]
        rows = [[666, 'Hello'], [-1, None], [888, 'AnDB']]
        return fields, rows
    
    def query(self, sql):
        return self.hello_andb(sql)

    def handle(self):
        r = IOBuffer(self.rfile)  # comes from users
        w = IOBuffer(self.wfile)  # response to users

        try:
            # there are other implementations for the SSL request
            sslcode = SSLRequest(r).read()
            NoticeResponse(w).write_none()

            # now, read from user's message
            version, parameters = StartupMessage(r).read()
            logging.info('[postgres protocol] client version is %s.', version)
            assert version == (3, 0)
            self.set_session_info(parameters)

            AuthenticationCleartextPassword(w).write()
            message_type = r.read_byte()
            logging.debug('[postgres protocol] password message type %s', message_type)
            if message_type != FeMessageType.PASSWORD_MESSAGE.value:
                ErrorResponse(w).write('FATAL', '12345', 'invalid authorization')
                return

            password = ClearPassword(r).read()
            if self.check_password(password):
                AuthenticationOk(w).write()
            else:
                ErrorResponse(w).write('FATAL', '28000', 'invalid user/password')
                return

            # if execute here, it means the password has passed the validation.
            # we can get started to handle the request now.
            while True:
                ReadyForQuery(w).write()

                message_type = r.read_byte()
                logging.debug('[postgres protocol] message type %s', message_type)

                if message_type == FeMessageType.QUERY.value:
                    sql = QueryMessage(r).read()
                    try:
                        fields, rows = self.query(sql)
                        QueryResult(w).write(fields, rows)
                    except RollbackError as e:
                        ErrorResponse(w).write('ERROR', '00001', str(e))
                    except Exception as e:
                        ErrorResponse(w).write('FATAL', '00004', str(e))
                    else:
                        if self.notice:
                            NoticeResponse(w).write('NOTICE', '00002', self.notice)
                        if self.warning:
                            NoticeResponse(w).write('WARN', '00003', self.warning)

                elif message_type == FeMessageType.TERMINATION.value:
                    break
                elif message_type == b'':
                    pass
                else:
                    raise NotImplementedError(f'unsupported messag type {message_type}')
        except ConnectionAbortedError as e:
            pass
        except ConnectionResetError as e:
            logging.info('client %s exits.', self.client_address)
        except Exception as e:
            logging.exception(e)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def start_server(host, port, handler=PGHandler):
    server = Server((host, port), handler)
    server.serve_forever()


