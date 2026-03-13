"""
PostgreSQL Frontend/Backend Protocol v3 implementation (non-SSL).

This module implements the PostgreSQL wire protocol for client-server
communication. The protocol uses a message-based format where each message
starts with a message type byte (for backend messages) followed by a 4-byte
length and the message body.

Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

Startup flow:
  1. Client sends StartupMessage (no type byte, just length + protocol version + params)
  2. Server replies with AuthenticationOk (or requests auth)
  3. Server sends ParameterStatus messages
  4. Server sends BackendKeyData
  5. Server sends ReadyForQuery

Simple query flow:
  1. Client sends Query message ('Q')
  2. Server sends RowDescription (if SELECT)
  3. Server sends DataRow messages (one per row)
  4. Server sends CommandComplete
  5. Server sends ReadyForQuery
"""

import struct
import logging

# ============================================================
# Message type bytes (Backend -> Frontend)
# ============================================================
MSG_AUTHENTICATION = b'R'
MSG_BACKEND_KEY_DATA = b'K'
MSG_PARAMETER_STATUS = b'S'
MSG_READY_FOR_QUERY = b'Z'
MSG_ROW_DESCRIPTION = b'T'
MSG_DATA_ROW = b'D'
MSG_COMMAND_COMPLETE = b'C'
MSG_ERROR_RESPONSE = b'E'
MSG_NOTICE_RESPONSE = b'N'
MSG_EMPTY_QUERY_RESPONSE = b'I'
MSG_PARSE_COMPLETE = b'1'
MSG_BIND_COMPLETE = b'2'
MSG_CLOSE_COMPLETE = b'3'
MSG_NO_DATA = b'n'
MSG_PARAMETER_DESCRIPTION = b't'

# ============================================================
# Message type bytes (Frontend -> Backend)
# ============================================================
MSG_QUERY = ord('Q')
MSG_TERMINATE = ord('X')
MSG_PARSE = ord('P')
MSG_BIND = ord('B')
MSG_DESCRIBE = ord('D')
MSG_EXECUTE = ord('E')
MSG_SYNC = ord('S')
MSG_FLUSH = ord('H')
MSG_CLOSE = ord('C')
MSG_COPY_DATA = ord('d')
MSG_COPY_DONE = ord('c')
MSG_COPY_FAIL = ord('f')
MSG_PASSWORD_MESSAGE = ord('p')

# Transaction status indicators
TRANSACTION_IDLE = b'I'
TRANSACTION_IN_BLOCK = b'T'
TRANSACTION_FAILED = b'E'

# Authentication types
AUTH_OK = 0
AUTH_CLEARTEXT_PASSWORD = 3
AUTH_MD5_PASSWORD = 5

# Protocol version
PROTOCOL_VERSION_3 = 196608  # 3.0 = (3 << 16) | 0

# SSL request
SSL_REQUEST_CODE = 80877103  # (1234 << 16) | 5679
CANCEL_REQUEST_CODE = 80877102  # (1234 << 16) | 5678


class ProtocolError(Exception):
    """Raised when the wire protocol is violated."""
    pass


def _read_exactly(sock, n):
    """Read exactly n bytes from a socket."""
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ProtocolError("Connection closed unexpectedly")
        data.extend(chunk)
    return bytes(data)


def _read_cstring(data, offset):
    """Read a null-terminated string from data starting at offset.
    Returns (string, new_offset)."""
    end = data.index(b'\x00', offset)
    return data[offset:end].decode('utf-8'), end + 1


# ============================================================
# Message Builders (Backend -> Frontend)
# ============================================================

def build_authentication_ok():
    """AuthenticationOk message: type 'R', int32 length, int32 0."""
    body = struct.pack('!i', AUTH_OK)
    return MSG_AUTHENTICATION + struct.pack('!i', 4 + len(body)) + body


def build_authentication_cleartext():
    """AuthenticationCleartextPassword: type 'R', int32 length, int32 3."""
    body = struct.pack('!i', AUTH_CLEARTEXT_PASSWORD)
    return MSG_AUTHENTICATION + struct.pack('!i', 4 + len(body)) + body


def build_parameter_status(name, value):
    """ParameterStatus message: type 'S', name\0, value\0."""
    body = name.encode('utf-8') + b'\x00' + value.encode('utf-8') + b'\x00'
    return MSG_PARAMETER_STATUS + struct.pack('!i', 4 + len(body)) + body


def build_backend_key_data(pid, secret_key):
    """BackendKeyData message: type 'K', int32 pid, int32 secret_key."""
    body = struct.pack('!ii', pid, secret_key)
    return MSG_BACKEND_KEY_DATA + struct.pack('!i', 4 + len(body)) + body


def build_ready_for_query(status=TRANSACTION_IDLE):
    """ReadyForQuery message: type 'Z', int32 length, byte status."""
    return MSG_READY_FOR_QUERY + struct.pack('!i', 5) + status


def build_row_description(columns):
    """RowDescription message.

    Args:
        columns: List of (name, type_oid, type_size, type_modifier) tuples.
            type_oid: PostgreSQL OID of the column type (use 25 for text).
            type_size: Size of the type in bytes (-1 for variable).
            type_modifier: Type modifier (-1 for no modifier).
    """
    body = struct.pack('!h', len(columns))
    for name, type_oid, type_size, type_modifier in columns:
        name_bytes = name.encode('utf-8') + b'\x00'
        # field: name, table_oid(4), column_attr(2), type_oid(4),
        #        type_size(2), type_modifier(4), format_code(2)
        body += name_bytes
        body += struct.pack('!ihihih',
                            0,            # table OID
                            0,            # column attribute number
                            type_oid,     # data type OID
                            type_size,    # data type size
                            type_modifier,  # type modifier
                            0)            # format code (0 = text)
    return MSG_ROW_DESCRIPTION + struct.pack('!i', 4 + len(body)) + body


def build_data_row(values):
    """DataRow message.

    Args:
        values: List of column values (strings or None for NULL).
    """
    body = struct.pack('!h', len(values))
    for value in values:
        if value is None:
            body += struct.pack('!i', -1)  # NULL
        else:
            encoded = str(value).encode('utf-8')
            body += struct.pack('!i', len(encoded)) + encoded
    return MSG_DATA_ROW + struct.pack('!i', 4 + len(body)) + body


def build_command_complete(tag):
    """CommandComplete message: type 'C', tag\0.

    Args:
        tag: Command tag string (e.g., 'SELECT 5', 'INSERT 0 1').
    """
    body = tag.encode('utf-8') + b'\x00'
    return MSG_COMMAND_COMPLETE + struct.pack('!i', 4 + len(body)) + body


def build_error_response(severity, code, message, detail=None, hint=None):
    """ErrorResponse message with field-based error reporting.

    Args:
        severity: 'ERROR', 'FATAL', or 'PANIC'.
        code: SQLSTATE error code (e.g., '42P01' for undefined table).
        message: Human-readable error message.
        detail: Optional detailed error message.
        hint: Optional hint message.
    """
    body = b'S' + severity.encode('utf-8') + b'\x00'
    body += b'V' + severity.encode('utf-8') + b'\x00'  # non-localized severity
    body += b'C' + code.encode('utf-8') + b'\x00'
    body += b'M' + message.encode('utf-8') + b'\x00'
    if detail:
        body += b'D' + detail.encode('utf-8') + b'\x00'
    if hint:
        body += b'H' + hint.encode('utf-8') + b'\x00'
    body += b'\x00'  # terminator
    return MSG_ERROR_RESPONSE + struct.pack('!i', 4 + len(body)) + body


def build_notice_response(severity, code, message):
    """NoticeResponse message (non-fatal messages)."""
    body = b'S' + severity.encode('utf-8') + b'\x00'
    body += b'V' + severity.encode('utf-8') + b'\x00'
    body += b'C' + code.encode('utf-8') + b'\x00'
    body += b'M' + message.encode('utf-8') + b'\x00'
    body += b'\x00'
    return MSG_NOTICE_RESPONSE + struct.pack('!i', 4 + len(body)) + body


def build_empty_query_response():
    """EmptyQueryResponse: sent when the query string is empty."""
    return MSG_EMPTY_QUERY_RESPONSE + struct.pack('!i', 4)


# ============================================================
# Message Parsing (Frontend -> Backend)
# ============================================================

def parse_startup_message(sock):
    """Parse the initial startup message from the client.

    Returns:
        dict with keys:
        - 'type': 'startup', 'ssl_request', or 'cancel_request'
        - 'params': dict of startup parameters (for startup messages)
        - 'version': protocol version tuple (major, minor)
    """
    # First 4 bytes = total message length (includes itself)
    length_data = _read_exactly(sock, 4)
    length = struct.unpack('!i', length_data)[0]

    body = _read_exactly(sock, length - 4)

    # First 4 bytes of body = protocol version or special request code
    version_or_code = struct.unpack('!i', body[:4])[0]

    if version_or_code == SSL_REQUEST_CODE:
        return {'type': 'ssl_request'}

    if version_or_code == CANCEL_REQUEST_CODE:
        pid, secret = struct.unpack('!ii', body[4:12])
        return {'type': 'cancel_request', 'pid': pid, 'secret': secret}

    # Normal startup message
    major = version_or_code >> 16
    minor = version_or_code & 0xFFFF

    params = {}
    offset = 4
    while offset < len(body):
        if body[offset] == 0:
            break
        name, offset = _read_cstring(body, offset)
        value, offset = _read_cstring(body, offset)
        params[name] = value

    return {
        'type': 'startup',
        'version': (major, minor),
        'params': params
    }


def parse_frontend_message(sock):
    """Parse a single frontend message after startup is complete.

    Returns:
        (message_type: int, payload: bytes) or None if connection closed.
    """
    type_data = sock.recv(1)
    if not type_data:
        return None

    msg_type = type_data[0]
    length_data = _read_exactly(sock, 4)
    length = struct.unpack('!i', length_data)[0]

    if length > 4:
        payload = _read_exactly(sock, length - 4)
    else:
        payload = b''

    return msg_type, payload


def parse_query_message(payload):
    """Parse the payload of a Query ('Q') message.
    Returns the query string."""
    # Query string is null-terminated
    if payload.endswith(b'\x00'):
        return payload[:-1].decode('utf-8')
    return payload.decode('utf-8')


def parse_password_message(payload):
    """Parse a PasswordMessage payload. Returns the password string."""
    if payload.endswith(b'\x00'):
        return payload[:-1].decode('utf-8')
    return payload.decode('utf-8')
