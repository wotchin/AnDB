"""
PostgreSQL-compatible network server for AnDB.

Handles client connections using the PostgreSQL wire protocol v3.
Supports the simple query protocol (no extended query protocol yet).
"""

import logging
import os
import socket
import struct
import threading
import random

from andb.entrance import execute_simple_query
from andb.executor.portal import ExecuteResultSet, ExecutionResult
from andb.net.protocol import (
    parse_startup_message,
    parse_frontend_message,
    parse_query_message,
    parse_password_message,
    build_authentication_ok,
    build_authentication_cleartext,
    build_parameter_status,
    build_backend_key_data,
    build_ready_for_query,
    build_row_description,
    build_data_row,
    build_command_complete,
    build_error_response,
    build_empty_query_response,
    build_notice_response,
    MSG_QUERY,
    MSG_TERMINATE,
    MSG_PARSE,
    MSG_BIND,
    MSG_DESCRIBE,
    MSG_EXECUTE,
    MSG_SYNC,
    MSG_FLUSH,
    MSG_CLOSE,
    MSG_PASSWORD_MESSAGE,
    TRANSACTION_IDLE,
    TRANSACTION_IN_BLOCK,
    TRANSACTION_FAILED,
    ProtocolError,
)
from andb.errno.errors import RollbackError, FatalError

logger = logging.getLogger(__name__)

# Default server parameters sent to client during startup
_DEFAULT_PARAMS = {
    'server_version': '15.0.0',
    'server_encoding': 'UTF8',
    'client_encoding': 'UTF8',
    'DateStyle': 'ISO, MDY',
    'integer_datetimes': 'on',
    'standard_conforming_strings': 'on',
    'application_name': '',
}

# Simple type OID mapping for PostgreSQL protocol
PG_TYPE_TEXT = 25
PG_TYPE_INT4 = 23
PG_TYPE_INT8 = 20
PG_TYPE_FLOAT8 = 701
PG_TYPE_BOOL = 16
PG_TYPE_VARCHAR = 1043


def _andb_type_to_pg_oid(type_oid):
    """Map AnDB type OIDs to PostgreSQL type OIDs for the wire protocol.
    Default to TEXT for unknown types (always safe since we send text format)."""
    return PG_TYPE_TEXT


class ClientConnection:
    """Handles a single client connection."""

    def __init__(self, sock, addr, server):
        self.sock = sock
        self.addr = addr
        self.server = server
        self.pid = os.getpid()
        self.secret_key = random.randint(0, 0x7FFFFFFF)
        self.user = None
        self.database = None
        self.params = {}
        self.transaction_status = TRANSACTION_IDLE
        self._closed = False

    def handle(self):
        """Main connection handler. Runs in its own thread."""
        try:
            if not self._do_startup():
                return
            self._main_loop()
        except ProtocolError as e:
            logger.warning(f"Protocol error from {self.addr}: {e}")
        except ConnectionResetError:
            logger.info(f"Connection reset by {self.addr}")
        except BrokenPipeError:
            logger.info(f"Broken pipe for {self.addr}")
        except Exception as e:
            logger.error(f"Unexpected error handling {self.addr}: {e}", exc_info=True)
        finally:
            self._close()

    def _do_startup(self):
        """Handle the startup phase of the connection.
        Returns True if startup was successful, False to close connection."""
        while True:
            msg = parse_startup_message(self.sock)

            if msg['type'] == 'ssl_request':
                # Decline SSL - send 'N' (not supported)
                self.sock.sendall(b'N')
                continue

            if msg['type'] == 'cancel_request':
                # Cancel request - not implemented yet, just close
                logger.info(f"Cancel request from {self.addr}")
                return False

            if msg['type'] == 'startup':
                self.params = msg.get('params', {})
                self.user = self.params.get('user', 'andb')
                self.database = self.params.get('database', self.user)
                break

        # Send AuthenticationOk (no password required for now)
        self.sock.sendall(build_authentication_ok())

        # Send server parameters
        for name, value in _DEFAULT_PARAMS.items():
            self.sock.sendall(build_parameter_status(name, value))

        # Send BackendKeyData
        self.sock.sendall(build_backend_key_data(self.pid, self.secret_key))

        # Send ReadyForQuery
        self.sock.sendall(build_ready_for_query(TRANSACTION_IDLE))

        logger.info(f"Client connected: user={self.user}, database={self.database}, addr={self.addr}")
        return True

    def _main_loop(self):
        """Process messages from the client after startup."""
        while not self._closed:
            result = parse_frontend_message(self.sock)
            if result is None:
                break

            msg_type, payload = result

            if msg_type == MSG_QUERY:
                self._handle_simple_query(payload)
            elif msg_type == MSG_TERMINATE:
                logger.info(f"Client {self.addr} terminated")
                break
            elif msg_type == MSG_PARSE:
                # Extended query protocol - not yet supported
                self._send_error('ERROR', '0A000',
                                 'Extended query protocol is not supported yet. Use simple query protocol.')
                self.sock.sendall(build_ready_for_query(self.transaction_status))
            elif msg_type == MSG_SYNC:
                self.sock.sendall(build_ready_for_query(self.transaction_status))
            elif msg_type == MSG_FLUSH:
                pass  # Nothing to flush in our implementation
            else:
                logger.warning(f"Unknown message type from {self.addr}: {msg_type}")
                self._send_error('ERROR', '08P01',
                                 f'Unsupported message type: {chr(msg_type)}')
                self.sock.sendall(build_ready_for_query(self.transaction_status))

    def _handle_simple_query(self, payload):
        """Handle a simple query message ('Q')."""
        query_string = parse_query_message(payload)

        if not query_string.strip():
            self.sock.sendall(build_empty_query_response())
            self.sock.sendall(build_ready_for_query(self.transaction_status))
            return

        logger.info(f"Query from {self.addr}: {query_string}")

        try:
            result = execute_simple_query(query_string)
            self._send_query_result(result, query_string)
        except RollbackError as e:
            self._send_error('ERROR', '42000', str(e))
        except FatalError as e:
            self._send_error('FATAL', '58000', str(e))
        except NotImplementedError as e:
            self._send_error('ERROR', '0A000', str(e))
        except Exception as e:
            logger.error(f"Query execution error: {e}", exc_info=True)
            self._send_error('ERROR', 'XX000', f'Internal error: {e}')

        self.sock.sendall(build_ready_for_query(self.transaction_status))

    def _send_query_result(self, result, query_string):
        """Send query results back to the client."""
        if result is None:
            self.sock.sendall(build_command_complete('SELECT 0'))
            return

        if isinstance(result, ExecuteResultSet):
            # Send RowDescription
            columns = []
            for attr_form in result.attr_forms:
                pg_type_oid = _andb_type_to_pg_oid(attr_form.type_oid)
                columns.append((attr_form.name, pg_type_oid, -1, -1))
            self.sock.sendall(build_row_description(columns))

            # Send DataRow for each tuple
            for row_tuple in result.tuples:
                values = []
                for val in row_tuple:
                    if val is None:
                        values.append(None)
                    else:
                        values.append(str(val))
                self.sock.sendall(build_data_row(values))

            # Send CommandComplete
            cmd_tag = self._make_command_tag(query_string, result)
            self.sock.sendall(build_command_complete(cmd_tag))

        elif isinstance(result, ExecutionResult):
            cmd_tag = self._make_command_tag(query_string, result)
            self.sock.sendall(build_command_complete(cmd_tag))

    def _make_command_tag(self, query_string, result):
        """Create the command complete tag based on the query type."""
        query_upper = query_string.strip().upper()

        if query_upper.startswith('SELECT'):
            row_count = 0
            if isinstance(result, ExecuteResultSet):
                row_count = len(result.tuples)
            return f'SELECT {row_count}'
        elif query_upper.startswith('INSERT'):
            return f'INSERT 0 {result.effect_rows}'
        elif query_upper.startswith('UPDATE'):
            return f'UPDATE {result.effect_rows}'
        elif query_upper.startswith('DELETE'):
            return f'DELETE {result.effect_rows}'
        elif query_upper.startswith('CREATE TABLE'):
            return 'CREATE TABLE'
        elif query_upper.startswith('CREATE INDEX'):
            return 'CREATE INDEX'
        elif query_upper.startswith('DROP TABLE'):
            return 'DROP TABLE'
        elif query_upper.startswith('DROP INDEX'):
            return 'DROP INDEX'
        elif query_upper.startswith('EXPLAIN'):
            row_count = 0
            if isinstance(result, ExecuteResultSet):
                row_count = len(result.tuples)
            return f'EXPLAIN {row_count}'
        else:
            return 'OK'

    def _send_error(self, severity, code, message):
        """Send an error response to the client."""
        self.sock.sendall(build_error_response(severity, code, message))

    def _close(self):
        """Close the connection."""
        if not self._closed:
            self._closed = True
            try:
                self.sock.close()
            except Exception:
                pass
            logger.info(f"Connection closed: {self.addr}")


class PGServer:
    """PostgreSQL-compatible TCP server for AnDB."""

    def __init__(self, host='0.0.0.0', port=5432, max_connections=100):
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self._server_socket = None
        self._running = False
        self._connections = []
        self._lock = threading.Lock()

    def start(self):
        """Start the server and listen for connections."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(self.max_connections)
        self._running = True

        logger.info(f"AnDB server listening on {self.host}:{self.port}")
        print(f"AnDB server listening on {self.host}:{self.port}")

        try:
            while self._running:
                try:
                    self._server_socket.settimeout(1.0)
                    client_sock, addr = self._server_socket.accept()
                    self._handle_new_connection(client_sock, addr)
                except socket.timeout:
                    continue
                except OSError:
                    if self._running:
                        raise
                    break
        finally:
            self.stop()

    def _handle_new_connection(self, client_sock, addr):
        """Spawn a new thread to handle a client connection."""
        conn = ClientConnection(client_sock, addr, self)
        with self._lock:
            self._connections.append(conn)
        thread = threading.Thread(target=self._run_connection, args=(conn,), daemon=True)
        thread.start()

    def _run_connection(self, conn):
        """Run a connection handler and clean up when done."""
        try:
            conn.handle()
        finally:
            with self._lock:
                if conn in self._connections:
                    self._connections.remove(conn)

    def stop(self):
        """Stop the server and close all connections."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass
        with self._lock:
            for conn in self._connections:
                conn._close()
            self._connections.clear()
        logger.info("AnDB server stopped")
