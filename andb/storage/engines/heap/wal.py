# import os
# import struct
# import pickle
#
# # Constants for struct packing/unpacking
# LONG_FORMAT = 'q'
# INT_FORMAT = 'i'
#
# # Constants for WAL file format
# WAL_HEADER_SIZE = 16
# LOG_RECORD_SIZE = 16
# LSN_OFFSET = 0
# PREV_LSN_OFFSET = 8
# LOG_RECORD_OFFSET = 12
# MARKER_RECORD = b"WAL_MARKER"
#
# # Maximum size of a single log record
# MAX_LOG_RECORD_SIZE = 1024
#
#
# class WriteAheadLog:
#     def __init__(self, log_dir, max_log_file_size=1024):
#         self.log_dir = log_dir
#         self.log_file_base = os.path.join(log_dir, 'wal.log')
#         self.current_log_file_index = 0
#         self.log_file_size = 0
#         self.current_lsn = 0
#         self.max_log_file_size = max_log_file_size
#
#         # Create the log directory if it doesn't exist
#         os.makedirs(log_dir, exist_ok=True)
#
#         # Get the initial log file size
#         self.log_file_size = self._get_log_file_size()
#
#         # Initialize the current LSN
#         self.current_lsn = self.log_file_size // LOG_RECORD_SIZE
#
#         # Create the first log file
#         self._create_log_file()
#
#     def _get_log_file_size(self):
#         total_size = 0
#         file_index = 0
#         while True:
#             log_file_path = self._get_log_file_path(file_index)
#             if not os.path.isfile(log_file_path):
#                 break
#             total_size += os.path.getsize(log_file_path)
#             file_index += 1
#         return total_size
#
#     def _get_log_file_path(self, file_index):
#         return f"{self.log_file_base}.{file_index}"
#
#     def _create_log_file(self):
#         log_file_path = self._get_log_file_path(self.current_log_file_index)
#         self.file = open(log_file_path, 'ab')
#
#         # Write marker record at the beginning of the log file
#         self.write_marker_record()
#
#     def _rotate_log_file(self):
#         self.file.close()
#         self.current_log_file_index += 1
#         log_file_path = self._get_log_file_path(self.current_log_file_index)
#         self.file = open(log_file_path, 'ab')
#
#         # Write marker record at the beginning of the new log file
#         self.write_marker_record()
#
#     def write_marker_record(self):
#         # Write the marker record to the log file
#         self.file.write(MARKER_RECORD)
#
#         # Update the log file size and current LSN
#         self.log_file_size += LOG_RECORD_SIZE
#         self.current_lsn += 1
#
#     def write_log_record(self, prev_lsn, log_record):
#         # Pack the log record fields
#         lsn_bytes = struct.pack(LONG_FORMAT, self.current_lsn)
#         prev_lsn_bytes = struct.pack(LONG_FORMAT, prev_lsn)
#         log_record_bytes = struct.pack(INT_FORMAT, log_record)
#
#         # Check if the current log file is full
#         if self.log_file_size >= self.max_log_file_size:
#             self._rotate_log_file()
#
#         # Write the log record to the log file
#         self.file.write(lsn_bytes)
#         self.file.write(prev_lsn_bytes)
#         self.file.write(log_record_bytes)
#
#         # Update the log file size and current LSN
#         self.log_file_size += LOG_RECORD_SIZE
#         self.current_lsn += 1
#
#         # Flush the changes to disk
#         self.file.flush()
#         os.fsync(self.file.fileno())
#
#     def close(self):
#         self.file.close()
#
#
# # Test case for update operation WAL modifications
# log_dir = 'wal_logs'
# wal = WriteAheadLog(log_dir)
#
# table_name = 'users'
# old_row_data = {'id': 1, 'name': 'John Doe', 'age': 30}
# new_row_data = {'id': 1, 'name': 'John Smith', 'age': 35}
#
# wal.log_update(table_name, old_row_data, new_row_data)
#
# # Close the WAL after testing
# wal.close()

import os
import struct

# Constants for struct packing/unpacking
INT_FORMAT = 'i'
LONG_FORMAT = 'q'

# Constants for physical log form
PHYSICAL_LOG_ENTRY_SIZE = 40  # Size of a physical log entry in bytes


class WriteAheadLog:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.current_log_file = None
        self.current_lsn = 0

    def write_log_record(self, page_id, offset, xid, action, values):
        if self.current_log_file is None or self.current_log_file.tell() + PHYSICAL_LOG_ENTRY_SIZE > os.stat(
                self.current_log_file.name).st_size:
            self._switch_to_new_log_file()

        log_record_bytes = (
                struct.pack(INT_FORMAT, self.current_lsn) +
                struct.pack(INT_FORMAT, page_id) +
                struct.pack(INT_FORMAT, offset) +
                struct.pack(INT_FORMAT, xid) +
                struct.pack(INT_FORMAT, action) +
                values.encode()
        )
        self.current_log_file.write(log_record_bytes)
        self.current_lsn += 1

    def _switch_to_new_log_file(self):
        if self.current_log_file:
            self.current_log_file.close()

        log_file_path = os.path.join(self.log_dir, f"log_{self.current_lsn}.log")
        self.current_log_file = open(log_file_path, "wb")

    def flush(self):
        if self.current_log_file:
            self.current_log_file.flush()

    def close(self):
        if self.current_log_file:
            self.current_log_file.close()

    def get_current_lsn(self):
        return self.current_lsn


# Test Case
if __name__ == "__main__":
    log_dir = "./logs"
    os.makedirs(log_dir, exist_ok=True)

    # Initialize the WriteAheadLog
    wal = WriteAheadLog(log_dir)

    # Test insert operation
    page_id = 1
    offset = 10
    xid = 1001
    action = 1  # Insert action
    values = "{'name': 'John', 'age': 30}"
    wal.write_log_record(page_id, offset, xid, action, values)

    # Test update operation
    page_id = 2
    offset = 20
    xid = 1002
    action = 2  # Update action
    values = "{'name': 'Jane', 'age': 35}"
    wal.write_log_record(page_id, offset, xid, action, values)

    # Test delete operation
    page_id = 3
    offset = 30
    xid = 1003
    action = 3  # Delete action
    values = ""
    wal.write_log_record(page_id, offset, xid, action, values)

    # Flush the log
    wal.flush()

    # Close the WriteAheadLog
    wal.close()


class LsnManager:
    def __init__(self):
        self.current_lsn = 0

    def next_lsn(self):
        self.current_lsn += 1
        return self.current_lsn

