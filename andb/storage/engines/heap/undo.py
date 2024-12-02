import os

from andb.constants.filename import UNDO_DIR
from andb.storage.utils import easy_tuple_serialize, easy_tuple_deserialize

LITTLE_ENDIAN = 'little'

class UndoOperation:
    BEGIN = 0
    HEAP_INSERT = 1
    HEAP_DELETE = 2
    HEAP_BATCH_DELETE = 3
    HEAP_UPDATE = 4
    COMMIT = 5
    ABORT = 6
    BTREE_INSERT = 7
    BTREE_DELETE = 8
    BTREE_UPDATE = 9
    # Additional operations can be added as needed, e.g, schema change

class UndoRecord:
    def __init__(self, xid, operation, relation, location, data: bytes):
        """
        Initialize an undo record.
        The critical information of the undo log is a tuple <T, X, v> where:
        - T: Transaction ID that modified the data
        - X: Database element that was changed
        - v: Previous value before the change
        
        The tuple must be written to disk before the new value is written.
        The undo commit information should only be written after all necessary data is on disk.
        """
        self.xid = xid
        self.operation = operation
        self.relation = relation
        self.location = location
        self.data = data
        self._bytes = easy_tuple_serialize((xid, operation, relation, location, data))

    def to_bytes(self):
        """
        Serialize the record with a fixed-size header indicating content size.
        The content_size is fixed at 8 bytes to indicate the length of the corresponding buffer.
        This is necessary to properly parse multiple serialized records.
        """
        content_size = len(self._bytes)
        return int.to_bytes(content_size, 8, LITTLE_ENDIAN, signed=False) + self._bytes

    @staticmethod
    def from_bytes(buff):
        content_size = int.from_bytes(buff[:8], LITTLE_ENDIAN, signed=False)
        data = buff[8:8+content_size]
        xid, operation, relation, location, data = easy_tuple_deserialize(data)
        return UndoRecord(xid, operation, relation, location, data)

    def __len__(self):
        return 8 + len(self._bytes)

    def __repr__(self):
        return f'<UndoRecord xid={self.xid} operation={self.operation}>'

class UndoManager:
    def __init__(self, file_directory=UNDO_DIR):
        self.file_directory = file_directory
        self.active_transactions = {}

    def write_record(self, record: UndoRecord):
        xid = record.xid
        assert xid in self.active_transactions
        self.active_transactions[xid].append(record)

    def flush(self, xid):
        if not os.path.exists(self.file_directory):
            os.makedirs(self.file_directory)
        filename = os.path.join(self.file_directory, str(xid))
        with open(filename, 'ab') as f:
            # todo: batch write
            for record in self.active_transactions[xid]:
                f.write(record.to_bytes())
            f.flush()  # Flush internal buffers
            os.fsync(f.fileno())  # Ensure data is written to disk
        self.active_transactions[xid].clear()

    def begin_transaction(self, xid):
        self.active_transactions[xid] = []
        undo_record = UndoRecord(
            xid=xid,
            operation=UndoOperation.BEGIN,
            relation=None,
            location=None,
            data=b''
        )
        self.write_record(undo_record)

    def commit_transaction(self, xid):
        undo_record = UndoRecord(
            xid=xid,
            operation=UndoOperation.COMMIT,
            relation=None,
            location=None,
            data=b''
        )
        self.write_record(undo_record)
        self.flush(xid)
        del self.active_transactions[xid]

    def abort_transaction(self, xid):
        undo_record = UndoRecord(
            xid=xid,
            operation=UndoOperation.ABORT,
            relation=None,
            location=None,
            data=b''
        )
        self.write_record(undo_record)
        self.flush(xid)
        del self.active_transactions[xid]

    def parse_record(self, xid):
        filename = os.path.join(self.file_directory, str(xid))
        if not os.path.exists(filename):
            return []
        undo_records = []
        with open(filename, 'rb') as f:
            while True:
                size_data = f.read(8)
                if not size_data:
                    break
                content_size = int.from_bytes(size_data, LITTLE_ENDIAN, signed=False)
                record_data = f.read(content_size)
                undo_record = UndoRecord.from_bytes(size_data + record_data)
                undo_records.append(undo_record)
        # Reverse the records for undo operations
        # e.g., [1,2,3] --> [3,2,1]
        undo_records.reverse()
        return undo_records
