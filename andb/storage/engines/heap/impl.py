import struct
import time

# Constants for struct packing/unpacking
INT_FORMAT = 'i'
LONG_FORMAT = 'q'
# todo: this page header size should be set by calculating
PAGE_HEADER_SIZE = 8
KEY_SIZE = 8
OFFSET_SIZE = 4
PAGE_FREE_SPACE_FORMAT = '<I'
PAGE_TUPLE_COUNT_FORMAT = '<I'
PAGE_FREE_SPACE_OFFSET = 0
PAGE_TUPLE_COUNT_OFFSET = 4

# Constants for transaction status
STATUS_ACTIVE = 0
STATUS_COMMITTED = 1
STATUS_ABORTED = 2

# Constants for logging
LOG_TYPE_INSERT = 0
LOG_TYPE_UPDATE = 1
LOG_TYPE_DELETE = 2

# Constants for indexing
INDEX_PAGE_SIZE = 4096
INDEX_NODE_HEADER_SIZE = 12
INDEX_LEAF_HEADER_SIZE = 8
INDEX_KEY_FORMAT = 'i'
INDEX_POINTER_FORMAT = 'q'
INDEX_KEY_SIZE = struct.calcsize(INDEX_KEY_FORMAT)
INDEX_POINTER_SIZE = struct.calcsize(INDEX_POINTER_FORMAT)
INDEX_NODE_MAX_KEYS = (INDEX_PAGE_SIZE - INDEX_NODE_HEADER_SIZE) // (INDEX_KEY_SIZE + INDEX_POINTER_SIZE)
INDEX_LEAF_MAX_KEYS = (INDEX_PAGE_SIZE - INDEX_LEAF_HEADER_SIZE) // (INDEX_KEY_SIZE + INDEX_POINTER_SIZE)


class TransactionManager:
    def __init__(self):
        self.active_transactions = {}

    def begin_transaction(self, transaction_id):
        if transaction_id not in self.active_transactions:
            self.active_transactions[transaction_id] = {
                'status': STATUS_ACTIVE,
                'start_time': time.time(),
                'last_lsn': None,
                'undo_log': []
            }

    def commit_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            if transaction['status'] == STATUS_ACTIVE:
                transaction['status'] = STATUS_COMMITTED
                transaction['last_lsn'] = self._generate_lsn()
                self._flush_transaction(transaction_id)

    def abort_transaction(self, transaction_id):
        if transaction_id in self.active_transactions:
            transaction = self.active_transactions[transaction_id]
            if transaction['status'] == STATUS_ACTIVE:
                transaction['status'] = STATUS_ABORTED
                transaction['last_lsn'] = self._generate_lsn()
                self._undo_transaction(transaction_id)

    def _generate_lsn(self):
        # TODO: Implement the generation of Log Sequence Number (LSN)
        return int(time.time())

    def _flush_transaction(self, transaction_id):
        # TODO: Implement the flushing of modified pages and logs associated with the transaction
        pass

    def _undo_transaction(self, transaction_id):
        # TODO: Implement the undoing of modifications made by the transaction using the undo log
        pass


class LogManager:
    def __init__(self):
        self.log_file = "log.txt"

    def write_log(self, lsn, transaction_id, log_type, page_id, old_data=None, new_data=None):
        # TODO: Implement the writing of logs to the log file
        pass


class HeapOrientedTable:
    def __init__(self, table_name):
        self.table_name = table_name
        self.buffer_manager = BufferManager()
        self.log_manager = LogManager()
        self.transaction_manager = TransactionManager()
        self.index_manager = IndexManager()

    def create_table(self):
        # TODO: Implement the creation of the table
        pass

    def drop_table(self):
        # TODO: Implement the dropping of the table
        pass

    def insert(self, values):
        # TODO: Implement the insertion of a tuple into the table
        pass

    def update(self, key, values):
        # TODO: Implement the update operation on a tuple in the table
        pass

    def delete(self, key):
        # TODO: Implement the deletion of a tuple from the table
        pass

    def select(self, key):
        # TODO: Implement the selection of a tuple from the table
        pass


# class HeapOrientedTable:
#     def __init__(self, table_name):
#         self.table_name = table_name
#         self.index = BPlusTreeIndex()
#         self.buffer_manager = BufferManager()
#         self.log_manager = LogManager()
#
#     def insert(self, key, value):
#         page, slot_offset, slot = self._find_slot_by_key(key)
#         if page is None:
#             page = self._get_free_page()
#             self._write_slot(page, slot_offset, slot)
#             self._update_page_header(page, KEY_SIZE)
#         else:
#             old_value = struct.unpack('{}s'.format(len(slot) - KEY_SIZE), slot[KEY_SIZE:])[0]
#             self.log_manager.log_update(self.table_name, key, page, slot_offset, slot, old_value)
#             self._write_slot(page, slot_offset, struct.pack('{}s'.format(len(slot) - KEY_SIZE), value))
#             self._update_page_header(page, 0)
#
#         self.index.insert(key, page.id, slot_offset)
#
#     def select(self, key):
#         page, slot_offset, slot = self._find_slot_by_key(key)
#         if page is None:
#             return None
#         return struct.unpack('{}s'.format(len(slot) - KEY_SIZE), slot[KEY_SIZE:])[0]
#
#     def update(self, key, value):
#         page, slot_offset, slot = self._find_slot_by_key(key)
#         if page is None:
#             raise KeyError(f"Key {key} not found in table {self.table_name}")
#         old_value = struct.unpack('{}s'.format(len(slot) - KEY_SIZE), slot[KEY_SIZE:])[0]
#         self.log_manager.log_update(self.table_name, key, page, slot_offset, slot, old_value)
#         self._write_slot(page, slot_offset, struct.pack('{}s'.format(len(slot) - KEY_SIZE), value))
#         self._update_page_header(page, 0)
#
#     def delete(self, key):
#         page, slot_offset, slot = self._find_slot_by_key(key)
#         if page is None:
#             raise KeyError(f"Key {key} not found in table {self.table_name}")
#         self.log_manager.log_delete(self.table_name, key)
#         self._write_slot(page, slot_offset, b'\x00')
#         self._update_page_header(page, -KEY_SIZE)
#         self.index.delete(key)
#
#     def _update_page_from_data(self, page, data):
#         page.free_space = struct.unpack(PAGE_FREE_SPACE_FORMAT, data[PAGE_FREE_SPACE_OFFSET:PAGE_TUPLE_COUNT_OFFSET])[0]
#         page.tuple_count = struct.unpack(PAGE_TUPLE_COUNT_FORMAT, data[PAGE_TUPLE_COUNT_OFFSET:])[0]
#         slots_data = data[PAGE_HEADER_SIZE:]
#         page.slots = []
#         for i in range(page.tuple_count):
#             slot_data = slots_data[i * 2 * KEY_SIZE:(i * 2 + 2) * KEY_SIZE]
#             page.slots.append(slot_data)
#
#     def _get_free_page(self):
#         if self.buffer_manager.has_free_page():
#             return self.buffer_manager.get_free_page()
#         else:
#             page = Page(page_id=generate_page_id())
#             self.buffer_manager.add_page(page)
#             return page
#
#     def _write_slot(self, page, slot_offset, slot_data):
#         page.slots[slot_offset] = slot_data
#
#     def _update_page_header(self, page, size_diff):
#         page.free_space -= size_diff
#         if size_diff > 0:
#             page.tuple_count += 1
#         elif size_diff < 0:
#             page.tuple_count -= 1
#
#     def _find_slot_by_key(self, key):
#         page_id, slot_offset = self.index.search(key)
#         if page_id is None:
#             return None, None, None
#         page = self.buffer_manager.get_page(page_id)
#         slot = page.slots[slot_offset]
#         return page, slot_offset, slot


# Helper functions
latest_page_id = 0


def generate_page_id():
    # Generate a unique page ID
    # Get the latest page ID, then increase it as a new one.
    # todo: Should be atomic. And here, we just for a test.
    global latest_page_id
    latest_page_id += 1
    return latest_page_id


# Test case
def test_heap_oriented_table():
    # Create a table
    table = HeapOrientedTable("users")
    table.create_table()

    # Insert tuples
    table.insert([1, "John", 25])
    table.insert([2, "Jane", 30])
    table.insert([3, "Alice", 35])
    table.insert([4, "Bob", 40])

    # Update a tuple
    table.update(2, [2, "Janet", 28])

    # Delete a tuple
    table.delete(3)

    # Select a tuple
    result = table.select(1)
    print(result)  # Expected output: [1, "John", 25]

    # Drop the table
    table.drop_table()


test_heap_oriented_table()
