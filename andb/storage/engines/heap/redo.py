import os

from andb.common.cstructure import CStructure, Integer4Field, Integer8Field
from andb.common.file_operation import directio_file_open, file_close, file_tell, file_extend, file_write
from andb.constants.filename import WAL_DIR
from andb.constants.values import WAL_SEGMENT_SIZE, WAL_PAGE_SIZE
from andb.errno.errors import WALError
from andb.runtime.global_vars import wal_buffer_size
from andb.storage.lock.lwlock import LWLockName, lwlock_release, lwlock_acquire


class WALAction:
    NONE = 0  # follow by previous page
    CHECKPOINT = 1
    COMMIT = 2
    ABORT = 3
    HEAP_INSERT = 4
    HEAP_DELETE = 5
    HEAP_INPLACE_UPDATE = 6


class WALRecord:
    class Header(CStructure):
        total_size = Integer4Field(unsigned=True)
        xid = Integer8Field(unsigned=True)
        action = Integer4Field(unsigned=True)

    def __init__(self, xid, action, data):
        assert isinstance(data, bytes)

        self.header = self.Header()
        self.header.total_size = self.header.size() + len(data)
        self.header.xid = xid
        self.header.action = action
        self.data = data

    def pack(self):
        return self.header.pack() + self.data

    @classmethod
    def unpack(cls, data):
        header = cls.Header()
        header.unpack(data[header.size()])
        real_data = (data[header.size():])
        assert len(data) == header.total_size  # validate
        return cls(xid=header.xid, action=header.action, data=real_data)

    @classmethod
    def header_size(cls):
        return cls.Header.size()

    @classmethod
    def parse_record_size(cls, data):
        assert len(data) >= cls.header_size()
        header = cls.Header()
        header.unpack(data[:cls.header_size()])
        return header.total_size


class WALPage:
    class Header(CStructure):
        lsn = Integer8Field(unsigned=True)
        last_page_written_size = Integer4Field(unsigned=True)

    def __init__(self, start_lsn, written_size):
        self.header = self.Header()
        self.header.lsn = start_lsn  # address of the page
        self.header.last_page_written_size = written_size
        self.records = []
        self.records_size = 0

    def is_full(self):
        assert self.records_size + self.header.size() <= WAL_PAGE_SIZE
        # If remaining space of the page cannot contain a record header,
        # we also think the page is full.
        return (WAL_PAGE_SIZE - WALRecord.Header.size()) <= (self.records_size + self.header.size()) <= WAL_PAGE_SIZE

    def calc_remaining_size(self, record: WALRecord):
        record_size = record.header.total_size
        later_records_size = record_size + self.records_size
        remaining = (later_records_size + self.header.size()) - WAL_PAGE_SIZE
        return remaining if remaining > 0 else 0

    def append_record(self, record: WALRecord):
        """If failed, return False."""
        assert not self.is_full()

        remaining = self.calc_remaining_size(record)
        if remaining > 0:
            return False

        self.records.append(record)
        self.records_size += record.header.total_size
        return True

    def pack(self):
        records_bytes = bytearray()
        for record in self.records:
            records_bytes += record.pack()

        assert self.header.size() + len(records_bytes) <= WAL_PAGE_SIZE
        if self.header.size() + len(records_bytes) > (WAL_PAGE_SIZE - WALRecord.Header.size()):
            # padding with blank bytes
            padding_size = WAL_PAGE_SIZE - (self.header.size() + len(records_bytes))
            records_bytes += bytes(padding_size)
        return self.header.pack() + bytes(records_bytes)

    @classmethod
    def unpack(cls, data):
        header = cls.Header()
        header.unpack(data[:header.size()])
        record_data = data[header.size():]
        o = cls(start_lsn=header.lsn, remaining_size=header.last_page_written_size)
        # parse record data
        i = 0
        while i < len(record_data):
            record_header_data = record_data[i: i + WALRecord.Header.size()]
            if len(record_header_data) < WALRecord.Header.size():
                # It is not enough to parse a record. In this case,
                # the page is filled with blank bytes to align.
                break
            record_size = WALRecord.parse_record_size(record_header_data)
            if record_size == 0:
                # Empty bytes will be parsed that record_size is zero, which means
                # we parsed all valid records.
                break
            record = WALRecord.unpack(record_data[i: i + record_size])
            o.append_record(record)
            i += record.header.total_size
        return o


def lsn_to_filename(lsn):
    """LSN is a 64bit-length integer, but WAL files organize by segments.
    In order to directly locate which file the corresponding log content
    is in according to the LSN, we can generate the corresponding WAL file name
    according to the LSN according to a conversion algorithm.
    """
    return '%016X' % (lsn // WAL_SEGMENT_SIZE)


class WALManager:
    def __init__(self):
        self.write_lsn = 0
        self.flush_lsn = 0

        self.wal_buffer = []  # contains WALPage
        self.broken_page_written = False

        self.current_wal_fd = None

    # def next_lsn(self):
    #     self.write += 1
    #     return self.write

    def max_lsn(self):
        return self.write_lsn

    def write_record(self, record: WALRecord):
        lwlock_acquire(LWLockName.WAL_WRITE)

        # if WAL buffer is empty, create one
        if len(self.wal_buffer) == 0:
            wal_page = WALPage(self.max_lsn(), 0)
            self.write_lsn += WALPage.Header.size()
            self.wal_buffer.append(wal_page)

        # get the first not full WAL page
        wal_page = None
        for p in self.wal_buffer:
            if not p.is_full():
                wal_page = p

        # create a new WALPage because all WALPages are full, which
        # cannot contain any records
        if wal_page is None:
            wal_page = WALPage(self.max_lsn(), 0)
            assert len(wal_page.pack()) == WALPage.Header.size()
            self.write_lsn += WALPage.Header.size()
            self.wal_buffer.append(wal_page)

        # create a new WALPage because current one
        # cannot contain this record
        if not wal_page.append_record(record):
            remaining = wal_page.calc_remaining_size(record)
            if remaining >= (WAL_PAGE_SIZE - WALPage.Header.size() - WALRecord.Header.size()):
                # todo: release locks if ...
                lwlock_release(LWLockName.WAL_WRITE)
                # xxx: this is a critical section!! Rollback above allocation?
                raise WALError('Not supported huge WAL record.')

            # split the record to tow new records
            written_size = record.header.total_size - WALRecord.Header.size() - remaining
            written_record = WALRecord(record.header.xid, record.header.action, record.data[:written_size])
            remaining_record = WALRecord(record.header.xid, WALAction.NONE, record.data[written_size:])
            success = wal_page.append_record(written_record)
            assert success
            assert WAL_PAGE_SIZE == len(wal_page.pack())
            # we already increased the size of header
            self.write_lsn += written_record.header.total_size

            wal_page = WALPage(self.max_lsn(), written_size)
            assert len(wal_page.pack()) == WALPage.Header.size()
            self.write_lsn += WALPage.Header.size()
            self.wal_buffer.append(wal_page)
            wal_page.append_record(remaining_record)
            record = remaining_record

        self.write_lsn += record.header.total_size

        if record.header.action == WALAction.COMMIT or \
                len(self.wal_buffer) >= wal_buffer_size:
            self.wal_buffer_flush()

        lwlock_release(LWLockName.WAL_WRITE)

    def wal_buffer_flush(self):
        i = 0
        while i < len(self.wal_buffer):
            wal_page: WALPage = self.wal_buffer[i]
            filename = lsn_to_filename(wal_page.header.lsn)
            if self.current_wal_fd is None:
                self.current_wal_fd = directio_file_open(
                    os.path.join(WAL_DIR, filename),
                    os.O_RDWR | os.O_CREAT | os.O_APPEND
                )
            elif filename != os.path.basename(self.current_wal_fd.filepath):
                file_close(self.current_wal_fd)
                self.current_wal_fd = directio_file_open(
                    os.path.join(WAL_DIR, filename),
                    os.O_RDWR | os.O_CREAT | os.O_APPEND
                )
                # pre-allocate
                file_extend(self.current_wal_fd, size=WAL_SEGMENT_SIZE)
            flush_location = self.flush_lsn % WAL_SEGMENT_SIZE
            write_location = self.write_lsn % WAL_SEGMENT_SIZE
            # todo: this assert
            # assert flush_location == file_tell(self.current_wal_fd)

            # if write_location - flush_location >= WAL_PAGE_SIZE:
            #     data = wal_page.pack()
            # else:
            #     data_size = write_location - flush_location
            #     data = wal_page.pack()[flush_location % WAL_PAGE_SIZE: flush_location % WAL_PAGE_SIZE + data_size]

            # determine if write_location and flush_location in a same page
            n = flush_location // WAL_PAGE_SIZE
            in_a_same_page = n * WAL_PAGE_SIZE <= flush_location <= write_location < (n + 1) * WAL_PAGE_SIZE
            if in_a_same_page:
                data = wal_page.pack()[flush_location % WAL_PAGE_SIZE: write_location % WAL_PAGE_SIZE]
            else:
                data = wal_page.pack()[flush_location % WAL_PAGE_SIZE:]

            if wal_page.is_full():
                assert len(wal_page.pack()) == WAL_PAGE_SIZE
            file_write(self.current_wal_fd, data, sync=True)
            self.flush_lsn += len(data)  # don't forget it!
            assert wal_page.header.lsn <= self.write_lsn

            # evict worthless page
            if wal_page.is_full():
                self.wal_buffer.pop(i)
                assert wal_page.header.lsn <= self.flush_lsn
                assert i == 0
                i -= 1

            i += 1
