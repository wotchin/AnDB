import os

from andb.common.cstructure import CStructure, Integer4Field, Integer8Field
from andb.common.file_operation import directio_file_open, file_close, file_tell, file_extend, file_write, file_read, \
    file_lseek
from andb.constants.filename import WAL_DIR
from andb.constants.values import WAL_SEGMENT_SIZE, WAL_PAGE_SIZE
from andb.errno.errors import WALError
from andb.runtime.global_vars import wal_buffer_size
from andb.storage.lock.lwlock import LWLockName, lwlock_release, lwlock_acquire


class WALAction:
    TO_BE_CONTINUED = 0  # follow by previous page
    CHECKPOINT = 1
    BEGIN = 2
    COMMIT = 3
    ABORT = 4
    HEAP_INSERT = 5
    HEAP_DELETE = 6
    HEAP_BATCH_DELETE = 7
    HEAP_UPDATE = 8
    BTREE_INSERT = 9
    BTREE_DELETE = 10
    BTREE_UPDATE = 11


class WALRecord:
    class Header(CStructure):
        total_size = Integer4Field(unsigned=True)
        padding_size = Integer4Field(unsigned=True)
        xid = Integer8Field(unsigned=True)
        oid = Integer8Field(unsigned=True)
        pageno = Integer4Field(unsigned=True)
        tid = Integer4Field(unsigned=True)
        action = Integer4Field(unsigned=True)

    def __init__(self, xid, oid, pageno, tid, action, data):
        assert isinstance(data, bytes)

        self._header = self.Header()
        self._header.total_size = self._header.size() + len(data)
        self._header.xid = xid
        self._header.oid = oid
        self._header.pageno = pageno
        self._header.tid = tid
        self._header.action = action
        self.data = data

        # there is some padding bytes follows the tuple due to align
        self._header.padding_size = 0

    def pack(self):
        return self._header.pack() + self.data

    @classmethod
    def unpack(cls, data):
        header = cls.Header()
        header.unpack(data[:header.size()])
        real_data = data[header.size():]
        assert len(data) == header.total_size  # validate
        return cls(xid=header.xid, oid=header.oid, pageno=header.pageno, tid=header.tid, action=header.action, data=real_data)

    @classmethod
    def header_size(cls):
        return cls.Header.size()

    @classmethod
    def parse_record_size(cls, data):
        assert len(data) >= cls.header_size()
        header = cls.Header()
        header.unpack(data[:cls.header_size()])
        return header.total_size
        
    @property
    def total_size(self):
        return self._header.total_size

    @property
    def xid(self):
        return self._header.xid

    @property
    def action(self):
        return self._header.action

    @property
    def relation_oid(self):
        return self._header.oid

    @property
    def location(self):
        return self._header.pageno, self._header.tid


class WALPage:
    class Header(CStructure):
        lsn = Integer8Field(unsigned=True)
        last_page_written_size = Integer4Field(unsigned=True)

    def __init__(self, start_lsn, written_size):
        self.header = self.Header()
        self.header.lsn = start_lsn  # address of the page
        self.header.last_page_written_size = written_size
        self.records = bytearray()

    @property
    def records_size(self):
        return len(self.records)

    def is_full(self):
        assert self.records_size + self.header.size() <= WAL_PAGE_SIZE
        # If remaining space of the page cannot contain a record header,
        # we also think the page is full.
        current_size = self.records_size + self.header.size()
        return (WAL_PAGE_SIZE - WALRecord.Header.size()) <= current_size <= WAL_PAGE_SIZE

    def calc_remaining_size(self, record: WALRecord):
        record_size = record._header.total_size
        later_records_size = record_size + self.records_size
        remaining = (later_records_size + self.header.size()) - WAL_PAGE_SIZE
        return remaining if remaining > 0 else 0

    def append_record(self, record: WALRecord):
        """If failed, return False."""
        assert not self.is_full()

        remaining = self.calc_remaining_size(record)
        if remaining > 0:
            return False

        self.records += record.pack()

        if self.header.size() + self.records_size + WALRecord.Header.size() >= WAL_PAGE_SIZE:
            # padding with blank bytes
            padding_size = WAL_PAGE_SIZE - (self.header.size() + self.records_size)
            self.records += bytes(padding_size)
            record._header.padding_size = padding_size  # mark
        return True

    def pack(self):
        assert self.header.size() + self.records_size <= WAL_PAGE_SIZE
        return self.header.pack() + bytes(self.records)

    @classmethod
    def unpack(cls, data):
        header = cls.Header()
        header.unpack(data[:header.size()])
        record_data = data[header.size():]
        o = cls(start_lsn=header.lsn, written_size=header.last_page_written_size)
        # parse record data
        i = 0
        while i < len(record_data):
            record_header_data = record_data[i: i + WALRecord.Header.size()]
            if len(record_header_data) < WALRecord.Header.size():
                # It is not enough to parse a record. In this case,
                # the page is filled with blank bytes to align.
                # The routine shouldn't run here as we check padding_size ahead.
                assert False
            record_size = WALRecord.parse_record_size(record_header_data)
            if record_size == 0:
                # Empty bytes will be parsed that record_size is zero, which means
                # we parsed all valid records.
                # A certain scenario is extended empty bytes for segment file.
                break
            record = WALRecord.unpack(record_data[i: i + record_size])
            o.append_record(record)
            i += record._header.total_size
            # if the record has a non-zero padding_size, it is the last one of page.
            if record._header.padding_size > 0:
                break
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

        self.current_wal_fd = None

    def max_lsn(self):
        return self.write_lsn

    def write_record(self, record: WALRecord):
        """
        Write a record to the WAL buffer.
        If the record is too large for the current page:
        1. Split it into two records
        2. Write the first part with TO_BE_CONTINUED flag
        3. Create a new page for the remaining part
        """
        lwlock_acquire(LWLockName.WAL_WRITE)

        # Create new WAL page if buffer is empty
        if len(self.wal_buffer) == 0:
            wal_page = WALPage(self.max_lsn(), 0)
            self.write_lsn += WALPage.Header.size()
            self.wal_buffer.append(wal_page)

        # Find first non-full WAL page
        wal_page = None
        i = 0
        while i < len(self.wal_buffer):
            p = self.wal_buffer[i]
            if not p.is_full():
                wal_page = p
                break
            i += 1

        if i != len(self.wal_buffer):
            assert not wal_page.is_full()

        # Create new page if all pages are full
        if wal_page is None:
            wal_page = WALPage(self.max_lsn(), 0)
            assert len(wal_page.pack()) == WALPage.Header.size()
            self.write_lsn += WALPage.Header.size()
            self.wal_buffer.append(wal_page)

        # Handle record splitting if it doesn't fit in current page
        if not wal_page.append_record(record):
            remaining = wal_page.calc_remaining_size(record)
            if remaining >= (WAL_PAGE_SIZE - WALPage.Header.size() - WALRecord.Header.size()):
                #TODO: release locks if ...
                lwlock_release(LWLockName.WAL_WRITE)
                # xxx: this is a critical section!! Rollback above allocation?
                raise WALError('Not supported huge WAL record.')

            # Split record into two parts
            written_size = record._header.total_size - WALRecord.Header.size() - remaining
            written_record = WALRecord(record._header.xid, record._header.oid, record._header.pageno, record._header.tid, WALAction.TO_BE_CONTINUED, record.data[:written_size])
            remaining_record = WALRecord(record._header.xid, record._header.oid, record._header.pageno, record._header.tid, record._header.action, record.data[written_size:])
            
            success = wal_page.append_record(written_record)
            assert success
            assert WAL_PAGE_SIZE == len(wal_page.pack())
            # we already increased the size of header
            self.write_lsn += written_record._header.total_size

            wal_page = WALPage(self.max_lsn(), written_size)
            assert len(wal_page.pack()) == WALPage.Header.size()
            self.write_lsn += WALPage.Header.size()
            self.wal_buffer.append(wal_page)
            wal_page.append_record(remaining_record)
            record = remaining_record

        self.write_lsn += record._header.total_size
        self.write_lsn += record._header.padding_size  # if has

        # Flush buffer on commit/abort or when buffer is full
        if (record._header.action == WALAction.COMMIT or
                record._header.action == WALAction.ABORT or
                len(self.wal_buffer) >= wal_buffer_size):
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
                    os.O_RDWR | os.O_CREAT
                )
            elif filename != os.path.basename(self.current_wal_fd.filepath):
                file_close(self.current_wal_fd)
                self.current_wal_fd = directio_file_open(
                    os.path.join(WAL_DIR, filename),
                    os.O_RDWR | os.O_CREAT
                )
                # pre-allocate
                file_extend(self.current_wal_fd, size=WAL_SEGMENT_SIZE)

            flush_location = self.flush_lsn % WAL_SEGMENT_SIZE
            write_location = self.write_lsn % WAL_SEGMENT_SIZE
            if write_location < flush_location:
                # this is because they are in the different segments
                write_location += WAL_PAGE_SIZE  # adjustment

            # determine if write_location and flush_location in a same page
            n = flush_location // WAL_PAGE_SIZE
            in_a_same_page = n * WAL_PAGE_SIZE <= flush_location <= write_location < (n + 1) * WAL_PAGE_SIZE
            if in_a_same_page:
                data = wal_page.pack()[ - (write_location % WAL_PAGE_SIZE - flush_location % WAL_PAGE_SIZE):]
                assert len(data) == write_location % WAL_PAGE_SIZE - flush_location % WAL_PAGE_SIZE
            else:
                assert wal_page.is_full()
                assert len(wal_page.pack()) == WAL_PAGE_SIZE
                data = wal_page.pack()[flush_location % WAL_PAGE_SIZE:]

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

    @staticmethod
    def replay(lsn):
        prev_filename = None
        wal_fd = None

        current_lsn = lsn
        hold_incomplete_record = None
        while True:
            filename = os.path.join(WAL_DIR, lsn_to_filename(current_lsn))
            if not os.path.exists(filename):
                break
            # close previous fd before opening new one
            if filename != prev_filename:
                if wal_fd is not None:
                    file_close(wal_fd)
                prev_filename = filename

            wal_fd = directio_file_open(
                filename,
                os.O_RDWR | os.O_CREAT
            )
            lsn_segment = current_lsn % WAL_SEGMENT_SIZE
            if lsn_segment % WAL_PAGE_SIZE != 0:
                # align to page size
                lsn_segment -= lsn_segment % WAL_PAGE_SIZE

            file_lseek(wal_fd, lsn_segment)
            page_bytes = file_read(wal_fd, WAL_PAGE_SIZE)
            # break if the remaining bytes is empty
            if len(page_bytes) == 0:
                break
            wal_page = WALPage.unpack(page_bytes)

            i = 0
            while i < len(wal_page.records):
                record_size = WALRecord.parse_record_size(wal_page.records[i: i + WALRecord.Header.size()])
                if record_size == 0:
                    # reach padding empty bytes
                    assert len(wal_page.records) - i <= WALRecord.Header.size()
                    break
                record = WALRecord.unpack(bytes(wal_page.records[i: i + record_size]))
                if record._header.action == WALAction.TO_BE_CONTINUED:
                    hold_incomplete_record = record
                else:
                    if hold_incomplete_record is not None:
                        # concat incomplete record with current record
                        data = hold_incomplete_record.data + record.data
                        hold_incomplete_record = None
                        pageno, tid = record.location
                        record = WALRecord(xid=record.xid, oid=record.relation_oid, pageno=pageno, tid=tid,
                                            action=record.action, data=data)
                    # skip gotten record
                    if i + record_size > current_lsn % WAL_PAGE_SIZE:
                        yield record
                i += record_size

            # we have skipped some LSN in the first page, so we should clear this flag now
            current_lsn -= current_lsn % WAL_PAGE_SIZE
            current_lsn += WAL_PAGE_SIZE
