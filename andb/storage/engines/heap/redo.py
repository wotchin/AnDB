"""Physical WAL positions are byte offsets, including page headers and padding.

A record's end_lsn is the *exclusive* end of its last fragment (and padding).
Only complete logical records are exposed to recovery. Existing WAL framing is
retained; the new page-image actions carry their own versioned payload.
"""
import os
import re
import threading

from andb.common.cstructure import CStructure, Integer4Field, Integer8Field
from andb.common.file_operation import sync_directory
from andb.constants.filename import WAL_DIR
from andb.constants.values import WAL_SEGMENT_SIZE, WAL_PAGE_SIZE
from andb.errno.errors import WALError
from andb.runtime.global_vars import wal_buffer_size


class WALAction:
    TO_BE_CONTINUED = 0
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
    # Normal heap/index DML publishes one or more changed page images. A single
    # record contains before+after bytes and optional old/new B+tree roots, so
    # recovery can redo a WAL-only commit or undo a loser even after page eviction.
    PAGE_UPDATE = 12
    # Rollback (including crash recovery) logs its inverse images before applying
    # them. A CLR is redo-only: undo_of identifies the original PAGE_UPDATE. If
    # undo crashes, recovery replays the CLR instead of compensating that change
    # again. ABORT is written only after all required compensation is complete.
    COMPENSATION = 13


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
        if not isinstance(data, bytes):
            raise TypeError('WAL data must be bytes')
        self._header = self.Header()
        self._header.total_size = self._header.size() + len(data)
        self._header.xid, self._header.oid = xid, oid
        self._header.pageno, self._header.tid = pageno, tid
        self._header.action = action
        self._header.padding_size = 0
        self.data = data
        self.start_lsn = None
        self.end_lsn = None

    def pack(self):
        return self._header.pack() + self.data

    @classmethod
    def unpack(cls, data):
        if len(data) < cls.header_size():
            raise WALError('Truncated WAL record header')
        header = cls.Header()
        header.unpack(data[:header.size()])
        if len(data) != header.total_size or header.total_size < header.size():
            raise WALError('Invalid WAL record length')
        record = cls(header.xid, header.oid, header.pageno, header.tid,
                     header.action, data[header.size():])
        record._header.padding_size = header.padding_size
        return record

    @classmethod
    def header_size(cls):
        return cls.Header.size()

    @classmethod
    def parse_record_size(cls, data):
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
        self.header.lsn = start_lsn
        self.header.last_page_written_size = written_size
        self.records = bytearray()

    @property
    def records_size(self):
        return len(self.records)

    def is_full(self):
        return len(self.pack()) == WAL_PAGE_SIZE

    def calc_remaining_size(self, record):
        return max(0, self.header.size() + self.records_size + record.total_size - WAL_PAGE_SIZE)

    def append_record(self, record):
        if self.is_full() or self.calc_remaining_size(record):
            return False
        remaining = WAL_PAGE_SIZE - self.header.size() - self.records_size - record.total_size
        record._header.padding_size = remaining if remaining <= WALRecord.header_size() else 0
        # Set padding BEFORE serializing the header; preserve it on unpack.
        self.records.extend(record.pack())
        self.records.extend(bytes(record._header.padding_size))
        return True

    def pack(self):
        return self.header.pack() + bytes(self.records)

    @classmethod
    def unpack(cls, data):
        if len(data) < cls.Header.size() or len(data) > WAL_PAGE_SIZE:
            raise WALError('Invalid WAL page length')
        header = cls.Header()
        header.unpack(data[:header.size()])
        page = cls(header.lsn, header.last_page_written_size)
        offset = header.size()
        while offset < len(data):
            tail = data[offset:]
            if not any(tail):
                if len(data) == WAL_PAGE_SIZE:
                    page.records.extend(tail)
                break
            if len(tail) < WALRecord.header_size():
                raise WALError('Truncated WAL page record')
            size = WALRecord.parse_record_size(tail)
            if size < WALRecord.header_size() or size > len(tail):
                raise WALError('Invalid WAL page record size')
            record = WALRecord.unpack(tail[:size])
            padding = record._header.padding_size
            if offset + size + padding > len(data) or any(tail[size:size + padding]):
                raise WALError('Invalid WAL padding')
            page.records.extend(tail[:size + padding])
            offset += size + padding
        return page


def lsn_to_filename(lsn):
    return '%016X' % (lsn // WAL_SEGMENT_SIZE)


class WALManager:
    MAX_RECORD_SIZE = 64 * 1024 * 1024

    def __init__(self):
        self.write_lsn = self.flush_lsn = 0
        self.wal_buffer = []
        self.current_wal_fd = None  # kept for compatibility with callers
        self._lock = threading.RLock()
        self.failed = False

    def max_lsn(self):
        return self.write_lsn

    def write_record(self, record):
        if record.action not in range(WALAction.CHECKPOINT, WALAction.COMPENSATION + 1):
            raise WALError('Invalid public WAL action')
        record.pack()  # Validate all fields before changing buffer state.
        if len(record.data) > self.MAX_RECORD_SIZE:
            raise WALError('WAL record exceeds maximum supported size')
        with self._lock:
            if self.failed:
                raise WALError('WAL I/O failed; restart/recovery is required')
            remaining = record.data
            start = None
            while True:
                if not self.wal_buffer or self.wal_buffer[-1].is_full():
                    if self.write_lsn % WAL_PAGE_SIZE:
                        raise WALError('Unaligned new WAL page')
                    self.wal_buffer.append(WALPage(self.write_lsn, 0))
                    self.write_lsn += WALPage.Header.size()
                page = self.wal_buffer[-1]
                if start is None:
                    start = self.write_lsn
                capacity = WAL_PAGE_SIZE - len(page.pack()) - WALRecord.header_size()
                count = min(len(remaining), capacity)
                final = count == len(remaining)
                fragment = WALRecord(record.xid, record.relation_oid, *record.location,
                                     record.action if final else WALAction.TO_BE_CONTINUED,
                                     remaining[:count])
                if not page.append_record(fragment):
                    raise WALError('WAL append invariant violated')
                self.write_lsn += fragment.total_size + fragment._header.padding_size
                remaining = remaining[count:]
                if final:
                    break
            record.start_lsn, record.end_lsn = start, self.write_lsn
            if record.action in (WALAction.COMMIT, WALAction.ABORT) or len(self.wal_buffer) >= wal_buffer_size:
                self.wal_buffer_flush()
            return record.end_lsn

    def wal_buffer_flush(self):
        with self._lock:
            if self.failed:
                raise WALError('WAL I/O failed; restart/recovery is required')
            if self.flush_lsn == self.write_lsn:
                return
            os.makedirs(WAL_DIR, exist_ok=True)
            target = self.write_lsn
            try:
                for page in self.wal_buffer:
                    packed = page.pack()
                    offset = max(0, self.flush_lsn - page.header.lsn)
                    if offset >= len(packed):
                        continue
                    path = os.path.join(WAL_DIR, lsn_to_filename(page.header.lsn))
                    created = not os.path.exists(path)
                    # Never share a cursor with the recovery reader or FD cache.
                    with open(path, 'r+b' if not created else 'w+b', buffering=0) as stream:
                        stream.seek(page.header.lsn % WAL_SEGMENT_SIZE + offset)
                        pending = memoryview(packed)[offset:]
                        while pending:
                            n = stream.write(pending)
                            if not n:
                                raise OSError('Short WAL write')
                            pending = pending[n:]
                        os.fsync(stream.fileno())
                    if created:
                        sync_directory(WAL_DIR)
                # A flush is acknowledged only after ALL affected segments fsync.
                self.flush_lsn = target
                self.wal_buffer[:] = [p for p in self.wal_buffer if not p.is_full()]
            except BaseException:
                self.failed = True
                raise

    @staticmethod
    def _segments():
        if not os.path.isdir(WAL_DIR):
            return []
        files = sorted(f for f in os.listdir(WAL_DIR) if re.fullmatch(r'[0-9A-Fa-f]{16}', f))
        for number, name in enumerate(files):
            if int(name, 16) != number:
                raise WALError('WAL segment missing; refusing incomplete recovery')
        return files

    @classmethod
    def replay(cls, lsn):
        """Yield complete logical records with physical start_lsn/end_lsn.

        1. Enumerate contiguous segments from zero and validate their sizes.
        2. Read pages in physical order; check page addresses, frame sizes and
           padding. Reject interior holes/corruption rather than skip records.
        3. Assemble TO_BE_CONTINUED fragments across pages/segments, requiring
           the same transaction, relation and tuple identity for every fragment.
        4. On the final fragment, attach physical offsets (including framing and
           padding), and yield only records whose start_lsn is at least lsn.
        5. Ignore an incomplete final tail or final zero-filled preallocation.
           restore() separately removes that tail before the next append.

        Even for nonzero lsn, scan from zero to validate preceding framing and
        assemble a record that straddles the requested position. lsn filters
        complete logical starts; it is not a raw seek into a fragment. This
        reader never applies pages or decides which transactions committed.
        """
        files = cls._segments()
        fragments = bytearray()
        pending_identity = None
        logical_start = None
        for index, name in enumerate(files):
            with open(os.path.join(WAL_DIR, name), 'rb') as stream:
                size = os.fstat(stream.fileno()).st_size
                if size > WAL_SEGMENT_SIZE:
                    raise WALError('Oversized WAL segment')
                if index < len(files) - 1 and size != WAL_SEGMENT_SIZE:
                    raise WALError('Truncated non-final WAL segment')
                for page_offset in range(0, size, WAL_PAGE_SIZE):
                    data = stream.read(WAL_PAGE_SIZE)
                    base = int(name, 16) * WAL_SEGMENT_SIZE + page_offset
                    final_page = index == len(files) - 1 and page_offset + len(data) == size
                    if not any(data):
                        # Old writers preallocated zero-filled segment tails.
                        if index != len(files) - 1 or any(stream.read()):
                            raise WALError('Hole in WAL')
                        return
                    if len(data) < WALPage.Header.size():
                        if final_page:
                            return
                        raise WALError('Truncated WAL page header')
                    header = WALPage.Header()
                    header.unpack(data[:header.size()])
                    if header.lsn != base:
                        raise WALError('WAL page address mismatch')
                    offset = header.size()
                    while offset < len(data):
                        tail = data[offset:]
                        if not any(tail):
                            # At most one record-header worth of page padding.
                            if len(tail) > WALRecord.header_size():
                                if index != len(files) - 1 or any(stream.read()):
                                    raise WALError('Hole in WAL page')
                                return
                            break
                        if len(tail) < WALRecord.header_size():
                            if final_page:
                                return
                            raise WALError('Truncated WAL record header')
                        record_size = WALRecord.parse_record_size(tail)
                        if record_size < WALRecord.header_size() or record_size > WAL_PAGE_SIZE - WALPage.Header.size():
                            raise WALError('Invalid WAL frame size')
                        if record_size > len(tail):
                            if final_page:
                                return
                            raise WALError('Truncated WAL frame')
                        record = WALRecord.unpack(tail[:record_size])
                        if record.action not in range(WALAction.COMPENSATION + 1):
                            raise WALError('Unknown WAL action')
                        padding = record._header.padding_size
                        rest = WAL_PAGE_SIZE - offset - record_size
                        # Compatibility with old pages whose padding flag was not serialized.
                        if not padding and rest <= WALRecord.header_size() and len(data) == WAL_PAGE_SIZE:
                            padding = rest
                        if padding > WALRecord.header_size() or (padding and padding != rest):
                            raise WALError('Invalid WAL padding length')
                        if record_size + padding > len(tail):
                            if final_page:
                                return
                            raise WALError('Truncated WAL padding')
                        if any(tail[record_size:record_size + padding]):
                            raise WALError('Nonzero WAL padding')
                        identity = (record.xid, record.relation_oid, record.location)
                        if pending_identity is not None and identity != pending_identity:
                            raise WALError('Mismatched WAL continuation')
                        if logical_start is None:
                            logical_start = base + offset
                        fragments.extend(record.data)
                        if len(fragments) > cls.MAX_RECORD_SIZE:
                            raise WALError('Oversized logical WAL record')
                        end = base + offset + record_size + padding
                        if record.action == WALAction.TO_BE_CONTINUED:
                            pending_identity = identity
                            if end % WAL_PAGE_SIZE:
                                raise WALError('Continuation must end at a page boundary')
                        else:
                            result = WALRecord(record.xid, record.relation_oid, *record.location,
                                               record.action, bytes(fragments))
                            result.start_lsn, result.end_lsn = logical_start, end
                            if result.start_lsn >= lsn:
                                yield result
                            fragments.clear()
                            pending_identity = logical_start = None
                        offset += record_size + padding

    def restore(self):
        """Discard an incomplete/preallocated tail and resume at the valid end."""
        with self._lock:
            end = 0
            for record in self.replay(0):
                end = record.end_lsn
            files = self._segments()
            for name in files:
                base = int(name, 16) * WAL_SEGMENT_SIZE
                path = os.path.join(WAL_DIR, name)
                if base >= end:
                    os.unlink(path)
                elif base + os.path.getsize(path) > end:
                    with open(path, 'r+b') as stream:
                        stream.truncate(end - base)
                        stream.flush()
                        os.fsync(stream.fileno())
            if files:
                sync_directory(WAL_DIR)
            self.wal_buffer.clear()
            self.write_lsn = self.flush_lsn = end
            self.failed = False
            if end % WAL_PAGE_SIZE:
                page_start = end - end % WAL_PAGE_SIZE
                path = os.path.join(WAL_DIR, lsn_to_filename(page_start))
                with open(path, 'rb') as stream:
                    stream.seek(page_start % WAL_SEGMENT_SIZE)
                    page = WALPage.unpack(stream.read(end - page_start))
                self.wal_buffer.append(page)
            return end
