"""Versioned, checksummed full-page redo/undo images.

Reuse common.cstructure for fixed-width binary fields. Callers already serialize
individual pages with SlotPage.pack() or B+tree node.pack(); WALRecord.pack() adds
the outer WAL frame. These two helpers only encode/decode the payload between
those layers: multiple before/after images, optional root changes, undo_of and a
CRC. Neither a single page nor the opaque WAL frame carries that information.
The pickle-based easy_tuple_serialize helpers are not a substitute for this
versioned on-disk layout. Keep PGI1 bytes stable when changing the implementation.

The inverse is in the same WAL record as the forward change. This deliberately
favours a small, auditable correctness path over WAL volume. Table-level strict
2PL is required: an inverse image must never erase another transaction's write.
"""
import zlib

from andb.common import cstructure
from andb.constants.values import PAGE_SIZE
from andb.errno.errors import WALError

_MAGIC = b'PGI1'
_HEADER_FORMAT = '4sBIIIQ'
_PAGE_FORMAT = 'III'
_HEADER_SIZE = cstructure.calcsize(_HEADER_FORMAT)
_PAGE_SIZE = cstructure.calcsize(_PAGE_FORMAT)
_CRC_SIZE = cstructure.calcsize('I')
_NONE = 0xffffffff


def encode_images(is_heap, pages, root=None, undo_of=0):
    """Group pre-serialized pages into the stable PGI1 WAL payload.

    pages contains (pageno, before, after), where before=None means a new page.
    root is an optional (old_root, new_root) pair for index splits. A forward
    PAGE_UPDATE uses undo_of=0; a COMPENSATION identifies the original record's
    physical end LSN. The leading CRC covers all payload fields and page bytes.
    """
    old_root, new_root = (_NONE, _NONE) if root is None else root
    data = bytearray(cstructure.pack(_HEADER_FORMAT, _MAGIC, int(is_heap),
                                    len(pages), old_root, new_root, undo_of))
    for pageno, before, after in pages:
        before = b'' if before is None else before
        if len(before) not in (0, PAGE_SIZE) or len(after) != PAGE_SIZE:
            raise WALError('Invalid page image size')
        data.extend(cstructure.pack(_PAGE_FORMAT, pageno, len(before), len(after)))
        data.extend(before)
        data.extend(after)
    return cstructure.pack('I', zlib.crc32(data)) + data


def decode_images(data):
    """Validate a PGI1 payload before exposing any images to recovery.

    Check the CRC/version/root metadata, then each page's address and lengths,
    rejecting duplicates, truncation and trailing bytes. Return
    (is_heap, pages, root, undo_of); page.unpack() remains the caller's job.
    """
    if len(data) < _CRC_SIZE + _HEADER_SIZE:
        raise WALError('Truncated page-image record')
    crc = cstructure.unpack_one('I', data[:_CRC_SIZE])
    if zlib.crc32(data[_CRC_SIZE:]) != crc:
        raise WALError('Page-image WAL checksum mismatch')
    magic, heap, count, old_root, new_root, undo_of = cstructure.unpack(
        _HEADER_FORMAT, data[_CRC_SIZE:_CRC_SIZE + _HEADER_SIZE])
    if magic != _MAGIC or heap not in (0, 1) or count > len(data) // PAGE_SIZE:
        raise WALError('Unsupported page-image WAL format')
    if (old_root == _NONE) != (new_root == _NONE) or (heap and old_root != _NONE):
        raise WALError('Invalid index root image')
    pages, pos, seen = [], _CRC_SIZE + _HEADER_SIZE, set()
    for _ in range(count):
        if pos + _PAGE_SIZE > len(data):
            raise WALError('Truncated image header')
        pageno, nb, na = cstructure.unpack(_PAGE_FORMAT, data[pos:pos + _PAGE_SIZE])
        pos += _PAGE_SIZE
        if pageno in seen or nb not in (0, PAGE_SIZE) or na != PAGE_SIZE or pos + nb + na > len(data):
            raise WALError('Invalid page image')
        seen.add(pageno)
        before = data[pos:pos + nb] or None
        pos += nb
        after = data[pos:pos + na]
        pos += na
        pages.append((pageno, before, after))
    if pos != len(data):
        raise WALError('Trailing bytes in page-image WAL')
    return bool(heap), pages, None if old_root == _NONE else (old_root, new_root), undo_of
