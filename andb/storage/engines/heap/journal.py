"""Versioned, checksummed full-page redo/undo images.

The inverse is in the same WAL record as the forward change.  This deliberately
favours a small, auditable correctness path over WAL volume.  Table-level strict
2PL is required: an inverse image must never erase another transaction's write.
"""
import struct
import zlib

from andb.constants.values import PAGE_SIZE
from andb.errno.errors import WALError

_MAGIC = b'PGI1'
_HEADER = struct.Struct('>4sBIIIQ')
_PAGE = struct.Struct('>III')
_NONE = 0xffffffff


def encode_images(is_heap, pages, root=None, undo_of=0):
    old_root, new_root = (_NONE, _NONE) if root is None else root
    data = bytearray(_HEADER.pack(_MAGIC, int(is_heap), len(pages), old_root, new_root, undo_of))
    for pageno, before, after in pages:
        before = b'' if before is None else before
        if len(before) not in (0, PAGE_SIZE) or len(after) != PAGE_SIZE:
            raise WALError('Invalid page image size')
        data.extend(_PAGE.pack(pageno, len(before), len(after)))
        data.extend(before)
        data.extend(after)
    return struct.pack('>I', zlib.crc32(data)) + data


def decode_images(data):
    if len(data) < 4 + _HEADER.size:
        raise WALError('Truncated page-image record')
    crc, = struct.unpack_from('>I', data)
    if zlib.crc32(data[4:]) != crc:
        raise WALError('Page-image WAL checksum mismatch')
    magic, heap, count, old_root, new_root, undo_of = _HEADER.unpack_from(data, 4)
    if magic != _MAGIC or heap not in (0, 1) or count > len(data) // PAGE_SIZE:
        raise WALError('Unsupported page-image WAL format')
    if (old_root == _NONE) != (new_root == _NONE) or (heap and old_root != _NONE):
        raise WALError('Invalid index root image')
    pages, pos, seen = [], 4 + _HEADER.size, set()
    for _ in range(count):
        if pos + _PAGE.size > len(data):
            raise WALError('Truncated image header')
        pageno, nb, na = _PAGE.unpack_from(data, pos)
        pos += _PAGE.size
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
