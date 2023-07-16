import threading
import time

NO_LOCK = 0
ACCESS_SHARE_LOCK = 1  # SELECT
ROW_SHARE_LOCK = 2  # SELECT FOR UPDATE/FOR SHARE
ROW_EXCLUSIVE_LOCK = 3  # INSERT, UPDATE, DELETE
SHARE_UPDATE_EXCLUSIVE_LOCK = 4  # ANALYZE
SHARE_LOCK = 5  # CREATE INDEX
SHARE_ROW_EXCLUSIVE_LOCK = 6  # EXCLUSIVE MODE
EXCLUSIVE_LOCK = 7  # SHARE/SELECT...FOR UPDATE
ACCESS_EXCLUSIVE_LOCK = 8  # ALTER TABLE, DROP TABLE
MAX_LOCK_MODE = ACCESS_EXCLUSIVE_LOCK

LOCK_NOT_AVAILABLE = 0
LOCK_OK = 1
LOCK_ALREADY_HELD = 2

_lock_table = {}


class LockEntry:
    def __init__(self, tag):
        self.tag = tag
        self.holders = set()
        self.mode = NO_LOCK


def lock_acquire(tag, lock_mode, dont_wait, wait_seconds):
    if tag not in _lock_table:
        _lock_table[tag] = LockEntry(tag)

    entry = _lock_table[tag]

    # if threading.get_ident() in entry.holders:
    #     # todo: not support lock upgrade directly
    #     return LOCK_ALREADY_HELD

    if entry.mode + lock_mode <= MAX_LOCK_MODE:
        entry.mode += lock_mode
        entry.holders.add(threading.get_ident())
        return LOCK_OK

    if not dont_wait:
        time.sleep(wait_seconds)

    if entry.mode + lock_mode <= MAX_LOCK_MODE:
        entry.mode += lock_mode
        entry.holders.add(threading.get_ident())
        return LOCK_OK
    else:
        return LOCK_NOT_AVAILABLE


def lock_release(tag, lock_mode):
    assert tag in _lock_table

    entry = _lock_table[tag]

    if threading.get_ident() in entry.holders:
        entry.holders.remove(threading.get_ident())

    if entry.mode - lock_mode >= NO_LOCK:
        entry.mode -= lock_mode
        return True

    return False
