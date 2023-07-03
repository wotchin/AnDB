NO_LOCK = 0
ACCESS_SHARE_LOCK = 1  # SELECT
ROW_SHARE_LOCK = 2  # SELECT FOR UPDATE/FOR SHARE
ROW_EXCLUSIVE_LOCK = 3  # INSERT, UPDATE, DELETE
SHARE_UPDATE_EXCLUSIVE_LOCK = 4  # ANALYZE, CREATE INDEX CONCURRENTLY
SHARE_LOCK = 5  # CREATE INDEX (WITHOUT CONCURRENTLY)
SHARE_ROW_EXCLUSIVE_LOCK = 6  # EXCLUSIVE MODE
EXCLUSIVE_LOCK = 7  # SHARE/SELECT...FOR UPDATE
ACCESS_EXCLUSIVE_LOCK = 8  # ALTER TABLE, DROP TABLE
MAX_LOCK_MODE = ACCESS_EXCLUSIVE_LOCK
