# Kernel recovery and transaction audit — 2026-10-16

Baseline: `kernel-wip` at `27f426c1c457207013c2a79b9521c717962a519d`.
Repair branch: `fix/kernel-recovery-acid-20261016`.

## Status and scope

This is a correctness-first repair of the heap/index DML, WAL, buffer and SQL
transaction paths. It is **not** a claim that every WIP feature is finished, that
heap MVCC is implemented, or that the engine is production-ready.

No failing user data directory was supplied. The findings below come from the
source and executable reproductions against it, not forensic identification of
one particular historical crash. Preserve the original branch and data directory.

**Upgrade warning:** new page-image WAL actions 12/13 use a checksummed `PGI1`
payload. Old binaries cannot safely read this WAL. Do not alternate old and new
binaries on one data directory. Test on a copy or a newly initialized database;
export/reimport verified committed data where legacy recovery is ambiguous.

## Why replay could finish with the wrong data

| Layer | Defect in the baseline | Repair |
|---|---|---|
| WAL positions | Recovery adds logical record sizes, omitting page headers, fragmentation and padding. It resumes writing at physical file size, including incomplete/preallocated tails. | Each replayed record carries authoritative physical `start_lsn` and exclusive `end_lsn`. Restore truncates only the incomplete tail and reconstructs a partial last page before appending. |
| WAL writeback | Padding was set after packing; readers/writers could share an implicit file cursor; a zero-byte flush could write a whole page again. The split path was limited to two fragments. | Encode padding first, retain it on decode, explicit per-segment offsets, independent handles, no-op empty flush, arbitrary bounded multi-page fragments and fail-stop I/O errors. Missing segments and interior holes are errors, not end-of-log. |
| Buffer | Serialized bytes outlived subsequent page mutations. Replayed heap changes were not reliably marked dirty. Eviction stored an LRU node where callers expected its value. | Live page serialization is authoritative. Image publication marks pages dirty. Rescue dirty evicted pages before reading disk; remove superseded evicted copies. Force WAL before every dirty data write. |
| Heap | Page rollover logged the old page number. `delete_inplace` renumbered surviving TIDs, breaking scans, indexes and undo. Unpacked tuple storage was immutable. | Journal the actual destination page. Mark-delete without moving live TIDs. Restore exact before images on rollback; make unpacked mutation buffers mutable. |
| Undo | ABORT was durable before undo finished. Redo/data could reach disk while inverse information existed only in memory. Recovery swallowed redo/undo exceptions. | Forward and inverse images share a WAL record. Undo emits redo-only compensation records (CLRs), with `undo_of` identifying the original physical record end. A terminal ABORT is forced only after all compensation is logged/published. Recovery exceptions stop startup. |
| Checkpoint | The checkpoint held only an LSN, yet recovery discarded earlier transaction state. A loser spanning the checkpoint could be forgotten. | The old checkpoint pointer is advisory. Analyze and repeat all retained WAL, then undo losers in reverse LSN order. Do not prune WAL with this implementation. |
| SQL transactions | The planner returns a physical `CommandOperator`, but the entrance checked its logical wrapper. BEGIN/COMMIT/ROLLBACK could be treated as no-op utility statements. Read transactions reused a dummy XID; session XID was stored on a module, not a thread-local instance. | Recognize physical commands, allocate unique XIDs for real transactions and use per-thread state. Keep failed explicit blocks failed until transaction termination. Roll back disconnected sessions on their own connection thread. |
| Isolation | CSN helpers were not connected to physical tuple visibility. Relation locks were released after statements. Snapshot CSN exposed the next value rather than the last assigned one. | Use strict table-level two-phase locking: shared reads and exclusive writes, retained to transaction completion. Correct CSN high-water and retain statuses until a freezing/durable lookup mechanism exists. This is locking, **not heap MVCC**. |
| Index | Buffered allocation could reuse existing page numbers; new pages/root changes were not coherently published. Internal splits used leaf logic. SQL deletion/update of one duplicate key could remove other tuple pointers. Scans on non-first indexed columns could address the wrong attribute array. | Track allocation high-water, copy private nodes, journal affected split pages plus root together, distinguish leaf/internal split rules, remove the specific `(key, TID)` on SQL DML, and fix index-column addressing. |
| Metadata/IO | Dropped OIDs could be reused while old WAL still referenced them. Catalog fsync preceded buffered flush. Shared FD cursors and eviction/reopen could interfere with I/O. | Durable OID allocation high-water, raised from retained WAL at recovery; per-file atomic catalog replacement; positional page I/O and synchronized FD-cache bookkeeping; do not reapply `O_TRUNC` on reopen. |

## Recovery protocol and invariants

A DML operation constructs private before/after page images while holding its
transaction's exclusive relation lock. Under a short storage/writeback latch it
appends the complete logical WAL record, then publishes dirty buffer pages with
the record's physical end LSN. This avoids an unlogged public intermediate page.
An oversized/invalid tuple is rejected before publication.

Commit is **no-force**: force the WAL including COMMIT before acknowledgement;
data pages may be written later. Buffer eviction/checkpoint always forces WAL
before dirty data. A failed WAL/data write stops the engine for recovery rather
than allowing later queries to claim a successful commit with uncertain state.

Rollback walks changes backwards. For each change it logs a CLR containing the
inverse page images and the compensated record's end LSN, then publishes those
images. A new-format ABORT means *undo complete*. If recovery crashes after a CLR
is forced but before the page is published, the next recovery redoes the CLR and
skips undoing the original record a second time.

Recovery scans retained WAL from zero, restores the writer at the last complete
logical record, reconstructs commit/XID state, repeats page images, redoes CLRs,
and compensates remaining losers. Full images do not depend on an unchecksummed
on-disk pageLSN being trustworthy; the tests also exercise a truncated heap page
with intact WAL. Root pointers are published after their referenced index pages
are durable. Any recovery error aborts startup instead of returning partial data.

Checksums cover the new page-image payload. This is not comprehensive protection
against arbitrary corruption of every legacy WAL header/control record, nor a
substitute for backups.

## Regression evidence

The baseline's default suite passed **74 tests**. The original
`a_test_wal_manager` was not collected at all; it also omitted required WALRecord
arguments. The first seven focused additions produced **5 failures / 2 passes**
on the baseline. They covered WAL-only durability, rollover, loser recovery,
consecutive restart, stable deletion, cached writeback and CSN visibility.

The expanded suite contains **139 cases**: 75 existing/reactivated cases plus
64 new cases. Two existing expectations were corrected rather than removed:
current CSN means *last assigned*, and persisted relation OIDs must not go
backwards or be reused. The long 50,000-operation WAL test is re-enabled.

| New test module | Cases | What is checked |
|---|---:|---|
| `tests/recovery/test_wal_boundaries.py` | 32 | Exact page/segment boundaries, >2 fragments, physical replay start/end, many tail truncations, preallocation, append after restore, zero-byte flush, missing segments, interior holes, fsync failure and lock-safe rejection. |
| `tests/recovery/test_crash_recovery.py` | 17 | Fresh processes, WAL-only commit, repeated restart, loser pages on disk, rollover, crash around COMMIT, repeated crash during CLR replay/undo, NOT NULL rollback, explicit DDL rejection, torn heap page, OID reuse, duplicate indexes and height-3 index splits with eviction. |
| `tests/recovery/test_isolation.py` | 6 | No dirty reads, retained read locks/repeatability/phantom prevention, concurrent increments and unique session XIDs, independent-table winner/loser, disconnect rollback and concurrent readers under eviction. |
| `tests/recovery/test_storage_invariants.py` | 6 | Mutation after serialization, FD eviction/reopen, WAL-before-data, fail-stop data-write errors, checksum corruption refusal and recovered XID/CSN state. |
| `tests/recovery/test_model_histories.py` | 3 | Seeds 7/41/2026, each with 18 transactions of six mixed DML operations; commit/rollback/checkpoint, a final real SIGKILL, and two fresh recoveries checked against a Python reference model and index contents. |

Reproduce from repository root:

```sh
python -m pip install -r requirements.txt -r requirements-test.txt pytest-timeout
python -m pytest tests -ra --timeout=120 --junitxml=kernel-tests.xml
```

CI runs the complete suite on Python 3.11 and 3.13, uploading JUnit results. The
fault tests use isolated temporary databases, real process death (`os._exit` and
SIGKILL), injected I/O failures and byte-level truncation/corruption. They do not
constitute a physical-power-loss campaign on multiple filesystems or devices.

## Boundaries that remain deliberately explicit

* **Transactional DDL is not implemented.** Explicit SQL transactions reject DDL
  rather than falsely promising catalog rollback. Per-catalog-file replacement
  is atomic, but CREATE/DROP span multiple catalogs/files; crash-atomic and
  concurrent DDL are outside this repair's guarantee. PK/FK/unique-constraint
  semantics not implemented by the original SQL engine are not added here.
* **No physical heap MVCC/version chain or VACUUM is added.** Isolation comes from
  coarse strict table locks; readers can block writers and vice versa. Tombstones,
  retained transaction statuses, and index allocation high-water consume space.
* **The repair favours correctness over write amplification and restart cost.**
  Before+after images cost roughly 16 KiB per changed page before WAL framing;
  update currently uses delete+insert. Full retained-WAL scanning and in-memory
  recovery analysis are not scalable checkpoint/WAL-retention solutions.
* **Legacy recovery is conservative.** Committed legacy heap logs have a guarded
  compatibility path. Unresolved legacy transactions, legacy ABORT-before-undo,
  and logical legacy index WAL are refused where correctness cannot be proved.
  Wrong page numbers, reused OIDs or missing inverse data already emitted by old
  code cannot universally be reconstructed from the log. This patch does not
  certify an existing corrupted database as repaired.
* **Storage still depends on durable filesystem semantics.** The tests verify
  ordering/failure handling, not every possible device reorder, sector tear,
  media error, malicious file edit, or OS/filesystem failure mode.
