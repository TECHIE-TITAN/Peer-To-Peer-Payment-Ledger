# A: ledger, storage, auditor, load generator, analysis

Design notes for teammates. The contracts these implement are in `docs/implementation.md` Section 3; nothing
here changes a contract.

## What is where

| Piece | File | Plug it fills |
|---|---|---|
| Ledger state machine (SQLite) | `ledgernet/ledger/sqlite_ledger.py` | `SqliteLedger(db_path, genesis)` → `LedgerStateMachine` |
| genesis.json loader | `ledgernet/ledger/genesis.py` | `load_genesis(path) -> dict[bytes, int]` |
| Write-ahead log | `ledgernet/storage/file_log.py` | `FileLogStore(dir_path)` → `LogStore` |
| Snapshot files | `ledgernet/storage/snapshot_store.py` | `FileSnapshotStore(dir_path, keep=2)` → `SnapshotStore` |
| Offline auditor | `tools/audit.py` | `tools.audit.audit_dirs(data_dirs, genesis)` |
| Fake node directories (no Raft) | `tools/fake_cluster.py` | demo / auditor development |
| Load generator | `bench/loadgen.py` (+ `bench/raw_client.py`, `bench/stub_server.py`, `bench/accounts.py`) | writes `requests.csv`, `acked.json`, `meta.json` |
| WAL benchmark | `bench/bench_wal.py` | fsync latency and append throughput |
| Plots | `analysis/{make_synthetic,latency,failover,recovery}.py` | figures + CSV tables in `results/figures/` |

## Durability (what "saved" means)

* `FileLogStore.append` writes every record, then **one fsync per call**, then updates memory. When it returns,
  the entries are on disk. Raft may count itself towards a commit and answer AppendEntries success.
* `truncate_from` appends a TRUNCATE record and fsyncs before returning.
* `save_hard_state` is atomic: tmp file, fsync, rename, directory fsync.
* `save_commit_index` writes in place with a CRC and fsyncs at most every 100 ms. A stale value is safe (a lower
  bound); a corrupt file reads as 0. Call `close()` on shutdown to flush it.
* `compact_to` rewrites the surviving suffix to `log.wal.tmp` (fsync), atomically replaces `logmeta.json`,
  then renames the log. A crash between the two is repaired on open: entries at or below the snapshot index in
  `logmeta.json` are skipped.
* `FileSnapshotStore.save` is tmp, fsync, rename, directory fsync. `load_latest` skips a file whose header or
  SHA-256 does not verify and falls back to the previous snapshot. Raft must save the snapshot **before** calling
  `compact_to`, as `RaftNode` already does.
* Torn writes: on open, a short or CRC-failing record ends the log, and the file is cut there. The crash tests cut
  `log.wal` at every byte of the last record.
* Directory fsync is a no-op on Windows. The real durability numbers come from Linux and RCE.

## Ledger

* `apply` implements Section 3.6 exactly. `tests/ledger/test_differential.py` compares it with `ReferenceLedger`
  after every entry: equal `ApplyResult`, chain head, state hash and receipts, with conservation checked each time.
  The quick suite runs 300 examples. `LEDGERNET_FUZZ=1` runs 10 000, which passed in about 3 minutes.
* Decoding and the signature check happen before `BEGIN`. Each entry is one SQLite transaction.
* The database is a cache: the constructor always starts from genesis and deletes the old file. Durability comes
  from the WAL and snapshots, so SQLite runs with `synchronous=OFF`.
* A snapshot is the serialized database (`Connection.serialize()` on 3.11+, a temporary backup file on 3.10).
  `restore` validates the bytes, and `meta.last_applied` must equal `last_included_index`, before it swaps the
  live file. A bad snapshot raises `ValueError` and leaves the ledger untouched.
* Snapshot bytes are only meaningful to `SqliteLedger`. Every node must run the same ledger kind, which is already
  required because the leader sends its own snapshot.

## Auditor

`python tools/audit.py --genesis config/genesis.json data/n1 ... data/n5` exits 0 on PASS and 1 on FAIL.

For each directory, the auditor restores the latest valid snapshot (or starts from genesis). It opens the WAL
**read-only** (nothing on disk is repaired) and replays it up to the persisted commit index. At every entry it
checks:

* chain continuity;
* contiguous indexes and non-decreasing terms;
* nonce steps;
* conservation and no negative balance;
* agreement with `ReferenceLedger` (skip with `--no-reference`).

Across directories, block hashes must agree on every common index, and nodes at the same `last_applied` must have
equal state hashes.

It reports FAIL in these cases:

* the commit index is beyond the end of the log, which means committed records are lost or corrupt;
* a compacted log has no valid snapshot to cover it;
* a rewritten record carries a valid CRC (caught by the cross-node mismatch);
* a snapshot's balances are tampered with (conservation fails);
* a snapshot's chain head is tampered with (cross-node mismatch).

A torn tail beyond the commit index only gives a warning.

On the "tamper-proof" wording (open decision 4): with crash faults only, the hash chain *detects* a rewritten
history by comparing nodes (the auditor does exactly that). It cannot *prevent* a malicious majority from rewriting
history.

## Load generator

Until V's `ledgernet.client.LedgerClient` is merged, `bench/raw_client.py` uses the generated stubs directly and
follows the same rules from V7.6:

* follows leader hints;
* backs off on errors;
* resubmits the same bytes on `RETRY`;
* resolves a `TIMEOUT` through the receipt and the account nonce.

Swapping to V's client only changes `RawLedgerClient(...)` in `bench/loadgen.py`.

Extra outcome values in `requests.csv`, beyond the proto's `COMMITTED` and `PRECHECK_FAILED`:

* `NOT_APPLIED`: after a timeout, the nonce turned out to be consumed by another transfer;
* `FAILED`: the client gave up after `max_attempts`;
* `NO_FREE_SENDER`: in an open loop, the arrival found every account busy, so raise `--accounts`.

Latency is the client-side time to the final answer, including redirects and retries.

## Analysis

`failover.py` measures *unavailability* from the kill to the first commit of a request **sent after** the kill.
Responses to requests already in flight to the old leader can arrive just after the kill, and counting them would
report a 0 ms outage. Every figure is saved as PDF and PNG, with a CSV table next to it.
