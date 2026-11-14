# Raft consensus (P)

`RaftNode` makes the five LedgerNet computers agree on one ordered log of commands. It never looks inside a
command: to Raft a payment is a sealed envelope that must sit in the same slot on every computer.
It only uses the interfaces in `ledgernet/common/interfaces.py` (`LogStore`, `SnapshotStore`, `StateMachine`,
`Transport`), so it runs unchanged on the in-memory fakes today and on A's disk storage and V's gRPC later.

## Files

| File | What it is |
|---|---|
| `node.py` | `RaftNode`: election, replication, commit, apply loop, recovery, snapshots, PreVote, CheckQuorum |
| `sim_clock.py` | Simulated-time asyncio loop: real timeouts, no waiting, exact replay of a seed |
| `sim_network.py` | `SimNetwork` and `SimTransport`: drop, delay, duplicate, reorder, partition, isolate |
| `sim_cluster.py` | `SimCluster`: N nodes in one process, crash/restart, and the safety-invariant checker |
| `sim_chaos.py` | Random crashes, partitions and message faults while clients propose |
| `tests/raft/` | The tests (see "Running the tests") |

The `sim_*` files are test infrastructure. Nothing in production imports them.

## How a command moves through Raft

1. `propose(cmd)` on the leader appends the entry to its log (fsync) and wakes one replication task per follower.
2. Each follower checks that its log matches the leader's just before the new entry, fixes any mismatch,
   appends (fsync), and replies with how far its log matches.
3. When an entry is stored on 3 of 5 nodes **and** belongs to the leader's own term, the leader commits it.
4. Every node applies committed entries to its state machine in index order. The leader then resolves the
   waiting `wait_for_apply(index, term)` call, or raises `LeadershipLostError` if a different term's entry
   ended up in that slot.

## What must be on disk before what (durability ordering)

| Before this happens | this must already be durable |
|---|---|
| Replying to `RequestVote` with a granted vote | `save_hard_state(term, voted_for)` |
| Adopting a higher term | `save_hard_state(new_term, None)` |
| A follower replying `success` to `AppendEntries` | `append` (and `truncate_from` if needed) |
| The leader counting its own copy toward a commit | its own `append` in `propose` |
| Trimming the log (`compact_to`) | `snapshot_store.save(snapshot)` |
| A follower replying to `InstallSnapshot` | `snapshot_store.save`, then the log is trimmed, then the state machine is restored |

On restart (`_recover`): load term and vote, restore the newest snapshot, finish a compaction that a crash
interrupted, replay the local log up to the persisted commit index, then start as a follower and let the
leader supply the rest.

## How each safety rule is guaranteed

| Rule | Mechanism in the code |
|---|---|
| One leader per term | A node grants one vote per term, and the vote is saved before it is reported. A leader needs 3 of 5 votes, and any two sets of 3 overlap. |
| Only an up-to-date log can win | `_log_is_up_to_date` compares `(last term, last index)` in every vote. A node that missed committed entries is refused. |
| Logs that agree at one entry agree on everything before it | `AppendEntries` is only accepted when `prev_log_index/prev_log_term` match. Conflicting entries are replaced, matching ones are never truncated (a delayed duplicate cannot delete newer entries). |
| Committed entries are never lost | An entry commits only once stored on a majority, and a majority-held entry exists in every later winner's log by the vote rule. `_merge_entries` refuses to overwrite anything at or below the commit index. |
| No committing old-term entries by counting copies | `_advance_commit` requires `term_at(N) == current_term`. Each new leader appends a NOOP so older entries commit with it (Raft paper Figure 8). |
| All nodes apply the same thing | Entries are applied strictly in index order, one batch at a time with no `await` inside, and `StateMachine.apply` is deterministic (A's rules). |
| Minority side refuses writes | CheckQuorum: a leader with no successful reply from a majority for one `election_timeout_max` steps down, so `propose` raises `NotLeaderError`. |
| A cut-off node cannot disturb the cluster | PreVote: a node first asks "would you vote for me?" without changing any term, and only starts a real election if a majority says yes. |

## Running the tests

```bash
make test-raft                       # about 90 s: 20 seeds per scenario
LEDGERNET_SEEDS=100 make test-raft   # more seeds for the quick scenarios
make fuzz-raft                       # 200 chaos seeds, about 5.5 min on a laptop
```

A failing assertion prints the seed. Rerun exactly that case with, for example,
`pytest tests/raft/test_fuzz.py -k "chaos_with_snapshots and 17"`.

Because the clock is simulated and every random choice comes from a seed, the same seed always produces the
same run. Raft uses its real production timeouts in the tests (300-600 ms elections, 75 ms heartbeats).

Invariants checked every 20 simulated milliseconds during the chaos tests (`SimCluster.check_invariants`):

- at most one leader per term;
- Log Matching: any two nodes hold identical entries at every index both have committed;
- State Machine Safety: every node that applied index *i* applied the same entry and reached the same hash;
- committed index never goes backwards on a running node;
- Leader Completeness: each newly elected leader holds every entry acknowledged in an earlier term;
- no background task has crashed (`RaftNode.fatal_error` is `None`).

`test_safety_rules.py` checks the same rules one handler call at a time. The tests were checked by breaking the
code on purpose (committing old-term entries by count, skipping the log up-to-date vote check, not saving the
vote, truncating on matching terms, removing CheckQuorum); each break makes a test fail.

## Decisions teammates need to know about

- **RaftNode owns the transport lifecycle.** `RaftNode.start()` calls `transport.start(self)` and `stop()`
  calls `transport.stop()`. V's `node.py` must not start the transport itself.
- **`peers` may include the node's own id.** It is ignored, so V can pass the full node list.
- **No contract change.** The constructor matches Section 3.8. Test hooks (`_now`, `_rng`,
  `on_leader_elected`) are plain attributes set after construction.
- **Events.** `STATE_RESTORED`, `LOCAL_REPLAY_DONE`, `ROLE_CHANGE`, `LEADER_ELECTED`, `SNAPSHOT_TAKEN` and
  `SNAPSHOT_INSTALLED` are emitted with the field names from Section 3.10. `events.configure` holds one node
  id per process, so the in-process simulator leaves events off.
- **Snapshots travel in one message.** gRPC rejects messages over 4 MB by default. V should raise the limit
  on the Raft service (or the team keeps account counts small).
- **`wait_for_apply` after the result cache.** Results for the last 10,000 applied entries are cached. A call
  for an older index raises `LeadershipLostError`, which V maps to `RETRY`; resubmitting the same bytes is
  safe because the nonce rejects a duplicate.
- **Log compaction keeps nothing extra.** The log is trimmed to the snapshot index, so a follower that is
  even one entry behind a fresh snapshot gets `InstallSnapshot`. Fine at demo scale.

## Not done yet (stretch, per the plan)

ReadIndex linearizable reads, leadership transfer, pipelined AppendEntries. Real-disk crash behaviour and
real-network timing are only exercised in integration week.
