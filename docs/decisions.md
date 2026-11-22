# Decision log

Open decisions from the team guide, with the choice made and why. Update the status when the team changes one.

| # | Decision | Choice | Status | Why |
|---|---|---|---|---|
| 1 | Scope | Fixed 5 nodes (n1 to n5), crash faults only (no lying nodes), money created only at genesis, no fees | Decided | Removes membership changes and Byzantine handling, which would double the work. Matches the project brief ("Raft for crash faults"). |
| 2 | Insufficient-funds rule | A payment rejected for lack of money still uses up its nonce | Decided (A) | Already in implementation.md 3.6. Stops a rejected payment being replayed once the account is funded. |
| 3 | Balance reads | Read the node's local copy; may be a moment behind. Linearizable reads (ReadIndex) are a stretch goal | Decided (P) | Local reads need nothing from Raft. ReadIndex can be added later through a `CONTRACT:` PR. |
| 4 | "Tamper-proof" wording | "With crash faults only, the hash chain detects a rewritten history by comparing nodes, but it cannot stop a malicious majority from rewriting it." | Proposed (P and A) | Honest about what a crash-fault design guarantees. A to confirm for the report. |

## P-level decisions (Raft)

| Decision | Choice | Why |
|---|---|---|
| How tests handle time | A simulated-time event loop (`sim_clock.py`), not shortened timeouts | Real 300-600 ms timeouts, runs in milliseconds, and a seed replays exactly. 200 chaos seeds fit in the 5-minute budget. |
| Who starts and stops the transport | `RaftNode` | One owner, so start and stop order cannot get out of step. V must not call `transport.start` itself. |
| Leader election extras | PreVote and CheckQuorum are on by default (`RaftConfig`) | Needed for the partition demo: the minority leader steps down and a rejoining node cannot disrupt the cluster. |
| Events in the simulator | Off | `events.configure` stores one node id per process. |
| Python version | Code is written for 3.10 (no `asyncio.timeout` or `TaskGroup`); developed on 3.12 | CI runs 3.10. Only 3.12 is installed on the development laptop, so run the suite on RCE (3.10) before integration week. |
