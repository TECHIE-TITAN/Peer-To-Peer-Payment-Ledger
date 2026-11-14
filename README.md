# LedgerNet: Peer-to-Peer Payment Ledger

A permissioned payment ledger on five nodes. Transfers are signed with Ed25519, every account has a balance
and a nonce, and the nodes agree on one order of transfers using Raft (crash faults only). Every node applies
the same committed log with the same deterministic rules, so all five hold identical balances.

## Where to read

| Document | What it covers |
|---|---|
| [docs/implementation.md](docs/implementation.md) | The single source of truth: frozen interfaces, formats, file layouts, tasks, timeline |
| [docs/LedgerNet — Team Roles and Work Breakdown (Plain-Language Guide).pdf](docs/LedgerNet%20—%20Team%20Roles%20and%20Work%20Breakdown%20(Plain-Language%20Guide).pdf) | Who builds what, in plain language |
| [docs/decisions.md](docs/decisions.md) | Decisions made and why |
| [ledgernet/raft/README.md](ledgernet/raft/README.md) | Raft design note, safety argument, how to run the Raft tests |

## Repository layout

```
ledgernet/
  common/      frozen contracts shared by everyone (types, messages, tx format, hashing, interfaces)
  testing/     shared fakes: in-memory stores, ReferenceLedger, SingleNodeConsensus
  raft/        consensus (P) and its simulated test network
  ledger/      SQLite ledger (A, stub)
  storage/     write-ahead log and snapshot files (A, stubs)
  net/         gRPC transport (V, stub)
  client/      client library (V)
  node.py      node process wiring (V, stub)
proto/         ledgernet.proto (contract)
tests/         contract/ raft/ ledger/ storage/ net/ e2e/
tools/ bench/ analysis/        auditor, load generator, graphs (A)
cluster/ experiments/          launcher and experiments (V)
docs/          design documents
```

Contract files (`ledgernet/common/`, `ledgernet/testing/`, `proto/`) are copied verbatim from
`docs/implementation.md` Section 3 and change only through a `CONTRACT:` pull request.

## Getting started

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # Python 3.10 or newer
make test                    # contract and Raft tests (quick suite)
make lint                    # black and ruff
```

`make proto` generates the gRPC code and needs `grpcio-tools` (already in the dependencies).

## Status

| Area | Owner | State |
|---|---|---|
| Repository bootstrap and contract tests | all | done |
| Raft consensus and its test lab | P | built; full Raft suite plus 200 chaos seeds pass (not yet run on Python 3.10) |
| Ledger, storage, auditor, load generator, graphs | A | not started (stubs) |
| Networking, node program, wallet, launcher, experiments | V | not started (stubs) |
