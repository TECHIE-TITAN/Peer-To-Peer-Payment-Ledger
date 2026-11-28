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
  ledger/      SQLite ledger and genesis format (A)
  storage/     write-ahead log and snapshot files (A)
  net/         gRPC transport, fault injector, Ledger and Admin services (V)
  client/      LedgerClient and the wallet CLI (V)
  node.py      node process wiring; node_config.py cluster config loader (V)
proto/         ledgernet.proto (contract)
tests/         contract/ raft/ ledger/ storage/ net/ bench/ analysis/ e2e/
tools/         offline auditor (A)
bench/         load generator and benchmark accounts (A)
analysis/      plots and tables from experiment results (A)
cluster/       local cluster launcher and the live demo (V)
experiments/   failover, latency and recovery experiments (V)
docs/          design documents
```

Contract files (`ledgernet/common/`, `ledgernet/testing/`, `proto/`) are copied verbatim from
`docs/implementation.md` Section 3 and change only through a `CONTRACT:` pull request.

## Getting started

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"      # Python 3.10 or newer
make proto                   # generate the gRPC code (needs grpcio-tools, already a dependency)
make lint                    # black and ruff
pytest -q --ignore=tests/e2e # unit, contract, Raft, ledger, storage and networking tests
pytest tests/e2e             # end-to-end scenarios on real node processes (slower, several minutes)
```

`make test` runs everything under `tests/`, including the end-to-end scenarios. `make fuzz-raft` runs the long
randomized Raft tests.

## Running it

Everything below runs on one machine: each node is a separate OS process talking gRPC over loopback, with its
log and snapshots on disk.

**Live demo** (S1 double spend, S3 partition, S2 leader failure under load, S9 snapshot catch-up, then the
offline auditor on every node's data directory):

```bash
python -m cluster.demo            # press Enter between steps
python -m cluster.demo --auto     # no pauses
python -m cluster.demo --only s1 s3
```

**A cluster and the wallet CLI.** `cluster/cluster.local.yaml` describes five nodes on ports 50051 to 50055.

```bash
python -m ledgernet.client.wallet keygen alice bob
python -m ledgernet.client.wallet genesis --out config/genesis.json alice=500 bob=500
for i in 1 2 3 4 5; do python -m ledgernet.node --config cluster/cluster.local.yaml --id n$i & done
python -m ledgernet.client.wallet send alice bob 25.50
python -m ledgernet.client.wallet balance bob
python -m ledgernet.client.wallet status          # role, term, commit index and state hash of every node
python tools/audit.py --genesis config/genesis.json data/n1 data/n2 data/n3 data/n4 data/n5   # after stopping
```

Nodes also take `--consensus single --storage memory --ledger reference` to run one node on the in-memory fakes.

**Experiments.** Results go to `results/<experiment>/<timestamp>/`, and the analysis scripts turn them into
figures and tables under `results/figures/`.

```bash
python -m experiments.failover --runs 10        # kill the leader under load; election time, unavailability
python -m experiments.latency --rates 50 100 200 400 800 1200
python -m experiments.recovery --lengths 10000 50000 100000 --intervals 0 1000 --runs 5
python analysis/failover.py results/failover/<timestamp> --out results/figures
python analysis/latency.py results/latency/<timestamp> --out results/figures
python analysis/recovery.py results/recovery/<timestamp>/recovery.csv --out results/figures
```

Set `GRPC_ENABLE_FORK_SUPPORT=0` to hide a harmless gRPC warning about forking.

## Status

| Area | Owner | State |
|---|---|---|
| Repository bootstrap and contract tests | all | done |
| Raft consensus and its test lab | P | done; full Raft suite and chaos seeds pass; runs on the real cluster |
| Ledger, storage, auditor, load generator, analysis | A | done; the auditor passes after every end-to-end scenario |
| Networking, node program, wallet, launcher, e2e scenarios S1 to S9, experiments, demo | V | done and verified on 5 local processes |
| Multi-machine (RCE) deployment | V | not done: the launcher supports local mode only |

Measured on one laptop (5 nodes and the load generator on the same machine, so no real network latency):
leader failover takes about 200 ms to elect a new leader with a 150 to 300 ms election timeout, the cluster
commits all offered load up to about 1,200 transfers per second and saturates at about 2,000, and a node with a
snapshot recovers in about 1 second at 100,000 log entries against about 12 seconds for full replay.
