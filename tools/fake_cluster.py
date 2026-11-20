"""Build realistic node data directories without Raft, for developing and demonstrating the auditor.

    python tools/fake_cluster.py --out /tmp/fake --nodes 5 --entries 3000 --snapshot-interval 1000
    python tools/audit.py --genesis /tmp/fake/genesis.json /tmp/fake/n1 /tmp/fake/n2 ...

It drives ``SqliteLedger`` + ``FileLogStore`` + ``FileSnapshotStore`` directly, in the same order Raft does:
append entries durably, advance and save the commit index, apply, and every ``snapshot_interval`` entries
save a snapshot *before* compacting the log. Each node can lag behind by a number of entries, which models
a follower that crashed earlier. The command stream mixes valid transfers, same-nonce conflicts,
overdrafts, replays, forged signatures and NOOPs; it is fully determined by ``seed``.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sys
from dataclasses import dataclass

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ledgernet.common.tx import NOOP_COMMAND, encode_transfer, public_bytes, sign_transfer
from ledgernet.common.types import LogEntry
from ledgernet.ledger.genesis import dump_genesis
from ledgernet.ledger.sqlite_ledger import SqliteLedger
from ledgernet.storage.file_log import FileLogStore
from ledgernet.storage.snapshot_store import FileSnapshotStore


def deterministic_key(label: str) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(hashlib.sha256(label.encode()).digest())


@dataclass
class FakeCluster:
    base_dir: str
    genesis_path: str
    genesis: dict[bytes, int]
    node_dirs: list[str]
    commands: list[bytes]


def make_commands(keys: list[Ed25519PrivateKey], n: int, rng: random.Random) -> list[bytes]:
    pubs = [public_bytes(k) for k in keys]
    nonces = [0] * len(keys)
    cmds: list[bytes] = []
    for _ in range(n):
        r = rng.random()
        s = rng.randrange(len(keys))
        to = (s + 1 + rng.randrange(len(keys) - 1)) % len(keys)
        if r < 0.80:  # valid transfer
            nonces[s] += 1
            cmds.append(encode_transfer(sign_transfer(keys[s], pubs[to], rng.randint(1, 5_000), nonces[s])))
        elif r < 0.85:  # same-nonce conflict: a second transfer reusing the last nonce
            cmds.append(encode_transfer(sign_transfer(keys[s], pubs[to], 1, max(nonces[s], 1))))
        elif r < 0.88:  # overdraft
            nonces[s] += 1
            cmds.append(encode_transfer(sign_transfer(keys[s], pubs[to], 10**12, nonces[s])))
        elif r < 0.91 and cmds:  # replay of an earlier command
            cmds.append(rng.choice(cmds))
        elif r < 0.94:  # forged signature
            raw = bytearray(encode_transfer(sign_transfer(keys[s], pubs[to], 7, nonces[s] + 1)))
            raw[100] ^= 0x01
            cmds.append(bytes(raw))
        else:
            cmds.append(NOOP_COMMAND)
    return cmds


def build_fake_cluster(
    base_dir: str,
    n_nodes: int = 5,
    n_entries: int = 2000,
    snapshot_interval: int = 500,
    n_accounts: int = 10,
    lag: dict[int, int] | None = None,
    seed: int = 1,
    balance: int = 5_000_000,
) -> FakeCluster:
    """Create ``<base_dir>/genesis.json`` and ``<base_dir>/n1..nN``. ``lag[i]`` (0-based node) is how many
    of the last entries node i never received."""
    rng = random.Random(seed)
    keys = [deterministic_key(f"fake-cluster/{seed}/{i}") for i in range(n_accounts)]
    genesis = {public_bytes(k): balance for k in keys}
    os.makedirs(base_dir, exist_ok=True)
    genesis_path = os.path.join(base_dir, "genesis.json")
    with open(genesis_path, "w", encoding="utf-8") as f:
        f.write(dump_genesis(genesis))

    commands = make_commands(keys, n_entries, rng)
    terms, term = [], 1
    for i in range(n_entries):
        if i and i % 400 == 0:
            term += 1  # a leader change every 400 entries; the new leader's NOOP is in the stream anyway
        terms.append(term)

    lag = lag or {}
    node_dirs = []
    for node in range(n_nodes):
        d = os.path.join(base_dir, f"n{node + 1}")
        node_dirs.append(d)
        upto = n_entries - lag.get(node, 0)
        log = FileLogStore(os.path.join(d, "wal"))
        snaps = FileSnapshotStore(os.path.join(d, "snapshots"))
        ledger = SqliteLedger(os.path.join(d, "state.db"), genesis)
        log.save_hard_state(term, "n1")
        batch: list[LogEntry] = []
        for i in range(1, upto + 1):
            batch.append(LogEntry(i, terms[i - 1], commands[i - 1]))
            if len(batch) == 64 or i == upto:
                log.append(batch)
                log.save_commit_index(i)
                for e in batch:
                    ledger.apply(e)
                    if (
                        snapshot_interval > 0
                        and ledger.last_applied() - log.snapshot_index() >= snapshot_interval
                    ):
                        snap = ledger.take_snapshot()
                        snaps.save(snap)  # durable first ...
                        log.compact_to(snap.last_included_index, snap.last_included_term)  # ... then trim
                batch = []
        log.close()
        ledger.close()
    return FakeCluster(base_dir, genesis_path, genesis, node_dirs, commands)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Create fake node data directories for the auditor")
    p.add_argument("--out", required=True)
    p.add_argument("--nodes", type=int, default=5)
    p.add_argument("--entries", type=int, default=2000)
    p.add_argument("--snapshot-interval", type=int, default=500)
    p.add_argument("--accounts", type=int, default=10)
    p.add_argument("--lag", action="append", default=[], help="NODE=ENTRIES, e.g. n3=150 (repeatable)")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args(argv)
    lag = {}
    for item in args.lag:
        name, _, count = item.partition("=")
        lag[int(name.lstrip("n")) - 1] = int(count)
    fc = build_fake_cluster(
        args.out, args.nodes, args.entries, args.snapshot_interval, args.accounts, lag, args.seed
    )
    print(f"genesis: {fc.genesis_path}")
    print("nodes:   " + " ".join(fc.node_dirs))
    return 0


if __name__ == "__main__":
    sys.exit(main())
