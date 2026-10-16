# Peer-to-Peer Payment Ledger

Implement a permissioned payment ledger on 4–5 nodes with signed transfers, account balances,
and per-account nonces. Use Raft for crash faults, or a simplified PBFT protocol with an explicit
Byzantine fault limit, to agree on a transaction order. Validate signatures, nonces, and balances
before applying committed transactions deterministically. Persist the log and recover returning
nodes through replay or snapshots. Demonstrate conflicting transfers, a leader failure, and a
network partition; compare final balance hashes and measure commit latency and recovery time.
Concepts: consensus, total-order broadcast, state-machine replication, replay protection, fault
models, recovery
