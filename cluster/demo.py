"""Live demo (V7.10): S1, S3, S2 and S9 on five real node processes, each ending with the status table.

    python -m cluster.demo            # press Enter between steps
    python -m cluster.demo --auto     # no waiting, for a dry run
    python -m cluster.demo --only s1 s3

Everything runs on this machine: a temporary 5-node cluster (real processes, real gRPC, files on disk).
The finale stops the nodes and runs the offline auditor over their data directories.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import grpc
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cluster.launcher import Cluster, ClusterError, write_local_config
from ledgernet.client import COMMITTED, NOT_APPLIED
from ledgernet.client.wallet import format_rupees, format_status_table
from ledgernet.common.tx import encode_transfer, public_bytes, sign_transfer, tx_id
from ledgernet.common.types import TxStatus
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.gen import ledgernet_pb2_grpc as pb_grpc
from ledgernet.ledger.genesis import dump_genesis
from tools.audit import audit_dirs

ROOT = str(Path(__file__).resolve().parents[1])
RS = 100  # paise per rupee
RAFT = {
    "election_timeout_min_ms": 150,
    "election_timeout_max_ms": 300,
    "heartbeat_interval_ms": 30,
    "rpc_timeout_ms": 150,
    "snapshot_interval": 500,
}


@dataclass(eq=False)
class Person:
    name: str
    priv: Ed25519PrivateKey

    @classmethod
    def new(cls, name: str) -> Person:
        return cls(name, Ed25519PrivateKey.generate())

    @property
    def pub(self) -> bytes:
        return public_bytes(self.priv)

    def transfer(self, to: Person, paise: int, nonce: int) -> bytes:
        return encode_transfer(sign_transfer(self.priv, to.pub, paise, nonce))


class Demo:
    def __init__(self, cluster: Cluster, people: dict[str, Person], auto: bool, work: str) -> None:
        self.cluster, self.people, self.auto, self.work = cluster, people, auto, work

    # ---- presentation

    def banner(self, title: str) -> None:
        print(f"\n{'=' * 78}\n  {title}\n{'=' * 78}", flush=True)

    def say(self, text: str) -> None:
        print(f"  {text}", flush=True)

    async def pause(self, prompt: str = "press Enter to continue") -> None:
        if self.auto:
            await asyncio.sleep(0.3)
            return
        await asyncio.to_thread(input, f"\n  [{prompt}] ")

    async def table(self, converge: bool = True, split: bool = False) -> None:
        """The `wallet status` table, and whether the live nodes agree."""
        if converge:
            await self.cluster.wait_converged(timeout=60)
        statuses = await self.cluster.client.status_all()
        print("\n" + "\n".join("  " + line for line in format_status_table(statuses).splitlines()))
        live = [s for s in statuses.values() if s is not None]
        agree = len({(s.state_hash, s.chain_head) for s in live}) == 1
        total = {s.total_balance for s in live}
        note = (
            "all agree"
            if agree
            else ("different on purpose: the cluster is split" if split else "NOT identical")
        )
        self.say(
            f"state hash and chain head on {len(live)} node(s): {note}; money in the system: "
            f"Rs {format_rupees(total.pop()) if len(total) == 1 else '?'} (never changes)"
        )

    async def balance(self, name: str, node: str | None = None) -> str:
        paise, nonce = await self.cluster.client.get_account(self.people[name].pub, node)
        return f"{name}: Rs {format_rupees(paise)} (nonce {nonce})"

    # ---- S1: double spend

    async def s1(self) -> None:
        self.banner("S1  Double spend: Alice has Rs 500 and tries to pay it twice at the same moment")
        alice, bob, carol = (self.people[n] for n in ("alice", "bob", "carol"))
        self.say("Two different Rs 500 transfers, both nonce 1, one to Bob via n1, one to Carol via n4.")
        await self.pause()
        first, second = await asyncio.gather(
            self.cluster.client.submit(alice.transfer(bob, 500 * RS, 1), node="n1"),
            self.cluster.client.submit(alice.transfer(carol, 500 * RS, 1), node="n4"),
        )
        for who, result in (("-> Bob   via n1", first), ("-> Carol via n4", second)):
            self.say(
                f"{who}: {result.outcome} / {TxStatus(result.tx_status).name} (log index {result.index})"
            )
        await self.cluster.wait_converged()
        for name in ("alice", "bob", "carol"):
            self.say(await self.balance(name))
        self.say("Exactly one transfer was applied. The same verdict is on every node:")
        await self.table()
        await self.pause()

    # ---- S3: partition

    async def s3(self) -> None:
        self.banner("S3  Network partition: the leader and one follower are cut off from the other three")
        erin, frank, gina = (self.people[n] for n in ("erin", "frank", "gina"))
        old = await self.cluster.wait_for_leader()
        buddy = next(n for n in self.cluster.node_ids if n != old)
        majority = [n for n in self.cluster.node_ids if n not in (old, buddy)]
        self.say(f"Leader is {old}. Partition: [{old}, {buddy}] | {majority}")
        await self.pause()
        await self.cluster.partition([[old, buddy]])

        self.say(f"Erin asks the cut-off leader {old} to pay Frank Rs 100 (nonce 1)...")
        async with grpc.aio.insecure_channel(self.cluster.address(old)) as channel:
            response = await pb_grpc.LedgerStub(channel).SubmitTransfer(
                pb.SubmitTransferRequest(command=erin.transfer(frank, 100 * RS, 1)), timeout=15
            )
        self.say(
            f"  {old} answers {pb.SubmitTransferResponse.Outcome.Name(response.outcome)}: it cannot reach a "
            "majority, so it does not claim the transfer is committed."
        )
        new = await self.cluster.wait_for_leader(exclude=[old, buddy])
        self.say(
            f"The majority elected {new}. The same nonce is now spent there on a different transfer: Erin -> Gina."
        )
        won = await self.cluster.client.submit(erin.transfer(gina, 100 * RS, 1), node=majority[0])
        self.say(f"  {won.outcome} / {TxStatus(won.tx_status).name} (log index {won.index})")
        await self.table(converge=False, split=True)
        await self.pause("press Enter to heal the partition")
        await self.cluster.heal()
        self.say("Healed. The cut-off leader steps down and discards its uncommitted entry.")
        for name in ("erin", "frank", "gina"):
            self.say(await self.balance(name))
        await self.table()
        await self.pause()

    # ---- S2: leader failure under load

    async def s2(self) -> None:
        self.banner("S2  Leader failure under load: SIGKILL the leader while 6 accounts keep paying")
        senders = [self.people[f"load{i}"] for i in range(6)]
        sink = self.people["sink"]
        stop, acked = asyncio.Event(), []

        async def loop(person: Person) -> None:
            nonce = 1
            while not stop.is_set():
                await asyncio.sleep(0.02)
                command = person.transfer(sink, 1, nonce)
                result = await self.cluster.client.submit(command, max_attempts=6)
                if result.outcome == COMMITTED:
                    if result.tx_status == TxStatus.APPLIED:
                        acked.append(tx_id(command))
                    nonce += 1
                elif result.outcome == NOT_APPLIED:
                    nonce += 1
                else:
                    await asyncio.sleep(0.1)

        await self.pause()
        started = time.monotonic()
        tasks = [asyncio.create_task(loop(p)) for p in senders]

        async def report(seconds: int, kill_at: int, restart_at: int) -> tuple[str, float]:
            victim, killed_at = "", 0.0
            restarting: asyncio.Task | None = None
            last = 0
            for second in range(1, seconds + 1):
                await asyncio.sleep(1)
                leader = await self.cluster.leader() or "none"
                self.say(
                    f"t+{second:>2}s  {len(acked) - last:>4} commits/s   leader: {leader}   "
                    f"total acknowledged: {len(acked)}"
                )
                last = len(acked)
                if second == kill_at:
                    victim = leader
                    killed_at = time.monotonic()
                    self.cluster.kill(victim)
                    self.say(f"        *** SIGKILL {victim} (the leader) ***")
                if second == restart_at and victim:
                    restarting = asyncio.create_task(self.cluster.start(victim))  # do not stall the counter
                    self.say(f"        *** restarting {victim} from its own disk ***")
            if restarting is not None:
                await restarting
            return victim, killed_at

        try:
            victim, _ = await report(14, kill_at=4, restart_at=9)
        finally:
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        self.say(f"Load stopped after {time.monotonic() - started:.0f}s. {victim} was the leader that died.")
        await self.cluster.wait_converged(timeout=60)
        missing = 0
        for node_id in self.cluster.node_ids:
            for start in range(0, len(acked), 100):
                batch = acked[start : start + 100]
                found = await asyncio.gather(*(self.cluster.client.get_receipt(t, node_id) for t in batch))
                missing += sum(1 for r in found if not r.found)
        self.say(f"Checked every acknowledged transfer ({len(acked)}) on all 5 nodes: {missing} missing.")
        await self.table()
        await self.pause()

    # ---- S9: snapshot catch-up

    async def s9(self) -> None:
        self.banner(
            "S9  A follower is down while 3000 more transfers commit; it returns and is brought up to date"
        )
        senders = [self.people[f"bulk{i}"] for i in range(10)]
        sink = self.people["sink"]
        leader = await self.cluster.wait_for_leader()
        victim = next(n for n in self.cluster.node_ids if n != leader)
        self.say(
            f"Killing follower {victim}. Snapshots are taken every {RAFT['snapshot_interval']} entries, "
            "so the log is compacted while it is away."
        )
        await self.pause()
        self.cluster.kill(victim)

        async def burst(person: Person) -> None:
            for nonce in range(1, 301):
                await self.cluster.client.submit(person.transfer(sink, 1, nonce))

        began = time.monotonic()
        await asyncio.gather(*(burst(p) for p in senders))
        self.say(f"3000 transfers committed by the other 4 nodes in {time.monotonic() - began:.1f}s.")
        await self.pause(f"press Enter to restart {victim}")
        await self.cluster.start(victim)
        await self.cluster.wait_converged(timeout=90)
        events = [
            e
            for e in self.cluster.collect_events()
            if e["event"] == "SNAPSHOT_INSTALLED" and e["node"] == victim
        ]
        if events:
            self.say(
                f"{victim} received a snapshot (SNAPSHOT_INSTALLED, index {events[-1]['index']}, "
                f"{events[-1]['ms']:.0f} ms) instead of replaying 3000 entries one by one."
            )
        else:
            self.say(f"{victim} caught up from the leader's log without needing a snapshot.")
        await self.table()
        await self.pause()

    # ---- finale

    async def finale(self) -> None:
        self.banner("Finale  Stop every node and audit their data directories offline")
        for node_id in self.cluster.running():
            await self.cluster.stop(node_id)
        dirs = [self.cluster.data_dir(n) for n in self.cluster.node_ids]
        report = await asyncio.to_thread(audit_dirs, dirs, self.cluster.genesis_path)
        text = report.render().replace(self.work + "/", "").replace(self.work + "\\", "")
        print("\n" + "\n".join("  " + line for line in text.splitlines()))
        self.say("AUDIT PASSED" if report.ok else "AUDIT FAILED")


async def run(args: argparse.Namespace) -> int:
    names = ["alice", "bob", "carol", "erin", "frank", "gina", "sink"]
    names += [f"load{i}" for i in range(6)] + [f"bulk{i}" for i in range(10)]
    people = {n: Person.new(n) for n in names}
    funded = {"alice": 500 * RS, "erin": 500 * RS}
    funded |= {f"load{i}": 1000 * RS for i in range(6)} | {f"bulk{i}": 1000 * RS for i in range(10)}
    scenarios = {"s1": "s1", "s3": "s3", "s2": "s2", "s9": "s9"}
    chosen = [s for s in scenarios if not args.only or s in args.only]

    with tempfile.TemporaryDirectory(prefix="ledgernet-demo-") as work:
        genesis = Path(work) / "genesis.json"
        genesis.write_text(dump_genesis({people[n].pub: paise for n, paise in funded.items()}))
        config = write_local_config(work, str(genesis), n_nodes=5, raft=RAFT)
        cluster = Cluster(config, cwd=ROOT)
        demo = Demo(cluster, people, args.auto, work)
        try:
            demo.banner("LedgerNet: a 5-node Raft ledger (5 processes on this machine)")
            await cluster.start_all()
            leader = await cluster.wait_for_leader()
            demo.say(f"Cluster up; leader is {leader}. Crash faults only: up to 2 of 5 nodes may fail.")
            await demo.table()
            await demo.pause()
            for name in chosen:
                await getattr(demo, name)()
            await demo.finale()
            return 0
        except ClusterError as exc:
            print(f"\ndemo failed: {exc}", file=sys.stderr)
            return 1
        finally:
            await cluster.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="LedgerNet live demo")
    p.add_argument("--auto", action="store_true", help="do not wait for Enter between steps")
    p.add_argument("--only", nargs="+", choices=["s1", "s2", "s3", "s9"], help="run just these scenarios")
    return asyncio.run(run(p.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
