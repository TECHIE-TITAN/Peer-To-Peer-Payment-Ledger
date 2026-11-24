"""Fault injector (V7.2): makes the real network misbehave on demand, so the failure experiments do not need
iptables or root.

Every outbound Raft RPC goes through `FaultInjector.around(target, call)`. With no rules it just awaits the
call. With a rule for `target` (or "*") it can add delay, drop the request, drop the response, duplicate the
request, or behave as a partition. The Admin service installs and clears rules at run time (V7.4).

Order inside `around`:  partition -> delay -> drop request -> send (maybe twice) -> drop response.
A rule for the exact target wins over a "*" rule.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any, TypeVar

from ledgernet.common.errors import TransportError

T = TypeVar("T")

WILDCARD = "*"
PARTITION_FAIL_MIN_S = 0.001  # a partitioned call fails after 1-5 ms, like a quick connection error
PARTITION_FAIL_MAX_S = 0.005


@dataclass(frozen=True)
class Rule:
    target: str
    drop_prob: float = 0.0
    delay_min_ms: int = 0
    delay_max_ms: int = 0
    partitioned: bool = False
    duplicate_prob: float = 0.0

    @classmethod
    def from_any(cls, rule: Any) -> Rule:
        """Copy a Rule or a protobuf FaultRule (anything with the same attributes) into an immutable Rule."""
        return cls(
            target=rule.target,
            drop_prob=float(rule.drop_prob),
            delay_min_ms=int(rule.delay_min_ms),
            delay_max_ms=int(rule.delay_max_ms),
            partitioned=bool(rule.partitioned),
            duplicate_prob=float(rule.duplicate_prob),
        )


class FaultInjector:
    def __init__(
        self,
        seed: int | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._rules: dict[str, Rule] = {}
        self._rng = random.Random(seed)
        self._sleep = sleep

    # ---- rule management (called by the Admin service)

    def set_rules(self, rules: Iterable[Any]) -> None:
        """Replace all rules. If two rules name the same target, the later one wins."""
        converted = [Rule.from_any(r) for r in rules]
        self._rules = {r.target: r for r in converted}

    def clear(self) -> None:
        self._rules = {}

    def rules(self) -> list[Rule]:
        return list(self._rules.values())

    def _rule_for(self, target: str) -> Rule | None:
        return self._rules.get(target) or self._rules.get(WILDCARD)

    # ---- the hook

    async def around(self, target: str, call: Callable[[], Awaitable[T]]) -> T:
        """Run `call()` (one outbound RPC to `target`) subject to the current rules.

        Raises TransportError for an injected partition or drop, exactly like a real network failure.
        """
        rule = self._rule_for(target)
        if rule is None:
            return await call()

        if rule.partitioned:
            await self._sleep(self._rng.uniform(PARTITION_FAIL_MIN_S, PARTITION_FAIL_MAX_S))
            raise TransportError(f"injected partition to {target}")

        if rule.delay_max_ms > 0:
            low, high = sorted((rule.delay_min_ms, rule.delay_max_ms))
            await self._sleep(self._rng.uniform(low, high) / 1000.0)

        # One draw decides the drop: [0, p/2) loses the request, [p/2, p) loses the response.
        draw = self._rng.random() if rule.drop_prob > 0 else 1.0
        if draw < rule.drop_prob / 2:
            raise TransportError(f"injected request drop to {target}")

        if rule.duplicate_prob > 0 and self._rng.random() < rule.duplicate_prob:
            result = await self._send_twice(call)
        else:
            result = await call()

        if rule.drop_prob / 2 <= draw < rule.drop_prob:
            # The peer processed the request, but we never hear the answer.
            raise TransportError(f"injected response drop from {target}")
        return result

    @staticmethod
    async def _send_twice(call: Callable[[], Awaitable[T]]) -> T:
        first, second = await asyncio.gather(call(), call(), return_exceptions=True)
        for outcome in (first, second):
            if not isinstance(outcome, BaseException):
                return outcome
        raise first  # type: ignore[misc]  # both failed: report the first failure
