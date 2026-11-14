"""Simulated-time asyncio event loop for deterministic, fast Raft tests (owned by P).

The loop never sleeps. Whenever every task is waiting on a timer, virtual time jumps straight to the
next timer. Raft runs with its real production timeouts (300-600 ms elections), yet minutes of cluster
life take a fraction of a second, and a failing seed replays exactly because nothing depends on the
wall clock.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class SimulatedTimeLoop(asyncio.SelectorEventLoop):
    def __init__(self) -> None:
        super().__init__()
        self._virtual_now = 0.0
        real_select = self._selector.select

        def select(timeout=None):
            events = real_select(0)
            if events:
                return events
            if timeout is None:
                raise RuntimeError("simulated loop would block forever: no task or timer is scheduled")
            if timeout > 0:
                self._virtual_now += timeout
            return events

        self._selector.select = select  # type: ignore[method-assign]

    def time(self) -> float:
        return self._virtual_now


def run_simulated(scenario: Callable[..., Awaitable[Any]], *args: Any) -> Any:
    """Run `await scenario(*args)` on a fresh simulated-time loop, then cancel and drain leftovers."""
    loop = SimulatedTimeLoop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(scenario(*args))
    finally:
        leftovers = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in leftovers:
            task.cancel()
        if leftovers:
            loop.run_until_complete(asyncio.gather(*leftovers, return_exceptions=True))
        loop.close()
        asyncio.set_event_loop(None)
