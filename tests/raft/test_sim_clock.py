"""The simulated-time event loop: real timeouts, no real waiting, deterministic order."""

import asyncio
import time

from ledgernet.raft.sim_clock import run_simulated


def test_an_hour_of_simulated_time_passes_instantly():
    async def scenario():
        loop = asyncio.get_running_loop()
        started = loop.time()
        await asyncio.sleep(3600)
        return loop.time() - started

    wall_started = time.monotonic()
    elapsed = run_simulated(scenario)
    assert abs(elapsed - 3600) < 1e-6
    assert time.monotonic() - wall_started < 2.0


def test_timers_fire_in_time_order_not_creation_order():
    order = []

    async def scenario():
        async def after(delay, label):
            await asyncio.sleep(delay)
            order.append(label)

        await asyncio.gather(after(0.3, "c"), after(0.1, "a"), after(0.2, "b"))

    run_simulated(scenario)
    assert order == ["a", "b", "c"]


def test_wait_for_times_out_in_simulated_time():
    async def scenario():
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            await asyncio.wait_for(asyncio.sleep(10), timeout=0.25)
        except asyncio.TimeoutError:
            return loop.time() - started

    assert abs(run_simulated(scenario) - 0.25) < 1e-6


def test_leftover_tasks_are_cancelled_when_the_scenario_ends():
    state = {"cancelled": False}

    async def forever():
        try:
            await asyncio.sleep(10**9)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    async def scenario():
        asyncio.ensure_future(forever())
        await asyncio.sleep(0.1)

    run_simulated(scenario)
    assert state["cancelled"]
