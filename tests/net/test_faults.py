"""FaultInjector (V7.2) with a fake call function and a recording sleep, so nothing really waits."""

from __future__ import annotations

import pytest

from ledgernet.common.errors import TransportError
from ledgernet.gen import ledgernet_pb2 as pb
from ledgernet.net.faults import FaultInjector, Rule


class Recorder:
    """A fake RPC that counts how often the 'peer' processed it."""

    def __init__(self, fail_with: Exception | None = None) -> None:
        self.calls = 0
        self.fail_with = fail_with

    async def __call__(self):
        self.calls += 1
        if self.fail_with is not None:
            raise self.fail_with
        return f"response-{self.calls}"


def make(seed=1):
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    return FaultInjector(seed=seed, sleep=fake_sleep), slept


async def test_no_rules_passes_straight_through():
    injector, slept = make()
    call = Recorder()
    assert await injector.around("n2", call) == "response-1"
    assert call.calls == 1 and slept == []


async def test_partition_fails_fast_without_sending():
    injector, slept = make()
    injector.set_rules([Rule("n2", partitioned=True)])
    call = Recorder()
    with pytest.raises(TransportError):
        await injector.around("n2", call)
    assert call.calls == 0
    assert len(slept) == 1 and 0.001 <= slept[0] <= 0.005


async def test_rules_apply_only_to_their_target():
    injector, _ = make()
    injector.set_rules([Rule("n2", partitioned=True)])
    call = Recorder()
    assert await injector.around("n3", call) == "response-1"
    with pytest.raises(TransportError):
        await injector.around("n2", Recorder())


async def test_wildcard_applies_to_everyone_and_exact_rule_wins():
    injector, _ = make()
    injector.set_rules([Rule("*", partitioned=True), Rule("n3")])
    with pytest.raises(TransportError):
        await injector.around("n2", Recorder())
    assert await injector.around("n3", Recorder()) == "response-1"  # n3's empty rule overrides "*"


async def test_delay_is_within_bounds():
    injector, slept = make()
    injector.set_rules([Rule("n2", delay_min_ms=20, delay_max_ms=50)])
    for _ in range(200):
        await injector.around("n2", Recorder())
    assert len(slept) == 200
    assert all(0.020 <= s <= 0.050 for s in slept)
    assert max(slept) - min(slept) > 0.01  # actually random, not constant


async def test_fixed_delay_when_min_equals_max():
    injector, slept = make()
    injector.set_rules([Rule("n2", delay_min_ms=30, delay_max_ms=30)])
    await injector.around("n2", Recorder())
    assert slept == [0.030]


async def test_certain_drop_splits_between_lost_request_and_lost_response():
    injector, _ = make(seed=7)
    injector.set_rules([Rule("n2", drop_prob=1.0)])
    processed_when_dropped = []
    for _ in range(2000):
        call = Recorder()
        with pytest.raises(TransportError):
            await injector.around("n2", call)
        processed_when_dropped.append(call.calls)
    lost_responses = sum(processed_when_dropped)  # peer saw the request but the answer was lost
    assert all(c in (0, 1) for c in processed_when_dropped)
    assert 0.45 < lost_responses / 2000 < 0.55  # p/2 each


async def test_drop_rate_matches_probability():
    injector, _ = make(seed=3)
    injector.set_rules([Rule("n2", drop_prob=0.3)])
    failures = 0
    for _ in range(4000):
        try:
            await injector.around("n2", Recorder())
        except TransportError:
            failures += 1
    assert 0.26 < failures / 4000 < 0.34


async def test_zero_drop_never_drops():
    injector, _ = make()
    injector.set_rules([Rule("n2", drop_prob=0.0)])
    for _ in range(500):
        await injector.around("n2", Recorder())


async def test_duplicate_sends_twice_and_returns_one_response():
    injector, _ = make()
    injector.set_rules([Rule("n2", duplicate_prob=1.0)])
    call = Recorder()
    result = await injector.around("n2", call)
    assert call.calls == 2
    assert result in ("response-1", "response-2")


async def test_duplicate_rate_matches_probability():
    injector, _ = make(seed=5)
    injector.set_rules([Rule("n2", duplicate_prob=0.25)])
    call = Recorder()
    for _ in range(4000):
        await injector.around("n2", call)
    assert 0.21 < (call.calls - 4000) / 4000 < 0.29


async def test_duplicate_returns_the_error_when_both_copies_fail():
    injector, _ = make()
    injector.set_rules([Rule("n2", duplicate_prob=1.0)])
    boom = TransportError("real failure")
    with pytest.raises(TransportError, match="real failure"):
        await injector.around("n2", Recorder(fail_with=boom))


async def test_real_errors_pass_through_untouched():
    injector, _ = make()
    injector.set_rules([Rule("n2", delay_min_ms=1, delay_max_ms=2)])
    with pytest.raises(TransportError, match="peer down"):
        await injector.around("n2", Recorder(fail_with=TransportError("peer down")))


async def test_clear_removes_all_rules():
    injector, _ = make()
    injector.set_rules([Rule("*", partitioned=True)])
    injector.clear()
    assert injector.rules() == []
    assert await injector.around("n2", Recorder()) == "response-1"


async def test_set_rules_replaces_the_previous_set():
    injector, _ = make()
    injector.set_rules([Rule("n2", partitioned=True)])
    injector.set_rules([Rule("n3", partitioned=True)])
    assert await injector.around("n2", Recorder()) == "response-1"
    with pytest.raises(TransportError):
        await injector.around("n3", Recorder())


async def test_accepts_protobuf_fault_rules():
    """The Admin service hands the injector pb.FaultRule objects directly."""
    injector, _ = make()
    injector.set_rules([pb.FaultRule(target="n2", partitioned=True, drop_prob=0.5, delay_max_ms=9)])
    (rule,) = injector.rules()
    assert rule == Rule("n2", drop_prob=0.5, delay_min_ms=0, delay_max_ms=9, partitioned=True)
    with pytest.raises(TransportError):
        await injector.around("n2", Recorder())


async def test_same_seed_gives_the_same_drop_pattern():
    async def pattern(seed):
        injector, _ = make(seed)
        injector.set_rules([Rule("n2", drop_prob=0.5)])
        out = []
        for _ in range(50):
            try:
                await injector.around("n2", Recorder())
                out.append(True)
            except TransportError:
                out.append(False)
        return out

    assert await pattern(11) == await pattern(11)
    assert await pattern(11) != await pattern(12)
