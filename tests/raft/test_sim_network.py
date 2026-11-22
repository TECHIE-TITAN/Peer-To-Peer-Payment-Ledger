"""SimNetwork: loss, delay, duplication, partitions, crashes and late messages, on a stub Raft handler."""

import asyncio

import pytest

from ledgernet.common.errors import TransportError
from ledgernet.common.interfaces import RaftRpcHandler
from ledgernet.common.rpc_types import RequestVoteRequest, RequestVoteResponse
from ledgernet.raft.sim_clock import run_simulated
from ledgernet.raft.sim_network import SimNetwork, SimTransport

VOTE = RequestVoteRequest(term=1, candidate_id="n1", last_log_index=0, last_log_term=0)


class CountingHandler(RaftRpcHandler):
    def __init__(self):
        self.votes = 0

    async def on_request_vote(self, req):
        self.votes += 1
        return RequestVoteResponse(req.term, True)

    async def on_append_entries(self, req):
        raise NotImplementedError

    async def on_install_snapshot(self, req):
        raise NotImplementedError


async def make_pair(**network_args):
    network = SimNetwork(seed=1, **network_args)
    handlers = {"n1": CountingHandler(), "n2": CountingHandler()}
    transports = {name: SimTransport(name, network) for name in handlers}
    for name, transport in transports.items():
        await transport.start(handlers[name])
    return network, handlers, transports


def test_a_call_reaches_the_handler_and_returns_its_answer():
    async def scenario():
        _, handlers, transports = await make_pair()
        response = await transports["n1"].request_vote("n2", VOTE)
        assert response.vote_granted and handlers["n2"].votes == 1

    run_simulated(scenario)


def test_a_partition_blocks_traffic_for_one_timeout_then_heal_restores_it():
    async def scenario():
        network, handlers, transports = await make_pair(rpc_timeout_s=0.2)
        network.partition([{"n1"}, {"n2"}])
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(TransportError):
            await transports["n1"].request_vote("n2", VOTE)
        assert abs(loop.time() - started - 0.2) < 1e-6  # failed like a timeout, not instantly
        assert handlers["n2"].votes == 0
        network.heal()
        assert (await transports["n1"].request_vote("n2", VOTE)).vote_granted

    run_simulated(scenario)


def test_isolating_a_node_cuts_it_off_in_both_directions():
    async def scenario():
        network, _, transports = await make_pair()
        network.isolate("n2")
        for source, target in (("n1", "n2"), ("n2", "n1")):
            with pytest.raises(TransportError):
                await transports[source].request_vote(target, VOTE)

    run_simulated(scenario)


def test_full_message_loss_fails_every_call():
    async def scenario():
        network, handlers, transports = await make_pair()
        network.set_drop(1.0)
        with pytest.raises(TransportError):
            await transports["n1"].request_vote("n2", VOTE)
        assert handlers["n2"].votes == 0
        assert network.stats["dropped"] >= 1

    run_simulated(scenario)


def test_delay_is_applied_in_both_directions():
    async def scenario():
        network, _, transports = await make_pair()
        network.set_delay(40, 40)
        loop = asyncio.get_running_loop()
        started = loop.time()
        await transports["n1"].request_vote("n2", VOTE)
        assert abs(loop.time() - started - 0.08) < 1e-6  # 40 ms there, 40 ms back

    run_simulated(scenario)


def test_duplication_delivers_the_request_twice_but_answers_once():
    async def scenario():
        network, handlers, transports = await make_pair()
        network.set_duplicate(1.0)
        await transports["n1"].request_vote("n2", VOTE)
        await asyncio.sleep(1.0)
        assert handlers["n2"].votes == 2
        assert network.stats["duplicated"] == 1

    run_simulated(scenario)


def test_a_stopped_node_refuses_connections_immediately():
    async def scenario():
        _, _, transports = await make_pair()
        await transports["n2"].stop()
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(TransportError, match="refused"):
            await transports["n1"].request_vote("n2", VOTE)
        assert loop.time() == started

    run_simulated(scenario)


def test_a_late_request_still_arrives_after_the_caller_gave_up():
    async def scenario():
        network, handlers, transports = await make_pair()
        network.set_delay(500, 500)  # slower than any caller will wait
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(transports["n1"].request_vote("n2", VOTE), timeout=0.2)
        assert handlers["n2"].votes == 0
        await asyncio.sleep(1.0)
        assert handlers["n2"].votes == 1  # the stale message was delivered anyway

    run_simulated(scenario)


def test_the_same_seed_gives_the_same_losses():
    async def run_once(seed):
        network = SimNetwork(seed)
        handler = CountingHandler()
        sender, receiver = SimTransport("n1", network), SimTransport("n2", network)
        await receiver.start(handler)
        await sender.start(CountingHandler())
        network.set_drop(0.5)
        outcome = []
        for _ in range(20):
            try:
                await sender.request_vote("n2", VOTE)
                outcome.append(1)
            except TransportError:
                outcome.append(0)
        return outcome

    first = run_simulated(run_once, 7)
    assert first == run_simulated(run_once, 7)
    assert 0 < sum(first) < 20
