from typing import Optional


class NotLeaderError(Exception):
    def __init__(self, leader_hint: Optional[str] = None):
        super().__init__(f"not leader (hint={leader_hint})")
        self.leader_hint = leader_hint


class LeadershipLostError(Exception):
    """The entry proposed at (index, term) was overwritten by a different term: it will never commit."""


class TransportError(Exception):
    """RPC failed: timeout, connection refused, injected drop or partition."""
