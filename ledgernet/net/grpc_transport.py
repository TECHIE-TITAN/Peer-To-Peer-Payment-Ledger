"""GrpcTransport: owned by V. Stub so imports work from day one."""

from __future__ import annotations


class GrpcTransport:
    def __init__(
        self, node_id: str, peer_addresses: dict[str, str], rpc_timeout_ms: int, injector: object
    ) -> None:
        raise NotImplementedError("GrpcTransport is implemented by V")
