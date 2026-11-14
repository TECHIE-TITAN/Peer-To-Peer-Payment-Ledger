"""SqliteLedger: owned by A. Stub so imports work from day one."""

from __future__ import annotations


class SqliteLedger:
    def __init__(self, db_path: str, genesis: dict[bytes, int]) -> None:
        raise NotImplementedError("SqliteLedger is implemented by A")
