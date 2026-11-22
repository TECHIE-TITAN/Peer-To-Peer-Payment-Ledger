import json
import time
from typing import Optional

_path: Optional[str] = None
_node: Optional[str] = None


def configure(node_id: str, path: str) -> None:
    global _path, _node
    _path, _node = path, node_id


def emit(event: str, **fields) -> None:
    if _path is None:
        return
    rec = {"ts": time.time(), "node": _node, "event": event, **fields}
    with open(_path, "a") as f:
        f.write(json.dumps(rec) + "\n")
