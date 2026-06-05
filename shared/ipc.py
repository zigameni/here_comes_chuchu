"""
shared/ipc.py
─────────────
Zero-configuration ZeroMQ PUB/SUB wrappers over IPC sockets.

Usage
-----
Publisher (one process):
    pub = get_publisher("ipc:///tmp/binance_bbo.ipc")
    pub.send(pack([ts_ms, bid, ask]))

Subscriber (another process):
    sub = get_subscriber("ipc:///tmp/binance_bbo.ipc")
    while True:
        payload = sub.recv()          # blocks
        ts_ms, bid, ask = unpack(payload)

Design notes
------------
* Brokerless — no Redis, no Kafka, no config.
* IPC sockets are OS-level named pipes; latency is effectively zero on localhost.
* On Windows IPC sockets fall back to TCP loopback automatically via the
  tcp://127.0.0.1 address family — see WINDOWS_IPC_MAP below.
* msgpack is used instead of JSON: binary, ~10x faster to parse.
"""

import platform
import zmq
import msgpack
import os

# ── Platform-specific socket address ──────────────────────────────────────────
# ZMQ ipc:// sockets require Unix domain socket support.  On Windows we map
# each logical channel to a distinct loopback TCP port so the rest of the code
# is identical on both OSes.
_WINDOWS_IPC_MAP: dict[str, str] = {
    "ipc:///tmp/binance_bbo.ipc":   "tcp://127.0.0.1:5550",
    "ipc:///tmp/fv_stream.ipc":     "tcp://127.0.0.1:5551",
    "ipc:///tmp/pm_book.ipc":       "tcp://127.0.0.1:5552",
    "ipc:///tmp/exec_report.ipc":   "tcp://127.0.0.1:5553",
    "ipc:///tmp/replay_stream.ipc": "tcp://127.0.0.1:5554",
    "ipc:///tmp/replay_ready.ipc":  "tcp://127.0.0.1:5555",  # <-- ADD THIS

}

_IS_WINDOWS = platform.system() == "Windows"


def _resolve_addr(addr: str) -> str:
    """Translate ipc:// addresses to tcp:// on Windows."""
    if _IS_WINDOWS and addr in _WINDOWS_IPC_MAP:
        return _WINDOWS_IPC_MAP[addr]
    return addr


# ── Shared ZMQ context (one per process) ──────────────────────────────────────
_ctx: zmq.Context | None = None


def _context() -> zmq.Context:
    global _ctx
    if _ctx is None:
        _ctx = zmq.Context.instance()
    return _ctx


# ── Public factory functions ───────────────────────────────────────────────────

def get_publisher(addr: str, *, hwm: int = 1000) -> zmq.Socket:
    """
    Create and bind a ZMQ PUB socket.

    Parameters
    ----------
    addr : str
        Logical IPC address, e.g. ``"ipc:///tmp/binance_bbo.ipc"``.
    hwm : int
        High-water mark — max messages buffered before dropping.
        Keep low for market-data streams so slow consumers don't build a queue.

    Returns
    -------
    zmq.Socket
        Bound PUB socket ready to call ``.send()``.
    """
    resolved = _resolve_addr(addr)
    sock = _context().socket(zmq.PUB)
    sock.set_hwm(hwm)
    sock.bind(resolved)
    return sock


def get_subscriber(addr: str, *, topic: bytes = b"", hwm: int = 1000) -> zmq.Socket:
    """
    Create and connect a ZMQ SUB socket.

    Parameters
    ----------
    addr : str
        Logical IPC address matching a bound publisher.
    topic : bytes
        Topic filter prefix.  ``b""`` subscribes to all messages (default).
    hwm : int
        High-water mark on the receive side.

    Returns
    -------
    zmq.Socket
        Connected SUB socket ready to call ``.recv()``.
    """
    resolved = _resolve_addr(addr)
    sock = _context().socket(zmq.SUB)
    sock.set_hwm(hwm)
    sock.connect(resolved)
    sock.setsockopt(zmq.SUBSCRIBE, topic)
    return sock


# ── Serialization helpers ──────────────────────────────────────────────────────

def pack(obj) -> bytes:
    """Serialize *obj* to msgpack bytes."""
    return msgpack.packb(obj, use_bin_type=True)


def unpack(data: bytes):
    """Deserialize msgpack *data* back to a Python object."""
    return msgpack.unpackb(data, raw=False)


# ── Well-known channel addresses (single source of truth) ─────────────────────

class Channel:
    """Logical channel addresses used across all daemons."""
    BINANCE_BBO   = "ipc:///tmp/binance_bbo.ipc"
    FV_STREAM     = "ipc:///tmp/fv_stream.ipc"
    PM_BOOK       = "ipc:///tmp/pm_book.ipc"
    EXEC_REPORT   = "ipc:///tmp/exec_report.ipc"
    REPLAY_STREAM = "ipc:///tmp/replay_stream.ipc"
    REPLAY_READY  = "ipc:///tmp/replay_ready.ipc"  # <-- ADD THIS
