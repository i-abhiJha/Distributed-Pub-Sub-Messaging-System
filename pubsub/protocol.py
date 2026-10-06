"""
Wire protocol.

Every message on the wire is length-prefixed JSON:

    [4 bytes big-endian length][UTF-8 JSON payload]

Choosing JSON over a binary format (protobuf, custom binary) is a deliberate
trade-off: it costs throughput but makes the protocol trivially inspectable
with tcpdump/wireshark and easy to extend. For the message *payload* (the
record value), JSON is not used — the broker stores raw bytes.
"""

import json
import socket
import struct
import threading

# ---------- Request / response types ----------
# Client → any broker
METADATA       = "METADATA"
CREATE_TOPIC   = "CREATE_TOPIC"

# Producer → partition leader
PRODUCE        = "PRODUCE"

# Consumer → partition leader
FETCH          = "FETCH"

# Leader → follower (internal replication)
REPLICATE      = "REPLICATE"

# Broker → controller
REGISTER_BROKER    = "REGISTER_BROKER"
BROKER_HEARTBEAT   = "BROKER_HEARTBEAT"
METADATA_PUSH      = "METADATA_PUSH"  # Controller pushes updated metadata to brokers

# Consumer → group coordinator
JOIN_GROUP         = "JOIN_GROUP"
GROUP_HEARTBEAT    = "GROUP_HEARTBEAT"
COMMIT_OFFSET      = "COMMIT_OFFSET"
FETCH_OFFSET       = "FETCH_OFFSET"
LEAVE_GROUP        = "LEAVE_GROUP"

# ---------- Error codes ----------
OK                       = "OK"
ERR_UNKNOWN_TOPIC        = "ERR_UNKNOWN_TOPIC"
ERR_NOT_LEADER           = "ERR_NOT_LEADER"
ERR_UNKNOWN              = "ERR_UNKNOWN"
ERR_REBALANCE_NEEDED     = "ERR_REBALANCE_NEEDED"
ERR_STALE_GENERATION     = "ERR_STALE_GENERATION"
ERR_UNKNOWN_MEMBER       = "ERR_UNKNOWN_MEMBER"
ERR_NOT_CONTROLLER       = "ERR_NOT_CONTROLLER"


# ---------- Framing ----------
_HEADER_FMT = ">I"
_HEADER_SIZE = struct.calcsize(_HEADER_FMT)


def send_message(sock, msg):
    """Send a length-prefixed JSON message over a socket."""
    data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
    sock.sendall(struct.pack(_HEADER_FMT, len(data)) + data)


def recv_message(sock):
    """Receive one length-prefixed JSON message. Returns None on clean EOF."""
    hdr = _recv_exactly(sock, _HEADER_SIZE)
    if hdr is None:
        return None
    (length,) = struct.unpack(_HEADER_FMT, hdr)
    body = _recv_exactly(sock, length)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


def _recv_exactly(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


# ---------- Connection pool ----------
# A persistent socket per (host, port), serialized by a per-key lock. The
# broker's handler loop reads requests forever, so we can reuse the same TCP
# connection for many requests. This avoids exhausting the OS's ephemeral port
# range (TIME_WAIT pileup) when there's a lot of inter-broker chatter
# (heartbeats, replication, metadata pushes).

_pool_master_lock = threading.Lock()
_pool = {}  # (host, port) -> {"sock": Optional[socket], "lock": threading.Lock()}


def _pool_entry(host, port):
    key = (host, port)
    with _pool_master_lock:
        e = _pool.get(key)
        if e is None:
            e = {"sock": None, "lock": threading.Lock()}
            _pool[key] = e
        return e


def request(host, port, msg, timeout=5.0):
    """
    Request/response on a pooled persistent connection to (host, port).

    Raises ConnectionError on transport failure (after one retry with a fresh
    socket). The pool is per-process and serializes calls to the same peer.
    """
    entry = _pool_entry(host, port)
    with entry["lock"]:
        last_err = None
        for attempt in range(2):
            sock = entry["sock"]
            if sock is None:
                try:
                    sock = socket.create_connection((host, port), timeout=timeout)
                    sock.settimeout(timeout)
                    entry["sock"] = sock
                except OSError as e:
                    last_err = e
                    entry["sock"] = None
                    continue
            try:
                send_message(sock, msg)
                resp = recv_message(sock)
                if resp is None:
                    raise ConnectionError(f"peer {host}:{port} closed connection")
                return resp
            except (OSError, ConnectionError) as e:
                last_err = e
                try:
                    sock.close()
                except OSError:
                    pass
                entry["sock"] = None
                # fall through to retry once with a fresh socket
        raise ConnectionError(f"request to {host}:{port} failed: {last_err}")


def close_pool():
    """Close all pooled connections (used in tests / clean shutdown)."""
    with _pool_master_lock:
        for entry in _pool.values():
            with entry["lock"]:
                if entry["sock"] is not None:
                    try:
                        entry["sock"].close()
                    except OSError:
                        pass
                    entry["sock"] = None
