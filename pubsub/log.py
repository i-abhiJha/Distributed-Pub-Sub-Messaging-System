"""
Per-partition append-only log.

Each partition is a single file. Records are framed as:

    [offset      : 8 bytes, big-endian unsigned]
    [timestamp_ms: 8 bytes, big-endian unsigned]
    [key_len     : 4 bytes, big-endian unsigned]
    [value_len   : 4 bytes, big-endian unsigned]
    [key bytes   : key_len]
    [value bytes : value_len]

On startup the log is scanned end-to-end to rebuild an in-memory
offset → file-position index and discover next_offset. Truncated
records at EOF (from a crash mid-write) are discarded.

Writes call fsync to make appends durable on the leader before
acknowledging the producer. Followers fsync before acking replication.
"""

import os
import struct
import threading
import time

_REC_HEADER_FMT = ">QQII"
_REC_HEADER_SIZE = struct.calcsize(_REC_HEADER_FMT)


class PartitionLog:
    def __init__(self, base_dir, topic, partition):
        self.topic = topic
        self.partition = partition
        self.dir = os.path.join(base_dir, topic, f"partition-{partition}")
        os.makedirs(self.dir, exist_ok=True)
        self.path = os.path.join(self.dir, "00000000.log")

        self._lock = threading.RLock()
        self._offset_to_pos = {}     # offset -> byte position in file
        self.next_offset = 0          # next offset to assign
        self._fd = None
        self._recover()

    # ---------- recovery ----------
    def _recover(self):
        if os.path.exists(self.path):
            with open(self.path, "rb") as f:
                while True:
                    pos = f.tell()
                    hdr = f.read(_REC_HEADER_SIZE)
                    if len(hdr) < _REC_HEADER_SIZE:
                        # Trailing partial header → truncate here on next open
                        self._truncate_to(pos)
                        break
                    offset, _ts, key_len, value_len = struct.unpack(_REC_HEADER_FMT, hdr)
                    body = f.read(key_len + value_len)
                    if len(body) < key_len + value_len:
                        # Truncated body
                        self._truncate_to(pos)
                        break
                    self._offset_to_pos[offset] = pos
                    self.next_offset = offset + 1
        # Open for appending
        self._fd = open(self.path, "ab")

    def _truncate_to(self, pos):
        # Truncate file to pos (clean up partial trailing record from a crash)
        with open(self.path, "r+b") as t:
            t.truncate(pos)

    # ---------- writes ----------
    def append(self, key, value, offset=None, timestamp_ms=None):
        """
        Append a record. If `offset` is given (replication path from leader),
        it must equal next_offset; otherwise next_offset is assigned.

        Returns the assigned offset. Durably flushed before return.
        """
        if isinstance(key, str):
            key = key.encode("utf-8")
        elif key is None:
            key = b""
        if isinstance(value, str):
            value = value.encode("utf-8")
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)

        with self._lock:
            if offset is None:
                offset = self.next_offset
            elif offset != self.next_offset:
                raise ValueError(
                    f"Offset mismatch on append: got {offset}, expected {self.next_offset}"
                )
            pos = self._fd.tell()
            hdr = struct.pack(_REC_HEADER_FMT, offset, timestamp_ms, len(key), len(value))
            self._fd.write(hdr + key + value)
            self._fd.flush()
            os.fsync(self._fd.fileno())
            self._offset_to_pos[offset] = pos
            self.next_offset = offset + 1
            return offset

    # ---------- reads ----------
    def read(self, from_offset, max_records=100, max_bytes=1024 * 1024):
        """
        Return up to `max_records` records starting at `from_offset`, capped by `max_bytes`.

        If from_offset >= next_offset, returns []. If from_offset is below the
        current log start (always 0 in this MVP since we don't compact), returns
        records starting from the earliest available offset.
        """
        out = []
        with self._lock:
            if from_offset >= self.next_offset:
                return out
            if from_offset < 0:
                from_offset = 0
            # In the MVP we never compact, so offset 0 is the earliest. If the
            # exact offset isn't indexed, we just start from the nearest known one
            # at or below it (in practice every offset is indexed here).
            start_pos = self._offset_to_pos.get(from_offset)
            if start_pos is None:
                # Caller asked for an offset we don't have — fall back to scan.
                # For MVP all offsets [0, next_offset) are indexed, so this is unused.
                return out

        # Read outside the lock with a fresh fd so we don't contend with writers.
        bytes_read = 0
        with open(self.path, "rb") as f:
            f.seek(start_pos)
            while len(out) < max_records and bytes_read < max_bytes:
                hdr = f.read(_REC_HEADER_SIZE)
                if len(hdr) < _REC_HEADER_SIZE:
                    break
                offset, ts, key_len, value_len = struct.unpack(_REC_HEADER_FMT, hdr)
                body = f.read(key_len + value_len)
                if len(body) < key_len + value_len:
                    break
                key = body[:key_len]
                value = body[key_len:]
                out.append({
                    "offset": offset,
                    "timestamp_ms": ts,
                    "key": key.decode("utf-8", errors="replace") if key else None,
                    "value": value.decode("utf-8", errors="replace"),
                })
                bytes_read += _REC_HEADER_SIZE + key_len + value_len
        return out

    def high_watermark(self):
        with self._lock:
            return self.next_offset

    def close(self):
        with self._lock:
            if self._fd is not None:
                self._fd.close()
                self._fd = None
