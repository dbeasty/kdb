"""Self-repair between real processes (docs/kdb-distributed-self-healing-research.md, Phase 13).

Node A replicates with B. A byte is flipped in a body in A's delta log while A is down - bit rot on
disk. A restarts: it must keep its head (a damaged frame used to roll the namespace back past it),
and its background scrub must find the damaged body and restore it from B by content hash, with
no operator involved.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from server_fixtures import KdbServer, get, put

pytestmark = [pytest.mark.server, pytest.mark.cluster, pytest.mark.destructive]


def wait_for(what: str, cond, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def flip_in_log(data_dir: str, namespace: str, needle: bytes) -> None:
    delta = os.path.join(data_dir, "ns", *namespace.split("/"), "delta")
    for name in os.listdir(delta):
        path = os.path.join(delta, name)
        with open(path, "rb") as f:
            raw = bytearray(f.read())
        i = raw.find(needle)
        if i >= 0:
            raw[i] ^= 0x01
            with open(path, "wb") as f:
                f.write(raw)
            return
    raise AssertionError(f"{needle!r} not found uncompressed in {delta}")


def safe_get(srv: KdbServer, doc: str):
    try:
        return get(srv, doc)
    except Exception:  # an unreadable body surfaces as an error from the client
        return None


def test_a_body_damaged_on_disk_is_repaired_from_a_peer():
    b = KdbServer().start()
    a = KdbServer(extra_args=[
        "--peer", f"name=b,addr={b.peer_addr},interval=500ms",
        "--scrub-interval", "1s",
        # Uncompressed frames, only so the test can put the flipped bit inside one known body.
        "--compression", "none",
    ]).start()
    try:
        docs = [f"{i:032x}" for i in range(0xd00, 0xd06)]
        for i, doc in enumerate(docs):
            put(a, doc, {"id": doc, "payload": f"needle-{i}-{doc}"})
        wait_for("B to hold A's writes", lambda: all((get(b, d) or {}).get("payload") for d in docs))

        assert a.stop() == 0, a.logs()
        flip_in_log(a.data_dir, a.namespace, f"needle-2-{docs[2]}".encode())
        a.restart()

        # The head survives: every document after the damaged one is still there.
        for i in (3, 4, 5):
            assert (safe_get(a, docs[i]) or {}).get("payload") == f"needle-{i}-{docs[i]}", a.logs()

        # The damaged body reads again: either it never stopped (it was also flushed to the
        # document store before the restart, so the log damage was masked) or the background
        # scrub restored it from B. When it was unreadable, the scrub must be what fixed it.
        was_unreadable = safe_get(a, docs[2]) is None
        wait_for("the damaged document to read again",
                 lambda: (safe_get(a, docs[2]) or {}).get("payload") == f"needle-2-{docs[2]}",
                 timeout=30)
        if was_unreadable:
            # The repair commit makes the document readable a moment before the scrub logs it.
            wait_for("the scrub to report the repair", lambda: any(
                line.get("msg") == "scrub repaired damaged documents from peers" for line in a.log_lines()
            ), timeout=10)
    finally:
        for srv in (a, b):
            try:
                srv.stop()
            finally:
                srv.cleanup()
