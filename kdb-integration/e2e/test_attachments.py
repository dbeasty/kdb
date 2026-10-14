"""Attachments between real processes (docs/kdb-attachments-plan.md).

A writes attachments through the Go client - one large (several 1 MiB chunks, its own segments),
one small (inline in its pointer) - and B, pulling from A, ends with the same head and the same
bytes, fetched with BLOB_FETCH and checked chunk by chunk. B is configured for squash catch-up,
which a namespace with attachments refuses: B replays, so it holds the attachments' history too.
B then restarts and still reads them.
"""

from __future__ import annotations

import hashlib
import os
import time

import pytest

from cluster_fixtures import head
from server_fixtures import KdbServer, attach, read_attachment, sql_query, upsert

pytestmark = [pytest.mark.server, pytest.mark.cluster]


def wait_for(what: str, cond, timeout: float = 60.0, on_fail=lambda: "") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}\n{on_fail()}")


def write_file(path, data: bytes) -> str:
    with open(path, "wb") as f:
        f.write(data)
    return str(path)


def test_attachments_replicate_between_real_processes(tmp_path):
    a = KdbServer(control=True, control_write=True).start()
    b = None
    try:
        doc = f"{0x7a0:032x}"
        upsert(a, doc, {"id": doc, "title": "report"})
        big = os.urandom(3 * 1024 * 1024 + 12345)
        small = b"small attachment"
        attach(a, doc, "big.bin", write_file(tmp_path / "big", big))
        attach(a, doc, "note.txt", write_file(tmp_path / "small", small))

        b = KdbServer(control=True, extra_args=[
            "--peer", f"name=a,addr={a.peer_addr},mode=pull,interval=300ms,"
                      f"namespaces={a.namespace},catchup=squash",
        ]).start()

        def diagnose() -> str:
            return f"A head {head(a)}, B head {head(b)}\n--- B:\n{b.logs()[-6000:]}"

        wait_for("B to catch up", lambda: head(a) is not None and head(b) == head(a), on_fail=diagnose)
        got = read_attachment(b, doc, "big.bin")
        assert got.get("sha256") == hashlib.sha256(big).hexdigest(), got
        assert read_attachment(b, doc, "note.txt").get("sha256") == hashlib.sha256(small).hexdigest()
        rows = sql_query(b, "SELECT ATTACHMENT_COUNT(), ATTACHMENT_SIZE('big.bin') FROM " + a.namespace.split("/")[-1])
        assert rows and rows[0] == ["2", str(len(big))], rows

        # A replacement replicates too, and B serves the new bytes after a restart.
        bigger = os.urandom(2 * 1024 * 1024)
        attach(a, doc, "big.bin", write_file(tmp_path / "bigger", bigger))
        wait_for("B to take the replacement", lambda: head(b) == head(a), on_fail=diagnose)
        b.stop()
        b.restart()
        wait_for("B back", lambda: head(b) == head(a), on_fail=diagnose)
        assert read_attachment(b, doc, "big.bin").get("sha256") == hashlib.sha256(bigger).hexdigest()
    finally:
        if b is not None:
            b.stop()
        a.stop()
