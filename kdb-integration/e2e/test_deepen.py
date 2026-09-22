"""Deepen between real processes (docs/kdb-distributed-self-healing-research.md, Phase 14).

A joins B by snapshot (bootstrap=snapshot) and, with deepen=true, fetches the history below that
snapshot. C is an independent node with data of its own, replicating with A. Without the history,
C and A are unrelated histories and can never sync; with it they converge - and keep converging
across a kill -9 of A.
"""

from __future__ import annotations

import time

import pytest

from server_fixtures import KdbServer, get, put

pytestmark = [pytest.mark.server, pytest.mark.cluster]


def wait_for(what: str, cond, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def test_a_snapshot_joined_node_deepens_and_syncs_with_an_independent_one():
    # A dials both peers, so its restart (which the fixture gives new ports) strands no one.
    b = KdbServer().start()
    c = KdbServer().start()
    a = None
    try:
        b_docs = [f"{i:032x}" for i in range(0xb00, 0xb05)]
        for i, doc in enumerate(b_docs):
            put(b, doc, {"id": doc, "from": "b", "n": i})
        c_doc = "00000000000000000000000000000c01"
        put(c, c_doc, {"id": c_doc, "from": "c"})

        a = KdbServer(extra_args=[
            "--peer", f"name=b,addr={b.peer_addr},mode=pull,interval=300ms,bootstrap=snapshot,deepen=true",
            "--peer", f"name=c,addr={c.peer_addr},interval=300ms",
        ]).start()
        wait_for("A to join B", lambda: all(get(a, d) for d in b_docs))
        wait_for("A to fetch the history below its snapshot",
                 lambda: any(line.get("msg") == "replication: fetched history below a snapshot"
                             for line in a.log_lines()))

        # C has history of its own, unrelated to A's snapshot until A deepened.
        wait_for("C's write to reach A", lambda: (get(a, c_doc) or {}).get("from") == "c", timeout=60)
        wait_for("B's writes to reach C through A", lambda: all(get(c, d) for d in b_docs), timeout=60)

        # The deepened history survives an unclean restart, and A keeps syncing with C.
        a.kill9()
        a.restart()
        c_doc2 = "00000000000000000000000000000c02"
        put(c, c_doc2, {"id": c_doc2, "from": "c", "after": "restart"})
        try:
            wait_for("C's later write to reach the restarted A",
                     lambda: (get(a, c_doc2) or {}).get("after") == "restart", timeout=60)
        except AssertionError:
            raise AssertionError("C's later write never reached the restarted A.\n--- A:\n"
                                 + a.logs()[-6000:])
        assert all(get(a, d) for d in b_docs)
    finally:
        for srv in (a, b, c):
            if srv is None:
                continue
            try:
                srv.stop()
            finally:
                srv.cleanup()
