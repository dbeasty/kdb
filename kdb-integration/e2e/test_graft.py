"""Grafting an unrelated history between real processes (docs/kdb-distributed-self-healing-research.md,
Phase 11).

A joins B by snapshot, and then B is gone - nobody holds the history below A's snapshot, so deepen
has nothing to fetch. C is an independent node with data of its own. With allowUnrelated in the
namespace's resolution chain, C grafts A's snapshot root, merges the two histories, and the two
converge - and C keeps converging across a kill -9.
"""

from __future__ import annotations

import time

import pytest

from server_fixtures import get, put
from test_resolver_authority import control, with_control

pytestmark = [pytest.mark.server, pytest.mark.cluster]

CHAIN = {"allowUnrelated": True, "rules": [{"kind": "last-write"}]}


def wait_for(what: str, cond, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


def test_an_independent_node_grafts_a_snapshot_joined_one_whose_source_is_gone():
    b = with_control([]).start()
    a = c = None
    try:
        b_docs = [f"{i:032x}" for i in range(0xb00, 0xb05)]
        for i, doc in enumerate(b_docs):
            put(b, doc, {"id": doc, "from": "b", "n": i})
        a = with_control([
            "--peer", f"name=b,addr={b.peer_addr},mode=pull,interval=300ms,bootstrap=snapshot",
        ]).start()
        wait_for("A to join B", lambda: all(get(a, d) for d in b_docs))
        b.stop()  # the snapshot's source is gone

        a_doc = "00000000000000000000000000000a01"
        put(a, a_doc, {"id": a_doc, "from": "a"})
        status, body = control(a, "PUT", "/resolution", CHAIN)
        assert status == 200, body

        # C has history of its own before it ever syncs - otherwise its first sync would simply
        # bootstrap it from A by snapshot. Then it dials A, so A's ports never change under it.
        c = with_control([]).start()
        c_doc = "00000000000000000000000000000c01"
        put(c, c_doc, {"id": c_doc, "from": "c"})
        status, body = control(c, "PUT", "/resolution", CHAIN)
        assert status == 200, body
        assert c.stop() == 0
        c.extra_args += ["--peer", f"name=a,addr={a.peer_addr},interval=300ms"]
        c.restart()

        try:
            wait_for("A's snapshot documents to reach C", lambda: all(get(c, d) for d in b_docs), timeout=60)
            wait_for("A's own write to reach C", lambda: (get(c, a_doc) or {}).get("from") == "a", timeout=60)
            wait_for("C's write to reach A", lambda: (get(a, c_doc) or {}).get("from") == "c", timeout=60)
        except AssertionError:
            raise AssertionError("A and C did not converge.\n--- C:\n" + c.logs()[-6000:]
                                 + "\n--- A:\n" + a.logs()[-6000:])
        assert any(line.get("msg") == "replication: grafted an unrelated history" for line in c.log_lines()), c.logs()[-5000:]

        # The graft survives an unclean restart, and the two keep syncing.
        c.kill9()
        c.restart()
        assert all(get(c, d) for d in b_docs)
        a_doc2 = "00000000000000000000000000000a02"
        put(a, a_doc2, {"id": a_doc2, "from": "a", "after": "restart"})
        wait_for("A's later write to reach the restarted C",
                 lambda: (get(c, a_doc2) or {}).get("after") == "restart", timeout=60)
    finally:
        for srv in (a, b, c):
            if srv is None:
                continue
            try:
                srv.stop()
            except Exception:
                pass
            finally:
                srv.cleanup()
