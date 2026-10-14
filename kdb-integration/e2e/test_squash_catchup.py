"""Squash catch-up between real processes (docs/kdb-memory-cassandra-plan.md C7).

B pulls from A with catchup=squash, so a backlog on A reaches B as one jump to A's head - B's log
then stops at that head, a shallow root, instead of holding every commit. Once A's catch-up policy
refuses squash (PUT /v1/ns/{ns}/catchup), the same peer replays: B converges with every commit A
made since. B does not replicate the metadata namespace, so the refusal it meets is A's alone.
"""

from __future__ import annotations

import time

import pytest

from cluster_fixtures import control, documents, head, ns_path
from server_fixtures import KdbServer, upsert

pytestmark = [pytest.mark.server, pytest.mark.cluster]

SQUASHED = "replication: caught up by squash"


def wait_for(what: str, cond, timeout: float = 60.0, on_fail=lambda: "") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}\n{on_fail()}")


def log_hashes(srv: KdbServer, limit: int) -> list[str]:
    status, body = control(srv, "GET", ns_path(srv, f"/log?limit={limit}"))
    assert status == 200, body
    return [c["hash"] for c in body["commits"]]


def squashes(srv: KdbServer) -> int:
    return sum(1 for line in srv.log_lines() if line.get("msg") == SQUASHED)


def test_a_namespace_refusing_squash_is_replayed_with_full_history():
    a = KdbServer(control=True, control_write=True).start()
    b = None
    try:
        docs = [f"{i:032x}" for i in range(0x5a0, 0x5a8)]
        for i, doc in enumerate(docs):
            upsert(a, doc, {"id": doc, "n": i})
        b = KdbServer(control=True, extra_args=[
            "--peer", f"name=a,addr={a.peer_addr},mode=pull,interval=300ms,"
                      f"namespaces={a.namespace},catchup=squash",
        ]).start()

        def converged() -> bool:
            h = head(a)
            return h is not None and head(b) == h

        def diagnose() -> str:
            return f"A head {head(a)}, B head {head(b)}\n--- B:\n{b.logs()[-6000:]}"

        wait_for("B's first sync", converged, on_fail=diagnose)

        # Allowed: a backlog reaches B by squash, and B's history has a gap below the jump.
        for r in range(40):
            upsert(a, docs[r % len(docs)], {"id": docs[r % len(docs)], "round": r})
        wait_for("B to catch up by squash", lambda: converged() and squashes(b) > 0, on_fail=diagnose)
        assert len(log_hashes(b, 500)) < len(log_hashes(a, 500)), \
            "B squashed, yet holds A's whole log"

        # Refused on A: the same peer now replays, and holds every commit A makes from here.
        status, body = control(a, "PUT", ns_path(a, "/catchup"),
                               {"refuseSquash": True, "reason": "e2e: full history on every replica"})
        assert status == 200 and body["refuseSquash"], body
        status, body = control(a, "GET", ns_path(a, "/catchup"))
        assert status == 200 and body["reason"] == "e2e: full history on every replica", body
        # B's head moves when a squash installs, but the replicator logs it once the sync returns:
        # let the last one from above land before counting.
        time.sleep(1.5)
        before = squashes(b)
        for r in range(40):
            upsert(a, docs[r % len(docs)], {"id": docs[r % len(docs)], "refused": r})
        wait_for("B to catch up by replay", converged, on_fail=diagnose)
        time.sleep(1)  # a couple more pull ticks, in case a late one squashes
        assert squashes(b) == before, "B squashed a namespace whose host refuses squash"
        assert converged()
        assert log_hashes(b, 40) == log_hashes(a, 40), "B is missing commits A made after refusing squash"
        assert documents(b) == documents(a)
    finally:
        for srv in (b, a):
            if srv is None:
                continue
            try:
                srv.stop()
            finally:
                srv.cleanup()
