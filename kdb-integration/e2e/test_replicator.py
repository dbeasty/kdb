"""Distributed Phase 3: replication configured with --peer alone, between real processes.

Unlike test_multi_peer_sync.py, no relay helper carries the sessions: node A is started with
--peer pointing at node B, and A's replicator pushes A's writes on commit and pulls B's on its
interval. See docs/kdb-distributed-implementation-plan.md Phase 3.
"""

from __future__ import annotations

import time

import pytest

from server_fixtures import KdbServer, get, put

pytestmark = [pytest.mark.server, pytest.mark.cluster]

DOC_A = "00000000000000000000000000000a01"
DOC_B = "00000000000000000000000000000b02"
DOC_A2 = "00000000000000000000000000000a03"


def wait_for(what: str, cond, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture
def pair():
    b = KdbServer().start()
    a = KdbServer(extra_args=["--peer", f"name=b,addr={b.peer_addr},interval=500ms"]).start()
    yield a, b
    for srv in (a, b):
        try:
            code = srv.stop()
            assert code == 0, f"exit {code}:\n{srv.logs()}"
        finally:
            srv.cleanup()


def test_replicator_carries_writes_both_ways(pair):
    a, b = pair
    put(a, DOC_A, {"id": DOC_A, "origin": "a"})
    wait_for("A's write on B", lambda: (get(b, DOC_A) or {}).get("origin") == "a")

    put(b, DOC_B, {"id": DOC_B, "origin": "b"})
    wait_for("B's write on A", lambda: (get(a, DOC_B) or {}).get("origin") == "b")

    status, metrics = a.admin_get("/metrics")
    assert status == 200
    assert 'kdb_replication_last_success_seconds{peer="b"}' in metrics, metrics


def test_replicator_resumes_after_a_crash(pair):
    a, b = pair
    put(a, DOC_A, {"id": DOC_A, "v": 1})
    wait_for("first write on B", lambda: get(b, DOC_A) is not None)

    a.kill9()
    put(b, DOC_B, {"id": DOC_B, "while": "a was down"})
    a.restart()

    wait_for("B's write on A after restart", lambda: get(a, DOC_B) is not None)
    put(a, DOC_A2, {"id": DOC_A2, "after": "restart"})
    wait_for("A's post-restart write on B", lambda: get(b, DOC_A2) is not None)


def test_new_node_bootstraps_from_a_snapshot_and_survives_kill9():
    """A node joining with bootstrap=snapshot installs the peer's current state without its
    history, and that state survives an unclean restart - it is held by a checkpoint and the blob
    store, not by any delta log this node ever wrote."""
    source = KdbServer().start()
    joiner = None
    try:
        docs = [f"{i:032x}" for i in range(200, 230)]
        for i, doc in enumerate(docs):
            put(source, doc, {"id": doc, "n": i})
        joiner = KdbServer(extra_args=[
            "--peer", f"name=src,addr={source.peer_addr},mode=pull,interval=300ms,bootstrap=snapshot"]).start()
        wait_for("the snapshot to arrive", lambda: (get(joiner, docs[-1]) or {}).get("n") == len(docs) - 1)

        joiner.kill9()
        joiner.restart()
        for i, doc in enumerate(docs):
            assert (get(joiner, doc) or {}).get("n") == i, f"doc {doc} lost across kill -9:\n{joiner.logs()}"

        # And it keeps following the source commit by commit.
        put(source, DOC_B, {"id": DOC_B, "after": "bootstrap"})
        wait_for("a later write to follow", lambda: get(joiner, DOC_B) is not None)
    finally:
        for srv in (joiner, source):
            if srv is None:
                continue
            try:
                srv.stop()
            finally:
                srv.cleanup()
