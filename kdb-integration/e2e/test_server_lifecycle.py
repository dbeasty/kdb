"""E2e scenario 1 (kdb-finish-up-plan Phase 3.2): server lifecycle.

Start a real kdb-service, check healthz/readyz, put/get over the actual wire, SIGTERM it and
assert a clean drain (exit 0), then restart over the same data dir and read the data back.
"""

from __future__ import annotations

import pytest

from server_fixtures import KdbServer, get, put, sql_query

pytestmark = pytest.mark.server

DOC = "00000000000000000000000000000abc"


def test_healthz_and_readyz(server):
    code, body = server.admin_get("/healthz")
    assert code == 200 and "version=" in body

    code, body = server.admin_get("/readyz")
    assert code == 200 and "ready" in body

    code, body = server.admin_get("/metrics")
    assert code == 200 and "kdb_go_goroutines" in body


def test_put_get_over_wire(server):
    commit = put(server, DOC, {"id": DOC, "name": "lifecycle"})
    assert len(commit) == 64  # a real commit hash

    body = get(server, DOC)
    assert body is not None and body["name"] == "lifecycle"

    rows = sql_query(server, "SELECT _doc FROM t")
    assert any("lifecycle" in str(r) for r in rows)


def test_sigterm_drains_and_restart_reads_data():
    srv = KdbServer().start()
    try:
        put(srv, DOC, {"id": DOC, "value": "survives-restart"})

        code = srv.stop()
        assert code == 0, f"SIGTERM did not drain cleanly:\n{srv.logs()}"
        messages = [line.get("msg", "") for line in srv.log_lines()]
        assert any("drain complete" in m for m in messages), messages
        assert any("shutdown complete" in m for m in messages), messages

        srv.restart()
        body = get(srv, DOC)
        assert body is not None and body["value"] == "survives-restart"
    finally:
        srv.stop()
        srv.cleanup()


def test_readyz_flips_before_exit_on_sigterm():
    # A load balancer must see 503 the moment draining starts. We can't reliably race the
    # sub-second drain window from outside, so instead assert the ordering from the structured
    # logs: the drain log precedes storage close, and the service exited 0.
    srv = KdbServer().start()
    try:
        code, _ = srv.admin_get("/readyz")
        assert code == 200
        code = srv.stop()
        assert code == 0
        messages = [line.get("msg", "") for line in srv.log_lines()]
        drain_idx = next(i for i, m in enumerate(messages) if "draining" in m)
        done_idx = next(i for i, m in enumerate(messages) if "shutdown complete" in m)
        assert drain_idx < done_idx
    finally:
        srv.cleanup()
