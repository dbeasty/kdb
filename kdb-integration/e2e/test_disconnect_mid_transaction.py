"""E2e scenario 5 (Phase 3.2): drop a real socket mid-transaction.

On the Go server, SqlExec INSERT buffers on the session's pending transaction; only TxCommit
persists. The tx-drop helper handshakes, opens a session, buffers one INSERT, and then kills
the TCP connection without committing. The buffered write must never become visible, and the
server must stay fully healthy for subsequent clients.
"""

from __future__ import annotations

import json

import pytest

from server_fixtures import get, helper, put, sql_query

pytestmark = pytest.mark.server

DOC = "00000000000000000000000000000dd1"


def test_uncommitted_insert_dropped_with_connection(server):
    marker = {"id": DOC, "leaked": "should-never-appear"}
    r = helper("tx-drop", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--json", json.dumps(marker))
    assert "dropped-mid-transaction" in r.stdout

    # The buffered write must not be visible to a fresh client.
    rows = sql_query(server, "SELECT _doc FROM t")
    assert not any("should-never-appear" in str(row) for row in rows), rows
    assert get(server, DOC) is None

    # And the server remains fully writable afterward.
    commit = put(server, DOC, {"id": DOC, "after": "clean"})
    assert len(commit) == 64
    assert get(server, DOC)["after"] == "clean"


def test_repeated_drops_do_not_exhaust_the_server(server):
    for i in range(10):
        helper("tx-drop", "--addr", server.sql_addr, "--namespace", server.namespace,
               "--json", json.dumps({"id": DOC, "n": i}))
    # Still healthy: readyz 200 and a real write round-trips.
    code, _ = server.admin_get("/readyz")
    assert code == 200
    put(server, DOC, {"id": DOC, "survived": 10})
    assert get(server, DOC)["survived"] == 10
