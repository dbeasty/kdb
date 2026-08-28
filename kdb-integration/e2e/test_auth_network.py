"""E2e scenario 6 (Phase 3.2): RBAC over a real network.

Durable-registry servers with bootstrapped users: bad credentials get a real error, grants are
namespace-scoped, a read-only principal cannot DDL (1-G6), and the peer-sync front door
rejects unauthorized peers (1-G9).
"""

from __future__ import annotations

import pytest

from server_fixtures import KdbServer, get, helper, put, sql_exec

pytestmark = pytest.mark.server

NS = "e2e/data"
DOC = "00000000000000000000000000000aa1"

ROLES = [
    ("admin", f"read:{NS},write:{NS},sync:{NS}"),
    ("reader", f"read:{NS}"),
    ("other-ns-admin", "read:other/ns,write:other/ns"),
]
USERS = [
    ("alice", "alice-pw", "admin"),
    ("bob", "bob-pw", "reader"),
    ("carol", "carol-pw", "other-ns-admin"),
]


@pytest.fixture
def rbac_server():
    srv = KdbServer(rbac=True, bootstrap_roles=ROLES, bootstrap_users=USERS).start()
    yield srv
    code = srv.stop()
    srv.cleanup()
    assert code == 0


def test_bad_credentials_get_a_real_error(rbac_server):
    r = helper("put", "--addr", rbac_server.sql_addr, "--namespace", NS,
               "--doc-id", DOC, "--json", "{}", token="alice:WRONG", check=False)
    assert r.returncode != 0
    assert "unauthenticated" in r.stderr.lower() or "invalid credentials" in r.stderr.lower(), r.stderr

    r = helper("put", "--addr", rbac_server.sql_addr, "--namespace", NS,
               "--doc-id", DOC, "--json", "{}", token="nobody:pw", check=False)
    assert r.returncode != 0
    assert "unknown user" in r.stderr.lower() or "unauthenticated" in r.stderr.lower(), r.stderr


def test_authorized_write_and_scoped_read(rbac_server):
    commit = put(rbac_server, DOC, {"id": DOC, "by": "alice"}, token="alice:alice-pw")
    assert len(commit) == 64

    body = get(rbac_server, DOC, token="bob:bob-pw")
    assert body is not None and body["by"] == "alice"

    # bob (read-only) cannot write.
    r = helper("put", "--addr", rbac_server.sql_addr, "--namespace", NS,
               "--doc-id", DOC, "--json", '{"by":"bob"}', token="bob:bob-pw", check=False)
    assert r.returncode != 0, "read-only principal wrote successfully"


def test_read_only_principal_cannot_ddl(rbac_server):
    r = sql_exec(rbac_server, "CREATE TABLE t (name TEXT)", token="bob:bob-pw", check=False)
    assert r.returncode != 0, "read-only principal executed DDL (1-G6 regression)"

    # alice (write grant) can.
    sql_exec(rbac_server, "CREATE TABLE t (name TEXT)", token="alice:alice-pw")


def test_namespace_scoped_grants_enforced(rbac_server):
    # carol's grants are for other/ns only - the e2e server namespace must reject her.
    r = helper("put", "--addr", rbac_server.sql_addr, "--namespace", NS,
               "--doc-id", DOC, "--json", "{}", token="carol:carol-pw", check=False)
    assert r.returncode != 0, "grant for a different namespace authorized this one"


def test_peer_sync_rejects_unauthorized(rbac_server):
    put(rbac_server, DOC, {"id": DOC, "by": "alice"}, token="alice:alice-pw")

    # No credentials at all: rejected.
    r = helper("relay", "--namespace", NS, "--servers", rbac_server.peer_addr,
               "--rounds", "1", check=False)
    assert r.returncode != 0, "unauthenticated peer synced against an RBAC server"

    # reader (no sync grant): rejected.
    r = helper("relay", "--namespace", NS, "--servers", rbac_server.peer_addr,
               "--rounds", "1", token="bob:bob-pw", check=False)
    assert r.returncode != 0, "peer without a sync grant synced successfully"

    # admin (sync grant): syncs.
    r = helper("relay", "--namespace", NS, "--servers", rbac_server.peer_addr,
               "--rounds", "1", token="alice:alice-pw")
    assert "applied=1" in r.stdout, r.stdout


def test_users_survive_restart(rbac_server):
    put(rbac_server, DOC, {"id": DOC, "pre": "restart"}, token="alice:alice-pw")
    code = rbac_server.stop()
    assert code == 0
    rbac_server.restart()

    # Same credentials still authenticate against the reopened durable registry.
    body = get(rbac_server, DOC, token="alice:alice-pw")
    assert body is not None and body["pre"] == "restart"
    r = helper("put", "--addr", rbac_server.sql_addr, "--namespace", NS,
               "--doc-id", DOC, "--json", "{}", token="alice:WRONG", check=False)
    assert r.returncode != 0
